/*
 * MIT License
 *
 * Copyright 2019 Ted Percival
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _GNU_SOURCE 1

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sysinfo.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#define PORT_NUMBER 8081
// Define to a number of processes to run.
// If undefined, automatically uses all CPUs.
#define NCPUS 1

#define MAX_REQUEST_SIZE 65536
#define MAX_PIPELINED_REQUEST_DATA (MAX_REQUEST_SIZE * 4)
// Should be at least 2 to handle long-lived keep-alive connections dropped by
// middleboxes.
#define MAX_CONNECT_ATTEMPTS 2u

__attribute__((noreturn))
static void fatal_perror(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void cork(int sock, bool whether) {
    const int value = whether ? 1 : 0;
    if (setsockopt(sock, IPPROTO_TCP, TCP_CORK, &value, sizeof(value)) == -1)
        perror("setsockopt(TCP_CORK)");
}

static void tcp_cork(int sock) {
    cork(sock, true);
}

static void tcp_uncork(int sock) {
    cork(sock, false);
}

struct streambuf {
    char *data;
    size_t len;
    size_t cap;
};

struct endpoint {
    int fd;
    struct streambuf incoming, outgoing;
};

#if 0 // Don't need to parse out a request yet. Just pass the entire request buffer.
// Request;
// All fields are intended to point into a streambuf, the only special case
// is that the headers array itself needs to be dynamically allocated to allow
// it to hold any number of headers.
struct request {
    char *method;
    char *path; // path + query in RFC 3986 terms
    char *headers[]; // NUL-terminated, dynamically allocated list.
};

void free_request_contents(struct request *req) {
    // See description of `struct request` for why these pointers aren't freed.
    req->method = NULL;
    req->path = NULL;
    free(req->headers);
    req->headers = NULL;
}
#endif

enum state {
    FRONTEND_READ_WAIT, // Wait for next request
    BACKEND_WRITE_WAIT, // Wait to write to backend
    BACKEND_READ_WAIT, // Wait for backend response
    FRONTEND_WRITE_WAIT, // Wait to write to frontend
};

struct job {
    struct endpoint client, backend;

    // Backend request has to be kept in case we need to retry on a new
    // connection. It should point into the client.incoming buffer and *not* be
    // dynamically allocated.
    struct {
        const char *data;
        size_t len;
        size_t remaining_len; // how much remains to send on *this* connection attempt
        // When using this field, start at data + (len - remaining_len) (sorry)
    } backend_request;

    unsigned connect_attempts;
};

// Advance the current data location / consume a block of data.
// Avoid doing small advances; repeated small advances are inefficient.
static void streambuf_advance(struct streambuf *streambuf, size_t amount) {
    assert(amount <= streambuf->len);

    streambuf->len -= amount;

    if (streambuf->len == 0)
        return; // nothing else to do :)

    memmove(streambuf->data, streambuf->data + amount, streambuf->len);
}

static bool streambuf_init(struct streambuf *streambuf) {
    streambuf->cap = 4096;
    streambuf->data = malloc(streambuf->cap);
    streambuf->len = 0;

    return streambuf->data != NULL ? true : false;
}

static void streambuf_free_contents(struct streambuf *streambuf) {
    free(streambuf->data);
    streambuf->data = NULL;
    streambuf->len = 0;
    streambuf->cap = 0;
}

static void streambuf_clear_contents(struct streambuf *streambuf) {
    streambuf->len = 0;
}

static bool endpoint_init(struct endpoint *ep) {
    ep->fd = -1;

    if (streambuf_init(&ep->incoming) == false)
        return false;

    if (streambuf_init(&ep->outgoing) == false) {
        streambuf_free_contents(&ep->incoming);
        return false;
    }

    return true;
}

static void endpoint_free_contents(struct endpoint *ep) {
    if (ep->fd != -1) {
        close(ep->fd);
        ep->fd = -1;
    }

    streambuf_free_contents(&ep->incoming);
    streambuf_free_contents(&ep->outgoing);
}

static void endpoint_clear_contents(struct endpoint *ep) {
    ep->fd = -1;

    streambuf_clear_contents(&ep->incoming);
    streambuf_clear_contents(&ep->outgoing);
}

static int endpoint_flush(struct endpoint *ep) {
    if (ep->outgoing.len == 0)
        return 0;

    ssize_t len = send(ep->fd, ep->outgoing.data, ep->outgoing.len, 0);
    if (len == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("endpoint_flush send");
        return -1;
    }

    // Shift by however much was sent
    memmove(ep->outgoing.data, ep->outgoing.data + len, ep->outgoing.len - len);
    ep->outgoing.len -= len;

    return len;
}

static int streambuf_ensure_capacity(struct streambuf *sb, size_t data_len) {
    if (sb->cap - sb->len >= data_len)
        return 0;

    if (sb->len == 0) {
        free(sb->data);
        sb->data = malloc(data_len);
        if (sb->data == NULL)
            return -1;
        sb->cap = data_len;
    } else {
        char *newbuf = realloc(sb->data, sb->len + data_len);
        if (!newbuf)
            return -1;
        sb->data = newbuf;
        sb->cap = sb->len + data_len;
    }

    return 0;
}

static ssize_t streambuf_enqueue(struct streambuf *sb, const void *data, size_t data_len) {
    ssize_t out_len;

    out_len = streambuf_ensure_capacity(sb, data_len);
    if (out_len < 0)
        return out_len;

    memcpy(sb->data + sb->len, data, data_len);
    sb->len += data_len;

    return sb->len;
}

#define RECV_NO_LIMIT (-1)

// Read as much data as possible into a streambuf from a file descriptor.
// Return values the same as recv(), except that the length returned is the
// total length of available data (including any that was already buffered).
// limit parameter is the maximum amount of outstanding data that will be
// buffered before returning, or -1 for no limit.
// (Combining a limit with edge-triggered polling is likely to cause your end
// of a connection to hang; you'd better use level-triggering or close the
// connection if it exceeds your limit.)
static ssize_t streambuf_recv(struct streambuf *sb, int fd, ssize_t limit) {
    ssize_t len;
    size_t available;

    do {
        size_t new_capacity;

        if (sb->len < 4096)
            new_capacity = 4096;
        else // TODO: Align to page size
            new_capacity = sb->len * 2;

        if (streambuf_ensure_capacity(sb, new_capacity) < 0)
            return -1;

        available = sb->cap - sb->len;

        if (limit != -1 && available > limit)
            available = limit;

        len = recv(fd, sb->data + sb->len, available, 0);
        if (len <= 0) {
            if (sb->len > 0)
                return sb->len;

            return len;
        }

        sb->len += len;

        if (limit != -1 && sb->len >= limit)
            break;
    } while (len == available);

    return sb->len;
}

// Like send(2), but queues data if it cannot be sent immediately.
// Return values are as for send(2):
// Returns -1 on fatal error (eg. EBADF, ENOMEM)
// Returns >0 if data were successfully sent or queued;
// the return value is always data_len in this case.
// Returns 0 if the socket is closed on the remote end (?)
static ssize_t endpoint_send(struct endpoint *ep, const void *data, size_t data_len) {
    if (ep->outgoing.len > 0)
        return streambuf_enqueue(&ep->outgoing, data, data_len);

    // Attempt immediate send
    ssize_t sent_len = send(ep->fd, data, data_len, 0);
    if (sent_len < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return streambuf_enqueue(&ep->outgoing, data, data_len);

        return sent_len; // error
    }

    if (sent_len == 0)
        return 0;

    if (sent_len < data_len)
        streambuf_enqueue(&ep->outgoing, data + sent_len, data_len - sent_len);

    return data_len;
}

// Remember to check for EAGAIN/EWOULDBLOCK when this returns -1.
// If you set a limit, make sure you don't retry indefinitely because of data
// waiting on the socket that you're not willing to read.
static ssize_t endpoint_recv(struct endpoint *ep, ssize_t limit) {
    return streambuf_recv(&ep->incoming, ep->fd, limit);
}

static int job_send_status(struct job *job, int code, const char *msg) {
    char buf[512];
    ssize_t len = snprintf(buf, sizeof(buf), "HTTP/1.1 %d %s\r\n", code, msg);
    if (len < 0 || endpoint_send(&job->client, buf, len) == -1)
        return -1;

    return 0;
}

static int job_terminate_headers(struct job *job) {
    char term[2] = { '\r', '\n' };

    if (endpoint_send(&job->client, term, sizeof(term)) == -1)
        return -1;

    return 0;
}

static int job_respond_status_only(struct job *job, int code, const char *msg) {
    int rc = -1;

    tcp_cork(job->client.fd);

    if (job_send_status(job, code, msg) == -1)
        goto cleanup;

    const size_t msglen = strlen(msg);

    char content_headers[256];
    int len = snprintf(content_headers, sizeof(content_headers),
            "Content-Type: text/plain; charset=us-ascii\r\n"
            "Content-Length: %zu\r\n", msglen + 1);
    if (len != -1 && len != sizeof(content_headers)) {
        if (endpoint_send(&job->client, content_headers, len) == -1)
            goto cleanup;
    }

    if (job_terminate_headers(job) == -1)
        goto cleanup;

    if (endpoint_send(&job->client, msg, msglen) == -1)
        goto cleanup;

    const char nl = '\n';

    if (endpoint_send(&job->client, &nl, sizeof(nl)) == -1)
        goto cleanup;

    rc = 0;

cleanup:
    tcp_uncork(job->client.fd);
    return rc;
}

static struct job *new_job(int client) {
    struct job *j = calloc(1, sizeof(*j));
    if (j == NULL)
        return NULL;

    j->connect_attempts = 0;
    j->backend_request.data = NULL;
    j->backend_request.len = 0;
    j->backend_request.remaining_len = 0;

    if (endpoint_init(&j->client) == false) {
        free(j);
        return NULL;
    }

    j->client.fd = client;

    if (endpoint_init(&j->backend) == false) {
        endpoint_free_contents(&j->client);
        free(j);
        return NULL;
    }

    return j;
}

struct cpool_node {
    int fd;
    struct cpool_node *next;
};

struct connection_pool {
    struct cpool_node *head;
    struct addrinfo *addrs;
    unsigned in_use_count, idle_count;
};

static struct connection_pool backend_pool;

static void setup_connection_pool(struct connection_pool *pool) {
    pool->in_use_count = 0;
    pool->idle_count = 0;
    pool->head = NULL;
    pool->addrs = NULL;

    const struct addrinfo hints = {
        .ai_flags = AI_V4MAPPED | AI_ADDRCONFIG,
        .ai_family = AF_INET6,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = 0,
    };

#define HOST "localhost"
#define SERVICE "23206"
    int gaierr = getaddrinfo(HOST, SERVICE, &hints, &pool->addrs);
    if (gaierr) {
        fprintf(stderr, "Failed to resolve backend host: %s\n", gai_strerror(gaierr));
        exit(1);
    }

    if (pool->addrs == NULL) {
        fprintf(stderr, "No addresses\n");
        exit(1);
    }
}

static unsigned connection_pool_idle_count(struct connection_pool *pool) {
    return pool->idle_count;
}

static void try_to_enable_fastopen_connect(int sock) {
#if defined(TCP_FASTOPEN_CONNECT)
    const int yes = 1;
    if (setsockopt(sock, IPPROTO_TCP, TCP_FASTOPEN_CONNECT, &yes, sizeof(yes)) == -1)
        fatal_perror("setsockopt(TCP_FASTOPEN_CONNECT)");
#endif
}

static ssize_t connect_and_try_to_send(
        int sock,
        const struct sockaddr *addr,
        socklen_t addrlen,
        const void *data,
        size_t len)
{
#if defined(TCP_FASTOPEN_CONNECT)
    return sendto(sock, data, len, MSG_FASTOPEN, addr, addrlen);
#else
    return connect(sock, addr, addrlen);
#endif
}

static ssize_t pool_new_connection_fastopen(
        struct connection_pool *pool,
        const void *data,
        size_t len,
        int *fdp)
{
    // FIXME: Iterate over addresses
    // (a little tricker with non-blocking connect)
    struct addrinfo *addr = pool->addrs;

    int sock = socket(addr->ai_family, addr->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, addr->ai_protocol);
    if (sock == -1) {
        int save_errno = errno;
        perror("Failed to allocate connection pool socket\n");
        errno = save_errno;
        return -1;
    }

    try_to_enable_fastopen_connect(sock);

    // connect() (maybe TCP Fast Open)
    ssize_t sent = connect_and_try_to_send(sock, addr->ai_addr, addr->ai_addrlen, data, len);
    if (sent == -1 && (errno != EINPROGRESS && errno != EAGAIN && errno != EWOULDBLOCK)) {
        int save_errno = errno;
#if defined(TCP_FASTOPEN_CONNECT)
        perror("Failed to fastopen connect to backend");
#else
        perror("Failed to connect to backend");
#endif
        close(sock);
        errno = save_errno;
        return -1;
    }

    *fdp = sock;
    ++pool->in_use_count;

    return sent;
}

// Optimized version of pool_delete_fd if the caller knows that the file was not
// on the idle list.
static void pool_delete_active_fd(struct connection_pool *pool, int fd __attribute__((unused))) {
    --pool->in_use_count;
}

static int pool_get_idle_fd(struct connection_pool *pool) {
    if (pool->head == NULL)
        return -1;

    --pool->idle_count;

    struct cpool_node *n = pool->head;

    pool->head = n->next;

    int fd = n->fd;
    free(n);

    return fd;
}

static ssize_t pool_get_fd_fastopen(
        struct connection_pool *pool,
        const void *data,
        size_t len,
        int *fdp)
{
    int fd = -1;

    while ((fd = pool_get_idle_fd(pool)) != -1) {
        ssize_t sent = send(fd, data, len, MSG_DONTWAIT);

        if (sent >= 0 || (sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
            // Socket seems good.
            ++pool->in_use_count;
            *fdp = fd;
            return sent;
        }

        if (sent == -1) {
            // fd is bad
            pool_delete_active_fd(pool, fd);
            close(fd);
            fd = -1;
        }
    }

    return pool_new_connection_fastopen(pool, data, len, fdp);
}

static void subscribe(int epfd, int fd) {
    struct epoll_event event = {
        .events = 0,
        .data = {
            .u64 = 0,
        },
    };

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) == -1)
        fatal_perror("epoll_ctl subscribe");
}

static void unsubscribe(int epfd, int fd) {
    if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL) == -1)
        perror("epoll_ctl DEL");
}

// use get_backend_fd_and_send to correctly track unsent bytes;
// it calls this.
static ssize_t get_backend_fd_fastopen(
        int epfd,
        struct connection_pool *pool,
        const void *data,
        size_t len,
        int *fdp)
{
    ssize_t sent = pool_get_fd_fastopen(pool, data, len, fdp);
    if (sent == -1 && errno != EINPROGRESS && errno != EAGAIN && errno != EWOULDBLOCK)
        return -1;

    int save_errno = errno;

    subscribe(epfd, *fdp);

    errno = save_errno;

    return sent;
}

static void delete_backend_fd(int epfd, struct connection_pool *pool, int fd) {
    unsubscribe(epfd, fd);
    pool_delete_active_fd(pool, fd);
    close(fd);
}

struct free_job_node {
    struct job *job;
    struct free_job_node *next;
};

struct free_job_node *free_jobs = NULL;

static void free_closed_jobs() {
    if (free_jobs == NULL)
        return;

    struct free_job_node *next = NULL;

    for (struct free_job_node *n = free_jobs; n != NULL; n = next) {
        free(n->job);
        next = n->next;
        free(n);
    }

    free_jobs = NULL;
}

static void free_job_later(struct job *job) {
    for (struct free_job_node *n = free_jobs; n != NULL; n = n->next) {
        if (n->job == job)
            return; // already queued
    }

    struct free_job_node *n = malloc(sizeof(*n));
    n->job = job;
    n->next = free_jobs;
    free_jobs = n;
}

static void close_job(int epfd, struct job *job) {
    if (job->client.fd != -1) {
        unsubscribe(epfd, job->client.fd);
        close(job->client.fd);
        job->client.fd = -1;
    }

    endpoint_free_contents(&job->client);

    if (job->backend.fd != -1) {
        // Kill the backend connection, it's the only way to cancel the
        // request in HTTP/1.1. Waiting for it and draining it is a waste
        // of time (potentially bigger).
        delete_backend_fd(epfd, &backend_pool, job->backend.fd);
        job->backend.fd = -1;
    }

    endpoint_free_contents(&job->backend);

    free_job_later(job);
}

static void to_next_state(int epfd, struct job *job);

static int get_backend_fd_and_send(int epfd, struct job *job) {
    assert(job->connect_attempts <= MAX_CONNECT_ATTEMPTS);

    if (job->connect_attempts >= MAX_CONNECT_ATTEMPTS)
        return -1;

    int fd = -1;

    if (connection_pool_idle_count(&backend_pool) == 0) {
        // Only if the idle pool was empty should we count this as a new
        // connection attempt. Otherwise a bunch of idle connections with broken
        // pipes would be pinned on us and fail the request.
        ++job->connect_attempts;
    }

    ssize_t sent = get_backend_fd_fastopen(
            epfd,
            &backend_pool,
            job->backend_request.data,
            job->backend_request.len,
            &fd);

    if (sent == -1) {
        if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
            assert(fd != -1);
            return fd;
        }

        return -1;
    }

    assert(fd != -1);

    job->backend_request.remaining_len -= sent;

    return fd;
}

static struct cpool_node *new_pool_node(int fd) {
    struct cpool_node *n = calloc(1, sizeof(*n));
    n->fd = fd;
    n->next = NULL;
    return n;
}

static void pool_release_fd(struct connection_pool *pool, int fd) {
    --pool->in_use_count;
    ++pool->idle_count;

    // prepend node to list.
    struct cpool_node *n = new_pool_node(fd);
    n->next = pool->head;
    pool->head = n;
}

static void release_backend_fd(int epfd, struct connection_pool *pool, int fd) {
    unsubscribe(epfd, fd);
    pool_release_fd(pool, fd);
}

// Prefer pool_delete_active_fd() if possible
__attribute__((unused))
static void pool_delete_fd(struct connection_pool *pool, int fd) {
    // Figure out whether it was in use or idle
    for (struct cpool_node *n = pool->head; n != NULL; n = n->next) {
        if (n->fd == fd) {
            --pool->idle_count;
            return;
        }
    }

    pool_delete_active_fd(pool, fd);
}

// Returns new length, or -1 on error
static ssize_t replace_string(char *buf, size_t buflen, size_t bufcap, const char *from, size_t fromlen, const char *to, size_t tolen) {
    if (buflen - fromlen + tolen > bufcap)
        return -1;

    char *location = memmem(buf, buflen, from, fromlen);
    if (location == NULL)
        return -1;

    const size_t trailer_len = buflen - (location - buf) - fromlen;

    // shift keep-memory
    memmove(location + tolen, location + fromlen, trailer_len);

    // replace
    memcpy(location, to, tolen);

    return buflen - fromlen + tolen;
}

static void ep_mod_or_cleanup(int epfd, int fd, struct epoll_event *event) {
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, event) == -1) {
        perror("epoll_ctl MOD EPOLLOUT");
        // This call might print spurious errors, but it's better than leaking.
        close_job(epfd, (struct job*)event->data.ptr);
        return;
    }
}

static void setup_event(struct epoll_event *event, struct job *job, uint32_t events) {
    event->events = events;
    event->data.ptr = job;
}

static void notify_when_readable(int epfd, struct job *job, int fd) {
    struct epoll_event event;
    setup_event(&event, job, EPOLLIN | EPOLLONESHOT | EPOLLET);
    ep_mod_or_cleanup(epfd, fd, &event);
}

static void notify_when_writable(int epfd, struct job *job, int fd) {
    struct epoll_event event;
    setup_event(&event, job, EPOLLOUT | EPOLLONESHOT);
    ep_mod_or_cleanup(epfd, fd, &event);
}

static void clear_backend_request(struct job *job) {
    job->backend_request.data = NULL;
    job->backend_request.len = 0;
    job->backend_request.remaining_len = 0;
}

static void discard_request_data(struct job *job) {
    streambuf_advance(&job->client.incoming, job->backend_request.len);
    clear_backend_request(job);
}

static enum state job_state(struct job *job) {
    if (job->client.outgoing.len > 0)
        return FRONTEND_WRITE_WAIT;

    // This one is different; it writes straight from the backend_request
    // pointer (which actually points into the client.incoming buffer).
    if (job->backend_request.remaining_len > 0)
        return BACKEND_WRITE_WAIT;

    if (job->backend.fd != -1)
        return BACKEND_READ_WAIT;

    else
        return FRONTEND_READ_WAIT;
}

static void poll_appropriate_fd(int epfd, struct job *job) {
    switch (job_state(job)) {
    case BACKEND_WRITE_WAIT:
        notify_when_writable(epfd, job, job->backend.fd);
        break;

    case FRONTEND_WRITE_WAIT:
        notify_when_writable(epfd, job, job->client.fd);
        break;

    case BACKEND_READ_WAIT:
        notify_when_readable(epfd, job, job->backend.fd);
        break;

    case FRONTEND_READ_WAIT:
        notify_when_readable(epfd, job, job->client.fd);
        break;
    }
}

// reset the job structure
static void finished_with_previous_request(int epfd, struct job *job) {
    job->connect_attempts = 0;

    discard_request_data(job);

    // Don't accidentally close the backend fd, just unsubscribe.
    if (job->backend.fd != -1) {
        release_backend_fd(epfd, &backend_pool, job->backend.fd);
        job->backend.fd = -1;
    }

    endpoint_clear_contents(&job->backend);
}

// This is meant to be a specialized, faster alternative to
// memmem(buf, len, "\r\n\r\n", 4) because memmem is really slow and we can make
// some educated guesses about the presence & location of newlines.
// Returns a pointer to the byte beyond the terminal "\n".
static char *find_end_of_header(char *buf, size_t len) {
    char *last_nl = memrchr(buf, '\n', len);
    if (last_nl == NULL)
        return NULL;

    // It's now safe to use rawmemchr() up to last_nl

    // Linear search forward to the first "\n\r\n" (or "\n\n" to be permissive).
    char *nl = NULL;
    for (char *start = buf; (nl = rawmemchr(start, '\n')) != last_nl; start = nl + 1) {
        if (nl[1] == '\n')
            return nl + 2;

        if (last_nl - nl >= 2) {
            if (nl[1] == '\r' && nl[2] == '\n')
                return nl + 3;
        }
    }

    return NULL;
}

static void setup_backend_request(struct job *job, const char *end_of_request_header) {
    job->backend_request.data = job->client.incoming.data;
    job->backend_request.len = end_of_request_header - job->client.incoming.data;
    job->backend_request.remaining_len = job->backend_request.len;
}

static void on_client_input(int epfd, struct job *job) {
    ssize_t len = endpoint_recv(&job->client, MAX_PIPELINED_REQUEST_DATA);
    if (len < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("recv");
        close_job(epfd, job);
        return;
    }

    if (len == 0) {
        // Client closed connection
        close_job(epfd, job);
        return;
    }

    if (len == MAX_PIPELINED_REQUEST_DATA) {
        if (job_respond_status_only(job, 413, "Requests too big") == -1) {
            close_job(epfd, job);
            return;
        }

        close_job(epfd, job);
        return;
    }

    // Beware that there might be a stack of pipelined requests waiting, so it's
    // inappropriate to reject the connection for sending a request that's too
    // big until we're sure it hasn't finished describing a single request yet.
    char *end_of_request_header = find_end_of_header(job->client.incoming.data, job->client.incoming.len);
    if (end_of_request_header == NULL) {
        if (job->client.incoming.len > MAX_REQUEST_SIZE) {
            if (job_respond_status_only(job, 413, "Request too big") == -1) {
                close_job(epfd, job);
                return;
            }

            // This is fairly abrupt, we try to tell the client that the request
            // was too big, but they might not get the message if the socket
            // wasn't ready. But they're misbehaving so we want to drop the
            // ASAP.
            close_job(epfd, job);
            return;
        }

        // remains in client_read state; re-arm epoll
        to_next_state(epfd, job);
        return;
    }

    setup_backend_request(job, end_of_request_header);

    // Grab a backend connection
    endpoint_clear_contents(&job->backend);
    job->backend.fd = get_backend_fd_and_send(epfd, job);

    if (job->backend.fd == -1) {
        clear_backend_request(job);
        job_respond_status_only(job, 503, "Backend unavailable");
        to_next_state(epfd, job);
        return;
    }

    to_next_state(epfd, job);
}

static void send_backend_request(int epfd, struct job *job) {
    ssize_t sent = send(
                job->backend.fd,
                job->backend_request.data + (job->backend_request.len - job->backend_request.remaining_len),
                job->backend_request.remaining_len,
                MSG_DONTWAIT);
    if (sent == -1 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
        if (job_respond_status_only(job, 503, "Backend send failed") == -1) {
            close_job(epfd, job);
            return;
        }

        to_next_state(epfd, job);
        return;
    }

    if (sent == -1)
        return;

    assert(sent <= job->backend_request.remaining_len);
    job->backend_request.remaining_len -= sent;
}

static void state_to_backend_write(int epfd, struct job *job) {
    poll_appropriate_fd(epfd, job);
}

static void state_to_backend_read(int epfd, struct job *job) {
    poll_appropriate_fd(epfd, job);
}

static void state_to_client_write(int epfd, struct job *job) {
    poll_appropriate_fd(epfd, job);
}

static void state_to_client_read(int epfd, struct job *job) {
    finished_with_previous_request(epfd, job);

    // If there's data in the pipe go inspect it to see if it contains a full
    // request already (eg. pipelined request).

    if (job->client.incoming.len > 0) {
        on_client_input(epfd, job);
        return;
    }

    poll_appropriate_fd(epfd, job);
}

static void to_next_state(int epfd, struct job *job) {
    assert(job->client.fd != -1);

    switch (job_state(job)) {
    case FRONTEND_WRITE_WAIT:
        state_to_client_write(epfd, job);
        break;
    case BACKEND_WRITE_WAIT:
        state_to_backend_write(epfd, job);
        break;
    case BACKEND_READ_WAIT:
        state_to_backend_read(epfd, job);
        break;
    case FRONTEND_READ_WAIT:
        state_to_client_read(epfd, job);
        break;
    }
}

// The transformation invalidates any pointers to the given buffer.
static ssize_t transform_response_body(struct streambuf *buf) {
    const char from[] = "http://watch.sling.com";
    const size_t fromlen = sizeof(from) - 1;
    const char to[] = "http://hahaha.com";
    const size_t tolen = sizeof(to) - 1;

    if (tolen > fromlen) {
        if (streambuf_ensure_capacity(buf, tolen - fromlen) < 0)
            return -1;
    }

    ssize_t new_len = replace_string(buf->data, buf->len, buf->cap, from, fromlen, to, tolen);

    if (new_len >= 0)
        buf->len = new_len;

    return new_len;
}

static ssize_t make_response_header(char *buf, size_t buflen, size_t content_length) {
    int len = snprintf(buf, buflen,
            "HTTP/1.1 200 OK\r\n"
            "Server: Proxymoron\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: %zu\r\n"
            "\r\n", content_length);

    if (len == buflen)
        return -1; // truncated string

    return len;
}

static void write_client_response(int epfd, struct job *job) {
    tcp_cork(job->client.fd);

    char header_buf[4096];
    bool ok = false;
    ssize_t len = make_response_header(header_buf, sizeof(header_buf), job->backend.incoming.len);
    if (!len) {
        if (job_respond_status_only(job, 500, "Failed to render response header") == -1)
            goto cleanup;

        // managed to inform the client that we had a problem,
        // so don't disconnect the client.
        ok = true;
        goto cleanup;
    }

    if (endpoint_send(&job->client, header_buf, len) == -1)
        goto cleanup;

    if (endpoint_send(&job->client, job->backend.incoming.data, job->backend.incoming.len) == -1)
        goto cleanup;

    ok = true;

cleanup:
    tcp_uncork(job->client.fd);

    if (ok)
        to_next_state(epfd, job);
    else
        close_job(epfd, job);
}

static void read_backend_response(int epfd, struct job *job) {
    ssize_t len = endpoint_recv(&job->backend, RECV_NO_LIMIT);

    if (len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // No data yet.
        to_next_state(epfd, job);
        return;
    }

    if (len == -1 || len == 0) {
        fprintf(stderr, "Backend fd %d was trash (%s); replacing\n",
                job->backend.fd,
                len == 0 ? "connection closed" : strerror(errno));

        delete_backend_fd(epfd, &backend_pool, job->backend.fd);

        job->backend_request.remaining_len = job->backend_request.len;

        job->backend.fd = get_backend_fd_and_send(epfd, job);

        if (job->backend.fd == -1) {
            clear_backend_request(job);

            if (job_respond_status_only(job, 503, "Backend unavailable (for real)") == -1) {
                close_job(epfd, job);
                return;
            }

            to_next_state(epfd, job);
            return;
        }

        to_next_state(epfd, job);
        return;
    }

    char *end_of_header = find_end_of_header(job->backend.incoming.data, job->backend.incoming.len);
    if (end_of_header == NULL) {
        // re-arm fd
        to_next_state(epfd, job);
        return;
    }

    // FIXME: Parse out Content-Length so we know when we've received the full
    // response.

    // TODO: If the response contained "Connection: close", close it and
    // prepare a replacement connection, but don't throw away the response
    // that we got.

    // Finished with the backend fd.
    release_backend_fd(epfd, &backend_pool, job->backend.fd);
    job->backend.fd = -1;

    // Throw away response headers (later: store them off for use in the client
    // response).
    streambuf_advance(&job->backend.incoming, end_of_header - job->backend.incoming.data);

    if (transform_response_body(&job->backend.incoming) == -1) {
        job_respond_status_only(job, 500, "Transformation failed");
        to_next_state(epfd, job);
        return;
    }

    write_client_response(epfd, job);
}

static void read_event(int epfd, struct job *job) {
    switch (job_state(job)) {
    case FRONTEND_READ_WAIT:
        on_client_input(epfd, job);
        break;
    case BACKEND_READ_WAIT:
        read_backend_response(epfd, job);
        break;
    default:
        abort();
        return;
    }
}

static void write_event(int epfd, const struct epoll_event *event) {
    struct job *job = event->data.ptr;

    if (job->client.outgoing.len > 0)
        endpoint_flush(&job->client);

    if (job->backend_request.remaining_len > 0)
        send_backend_request(epfd, job);

    if (job->client.fd == -1) {
        close_job(epfd, job);
        return;
    }

    to_next_state(epfd, job);
}

static void new_client(int epfd, struct job *listen_job) {
    int client = accept4(listen_job->client.fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (client == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // other process probably got it.
            return;
        }

        perror("accept4");
        return;
    }

    struct job *job = new_job(client);
    if (job == NULL) {
        perror("failed to allocate job");
        char msg[] = "HTTP/1.1 503 Service Unavailable\r\n\r\n";
        send(client, msg, sizeof(msg) - 1, MSG_DONTWAIT);
        close(client);
        return;
    }

    // Here we don't want to subscribe to any events because we'll immediately
    // do a socket read, *then* subscribe to read events (or go straight to
    // BACKEND_WRITE_WAIT), but we need the client FD to be part of the epoll
    // set (so we can do a CTL_MOD later).
    struct epoll_event revent = {
        .events = 0 | EPOLLET | EPOLLONESHOT,
        .data = {
            .ptr = job,
        },
    };

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, client, &revent) == -1) {
        perror("epoll_ctl ADD");
        if (close(client) == -1)
            perror("close");
    }

    // Optimize for TCP_FASTOPEN and/or TCP_DEFER_ACCEPT, wherein data
    // (a request) will be waiting immediately upon connection receipt.
    on_client_input(epfd, job);
}

static void dispatch(int epfd, const struct epoll_event *event, int listensock) {
    if (event->events & EPOLLIN) {
        struct job *job = event->data.ptr;

        if (job->client.fd == listensock)
            new_client(epfd, job);
        else
            read_event(epfd, job);
    } else if (event->events & EPOLLOUT) {
        write_event(epfd, event);
    }

    struct job *job = event->data.ptr;
    if (job->client.fd == -1) {
        // Client already closed
        return;
    }
}

static void multiply() {
    int ncpus;

#if defined(NCPUS)
    ncpus = NCPUS;
#else
    ncpus = get_nprocs_conf();
#endif

    fprintf(stderr, "Using %d processes\n", ncpus);

    // parent keeps running too
    for (unsigned i = 1; i < ncpus; ++i) {
        pid_t pid = fork();
        if (pid == -1)
            perror("fork");
        if (pid == 0)
            return;
    }
}

__attribute__((unused))
static void enable_defer_accept(int sock) {
#if defined (TCP_DEFER_ACCEPT)
    const int seconds = 2;
    if (setsockopt(sock, IPPROTO_TCP, TCP_DEFER_ACCEPT, &seconds, sizeof(seconds)) == -1)
        perror("setsockopt TCP_DEFER_ACCEPT");
#endif
}

static void enable_fastopen_on_listener(int sock) {
#if defined(TCP_FASTOPEN)
    const int queue_len = 100;
    if (setsockopt(sock, IPPROTO_TCP, TCP_FASTOPEN, &queue_len, sizeof(queue_len)) == -1)
        perror("setsockopt(TCP_FASTOPEN)");
#endif
}

static void reuseport(int sock) {
    const int yes = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) == -1)
        perror("setsockopt(SO_REUSEPORT)");
}

static int server_socket(void) {
    int sock = socket(PF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (sock == -1)
        fatal_perror("socket");

    reuseport(sock);

    const struct sockaddr_in6 bind_addr = {
        .sin6_family = AF_INET6,
        .sin6_port = htons(PORT_NUMBER),
        .sin6_flowinfo = 0,
        .sin6_addr = in6addr_any,
        .sin6_scope_id = 0,
    };

    if (bind(sock, (const struct sockaddr*)&bind_addr, sizeof(bind_addr)) == -1)
        fatal_perror("bind");

    // This is slightly slower in this case, but probably want to enable it on a
    // real server. Might be faster once we do an immediate read on a new
    // socket.
    enable_defer_accept(sock);
    // nodelay seems to reduce throughput. leave it off for now.
    //enable_nodelay(sock);

    enable_fastopen_on_listener(sock);

    if (listen(sock, 1000) == -1)
        fatal_perror("listen");

    return sock;
}

static void quiesce_signals(void) {
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
        fatal_perror("signal(SIGPIPE)");
}

// Dummy `struct job` to store the listen sock for epoll.
static struct job listen_sock_job;

static int setup_epoll_fd(int listen_sock) {
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd == -1)
        fatal_perror("epoll_create");

    listen_sock_job.client.fd = listen_sock;

    struct epoll_event event = {
        .events = EPOLLIN,
        .data = {
            .ptr = &listen_sock_job,
        },
    };

    if (event.data.ptr == NULL)
        fatal_perror("Failed to create epoll wrapper for listen socket");

#if defined(EPOLLEXCLUSIVE)
    event.events |= EPOLLEXCLUSIVE;
#endif

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_sock, &event) == -1)
        fatal_perror("epoll_ctl add");

    return epfd;
}

static void event_loop_iteration(int epfd, int listen_sock) {
    struct epoll_event events[10];

    int count = epoll_wait(epfd, events, sizeof(events) / sizeof(events[0]), -1);

    if (count == -1)
        fatal_perror("epoll_wait");

    if (count == 0)
        return;

    for (unsigned i = 0; i < count; ++i)
        dispatch(epfd, &events[i], listen_sock);

    free_closed_jobs();
}

static void event_loop(int epfd, int listen_sock) {
    for (;;)
        event_loop_iteration(epfd, listen_sock);
}

static int serve(void) {
    int s = server_socket();
    int epfd = setup_epoll_fd(s);

    setup_connection_pool(&backend_pool);

    event_loop(epfd, s);

    close(epfd);
    close(s);

    return EXIT_SUCCESS;
}

int main(void) {
    quiesce_signals();
    multiply();
    return serve();
}
