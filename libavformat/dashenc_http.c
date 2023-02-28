/*
 * Manages connections and HTTP requests.
 *
 * Things to keep in mind.
 * s->io_open in this file will open the TCP connection and start the HTTP request.
 * There is some code in here that is also considering file based output, this is currently not used in production.
 *
 */

#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>

#include <sys/queue.h>

#include "dashenc_http.h"

#include "config.h"
#include "libavformat/internal.h"
#include "libavutil/avassert.h"
#include "libavutil/avutil.h"
#include "libavutil/avstring.h"
#include "libavutil/time.h"

#include "avio_internal.h"
#include "dashenc_pool.h"
#include "dashenc_stats.h"
#include "config_components.h"
#if CONFIG_HTTP_PROTOCOL
#include "http.h"
#endif

typedef struct buffer_data {
    uint8_t *buf;
    size_t size;   /* Current size of the buffer */
    uint8_t *ptr;
    size_t room;   /* Size left in the buffer */
} buffer_data;

typedef struct Chunk {
    unsigned char *buf;
    int size;
} Chunk;

typedef struct ChunksStorage {
    Chunk **storage;
    pthread_mutex_t mutex;
    pthread_cond_t cv;

    int nr_of_chunks;       /* Nr of chunks available, guarded by chunks_mutex */
    int last_chunk_written; /* Last chunk number that has been written */
} ChunksStorage;

/**
 *  Connection currently has too may responsibilities:
 *  - Represents a virtual connection to dashenc
 *  - Contains the actual TCP connection that can be closed and re-opened
 *  - Contains the request state and holds a buffer of chunks for that request
 */
typedef struct connection {
    int nr;                  /* Number of the connection, used to lookup this connection in the connections list */
    AVIOContext *out;        /* The TCP connection */
    _Atomic bool claimed;             /* This connection is claimed for a specific request */

    bool req_opened;         /* If true the request is opened and more data can be written to it, only accessed from w_thread */
    bool opened;     /* TCP connection (out) is opened */
    bool open_error; /* If true the connection could not be opened */
    pthread_mutex_t open_mutex;

    _Atomic int64_t release_time;    /* Time the last request of the connection has finished */
    AVFormatContext *s;      /* Used to clean up the TCP connection if closing of a request fails */
    pthread_t w_thread;      /* Thread that is used to write the chunks */
    LIST_ENTRY(connection) entries;

    ChunksStorage chunks;               /* A queue with pointers to chunks */
    _Atomic bool chunks_done;        /* Are all chunks for this request available in the buffer */

    //Request specific data
    int must_succeed;       /* If 1 the request must succeed, otherwise we'll crash the program */
    int retry;              /* If 1 the request can be retried */
    int retry_nr;           /* Current retry number, used to limit the nr of retries */
    char *url;              /* url of the current request */
    AVDictionary *options;
    int http_persistent;
    _Atomic bool cleanup_requested;  /* This conn should be deleted, can be caused by too many idle connections */
    buffer_data *mem;       /* Optional buffer to hold file content that will be written */
} connection;

/* If there will be to may connections, this should be replaced with hashtable */
LIST_HEAD(connections_head, connection) connections = LIST_HEAD_INITIALIZER(connections);

static pthread_mutex_t connections_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t connections_thread_exit_cv = PTHREAD_COND_INITIALIZER;

const static int max_idle_connections = 15;
static _Atomic int nr_of_connections = 0;
static int total_nr_of_connections = 0; /* nr of connections made in total */

static stats *chunk_write_time_stats;
static stats *conn_count_stats;
static _Atomic bool should_stop = false;

//defined here because it has a circular dependency with retry()
static void *thr_io_close(connection *conn);

/* This method expects the lock to be already done.*/
static void release_request(connection *conn) {
    int64_t release_time = av_gettime() / 1000;
    av_log(NULL, AV_LOG_INFO, "release_request conn_nr: %d.\n", conn->nr);

    if (conn->claimed) {
        free(conn->url);
        av_dict_free(&conn->options);
    }

    pthread_mutex_lock(&conn->chunks.mutex);
    for (int i = 0; i < conn->chunks.nr_of_chunks; i++) {
        Chunk *chunk = conn->chunks.storage[i];
        av_free(chunk->buf);
        av_free(chunk);
    }
    av_freep(&conn->chunks.storage);
    conn->chunks.nr_of_chunks = 0;
    conn->chunks.last_chunk_written = 0;
    pthread_mutex_unlock(&conn->chunks.mutex);

    conn->chunks_done = false;
    conn->claimed = false;
    conn->release_time = release_time;
    conn->retry_nr = 0;
    conn->open_error = false;
}

static void abort_if_needed(int mustSucceed) {
    if (mustSucceed) {
        av_log(NULL, AV_LOG_ERROR, "Abort because request needs to succeed and it did not.\n");
        abort();
    }
}

static inline bool chunk_is_available(ChunksStorage *chunks) {
    return chunks->last_chunk_written < chunks->nr_of_chunks;
}

/* returns true if all the chunks are written */
static bool write_chunk_if_available(connection *conn) {
    int64_t start_time_ms;
    int64_t write_time_ms;
    int64_t flush_time_ms;
    int64_t after_write_time_ms;

    if (!conn->out) {
        av_log(NULL, AV_LOG_WARNING, "Connection not open so skip avio_write. Conn_nr: %d, url: %s\n", conn->nr , conn->url);
        return false;
    }

    pthread_mutex_lock(&conn->chunks.mutex);
    if (!chunk_is_available(&conn->chunks)) {
        pthread_mutex_unlock(&conn->chunks.mutex);
        return false;
    }

    Chunk *chunk = conn->chunks.storage[conn->chunks.last_chunk_written++];
    pthread_mutex_unlock(&conn->chunks.mutex);


    start_time_ms = av_gettime() / 1000;
    // if (chunk_nr > 282 || chunk->size > 50000) {
    //     av_log(NULL, AV_LOG_WARNING, "chunk issue? chunk_nr: %d, conn_nr: %d, size: %d\n", chunk_nr, conn->nr, chunk->size);
    // }

    avio_write(conn->out, chunk->buf, chunk->size);

    after_write_time_ms = av_gettime() / 1000;
    write_time_ms = after_write_time_ms - start_time_ms;
    if (write_time_ms > 100) {
        av_log(NULL, AV_LOG_WARNING, "It took %"PRId64"(ms) to write chunk. conn_nr: %d\n", write_time_ms, conn->nr);
    }

    avio_flush(conn->out);
    flush_time_ms = av_gettime() / 1000 - after_write_time_ms;
    if (flush_time_ms > 100) {
        av_log(NULL, AV_LOG_WARNING, "It took %"PRId64"(ms) to flush chunk. conn_nr: %d\n", flush_time_ms, conn->nr);
    }

    print_complete_stats(chunk_write_time_stats, av_gettime() / 1000 - start_time_ms);
    print_complete_stats(conn_count_stats, nr_of_connections);

    return true;
}

static connection *get_conn(int conn_nr) {
    connection *conn = NULL;

    pthread_mutex_lock(&connections_mutex);
    LIST_FOREACH(conn, &connections, entries) {
        if (conn->nr == conn_nr) {
            break;
        }
    }
    pthread_mutex_unlock(&connections_mutex);
    if (conn->nr != conn_nr) {
        av_log(NULL, AV_LOG_ERROR, "connection %d not found.\n", conn_nr);
        av_log(NULL, AV_LOG_ERROR, "First conn_nr: %d.\n", LIST_FIRST(&connections)->nr);
        abort();
    }
    return conn;
}

// Open connection if not open and start doing the request
static int io_open_for_retry(connection *conn) {
    int ret = 0;
    URLContext *http_url_context = NULL;
    AVFormatContext *s = conn->s;

    pthread_mutex_lock(&conn->open_mutex);
    if (!conn->opened) {
        av_log(s, AV_LOG_INFO, "Connection for retry: %d not yet open. conn_nr: %d, url: %s\n", conn->retry_nr, conn->nr, conn->url);

        ret = s->io_open(s, &(conn->out), conn->url, AVIO_FLAG_WRITE, &conn->options);
        if (ret < 0) {
            av_log(s, AV_LOG_WARNING, "io_open_for_retry %d could not open url: %s\n", conn->retry_nr, conn->url);
            goto error;
        }

        conn->opened = true;
        pthread_mutex_unlock(&conn->open_mutex);
        return ret;
    }
    pthread_mutex_unlock(&conn->open_mutex);

    http_url_context = ffio_geturlcontext(conn->out);
    if (http_url_context == NULL) {
        av_log(s, AV_LOG_WARNING, "Failed to get url context");
        goto error_close;
    }

    ret = ff_http_do_new_request(http_url_context, conn->url);
    if (ret != 0) {
        const int64_t curr_time_ms = av_gettime() / 1000;
        const int64_t idle_tims_ms = curr_time_ms - conn->release_time;
        av_log(s, AV_LOG_WARNING, "io_open_for_retry error conn_nr: %d, idle_time: %"PRId64", error: %d, retry_nr: %d, url: %s\n", conn->nr, idle_tims_ms, ret, conn->retry_nr, conn->url);
        goto error_close;
    }

    return ret;

error_close:
    pthread_mutex_lock(&conn->open_mutex);
    ff_format_io_close(s, &conn->out);

error:
    conn->open_error = true;
    pthread_mutex_unlock(&conn->open_mutex);
    return ret;
}

/**
 * This will retry a previously failed request.
 * We assume this method is ran from one of our own threads so we can safely use usleep.
 */
static void retry(connection *conn) {
    if (conn->retry_nr > 10) {
        av_log(NULL, AV_LOG_WARNING, "-event- request retry failed. Giving up. request: %s, attempt: %d, conn_nr: %d.\n",
               conn->url, conn->retry_nr, conn->nr);
        return;
    }

    usleep(1 * 1000000);

    av_log(NULL, AV_LOG_INFO, "Request retry waiting for segment to be completely recorded. request: %s, attempt: %d, conn_nr: %d.\n",
        conn->url, conn->retry_nr, conn->nr);

    int chunk_wait_timeout = 10;
    // Wait until all chunks are recorded
    while (!conn->chunks_done && chunk_wait_timeout > 0) {
        usleep(1 * 1000000);
        chunk_wait_timeout --;
    }
    if (!conn->chunks_done) {
        av_log(NULL, AV_LOG_ERROR, "Retry could not collect all chunks for request %s, attempt: %d, conn_nr: %d\n", conn->url, conn->retry_nr, conn->nr);
    }

    conn->retry_nr = conn->retry_nr + 1;

    av_log(NULL, AV_LOG_WARNING, "Starting retry for request %s, attempt: %d, conn_nr: %d\n", conn->url, conn->retry_nr, conn->nr);
    int ret = io_open_for_retry(conn);
    if (ret < 0) {
        av_log(NULL, AV_LOG_WARNING, "-event- request retry failed request: %s, ret=%d, attempt: %d, conn_nr: %d.\n",
               conn->url, ret, conn->retry_nr, conn->nr);

        retry(conn);
        return;
    }
    pthread_mutex_lock(&conn->chunks.mutex);
    conn->chunks.last_chunk_written = 0; /* Restart writing chunks from the beginning */
    pthread_mutex_unlock(&conn->chunks.mutex);

    while (write_chunk_if_available(conn));

    av_log(NULL, AV_LOG_INFO, "request retry done, start reading response. Request: %s, conn_nr: %d, attempt: %d.\n", conn->url, conn->nr, conn->retry_nr);
    thr_io_close(conn);
}

static void remove_from_list(connection *conn) {
    av_log(NULL, AV_LOG_INFO, "Removing conn_nr: %d\n", conn->nr);
    LIST_REMOVE(conn, entries);
    nr_of_connections--;
}

/**
 * Remove a connection from the list and free it's memory.
 * This method expects to be started from the connection thread.
 */
static void connection_exit(connection *conn) {
    av_log(conn->s, AV_LOG_INFO, "Removing conn %d\n", conn->nr);
    pthread_mutex_lock(&connections_mutex);
    remove_from_list(conn);

    pthread_mutex_lock(&conn->open_mutex);
    conn->opened = false;
    ff_format_io_close(conn->s, &conn->out);
    pthread_mutex_unlock(&conn->open_mutex);

    pthread_mutex_destroy(&conn->open_mutex);
    pthread_mutex_destroy(&conn->chunks.mutex);
    pthread_cond_destroy(&conn->chunks.cv);
    free(conn);

    pthread_cond_signal(&connections_thread_exit_cv);
    pthread_mutex_unlock(&connections_mutex);
    pthread_exit(NULL);
}

/**
 * This method closes the request and reads the response.
 */
static void *thr_io_close(connection *conn) {
    int ret;
    int response_code;

    av_log(NULL, AV_LOG_INFO, "thr_io_close conn_nr: %d, out_addr: %p \n", conn->nr, conn->out);

    pthread_mutex_lock(&conn->open_mutex);
    bool open_error = conn->open_error;
    pthread_mutex_unlock(&conn->open_mutex);
    if (open_error) {
        ret = -1;
        response_code = 0;
    } else {
        URLContext *http_url_context = ffio_geturlcontext(conn->out);
        if (http_url_context == NULL) {
            pthread_mutex_lock(&conn->open_mutex);
            conn->open_error = true;
            pthread_mutex_unlock(&conn->open_mutex);
            ret = -1;
            response_code = 0;
        } else {
            avio_flush(conn->out);
            ret = ffurl_shutdown(http_url_context, AVIO_FLAG_WRITE);
            response_code = ff_http_get_code(http_url_context);
        }
    }

    if (ret < 0 || response_code >= 500) {
        av_log(NULL, AV_LOG_INFO, "-event- request failed ret=%d, conn_nr: %d, response_code: %d, url: %s.\n", ret, conn->nr, response_code, conn->url);
        abort_if_needed(conn->must_succeed);
        //must check if this is NULL or it causes crash on segmentation fault due to NULL pointer
        //check why conn->s (AVFormatContext) becomes NULL
        pthread_mutex_lock(&conn->open_mutex);
        conn->opened = false;
        if (conn->s != NULL) {
            ff_format_io_close(conn->s, &conn->out);
        }
        pthread_mutex_unlock(&conn->open_mutex);

        if (conn->retry)
            retry(conn);
    }

    release_request(conn);
    if (should_stop) {
        connection_exit(conn);
    }

    pthread_mutex_lock(&conn->open_mutex);
    conn->req_opened = false;
    pthread_mutex_unlock(&conn->open_mutex);

    return NULL;
}

/**
 * Opens the TCP connection if it's not open.
 * Opens the request if it's not open.
 *
 */
static int open_request_if_needed(connection *conn) {
    int ret;
    URLContext *http_url_context;

    pthread_mutex_lock(&conn->open_mutex);
    if (conn->req_opened) {
        pthread_mutex_unlock(&conn->open_mutex);
        return conn->nr;
    }

    if (!conn->opened) {
        av_log(conn->s, AV_LOG_INFO, "Connection(%d) not yet open, opening and starting req %s\n", conn->nr, conn->url);
        ret = conn->s->io_open(conn->s, &(conn->out), conn->url, AVIO_FLAG_WRITE, &conn->options);
        if (ret < 0) {
            av_log(conn->s, AV_LOG_WARNING, "Could not open %s\n", conn->url);
            goto error;
        }

        conn->req_opened = true;
        conn->opened = true;
        pthread_mutex_unlock(&conn->open_mutex);
        return conn->nr;
    }
    pthread_mutex_unlock(&conn->open_mutex);

    http_url_context = ffio_geturlcontext(conn->out);
    if (http_url_context == NULL) {
        av_log(conn->s, AV_LOG_WARNING, "Could not get http_url_context!\n");
        goto error_close;
    }

    av_log(conn->s, AV_LOG_INFO, "Connection(%d)\n", conn->nr);
    av_log(conn->s, AV_LOG_INFO, "Connection(%d) start req on existing connection %s\n", conn->nr, conn->url);

    ret = ff_http_do_new_request(http_url_context, conn->url);
    if (ret != 0) {
        const int64_t curr_time_ms = av_gettime() / 1000;
        const int64_t idle_tims_ms = curr_time_ms - conn->release_time;
        av_log(conn->s, AV_LOG_WARNING, "pool_io_open error conn_nr: %d, idle_time: %"PRId64", error: %d, name: %s\n", conn->nr, idle_tims_ms, ret, conn->url);
        goto error_close;
    }

    pthread_mutex_lock(&conn->open_mutex);
    conn->req_opened = true;
    pthread_mutex_unlock(&conn->open_mutex);
    return conn->nr;

error_close:
    pthread_mutex_lock(&conn->open_mutex);
    ff_format_io_close(conn->s, &conn->out);
error:
    abort_if_needed(conn->must_succeed);
    conn->open_error = true;
    pthread_mutex_unlock(&conn->open_mutex);
    if (!conn->retry) {
        return ret;
    }

    return conn->nr;
}

/**
 * This method writes the chunks.
 * It is supposed to be passed to pthread_create.
 */
static void *thr_io_write(void *arg) {
    connection *conn = (connection *)arg;
    int conn_nr = conn->nr;
    //https://computing.llnl.gov/tutorials/pthreads/#ConditionVariables

    for (;;) {
        pthread_mutex_lock(&conn->chunks.mutex);
        while (!chunk_is_available(&conn->chunks) && !conn->chunks_done) {
            pthread_cond_wait(&conn->chunks.cv, &conn->chunks.mutex);
            if (conn->cleanup_requested) {
                pthread_mutex_unlock(&conn->chunks.mutex);
                connection_exit(conn);
            }
        }
        pthread_mutex_unlock(&conn->chunks.mutex);

        open_request_if_needed(conn);
        while (write_chunk_if_available(conn));

        if (conn->chunks_done) {
            thr_io_close(conn);
            // after this no other action should be done on conn until a new request is started so make sure there are no statements below this.
            continue;
        }
    }

    av_log(NULL, AV_LOG_INFO, "dashenc_http thread done, conn_nr: %d.\n", conn_nr);
    return NULL;
}

static void request_cleanup(connection *conn) {
    if (conn->cleanup_requested)
        return;

    av_log(conn->s, AV_LOG_INFO, "Request cleanup of conn %d\n", conn->nr);
    conn->cleanup_requested = true;

    //signal connection thread
    pthread_mutex_lock(&conn->chunks.mutex);
    pthread_cond_signal(&conn->chunks.cv);
    pthread_mutex_unlock(&conn->chunks.mutex);
}

/**
 * Trigger deletion of idle connections.
 * Expects connections_mutex to be locked.
 */
static void free_idle_connections(int nr_of_idle_connections, int nr_of_connections_to_keep) {
    connection *conn = NULL;

    av_log(NULL, AV_LOG_INFO, "free_idle_connections, nr_of_idle_connections: %d, nr_of_connections_to_keep: %d\n", nr_of_idle_connections, nr_of_connections_to_keep);

    LIST_FOREACH(conn, &connections, entries) {
        if (nr_of_idle_connections <= nr_of_connections_to_keep) {
            break;
        }

        if (!conn->claimed) {
            request_cleanup(conn);
            nr_of_idle_connections--;
        }
    }
}

/**
 * Claims a free connection and returns it.
 * Released connections are used first.
 */
static connection *claim_connection(char *url, int need_new_connection) {
    int64_t lowest_release_time = av_gettime() / 1000;
    int conn_nr = -1;
    int conn_idle_count = 0;
    connection *conn = NULL;
    connection *conn_l = NULL;
    size_t len;

    if (url == NULL) {
        av_log(NULL, AV_LOG_INFO, "Claimed conn_id: -1, url: NULL\n");
        return NULL;
    }

    pthread_mutex_lock(&connections_mutex);
    LIST_FOREACH(conn_l, &connections, entries) {
        if (!conn_l->claimed && !conn_l->cleanup_requested) {
            if ((conn_nr == -1) || (conn->release_time != 0 && conn_l->release_time < lowest_release_time)) {
                conn_nr = conn_l->nr;
                conn = conn_l;
                lowest_release_time = conn->release_time;
            }
            conn_idle_count++;
        }
    }

    if (conn_nr == -1) {
        conn = calloc(1, sizeof(*conn));
        if (conn == NULL) {
            pthread_mutex_unlock(&connections_mutex);
            return conn;
        }

        pthread_mutex_init(&conn->open_mutex, NULL);

        //TODO: In theory when we have a rollover of total_nr_of_connections we could claim a connection number that is still in use.
        conn_nr = total_nr_of_connections;
        conn->nr = conn_nr;
        nr_of_connections++;
        total_nr_of_connections++;

        pthread_mutex_init(&conn->chunks.mutex, NULL);
        pthread_cond_init(&conn->chunks.cv, NULL);

        pthread_attr_t attr;
        if (pthread_attr_init(&attr)) {
            av_log(NULL, AV_LOG_ERROR, "Error creating thread attributes.\n");
            abort();
        }

        if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) {
            av_log(NULL, AV_LOG_ERROR, "Error setting thread attributes.\n");
            abort();
        }

        if(pthread_create(&conn->w_thread, &attr, thr_io_write, conn)) {
            av_log(NULL, AV_LOG_ERROR, "Error creating thread so abort.\n");
            abort();
        }

        LIST_INSERT_HEAD(&connections, conn, entries);
        av_log(NULL, AV_LOG_INFO, "No free connections so added one. Url: %s, conn_nr: %d\n", url, conn_nr);
    } else {
        pthread_mutex_lock(&conn->open_mutex);
        if (need_new_connection && conn->opened) {
            conn->opened = false;
            ff_format_io_close(conn->s, &conn->out);
        }
        pthread_mutex_unlock(&conn->open_mutex);

        conn->nr = conn_nr;
    }

    av_log(NULL, AV_LOG_INFO, "Claimed conn_id: %d, url: %s\n", conn_nr, url);
    len = strlen(url) + 1;
    conn->url = malloc(len);
    av_strlcpy(conn->url, url, len);
    conn->claimed = true;

    if(conn_idle_count > max_idle_connections){
        free_idle_connections(conn_idle_count, max_idle_connections);
    }

    pthread_mutex_unlock(&connections_mutex);

    return conn;
}

/**
 * Opens a request on a free connection and returns the connection number
 * Only used for non persistent HTTP connections or file based output
 */
static int open_request(AVFormatContext *s, char *url, AVDictionary **options) {
    connection *conn = claim_connection(url, 0);

    pthread_mutex_lock(&conn->open_mutex);
    if (conn->opened) {
        av_log(s, AV_LOG_WARNING, "open_request while connection might be open. This is TODO for when not using persistent connections. conn_nr: %d\n", conn->nr);
    }

    int ret = s->io_open(s, &conn->out, url, AVIO_FLAG_WRITE, options);
    if (ret >= 0) {
        ret = conn->nr;
        conn->opened = true;
    }
    pthread_mutex_unlock(&conn->open_mutex);
    return ret;
}


/**
 * Claim a connection and start a new request.
 * The claimed connection number is returned.
 */
int pool_io_open(AVFormatContext *s, char *filename,
                 AVDictionary **options, int http_persistent, int must_succeed, int retry, int need_new_connection) {

    int http_base_proto = filename ? ff_is_http_proto(filename) : 0;
    int ret = AVERROR_MUXER_NOT_FOUND;

    if (!http_base_proto || !http_persistent) {
        //open_request returns the newly claimed conn_nr
        ret = open_request(s, filename, options);
        av_log(s, AV_LOG_WARNING, "Non HTTP request %s\n", filename);
#if CONFIG_HTTP_PROTOCOL
    } else {
        AVDictionary *d = NULL;

        //claim new item from pool and open connection if needed
        connection *conn = claim_connection(filename, need_new_connection);

        conn = get_conn(conn->nr);
        conn->must_succeed = must_succeed;
        conn->retry = retry;
        conn->s = s;

        conn->options = d;
        ret = av_dict_copy(&conn->options, *options, 0);
        if (ret < 0) {
            av_log(s, AV_LOG_WARNING, "Could not copy options for %s\n", filename);
            abort_if_needed(must_succeed);
            return ret;
        }

        conn->http_persistent = http_persistent;
        ret = conn->nr;
#endif
    }

    return ret;
}

/**
 * Closes the request.
 */
static void pool_conn_close(connection *conn) {
    conn->chunks_done = true;

    pthread_mutex_lock(&conn->chunks.mutex);
    pthread_cond_signal(&conn->chunks.cv);
    pthread_mutex_unlock(&conn->chunks.mutex);
}

void pool_io_close(AVFormatContext *s, char *filename, int conn_nr) {
    if (conn_nr < 0) {
        av_log(s, AV_LOG_WARNING, "Invalid conn_nr in pool_io_close for filename: %s, conn_nr: %d\n", filename, conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);
    av_log(NULL, AV_LOG_INFO, "pool_io_close conn_nr: %d\n", conn_nr);
    pool_conn_close(conn);
}

void pool_free_all(AVFormatContext *s) {
    connection *conn = NULL;

    av_log(s, AV_LOG_INFO, "pool_free_all\n");

    // Signal the connections to close
    should_stop = true;

    pthread_mutex_lock(&connections_mutex);
    LIST_FOREACH(conn, &connections, entries) {
        if (conn->claimed) {
            pool_conn_close(conn);
        } else {
            request_cleanup(conn);
        }
    }

    while (!LIST_EMPTY(&connections)) {
        pthread_cond_wait(&connections_thread_exit_cv, &connections_mutex);
    }
    pthread_mutex_unlock(&connections_mutex);

    av_log(s, AV_LOG_INFO, "All requests are stopped\n");
}


void pool_write_flush_mem(int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_write_flush_mem. conn_nr: %d\n", conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);
    int read_size = conn->mem->ptr - conn->mem->buf;

    pool_write_flush(conn->mem->buf, read_size, conn_nr);
}

void pool_write_flush(const unsigned char *buf, int size, int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_write_flush. conn_nr: %d\n", conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);

    //Save the chunk in memory
    Chunk *new_chunk = (Chunk *)av_mallocz(sizeof(*new_chunk));
    if (!new_chunk) {
        av_log(NULL, AV_LOG_WARNING, "Could not malloc new_chunk.\n");
        return;
    }

    new_chunk->size = size;
    new_chunk->buf = av_mallocz(size);
    if (new_chunk->buf == NULL) {
        av_log(NULL, AV_LOG_WARNING, "Could not malloc pool_write_flush.\n");
        return;
    }
    memcpy(new_chunk->buf, buf, size);

    pthread_mutex_lock(&conn->chunks.mutex);
    av_dynarray_add(&conn->chunks.storage, &conn->chunks.nr_of_chunks, new_chunk);
    pthread_cond_signal(&conn->chunks.cv);
    pthread_mutex_unlock(&conn->chunks.mutex);
}

static int write_packet(void *opaque, uint8_t *buf, int buf_size) {
    struct buffer_data *bd = (struct buffer_data *)opaque;
    while (buf_size > bd->room) {
        int64_t offset = bd->ptr - bd->buf;
        bd->buf = av_realloc_f(bd->buf, 2, bd->size);
        if (!bd->buf)
            return AVERROR(ENOMEM);
        bd->size *= 2;
        bd->ptr = bd->buf + offset;
        bd->room = bd->size - offset;
    }

    memcpy(bd->ptr, buf, buf_size);
    bd->ptr  += buf_size;
    bd->room -= buf_size;

    return buf_size;
}

/**
 * Create AVIOContext for memory writing (instead of directly writing to the output)
 * Can be used with pool_write_flush_mem
 * Should be freed with pool_free_mem_context()
 */
AVIOContext *pool_create_mem_context(int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_create_mem_context. conn_nr: %d\n", conn_nr);
        return NULL;
    }

    const size_t bd_buf_size = 10;
    connection *conn = get_conn(conn_nr);

    conn->mem = av_malloc(sizeof(buffer_data));
    conn->mem->room = 0;
    conn->mem->size = 0;
    conn->mem->ptr = conn->mem->buf = av_malloc(bd_buf_size);
    if (!conn->mem->buf) {
        av_log(NULL, AV_LOG_WARNING, "Could not allocate memory bd_buf_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }
    conn->mem->size = conn->mem->room = bd_buf_size;

    const size_t avio_ctx_buffer_size = 20;
    unsigned char *avio_ctx_buffer = av_malloc(avio_ctx_buffer_size);
    if (!avio_ctx_buffer) {
        av_log(NULL, AV_LOG_WARNING, "Could not allocate memory avio_ctx_buffer_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }

    return avio_alloc_context(avio_ctx_buffer, avio_ctx_buffer_size, 1, conn->mem, NULL, write_packet, NULL);
}

void pool_free_mem_context(AVIOContext **out, int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_free_mem_context. conn_nr: %d\n", conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);

    if (conn->mem != NULL) {
        av_free((*out)->buffer);
        avio_context_free(out);
        av_free(conn->mem->buf);
        av_free(conn->mem);
        conn->mem = NULL;
    }
}

void pool_init() {
    chunk_write_time_stats = init_stats("chunk_write_time", 5 * 1000000);
    conn_count_stats = init_stats("nr_of_connections", 5 * 1000000);
}
