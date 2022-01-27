#include "dashenc_http.h"

#include "config.h"
#include "libavutil/avutil.h"
#include <pthread.h>

#include "avio_internal.h"
#include "utils.c"
#include "dashenc_pool.h"
#include "dashenc_stats.h"
#if CONFIG_HTTP_PROTOCOL
#include "http.h"
#endif

typedef struct buffer_data {
    uint8_t *buf;
    size_t size;   /* Current size of the buffer */
    uint8_t *ptr;
    size_t room;   /* Size left in the buffer */
} buffer_data;

typedef struct chunk {
    unsigned char *buf;
    int size;
    int nr;
} chunk;

typedef struct connection {
    int nr;                  /* Number of the connection, used to lookup this connection in the connections list */
    AVIOContext *out;        /* The TCP connection */
    int claimed;             /* This connection is claimed for a specific request */
    int opened;              /* TCP connection (out) is opened */
    int opened_error;        /* If 1 the connection could not be opened */
    int64_t release_time;    /* Time the last request of the connection has finished */
    AVFormatContext *s;      /* Used to clean up the TCP connection if closing of a request fails */
    pthread_t w_thread;      /* Thread that is used to write the chunks */
    struct connection *next; /* Pointer to the next connection in the list */
    struct connection *prev; /* Pointer to the previous connection in the list */

    //Request data that is not cleaned between requests
    pthread_mutex_t chunks_mutex;
    pthread_cond_t chunks_mutex_cv;

    //Request specific data
    int must_succeed;       /* If 1 the request must succeed, otherwise we'll crash the program */
    int retry;              /* If 1 the request can be retried */
    int retry_nr;           /* Current retry number, used to limit the nr of retries */
    char *url;              /* url of the current request */
    AVDictionary *options;
    int http_persistent;
    chunk **chunks_ptr;     /* An array with pointers to chunks */
    int nr_of_chunks;       /* Nr of chunks available, guarded by chunks_mutex */
    int chunks_done;        /* Are all chunks for this request available in the buffer */
    int last_chunk_written; /* Last chunk number that has been written */
    buffer_data *mem;       /* Optional buffer to hold file content that will be written */
} connection;

static connection *connections = NULL;  /* an array with pointers to connections */
static connection *connections_tail = NULL;
static int nr_of_connections = 0;
static int total_nr_of_connections = 0; /* nr of connections made in total */
static pthread_mutex_t connections_mutex = PTHREAD_MUTEX_INITIALIZER;
static stats *chunk_write_time_stats;
static stats *conn_count_stats;

static int should_stop_var;
static pthread_mutex_t stop_mutex = PTHREAD_MUTEX_INITIALIZER;;

//defined here because it has a circular dependency with retry()
static void *thr_io_close(connection *conn);

static int should_stop(void) {
    int ret = 0;
    pthread_mutex_lock(&stop_mutex);
    ret = should_stop_var;
    pthread_mutex_unlock(&stop_mutex);

    return ret;
}

/* This method expects the lock to be already done.*/
static void release_request(connection *conn) {
    int64_t release_time = av_gettime() / 1000;
    av_log(NULL, AV_LOG_DEBUG, "release_request conn_nr: %d.\n", conn->nr);

    if (conn->claimed) {
        free(conn->url);
        pthread_mutex_lock(&conn->chunks_mutex);
        for(int i = 0; i < conn->nr_of_chunks; i++) {
            chunk *chunk = conn->chunks_ptr[i];
            free(chunk->buf);
            free(chunk);
        }
        av_freep(&conn->chunks_ptr);
        av_dict_free(&conn->options);
        pthread_mutex_unlock(&conn->chunks_mutex);
    }
    conn->claimed = 0;
    conn->release_time = release_time;
    conn->nr_of_chunks = 0;
    conn->chunks_done = 0;
    conn->retry_nr = 0;
    conn->opened_error = 0;
    conn->last_chunk_written = 0;
}

static void abort_if_needed(int mustSucceed) {
    if (mustSucceed) {
        av_log(NULL, AV_LOG_ERROR, "Abort because request needs to succeed and it did not.\n");
        abort();
    }
}



static void write_chunk(connection *conn, int chunk_nr) {
    int64_t start_time_ms;
    int64_t write_time_ms;
    int64_t flush_time_ms;
    int64_t after_write_time_ms;
    chunk *chunk;

    pthread_mutex_lock(&conn->chunks_mutex);
    chunk = conn->chunks_ptr[chunk_nr];
    pthread_mutex_unlock(&conn->chunks_mutex);

    if (!conn->out) {
        av_log(NULL, AV_LOG_WARNING, "Connection not open so skip avio_write. Chunk_nr: %d, conn_nr: %d, url: %s\n", chunk_nr, conn->nr , conn->url);
        return;
    }

    start_time_ms = av_gettime() / 1000;
    // if (chunk_nr > 282 || chunk->size > 50000) {
    //     av_log(NULL, AV_LOG_WARNING, "chunk issue? chunk_nr: %d, conn_nr: %d, size: %d\n", chunk_nr, conn->nr, chunk->size);
    // }
    if (chunk_nr == 0) {
        av_log(NULL, AV_LOG_INFO, "first chunk chunk_nr: %d, conn_nr: %d, size: %d\n", chunk_nr, conn->nr, chunk->size);
    }

    if (chunk_nr != chunk->nr) {
        av_log(NULL, AV_LOG_ERROR, "chunk issue! chunk_nr: %d, conn_nr: %d, size: %d\n", chunk_nr, conn->nr, chunk->size);
    }

    avio_write(conn->out, chunk->buf, chunk->size);
    after_write_time_ms = av_gettime() / 1000;
    write_time_ms = after_write_time_ms - start_time_ms;
    if (write_time_ms > 100) {
        av_log(NULL, AV_LOG_WARNING, "It took %"PRId64"(ms) to write chunk %d. conn_nr: %d\n", write_time_ms, chunk_nr, conn->nr);
    }

    avio_flush(conn->out);
    flush_time_ms = av_gettime() / 1000 - after_write_time_ms;
    if (flush_time_ms > 100) {
        av_log(NULL, AV_LOG_WARNING, "It took %"PRId64"(ms) to flush chunk %d. conn_nr: %d\n", flush_time_ms, chunk_nr, conn->nr);
    }

    print_complete_stats(chunk_write_time_stats, av_gettime() / 1000 - start_time_ms);
    print_complete_stats(conn_count_stats, nr_of_connections);
}

static connection *get_conn(int conn_nr) {
    connection *conn;

    pthread_mutex_lock(&connections_mutex);
    conn = connections;

    while (conn) {
        if (conn->nr == conn_nr) {
            pthread_mutex_unlock(&connections_mutex);
            return conn;
        }

        conn = conn->next;
    }

    av_log(NULL, AV_LOG_ERROR, "connection %d not found.\n", conn_nr);
    av_log(NULL, AV_LOG_ERROR, "First conn_nr: %d.\n", connections->nr);
    abort();
}

static int io_open_for_retry(connection *conn) {
    URLContext *http_url_context;
    int ret;
    AVFormatContext *s = conn->s;

    if (!conn->opened) {
        av_log(s, AV_LOG_INFO, "Connection for retry: %d not yet open. conn_nr: %d, url: %s\n", conn->retry_nr, conn->nr, conn->url);

        ret = s->io_open(s, &(conn->out), conn->url, AVIO_FLAG_WRITE, &conn->options);
        if (ret < 0) {
            av_log(s, AV_LOG_WARNING, "io_open_for_retry %d could not open url: %s\n", conn->retry_nr, conn->url);
            conn->opened_error = 1;
            return ret;
        }

        pthread_mutex_lock(&connections_mutex);
        conn->opened = 1;
        pthread_mutex_unlock(&connections_mutex);
        return 0;
    }

    http_url_context = ffio_geturlcontext(conn->out);
    av_assert0(http_url_context);

    ret = ff_http_do_new_request(http_url_context, conn->url);
    if (ret != 0) {
        int64_t curr_time_ms = av_gettime() / 1000;
        int64_t idle_tims_ms = curr_time_ms - conn->release_time;
        av_log(s, AV_LOG_WARNING, "io_open_for_retry error conn_nr: %d, idle_time: %"PRId64", error: %d, retry_nr: %d, url: %s\n", conn->nr, idle_tims_ms, ret, conn->retry_nr, conn->url);
        ff_format_io_close(s, &conn->out);
        return ret;
    }
    return 0;
}

/**
 * This will retry a previously failed request.
 * We assume this method is ran from one of our own threads so we can safely use usleep.
 */
static void retry(connection *conn) {
    int chunk_wait_timeout = 10;
    int ret = 0;

    if (conn->retry_nr > 10) {
        av_log(NULL, AV_LOG_WARNING, "-event- request retry failed. Giving up. request: %s, attempt: %d, conn_nr: %d.\n",
               conn->url, conn->retry_nr, conn->nr);
        return;
    }

    usleep(1 * 1000000);

    av_log(NULL, AV_LOG_INFO, "Request retry waiting for segment to be completely recorded. request: %s, attempt: %d, conn_nr: %d.\n",
        conn->url, conn->retry_nr, conn->nr);

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
    ret = io_open_for_retry(conn);
    if (ret < 0) {
        av_log(NULL, AV_LOG_WARNING, "-event- request retry failed request: %s, ret=%d, attempt: %d, conn_nr: %d.\n",
               conn->url, ret, conn->retry_nr, conn->nr);

        retry(conn);
        return;
    }

    for (int i = 0; i < conn->nr_of_chunks; i++) {
        chunk *chunk;
        pthread_mutex_lock(&conn->chunks_mutex);
        chunk = conn->chunks_ptr[i];
        pthread_mutex_unlock(&conn->chunks_mutex);

        write_chunk(conn, chunk->nr);
    }

    av_log(NULL, AV_LOG_INFO, "request retry done, start reading response. Request: %s, conn_nr: %d, attempt: %d.\n", conn->url, conn->nr, conn->retry_nr);
    thr_io_close(conn);
}

static void remove_from_list(connection *conn) {
    connection *prev = conn->prev;
    connection *next = conn->next;

    if (conn == connections) {
        av_log(NULL, AV_LOG_INFO, "Removing conn_nr: %d, resetting next->prev: %d, tail: %d \n", conn->nr, next->nr, connections_tail->nr);
        next->prev = NULL;
        connections = next;
        return;
    }

    if (next != NULL) {
        av_log(NULL, AV_LOG_INFO, "Removing conn_nr: %d, set prev->next: %d and next->prev: %d, tail: %d\n", conn->nr, conn->next->nr, prev->nr, connections_tail->nr);
        prev->next = conn->next;
        next->prev = prev;
        return;
    }

    av_log(NULL, AV_LOG_INFO, "Removing conn_nr: %d, resetting prev: %d, tail: %d.\n", conn->nr, prev->nr, connections_tail->nr);
    prev->next = NULL;
    connections_tail = prev;
}

/**
 * Remove a connection from the list and free it's memory.
 * This method expects the connections_mutex to be active.
 */
static void remove_conn(connection *conn) {
    remove_from_list(conn);

    av_log(NULL, AV_LOG_INFO, "remove_conn tail: %d\n", connections_tail->nr);
    pthread_mutex_destroy(&conn->chunks_mutex);
    pthread_cond_destroy(&conn->chunks_mutex_cv);
    nr_of_connections--;
    free(conn);
    pthread_mutex_unlock(&connections_mutex);
    pthread_exit(NULL);
}

/**
 * This method closes the request and reads the response.
 */
static void *thr_io_close(connection *conn) {
    int ret;
    int response_code;

    av_log(NULL, AV_LOG_DEBUG, "thr_io_close conn_nr: %d, out_addr: %p \n", conn->nr, conn->out);

    if (conn->opened_error) {
        ret = -1;
        response_code = 0;
    } else {
        URLContext *http_url_context = ffio_geturlcontext(conn->out);
        av_assert0(http_url_context);
        avio_flush(conn->out);

        //av_log(NULL, AV_LOG_INFO, "thr_io_close conn_nr: %d, out_addr: %p \n", conn->nr, conn->out);

        ret = ffurl_shutdown(http_url_context, AVIO_FLAG_WRITE);
        response_code = ff_http_get_code(http_url_context);
    }

    if (ret < 0 || response_code >= 500) {
        av_log(NULL, AV_LOG_INFO, "-event- request failed ret=%d, conn_nr: %d, response_code: %d, url: %s.\n", ret, conn->nr, response_code, conn->url);
        abort_if_needed(conn->must_succeed);
        //must check if this is NULL or it causes crash on segmentation fault due to NULL pointer
        //check why conn->s (AVFormatContext) becomes NULL
        if (conn->s != NULL) {
            ff_format_io_close(conn->s, &conn->out);
        }
        pthread_mutex_lock(&connections_mutex);
        conn->opened = 0;
        pthread_mutex_unlock(&connections_mutex);

        if (conn->retry)
            retry(conn);
    }

    pthread_mutex_lock(&connections_mutex);
    release_request(conn);
    if (conn->opened == 0 || should_stop()) {
        remove_conn(conn);
        //The thread will be stopped at this point
    }
    pthread_mutex_unlock(&connections_mutex);

    return NULL;
}

static int chunk_available(connection *conn) {
    int ret;

    pthread_mutex_lock(&conn->chunks_mutex);
    ret = conn->last_chunk_written < conn->nr_of_chunks;
    pthread_mutex_unlock(&conn->chunks_mutex);

    return ret;
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
        pthread_mutex_lock(&conn->chunks_mutex);

        while (conn->last_chunk_written >= conn->nr_of_chunks && !conn->chunks_done) {
            pthread_cond_wait(&conn->chunks_mutex_cv, &conn->chunks_mutex);
        }

        pthread_mutex_unlock(&conn->chunks_mutex);

        while (chunk_available(conn)) {
            write_chunk(conn, conn->last_chunk_written);

            pthread_mutex_lock(&conn->chunks_mutex);
            conn->last_chunk_written++;
            pthread_mutex_unlock(&conn->chunks_mutex);
        }

        if (conn->chunks_done) {
            thr_io_close(conn);
            // after this no other action should be done on conn until a new request is started so make sure there are no statements below this.
            continue;
        }
    }

    av_log(NULL, AV_LOG_INFO, "dashenc_http thread done, conn_nr: %d.\n", conn_nr);
    return NULL;
}

static void free_idle_connections(int count, int limit) {
    connection *conn_l = connections;
    connection *next;
    while (conn_l && count > limit) {
        next = conn_l->next;
        if (!conn_l->claimed) {
            remove_conn(conn_l);
            count--;
        }
        conn_l = next;
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
    connection *conn_l = connections;
    size_t len;

    if (url == NULL) {
        av_log(NULL, AV_LOG_INFO, "Claimed conn_id: -1, url: NULL\n");
        return NULL;
    }

    pthread_mutex_lock(&connections_mutex);

    while (conn_l) {
        if (!conn_l->claimed) {
            if ((conn_nr == -1) || (conn->release_time != 0 && conn_l->release_time < lowest_release_time)) {
                conn_nr = conn_l->nr;
                conn = conn_l;
                lowest_release_time = conn->release_time;
            }
            conn_idle_count++;
        }
        conn_l = conn_l->next;
    }

    if (conn_nr == -1) {
        pthread_attr_t  attr;
        conn = calloc(1, sizeof(*conn));
        if (conn == NULL) {
            pthread_mutex_unlock(&connections_mutex);
            return conn;
        }
        //TODO: In theory when we have a rollover of total_nr_of_connections we could claim a connection number that is still in use.
        conn_nr = total_nr_of_connections;
        conn->last_chunk_written = 0;
        conn->nr_of_chunks = 0;
        conn->nr = conn_nr;
        conn->mem = NULL;
        nr_of_connections++;
        total_nr_of_connections++;
        pthread_mutex_init(&conn->chunks_mutex, NULL);
        pthread_cond_init(&conn->chunks_mutex_cv, NULL);

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

        if (connections_tail == NULL) {
            av_log(NULL, AV_LOG_INFO, "Creating first connection, conn_nr: %d\n", conn_nr);
            conn->next = NULL;
            conn->prev = NULL;
            connections = conn;
            connections_tail = conn;
        } else {
            av_log(NULL, AV_LOG_INFO, "adding connection, conn_nr: %d, prev: %d\n", conn_nr, connections_tail->nr);
            conn->prev = connections_tail;
            conn->next = NULL;
            connections_tail->next = conn;
            connections_tail = conn;
        }

        av_log(NULL, AV_LOG_INFO, "No free connections so added one. Url: %s, tail: %d\n", url, connections_tail->nr);
    }

    if (need_new_connection && conn->opened) {
        conn->opened = 0;
        ff_format_io_close(conn->s, &conn->out);
    }
    av_log(NULL, AV_LOG_DEBUG, "Claimed conn_id: %d, url: %s\n", conn_nr, url);
    len = strlen(url) + 1;
    conn->url = malloc(len);
    av_strlcpy(conn->url, url, len);
    conn->claimed = 1;
    conn->nr = conn_nr;

    if(conn_idle_count > 15){
        free_idle_connections(conn_idle_count, 15);
    }

    pthread_mutex_unlock(&connections_mutex);

    return conn;
}

/**
 * Opens a request on a free connection and returns the connection number
 */
static int open_request(AVFormatContext *s, char *url, AVDictionary **options) {
    int ret;
    connection *conn = claim_connection(url, 0);

    if (conn->opened)
        av_log(s, AV_LOG_WARNING, "open_request while connection might be open. This is TODO for when not using persistent connections. conn_nr: %d\n", conn->nr);

    ret = s->io_open(s, &conn->out, url, AVIO_FLAG_WRITE, options);
    if (ret >= 0) {
        ret = conn->nr;
        conn->opened = 1;
    }

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
        URLContext *http_url_context;
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
        if (!conn->opened) {
            av_log(s, AV_LOG_INFO, "Connection(%d) not yet open %s\n", conn->nr, filename);
            ret = s->io_open(s, &(conn->out), filename, AVIO_FLAG_WRITE, options);
            if (ret < 0) {
                av_log(s, AV_LOG_WARNING, "Could not open %s\n", filename);
                abort_if_needed(must_succeed);
                conn->opened_error = 1;
                if (!conn->retry) {
                    return ret;
                }
                return conn->nr;
            }

            pthread_mutex_lock(&connections_mutex);
            conn->opened = 1;
            pthread_mutex_unlock(&connections_mutex);
            return conn->nr;
        }

        http_url_context = ffio_geturlcontext(conn->out);
        av_assert0(http_url_context);

        ret = ff_http_do_new_request(http_url_context, filename);
        if (ret != 0) {
            int64_t curr_time_ms = av_gettime() / 1000;
            int64_t idle_tims_ms = curr_time_ms - conn->release_time;
            av_log(s, AV_LOG_WARNING, "pool_io_open error conn_nr: %d, idle_time: %"PRId64", error: %d, name: %s\n", conn->nr, idle_tims_ms, ret, filename);
            ff_format_io_close(s, &conn->out);
            abort_if_needed(must_succeed);
            conn->opened_error = 1;
            if (!conn->retry) {
                return ret;
            }
            return conn->nr;
        }
        ret = conn->nr;
#endif
    }

    return ret;
}

/**
 * Closes the request.
 */
void pool_io_close(AVFormatContext *s, char *filename, int conn_nr) {
    connection *conn;

    if (conn_nr < 0) {
        av_log(s, AV_LOG_WARNING, "Invalid conn_nr in pool_io_close for filename: %s, conn_nr: %d\n", filename, conn_nr);
        return;
    }

    conn = get_conn(conn_nr);
    av_log(NULL, AV_LOG_INFO, "pool_io_close conn_nr: %d, nr_of_chunks: %d\n", conn_nr, conn->nr_of_chunks);

    if (!conn->opened && !conn->opened_error) {
        av_log(s, AV_LOG_INFO, "Skip closing HTTP request because connection is not opened. conn_nr: %d, filename: %s\n", conn_nr, filename);
        abort_if_needed(conn->must_succeed);
        return;
    }

    conn->chunks_done = 1;

    pthread_mutex_lock(&conn->chunks_mutex);
    pthread_cond_signal(&conn->chunks_mutex_cv);
    pthread_mutex_unlock(&conn->chunks_mutex);
}

static void pool_free(AVFormatContext *s, int conn_nr) {
    connection *conn;

    if (conn_nr < 0) {
        av_log(s, AV_LOG_WARNING, "Invalid conn_nr in pool_free. conn_nr: %d\n", conn_nr);
        return;
    }

    conn = get_conn(conn_nr);
    av_log(NULL, AV_LOG_DEBUG, "pool_free conn_nr: %d\n", conn_nr);

    ff_format_io_close(s, &conn->out);

    pthread_mutex_lock(&connections_mutex);
    conn->opened = 0;
    release_request(conn);
    pthread_mutex_unlock(&connections_mutex);
}

void pool_free_all(AVFormatContext *s) {
    connection *conn = connections;
    int all_conns_done = 0;

    av_log(NULL, AV_LOG_INFO, "pool_free_all\n");


    // Signal the connections to close
    pthread_mutex_lock(&stop_mutex);
    should_stop_var = 1;
    pthread_mutex_unlock(&stop_mutex);

    av_log(NULL, AV_LOG_INFO, "pool_free_all waiting for requests to finish\n");

    while (!all_conns_done) {
        pthread_mutex_lock(&connections_mutex);
        all_conns_done = 1;
        while (conn) {
            if (conn->claimed) {
                all_conns_done = 0;
            }
            conn = conn->next;
        }
        pthread_mutex_unlock(&connections_mutex);

        usleep(10000);
    }

//        pool_io_close(s, "fake", conn->nr);

    av_log(NULL, AV_LOG_INFO, "pool_free_all free memory\n");
    while (conn) {
        //close conn->out
        if (conn->out)
            pool_free(s, conn->nr);

        conn = conn->next;
    }
}


void pool_write_flush_mem(int conn_nr) {
    connection *conn;
    int read_size;

    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_write_flush_mem. conn_nr: %d\n", conn_nr);
        return;
    }

    conn = get_conn(conn_nr);

    read_size = conn->mem->ptr - conn->mem->buf;

    pool_write_flush(conn->mem->buf, read_size, conn_nr);
}

void pool_write_flush(const unsigned char *buf, int size, int conn_nr) {
    connection *conn;
    chunk *chunk_ptr;

    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_write_flush. conn_nr: %d\n", conn_nr);
        return;
    }

    conn = get_conn(conn_nr);

    if (!conn->opened && !conn->opened_error) {
        av_log(NULL, AV_LOG_WARNING, "connection closed in pool_write_flush. conn_nr: %d, url: %s\n", conn_nr, conn->url);
        return;
    }

    //Save the chunk in memory
    chunk_ptr = malloc(sizeof(chunk));
    chunk_ptr->size = size;
    chunk_ptr->nr = conn->nr_of_chunks;
    chunk_ptr->buf = malloc(size);
    if (chunk_ptr->buf == NULL) {
        av_log(NULL, AV_LOG_WARNING, "Could not malloc pool_write_flush.\n");
        return;
    }
    memcpy(chunk_ptr->buf, buf, size);

    pthread_mutex_lock(&conn->chunks_mutex);
    av_dynarray_add(&conn->chunks_ptr, &conn->nr_of_chunks, chunk_ptr);
    pthread_mutex_unlock(&conn->chunks_mutex);

    if (conn->opened_error) {
        return;
    }

    pthread_mutex_lock(&conn->chunks_mutex);
    pthread_cond_signal(&conn->chunks_mutex_cv);
    pthread_mutex_unlock(&conn->chunks_mutex);
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
    connection *conn;
    unsigned char *avio_ctx_buffer;
    const size_t bd_buf_size = 10;
    size_t avio_ctx_buffer_size = 20;

    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_create_mem_context. conn_nr: %d\n", conn_nr);
        return NULL;
    }
    conn = get_conn(conn_nr);

    conn->mem = av_malloc(sizeof(buffer_data));
    conn->mem->room = 0;
    conn->mem->size = 0;
    conn->mem->ptr = conn->mem->buf = av_malloc(bd_buf_size);
    if (!conn->mem->buf) {
        av_log(NULL, AV_LOG_WARNING, "Could not allocate memory bd_buf_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }
    conn->mem->size = conn->mem->room = bd_buf_size;
    avio_ctx_buffer = av_malloc(avio_ctx_buffer_size);
    if (!avio_ctx_buffer) {
        av_log(NULL, AV_LOG_WARNING, "Could not allocate memory avio_ctx_buffer_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }
    
    return avio_alloc_context(avio_ctx_buffer, avio_ctx_buffer_size, 1, conn->mem, NULL, write_packet, NULL);
}

void pool_free_mem_context(AVIOContext **out, int conn_nr) {
     connection *conn;

    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "Invalid conn_nr in pool_free_mem_context. conn_nr: %d\n", conn_nr);
        return;
    }
    conn = get_conn(conn_nr);

    if (conn->mem != NULL) {
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
