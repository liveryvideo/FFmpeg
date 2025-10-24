/*
 * Manages connections and HTTP requests.
 *
 * Things to keep in mind.
 * s->io_open in this file will open the TCP connection and start the HTTP request.
 * There is some code in here that is also considering file based output, this is currently not used in production.
 *
 */
#include "dashenc_http.h"

#include <errno.h> /* NOLINT(misc-include-cleaner) */ /* Used for ENOMEM in AVERROR() macro */
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sys/queue.h>

#include <libavformat/avformat.h>
#include <libavformat/avio.h>
#include <libavformat/internal.h>
#include <libavformat/url.h>
#include <libavutil/avstring.h>
#include <libavutil/dict.h>
#include <libavutil/error.h>
#include <libavutil/log.h>
#include <libavutil/mem.h>
#include <libavutil/time.h>

#include "avio_internal.h"
#include "common.h"
#include "config_components.h"
#include "stats.h"
#if CONFIG_HTTP_PROTOCOL
#include "http.h"
#endif

/* clang-tidy complains about pthread types, because they're not included directly, but this is intended */
#define pthread_mutex_t /* NOLINT(misc-include-cleaner) */ pthread_mutex_t
#define pthread_t /* NOLINT(misc-include-cleaner) */ pthread_t
#define pthread_cond_t /* NOLINT(misc-include-cleaner) */ pthread_cond_t
#define pthread_attr_t /* NOLINT(misc-include-cleaner) */ pthread_attr_t

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

    _Atomic bool req_opened;         /* If true the request is opened and more data can be written to it, only accessed from w_thread */
    _Atomic bool opened;     /* TCP connection (out) is opened */
    _Atomic bool open_error; /* If true the connection could not be opened */
    pthread_mutex_t open_mutex;

    _Atomic int64_t release_time;    /* Time the last request of the connection has finished */
    AVFormatContext *s;      /* Used to clean up the TCP connection if closing of a request fails */
    pthread_t w_thread;      /* Thread that is used to write the chunks */
    LIST_ENTRY(connection) entries;

    ChunksStorage chunks;               /* A queue with pointers to chunks */
    _Atomic bool chunks_done;        /* Are all chunks for this request available in the buffer */
    _Atomic bool write_error;        /* Write error occurred, need to close and retry */

    //Request specific data
    int must_succeed;       /* If 1 the request must succeed, otherwise we'll crash the program */
    int retry_nr;           /* Current retry number, used to limit the nr of retries */
    char *url;              /* url of the current request */
    AVDictionary *options;
    int http_persistent;
    _Atomic bool cleanup_requested;  /* This conn should be deleted, can be caused by too many idle connections */
    buffer_data *mem;       /* Optional buffer to hold file content that will be written */
    int64_t request_start_time;     /* Time when the request was first opened (in ms), used for logging request duration */
} connection;

/* If there will be to may connections, this should be replaced with hashtable */
static LIST_HEAD(connections_head, connection) connections = LIST_HEAD_INITIALIZER(connections);

static pthread_mutex_t connections_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t connections_thread_exit_cv = PTHREAD_COND_INITIALIZER;

const static int max_idle_connections = 15;

static stats *chunk_write_time_stats;
static stats *conn_count_stats;
static _Atomic bool should_stop = false;

enum {
    kConnectionNumbersBitmapSize = 1 << 8,
    kRetrySleepInterval = 100 * kOneMillisecond,
};

static int nr_of_connections = 0;
static pthread_mutex_t connection_numbers_bitmap_mutex = PTHREAD_MUTEX_INITIALIZER;
typedef uint32_t ConnectionNumbersBitmapType;
static ConnectionNumbersBitmapType connection_numbers_bitmap[kConnectionNumbersBitmapSize];

#include <strings.h>
#define ffz(x) ffs(~(x))

static int get_connection_number(void) {
    pthread_mutex_lock(&connection_numbers_bitmap_mutex);
    for (int i = 0; i < kConnectionNumbersBitmapSize; i++) {
        if (connection_numbers_bitmap[i] != ~0U) {
            const int bit = ffz(connection_numbers_bitmap[i]) - 1; /* ffs return bit numbers in range 1-32 */
            if (bit != -1) {
                connection_numbers_bitmap[i] |= (1U << bit);
                nr_of_connections++;
                pthread_mutex_unlock(&connection_numbers_bitmap_mutex);

                const int connection_number = (i << sizeof(ConnectionNumbersBitmapType)) + bit;
                av_log(NULL, AV_LOG_INFO, "[dashenc_http] Claim connection id %d\n", connection_number);
                return connection_number;
            }
        }
    }

    pthread_mutex_unlock(&connection_numbers_bitmap_mutex);
    return -1;
}

static void free_connection_number(const int connection_number) {
    if (connection_number < 0 || connection_number > (kConnectionNumbersBitmapSize << sizeof(ConnectionNumbersBitmapType))) {
        av_log(NULL, AV_LOG_ERROR, "[dashenc_http] Trying to release invalid connection number: %d", connection_number);
        return;
    }

    av_log(NULL, AV_LOG_INFO, "[dashenc_http] Release connection id %d\n", connection_number);
    pthread_mutex_lock(&connection_numbers_bitmap_mutex);
    const int index = connection_number >> sizeof(ConnectionNumbersBitmapType);
    const int bit = connection_number - (index << sizeof(ConnectionNumbersBitmapType));
    connection_numbers_bitmap[index] &= ~(1U << bit);
    nr_of_connections--;
    pthread_mutex_unlock(&connection_numbers_bitmap_mutex);
}

static void print_nr_connections_stats(void) {
    pthread_mutex_lock(&connection_numbers_bitmap_mutex);
    print_complete_stats(conn_count_stats, nr_of_connections);
    pthread_mutex_unlock(&connection_numbers_bitmap_mutex);
}

//defined here because it has a circular dependency with retry()
static void *thr_io_close(connection *conn);

/* This method expects the lock to be already done.*/
static void release_request(connection *conn) {
    const int64_t release_time = US_TO_MS(av_gettime());
    av_log(NULL, AV_LOG_INFO, "[dashenc_http] release_request conn_nr: %d, opened: %d (now available for reuse)\n", 
           conn->nr, (int)conn->opened);

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
    av_freep((void*)&conn->chunks.storage);
    conn->chunks.nr_of_chunks = 0;
    conn->chunks.last_chunk_written = 0;
    pthread_mutex_unlock(&conn->chunks.mutex);

    conn->chunks_done = false;
    conn->write_error = false;
    conn->claimed = false;
    conn->release_time = release_time;
    conn->retry_nr = 0;
    conn->open_error = false;
    conn->request_start_time = 0;
}

static void abort_if_needed(const int mustSucceed) {
    if (mustSucceed) {
        av_log(NULL, AV_LOG_ERROR, "[dashenc_http] Abort because request needs to succeed and it did not.\n");
        abort();
    }
}

static inline bool chunk_is_available(ChunksStorage *chunks) {
    return chunks->last_chunk_written < chunks->nr_of_chunks;
}

enum {
    kWarningTreshold = 100
};

/* returns true if a chunk was written */
static bool write_chunk_if_available(connection *conn) {
    int64_t start_time_ms = 0;
    int64_t write_time_ms = 0;
    int64_t flush_time_ms = 0;
    int64_t after_write_time_ms = 0;

    if (!conn->out) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Connection not open so skip avio_write. Conn_nr: %d, url: %s\n", conn->nr , conn->url);
        return false;
    }

    pthread_mutex_lock(&conn->chunks.mutex);
    if (!chunk_is_available(&conn->chunks)) {
        pthread_mutex_unlock(&conn->chunks.mutex);
        return false;
    }

    Chunk *chunk = conn->chunks.storage[conn->chunks.last_chunk_written++];
    pthread_mutex_unlock(&conn->chunks.mutex);


    start_time_ms = US_TO_MS(av_gettime());
    avio_write(conn->out, chunk->buf, chunk->size);
    after_write_time_ms = US_TO_MS(av_gettime());
    write_time_ms = after_write_time_ms - start_time_ms;
    if (write_time_ms > kWarningTreshold) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] It took %"PRId64"(ms) to write chunk. conn_nr: %d\n", write_time_ms, conn->nr);
    }

    avio_flush(conn->out);
    flush_time_ms = US_TO_MS(av_gettime()) - after_write_time_ms;
    if (flush_time_ms > kWarningTreshold) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] It took %"PRId64"(ms) to flush chunk. conn_nr: %d\n", flush_time_ms, conn->nr);
    }

    // Check for errors after write/flush (e.g., 401 authentication errors detected during write)
    // avio_write/avio_flush are void functions, errors are stored in the context
    if (conn->out->error < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Chunk write/flush failed: %s, conn_nr: %d, will trigger early close for retry\n", 
               av_err2str(conn->out->error), conn->nr);
        // Set write_error flag to signal that we need to close the request and retry
        conn->write_error = true;
        pthread_mutex_lock(&conn->chunks.mutex);
        pthread_cond_signal(&conn->chunks.cv);
        pthread_mutex_unlock(&conn->chunks.mutex);
        return false;
    }

    print_complete_stats(chunk_write_time_stats, US_TO_MS(av_gettime()) - start_time_ms);
    print_nr_connections_stats();

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
    if (conn == NULL || conn->nr != conn_nr) {
        av_log(NULL, AV_LOG_FATAL, "[dashenc_http] connection %d not found. Aborting...\n", conn_nr);
        if (LIST_FIRST(&connections) != NULL) {
            av_log(NULL, AV_LOG_FATAL, "[dashenc_http] First conn_nr: %d.\n", LIST_FIRST(&connections)->nr);
        } else {
            av_log(NULL, AV_LOG_FATAL, "[dashenc_http] Connections list empty.\n");
        }
        abort();
    }
    return conn;
}

// Open connection if not open and start doing the request
static int io_open_for_retry(connection *conn) {
    int ret = 0;
    URLContext *http_url_context = NULL;
    AVFormatContext *ctx = conn->s;
    AVDictionary *options_copy = NULL;

    pthread_mutex_lock(&conn->open_mutex);
    if (!conn->opened) {
        av_log(ctx, AV_LOG_INFO, "[dashenc_http] Connection for retry: %d not yet open. conn_nr: %d, url: %s\n", conn->retry_nr, conn->nr, conn->url);

        /* Copy options to avoid them being consumed by io_open, so they're preserved for future retries */
        ret = av_dict_copy(&options_copy, conn->options, 0);
        if (ret < 0) {
            av_log(ctx, AV_LOG_WARNING, "[dashenc_http] io_open_for_retry %d could not copy options for url: %s\n", conn->retry_nr, conn->url);
            goto error;
        }

        ret = ctx->io_open(ctx, &(conn->out), conn->url, AVIO_FLAG_WRITE, &options_copy);
        av_dict_free(&options_copy);
        if (ret < 0) {
            av_log(ctx, AV_LOG_WARNING, "[dashenc_http] io_open_for_retry %d could not open url: %s\n", conn->retry_nr, conn->url);
            goto error;
        }

        conn->opened = true;
        pthread_mutex_unlock(&conn->open_mutex);
        return ret;
    }
    pthread_mutex_unlock(&conn->open_mutex);

    http_url_context = ffio_geturlcontext(conn->out);
    if (http_url_context == NULL) {
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] Failed to get url context");
        goto error_close;
    }

    av_log(ctx, AV_LOG_INFO, "[dashenc_http] io_open_for_retry calling ff_http_do_new_request, conn_nr: %d, req_opened: %d\n", 
           conn->nr, conn->req_opened);
    
    ret = ff_http_do_new_request(http_url_context, conn->url);
    if (ret != 0) {
        const int64_t curr_time_ms = US_TO_MS(av_gettime());
        const int64_t idle_tims_ms = curr_time_ms - conn->release_time;
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] io_open_for_retry error conn_nr: %d, idle_time: %"PRId64", error: %s, retry_nr: %d, url: %s\n", conn->nr, idle_tims_ms, av_err2str(ret), conn->retry_nr, conn->url);
        goto error_close;
    }
    
    av_log(ctx, AV_LOG_INFO, "[dashenc_http] io_open_for_retry ff_http_do_new_request succeeded, conn_nr: %d\n", conn->nr);

    return ret;

error_close:
    pthread_mutex_lock(&conn->open_mutex);
    ff_format_io_close(ctx, &conn->out);

error:
    conn->open_error = true;
    pthread_mutex_unlock(&conn->open_mutex);
    return ret;
}

enum {
    kRetryCount = 10,
    kServerErrorsStart = 500,
    kUnauthorized = 401
};

/**
 * This will retry a previously failed request.
 * We assume this method is ran from one of our own threads so we can safely use usleep.
 * This method retries immediately with buffered chunks and continues to receive new chunks.
 * Returns true if the retry succeeded and request is still ongoing (more chunks expected).
 * Returns false if retry failed or should release the request.
 */
static bool retry(connection *conn) { /* NOLINT(misc-no-recursion) */
    if (conn->retry_nr > kRetryCount) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] -event- request retry failed. Giving up. request: %s, attempt: %d, conn_nr: %d.\n",
                conn->url, conn->retry_nr, conn->nr);
        return false;
    }

    av_usleep(kRetrySleepInterval);

    conn->retry_nr = conn->retry_nr + 1;

    pthread_mutex_lock(&conn->chunks.mutex);
    const int buffered_chunks = conn->chunks.nr_of_chunks;
    const bool all_chunks_available = conn->chunks_done;
    pthread_mutex_unlock(&conn->chunks.mutex);

    av_log(NULL, AV_LOG_INFO, "[dashenc_http] Starting immediate retry for request %s, attempt: %d, conn_nr: %d (with %d buffered chunks, all_done=%d)\n", 
            conn->url, conn->retry_nr, conn->nr, buffered_chunks, all_chunks_available);
    
    const int ret = io_open_for_retry(conn);
    if (ret < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] -event- request retry failed request: %s, ret=%d, attempt: %d, conn_nr: %d.\n",
                conn->url, ret, conn->retry_nr, conn->nr);
        return retry(conn);
    }
    
    // Clear any previous error state from the AVIOContext so writes can succeed
    if (conn->out && conn->out->error < 0) {
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] Clearing AVIOContext error (%s) before retry. conn_nr: %d\n",
               av_err2str(conn->out->error), conn->nr);
        conn->out->error = 0;
    }
    
    pthread_mutex_lock(&conn->chunks.mutex);
    conn->chunks.last_chunk_written = 0; /* Restart writing chunks from the beginning */
    pthread_mutex_unlock(&conn->chunks.mutex);
    
    conn->req_opened = true; /* Mark request as opened so write thread can continue */

    // Write all buffered chunks immediately
    while (write_chunk_if_available(conn)) {}

    av_log(NULL, AV_LOG_INFO, "[dashenc_http] request retry buffered chunks written. Request: %s, conn_nr: %d, attempt: %d. All chunks available: %d\n", 
            conn->url, conn->nr, conn->retry_nr, all_chunks_available);
    
    // If all chunks were already available, we need to close the request.
    // But we should NOT call thr_io_close() recursively, as it would call release_request() twice.
    // Instead, we'll read the response here and return false to let the caller release the request.
    if (all_chunks_available) {
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] request retry complete, all chunks were already available. Reading response. Request: %s, conn_nr: %d, attempt: %d.\n", 
                conn->url, conn->nr, conn->retry_nr);
        
        int retry_ret = 0;
        int retry_response_code = 0;
        URLContext *http_url_context = ffio_geturlcontext(conn->out);
        
        if (http_url_context != NULL) {
            avio_flush(conn->out);
            retry_ret = ffurl_shutdown(http_url_context, AVIO_FLAG_WRITE);
            retry_response_code = ff_http_get_code(http_url_context);
        } else {
            retry_ret = -1;
        }
        
        if (retry_ret < 0 || retry_response_code >= kServerErrorsStart || retry_response_code == kUnauthorized) {
            av_log(NULL, AV_LOG_WARNING, "[dashenc_http] -event- retry attempt failed after writing all chunks. ret=%d, response_code=%d, conn_nr: %d, url: %s, attempt: %d\n",
                    retry_ret, retry_response_code, conn->nr, conn->url, conn->retry_nr);
            
            pthread_mutex_lock(&conn->open_mutex);
            
            // For 401, keep connection open to preserve auth state but mark request as closed
            // For others, close and reopen the connection
            if (retry_response_code != kUnauthorized) {
                conn->opened = false;
                if (conn->s != NULL) {
                    ff_format_io_close(conn->s, &conn->out);
                }
            } else {
                // For 401: keep TCP open but mark request as closed for next retry
                conn->req_opened = false;
            }
            
            pthread_mutex_unlock(&conn->open_mutex);
            
            // Recursively retry
            return retry(conn);
        }
        
        // Success! Return false to indicate the request should be released
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] Retry successful after writing all chunks. Request: %s, conn_nr: %d, attempt: %d.\n", 
                conn->url, conn->nr, conn->retry_nr);
        return false;
    }
    
    // More chunks are expected, return true so write thread continues
    av_log(NULL, AV_LOG_INFO, "[dashenc_http] request retry ongoing, waiting for more chunks. Request: %s, conn_nr: %d, attempt: %d.\n", 
            conn->url, conn->nr, conn->retry_nr);
    return true;
}

static void remove_from_list(connection *conn) {
    av_log(NULL, AV_LOG_INFO, "[dashenc_http] Removing conn_nr: %d\n", conn->nr);
    LIST_REMOVE(conn, entries);
    free_connection_number(conn->nr);
}

/**
 * Remove a connection from the list and free it's memory.
 * This method expects to be started from the connection thread.
 */
static void connection_exit(connection *conn) {
    av_log(conn->s, AV_LOG_INFO, "[dashenc_http] Removing conn %d\n", conn->nr);
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
static void *thr_io_close(connection *conn) { /* NOLINT(misc-no-recursion) */
    int ret = 0;
    int response_code = 0;

    if (conn->open_error) {
        ret = -1;
        response_code = 0;
    } else {
        URLContext *http_url_context = ffio_geturlcontext(conn->out);
        if (http_url_context == NULL) {
            conn->open_error = true;
            ret = -1;
            response_code = 0;
        } else {
            // Check if there's already an error from writing (e.g., 401 detected during chunk write)
            if (conn->out->error == AVERROR_HTTP_UNAUTHORIZED) {
                av_log(NULL, AV_LOG_INFO, "[dashenc_http] thr_io_close: write error already detected (401), conn_nr=%d\n", conn->nr);
                ret = conn->out->error;
                response_code = kUnauthorized;
            } else {
                avio_flush(conn->out);
                ret = ffurl_shutdown(http_url_context, AVIO_FLAG_WRITE);
                response_code = ff_http_get_code(http_url_context);
            }
            av_log(NULL, AV_LOG_INFO, "[dashenc_http] thr_io_close: ret=%d, response_code=%d, conn_nr=%d, url=%s\n", 
                   ret, response_code, conn->nr, conn->url);
        }
    }

    // Handle errors: 5xx server errors or network failures
    // For 401, we also retry but keep the connection open to preserve auth state
    if (ret < 0 || response_code >= kServerErrorsStart || response_code == kUnauthorized) {
        if (response_code != kUnauthorized) {
            av_log(NULL, AV_LOG_INFO, "[dashenc_http] -event- request failed ret=%d, conn_nr: %d, response_code: %d, url: %s.\n", ret, conn->nr, response_code, conn->url);
            abort_if_needed(conn->must_succeed);
        }
        
        
        pthread_mutex_lock(&conn->open_mutex);
        
        if (response_code == kUnauthorized) {
            // For 401: keep TCP open so auth state is preserved for retry, mark request as closed so next attempt sends new HTTP request
            conn->req_opened = false;
        } else {
            // For other errors, close and reopen the connection
            conn->opened = false;
            //must check if this is NULL or it causes crash on segmentation fault due to NULL pointer
            //check why conn->s (AVFormatContext) becomes NULL
            if (conn->s != NULL) {
                ff_format_io_close(conn->s, &conn->out);
            }
        }
        
        pthread_mutex_unlock(&conn->open_mutex);

        const bool retry_ongoing = retry(conn);
        if (retry_ongoing) {
            // Retry is in progress and more chunks are expected.
            // Return to write thread loop without releasing the request.
            av_log(NULL, AV_LOG_INFO, "[dashenc_http] Retry ongoing, returning to write loop. conn_nr: %d\n", conn->nr);
            return NULL;
        }

        // If retry returned false, either retry completed or failed.
        // Fall through to release_request.
    }

    // Log the HTTP response with duration
    const int64_t end_time_ms = US_TO_MS(av_gettime());
    const int64_t duration_ms = end_time_ms - conn->request_start_time;
    av_log(NULL, AV_LOG_INFO, "[dashenc_http] Final HTTP response: %d, duration: %"PRId64", url: %s\n", 
           response_code, duration_ms, conn->url);

    release_request(conn);
    if (should_stop) {
        connection_exit(conn);
    }

    conn->req_opened = false;
    return NULL;
}

/**
 * Opens the TCP connection if it's not open.
 * Opens the request if it's not open.
 */
static int open_request_if_needed(connection *conn) {
    int ret = 0;
    URLContext *http_url_context = NULL;
    AVDictionary *options_copy = NULL;

    pthread_mutex_lock(&conn->open_mutex);
    if (conn->req_opened) {
        pthread_mutex_unlock(&conn->open_mutex);
        return conn->nr;
    }

    /* Set timer for request duration measurement, but only on first attempt (not on retries) */
    if (conn->retry_nr == 0) {
        conn->request_start_time = US_TO_MS(av_gettime());
    }

    if (!conn->opened) {
        av_log(conn->s, AV_LOG_INFO, "[dashenc_http] connection not yet open, opening TCP and starting req. conn_nr: %d, url: %s\n", conn->nr, conn->url);
        
        /* Copy options to avoid them being consumed by io_open, so they're preserved for future retries */
        ret = av_dict_copy(&options_copy, conn->options, 0);
        if (ret < 0) {
            av_log(conn->s, AV_LOG_WARNING, "[dashenc_http] Could not copy options for %s\n", conn->url);
            goto error;
        }

        ret = conn->s->io_open(conn->s, &(conn->out), conn->url, AVIO_FLAG_WRITE, &options_copy);
        av_dict_free(&options_copy);
        if (ret < 0) {
            av_log(conn->s, AV_LOG_WARNING, "[dashenc_http] Could not open %s\n", conn->url);
            goto error;
        }

        conn->opened = true;
        goto exit_opened;
    }

    http_url_context = ffio_geturlcontext(conn->out);
    if (http_url_context == NULL) {
        av_log(conn->s, AV_LOG_ERROR, "[dashenc_http] Could not get http_url_context!\n");
        goto error_close;
    }

    av_log(conn->s, AV_LOG_INFO, "[dashenc_http] attempting to start new req on existing TCP connection, conn_nr: %d, url: %s\n", conn->nr, conn->url);

    ret = ff_http_do_new_request(http_url_context, conn->url);
    if (ret != 0) {
        const int64_t curr_time_ms = US_TO_MS(av_gettime());
        const int64_t idle_tims_ms = curr_time_ms - conn->release_time;
        av_log(conn->s, AV_LOG_WARNING, "[dashenc_http] pool_io_open error conn_nr: %d, idle_time: %"PRId64", error: %s, name: %s\n", conn->nr, idle_tims_ms, av_err2str(ret), conn->url);
        goto error_close;
    }

exit_opened:
    conn->req_opened = true;
    conn->open_error = false;  /* Clear any error from previous failed attempts */
    pthread_mutex_unlock(&conn->open_mutex);
    return conn->nr;

error_close:
    ff_format_io_close(conn->s, &conn->out);
    conn->opened = false;

error:
    abort_if_needed(conn->must_succeed);
    conn->open_error = true;
    conn->req_opened = false;
    pthread_mutex_unlock(&conn->open_mutex);
    return conn->nr;
}

/**
 * This method writes the chunks.
 * It is supposed to be passed to pthread_create.
 */
static void *thr_io_write(void *arg) { /* NOLINT(readability-function-cognitive-complexity) */
    int ret = 0;
    connection *conn = (connection *)arg;
    //https://computing.llnl.gov/tutorials/pthreads/#ConditionVariables

    for (;;) {
        pthread_mutex_lock(&conn->chunks.mutex);
        while ((!chunk_is_available(&conn->chunks) && !conn->chunks_done && !conn->write_error) || !conn->claimed) {
            pthread_cond_wait(&conn->chunks.cv, &conn->chunks.mutex);
            if (conn->cleanup_requested || should_stop) {
                pthread_mutex_unlock(&conn->chunks.mutex);
                release_request(conn);
                connection_exit(conn);
            }
        }
        const bool chunks_done = conn->chunks_done;
        const bool has_chunks = chunk_is_available(&conn->chunks);
        const bool write_error = conn->write_error;
        pthread_mutex_unlock(&conn->chunks.mutex);

        // If write error occurred, close and retry immediately
        if (write_error) {
            av_log(conn->s, AV_LOG_INFO, "[dashenc_http] Write error detected, closing request for retry. conn_nr: %d\n", conn->nr);
            conn->write_error = false;  // Reset for next attempt
            thr_io_close(conn);
            continue;
        }

        // If chunks_done is set but there are no chunks to write, close immediately without opening
        if (chunks_done && !has_chunks) {
            thr_io_close(conn);
            continue;
        }

        ret = open_request_if_needed(conn);
        if (ret < 0) {
            av_log(conn->s, AV_LOG_ERROR, "[dashenc_http] failed to open request, conn_nr: %d\n", conn->nr);
            // Even if opening failed, we need to check if chunks_done is set
            // to properly close the request and avoid calling open_request_if_needed() again
            if (chunks_done) {
                thr_io_close(conn);
                continue;
            }
            continue;
        }

        // Only write chunks if we have chunks to write at this point
        if (has_chunks) {
            while (write_chunk_if_available(conn)) {}
        }

        if (chunks_done) {
            thr_io_close(conn);
            // after this no other action should be done on conn until a new request is started so make sure there are no statements below this.
            continue;
        }
    }

    av_log(conn->s, AV_LOG_INFO, "[dashenc_http] dashenc_http thread done, conn_nr: %d.\n", conn->nr);
    return NULL;
}

static void request_cleanup(connection *conn) {
    if (conn->cleanup_requested) {
        return;
    }

    av_log(conn->s, AV_LOG_INFO, "[dashenc_http] Request cleanup of conn %d\n", conn->nr);
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
static void free_idle_connections(int nr_of_idle_connections, const int nr_of_connections_to_keep) {
    connection *conn = NULL;

    av_log(NULL, AV_LOG_INFO, "[dashenc_http] free_idle_connections, nr_of_idle_connections: %d, nr_of_connections_to_keep: %d\n", nr_of_idle_connections, nr_of_connections_to_keep);

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
static connection *claim_connection(const char *url, const int need_new_connection) {
    int64_t lowest_release_time = US_TO_MS(av_gettime());
    int conn_nr = -1;
    int conn_idle_count = 0;
    connection *conn = NULL;
    connection *conn_l = NULL;
    size_t len = 0;

    if (url == NULL) {
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] Claimed conn_nr: -1, url: NULL\n");
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
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] No free connection found, creating new one for url: %s\n", url);
        conn = av_mallocz(sizeof(*conn));
        if (conn == NULL) {
            pthread_mutex_unlock(&connections_mutex);
            return conn;
        }

        pthread_mutex_init(&conn->open_mutex, NULL);

        conn_nr = get_connection_number();
        conn->nr = conn_nr;

        pthread_mutex_init(&conn->chunks.mutex, NULL);
        pthread_cond_init(&conn->chunks.cv, NULL);

        pthread_attr_t attr;
        if (pthread_attr_init(&attr)) {
            av_log(NULL, AV_LOG_FATAL, "[dashenc_http] Error creating thread attributes.\n");
            abort();
        }

        if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) {
            av_log(NULL, AV_LOG_FATAL, "[dashenc_http] Error setting thread attributes.\n");
            abort();
        }

        if(pthread_create(&conn->w_thread, &attr, thr_io_write, conn)) {
            av_log(NULL, AV_LOG_FATAL, "[dashenc_http] Error creating thread so abort.\n");
            abort();
        }

        LIST_INSERT_HEAD(&connections, conn, entries);
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] No free connections so added one. Url: %s, conn_nr: %d\n", url, conn_nr);
    } else {
        const int64_t idle_time = US_TO_MS(av_gettime()) - conn->release_time;
        av_log(NULL, AV_LOG_INFO, "[dashenc_http] Reusing conn_nr: %d, idle for %lld ms, opened: %d, for url: %s\n", 
               conn_nr, (long long)idle_time, (int)conn->opened, url);
        pthread_mutex_lock(&conn->open_mutex);
        if (need_new_connection && conn->opened) {
            av_log(NULL, AV_LOG_INFO, "[dashenc_http] Closing connection because need_new_connection=1, conn_nr: %d\n", conn_nr);
            conn->opened = false;
            ff_format_io_close(conn->s, &conn->out);
        }
        pthread_mutex_unlock(&conn->open_mutex);

        conn->nr = conn_nr;
    }

    av_log(NULL, AV_LOG_INFO, "[dashenc_http] Claimed conn_nr: %d, url: %s\n", conn_nr, url);
    len = strlen(url) + 1;
    conn->url = malloc(len);
    av_strlcpy(conn->url, url, len);
    conn->claimed = true;

    if(conn_idle_count > max_idle_connections){
        free_idle_connections(conn_idle_count, max_idle_connections); /* NOLINT(readability-suspicious-call-argument) */
    }

    pthread_mutex_unlock(&connections_mutex);

    return conn;
}

/**
 * Opens a request on a free connection and returns the connection number
 * Only used for non persistent HTTP connections or file based output
 */
static int open_request(AVFormatContext *ctx, char *url, AVDictionary **options) {
    int ret = 0;
    connection *conn = claim_connection(url, 0);

    pthread_mutex_lock(&conn->open_mutex);
    if (conn->opened) {
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] open_request while connection might be open. This is TODO for when not using persistent connections. conn_nr: %d\n", conn->nr);
    }

    ret = ctx->io_open(ctx, &conn->out, url, AVIO_FLAG_WRITE, options);
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
int pool_io_open(AVFormatContext *ctx, const char *filename,
        AVDictionary **options, const int http_persistent, const int must_succeed, const int need_new_connection) {
    const int http_base_proto = filename ? ff_is_http_proto(filename) : 0;

    if (!http_base_proto || !http_persistent) {
        //open_request returns the newly claimed conn_nr
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] Non HTTP request %s\n", filename);
        return open_request(ctx, filename, options);
    }

#if CONFIG_HTTP_PROTOCOL

    //claim new item from pool and open connection if needed
    connection *conn = claim_connection(filename, need_new_connection);

    conn = get_conn(conn->nr);
    conn->must_succeed = must_succeed;
    conn->s = ctx;
    conn->options = NULL;

    const int ret = av_dict_copy(&conn->options, *options, 0);
    if (ret < 0) {
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] Could not copy options for %s\n", filename);
        abort_if_needed(must_succeed);
        return ret;
    }

    conn->http_persistent = http_persistent;
    return conn->nr;
#else
    UNUSED(http_persistent);
    UNUSED(must_succeed);
    UNUSED(need_new_connection);

    return AVERROR_MUXER_NOT_FOUND;
#endif
}

/**
 * Closes the request.
 */
static void pool_conn_close(connection *conn) {
    pthread_mutex_lock(&conn->chunks.mutex);
    conn->chunks_done = true;
    pthread_cond_signal(&conn->chunks.cv);
    pthread_mutex_unlock(&conn->chunks.mutex);
}

void pool_io_close(AVFormatContext *ctx, const char *filename, const int conn_nr) {
    if (conn_nr < 0) {
        av_log(ctx, AV_LOG_WARNING, "[dashenc_http] Invalid conn_nr in pool_io_close for filename: %s, conn_nr: %d\n", filename, conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);
    av_log(NULL, AV_LOG_INFO, "[dashenc_http] pool_io_close conn_nr: %d\n", conn_nr);
    pool_conn_close(conn);
}

void pool_free_all(AVFormatContext *ctx) {
    connection *conn = NULL;

    av_log(ctx, AV_LOG_INFO, "[dashenc_http] pool_free_all\n");

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

    free_stats(chunk_write_time_stats);
    free_stats(conn_count_stats);

    av_log(ctx, AV_LOG_INFO, "[dashenc_http] All requests are stopped\n");
}


void pool_write_flush_mem(const int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Invalid conn_nr in pool_write_flush_mem. conn_nr: %d\n", conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);
    const int read_size = (int)(conn->mem->ptr - conn->mem->buf);

    pool_write_flush(conn->mem->buf, read_size, conn_nr);
}

void pool_write_flush(const unsigned char *buf, const int size, const int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Invalid conn_nr in pool_write_flush. conn_nr: %d\n", conn_nr);
        return;
    }

    connection *conn = get_conn(conn_nr);

    //Save the chunk in memory
    Chunk *new_chunk = (Chunk *)av_mallocz(sizeof(*new_chunk));
    if (!new_chunk) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Could not malloc new_chunk.\n");
        return;
    }

    new_chunk->size = size;
    new_chunk->buf = av_mallocz(size);
    if (new_chunk->buf == NULL) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Could not malloc pool_write_flush.\n");
        return;
    }
    memcpy(new_chunk->buf, buf, size);

    pthread_mutex_lock(&conn->chunks.mutex);
    av_dynarray_add((void*)&conn->chunks.storage, &conn->chunks.nr_of_chunks, new_chunk);
    pthread_cond_signal(&conn->chunks.cv);
    pthread_mutex_unlock(&conn->chunks.mutex);
}

static int write_packet(void *opaque, const uint8_t *buf, int buf_size) {
    struct buffer_data *buffer_data = (struct buffer_data *)opaque;
    while (buf_size > buffer_data->room) {
        int64_t offset = buffer_data->ptr - buffer_data->buf;
        buffer_data->buf = av_realloc_f(buffer_data->buf, 2, buffer_data->size);
        if (!buffer_data->buf) {
            return AVERROR(ENOMEM); /* NOLINT(misc-include-cleaner) */
        }
        buffer_data->size *= 2;
        buffer_data->ptr = buffer_data->buf + offset;
        buffer_data->room = buffer_data->size - offset;
    }

    memcpy(buffer_data->ptr, buf, buf_size);
    buffer_data->ptr  += buf_size;
    buffer_data->room -= buf_size;

    return buf_size;
}

/**
 * Create AVIOContext for memory writing (instead of directly writing to the output)
 * Can be used with pool_write_flush_mem
 * Should be freed with pool_free_mem_context()
 */
AVIOContext *pool_create_mem_context(int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Invalid conn_nr in pool_create_mem_context. conn_nr: %d\n", conn_nr);
        return NULL;
    }

    const size_t bd_buf_size = 10;
    connection *conn = get_conn(conn_nr);

    conn->mem = av_malloc(sizeof(buffer_data));
    conn->mem->room = 0;
    conn->mem->size = 0;
    conn->mem->ptr = conn->mem->buf = av_malloc(bd_buf_size);
    if (!conn->mem->buf) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Could not allocate memory bd_buf_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }
    conn->mem->size = conn->mem->room = bd_buf_size;

    const int avio_ctx_buffer_size = 20;
    unsigned char *avio_ctx_buffer = av_malloc(avio_ctx_buffer_size);
    if (!avio_ctx_buffer) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Could not allocate memory avio_ctx_buffer_size. conn_nr: %d\n", conn_nr);
        return NULL;
    }

    return avio_alloc_context(avio_ctx_buffer, avio_ctx_buffer_size, 1, conn->mem, NULL, write_packet, NULL);
}

void pool_free_mem_context(AVIOContext **out, int conn_nr) {
    if (conn_nr < 0) {
        av_log(NULL, AV_LOG_WARNING, "[dashenc_http] Invalid conn_nr in pool_free_mem_context. conn_nr: %d\n", conn_nr);
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
    chunk_write_time_stats = init_stats("chunk_write_time", kDefaultStatsTime);
    conn_count_stats = init_stats("nr_of_connections", kDefaultStatsTime);
}
