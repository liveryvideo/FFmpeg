#ifndef AVFORMAT_DASH_HTTP_H
#define AVFORMAT_DASH_HTTP_H

#include "avformat.h"

AVIOContext *pool_create_mem_context(int conn_nr);
int pool_io_open(AVFormatContext *ctx, const char *filename, AVDictionary **options, int http_persistent, int must_succeed, int retry, int need_new_connection);
void pool_io_close(AVFormatContext *ctx, const char *filename, int conn_nr);
void pool_free_all(AVFormatContext *ctx);
void pool_free_mem_context(AVIOContext **out, int conn_nr);
void pool_write_flush(const unsigned char *buf, int size, int conn_nr);
void pool_write_flush_mem(int conn_nr);
void pool_init(void);

#endif /* AVFORMAT_DASH_HTTP_H */
