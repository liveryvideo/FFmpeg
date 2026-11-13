#include "stats_context.h"

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>

#include <libavcodec/packet.h>
#include <libavutil/avstring.h>
#include <libavutil/avutil.h>
#include <libavutil/log.h>
#include <libavutil/mem.h>
#include <libavutil/time.h>

#include "common.h"
#include "stats.h"

StatsContext *alloc_new_stats_context(const char *prefix, const int nb_streams, const int *bitrates) {
    StatsContext *s_ctx = av_mallocz(sizeof(StatsContext));
    if (s_ctx == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to allocate stats context");
        return NULL;
    }

    s_ctx->audio_time_stats = init_stats_prefix("audio_processing", prefix, kDefaultStatsTime);
    if (s_ctx->audio_time_stats == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to init audio time stats");
        goto error_free_text;
    }

    s_ctx->video_time_stats = init_stats_prefix("video_processing", prefix, kDefaultStatsTime);
    if (s_ctx->video_time_stats == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to init video time stats");
        goto error_free_audio_stats;
    }

    s_ctx->subtitle_time_stats = init_stats_prefix("subtitle_processing", prefix, kDefaultStatsTime);
    if (s_ctx->subtitle_time_stats == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to init subtitle time stats");
        goto error_free_video_stats;
    }

    s_ctx->bitrate_stats = (stats **)av_calloc(nb_streams, sizeof(stats *));
    if (s_ctx->bitrate_stats == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to alloc bitrate stats");
        goto error_free_subtitle_stats;
    }

    s_ctx->nb_streams = nb_streams;
    for (int i = 0; i < nb_streams; i++) {
        const char *bitrate_str = av_asprintf("bitrate_stats: %s.rep_%d_bitrate_%d, value", prefix, i, bitrates[i]);
        if (bitrate_str == NULL)  {
            av_log(NULL, AV_LOG_ERROR, "Failed to alloc bitrate str");
            goto error_free_bitrate_stats;
        }

        s_ctx->bitrate_stats[i] = init_stats(bitrate_str, kOneSecond);
        av_free(bitrate_str);
        if (s_ctx->bitrate_stats[i] == NULL)  {
            av_log(NULL, AV_LOG_ERROR, "Failed to init stats for bitrate %d", bitrates[i]);
            goto error_free_bitrate_stats;
        }
    }

    return s_ctx;

error_free_bitrate_stats:
    for (int i = 0; i < nb_streams; i++) {
        free_stats(s_ctx->bitrate_stats[i]);
    }

    av_free((void *)s_ctx->bitrate_stats);

error_free_subtitle_stats:
    free_stats(s_ctx->video_time_stats);

error_free_video_stats:
    free_stats(s_ctx->video_time_stats);

error_free_audio_stats:
    free_stats(s_ctx->audio_time_stats);

error_free_text:
    av_free(s_ctx);
    return NULL;
}

static inline int64_t bytes2bits(const int64_t bytes) {
    return bytes << 3;
}


void print_stats(StatsContext *s_ctx, const enum AVMediaType pkt_type, const AVPacket *pkt)
{
    const int64_t pkt_init_time = get_init_time(pkt);
    if (pkt_init_time >= 0) {
        const int64_t pTime = US_TO_MS(av_gettime_relative() - pkt_init_time);

        if (pkt_type == AVMEDIA_TYPE_VIDEO) {
            print_complete_stats(s_ctx->video_time_stats, pTime);
        } else if (pkt_type == AVMEDIA_TYPE_AUDIO) {
            print_complete_stats(s_ctx->audio_time_stats, pTime);
        } else if  (pkt_type == AVMEDIA_TYPE_SUBTITLE) {
            print_complete_stats(s_ctx->subtitle_time_stats, pTime);
        }
    } else {
        av_log(NULL, AV_LOG_INFO, "missing packet time ret: %" PRId64 "\n", pkt_init_time);
    }

    print_total_stats(s_ctx->bitrate_stats[pkt->stream_index], bytes2bits(pkt->size));
}

void free_stats_context(StatsContext *s_ctx) {
    free_stats(s_ctx->audio_time_stats);
    free_stats(s_ctx->video_time_stats);
    free_stats(s_ctx->subtitle_time_stats);
    for (int i = 0; i < s_ctx->nb_streams; i++) {
        free_stats(s_ctx->bitrate_stats[i]);
    }

    av_free((void *)s_ctx->bitrate_stats);
    av_free(s_ctx);
}
