#pragma once

#include "common.h"
#include "libavutil/avutil.h"
#include "libavcodec/packet.h"
#include "stats.h"

typedef struct {
    stats *audio_time_stats;
    stats *video_time_stats;
    stats *subtitle_time_stats;

    int nb_streams;
    stats **bitrate_stats;
} StatsContext;

StatsContext *alloc_new_stats_context(const char *prefix, int nb_streams, const int *bitrates);
void print_stats(StatsContext *s_ctx, enum AVMediaType pkt_type, const AVPacket *pkt);
void free_stats_context(StatsContext *);
