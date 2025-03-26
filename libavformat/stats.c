#include "stats.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <libavcodec/packet.h>
#include <libavutil/avstring.h>
#include <libavutil/dict.h>
#include <libavutil/error.h>
#include <libavutil/log.h>
#include <libavutil/mem.h>
#include <libavutil/time.h>

#include "common.h"

/**
 * Call his method with a value and it will print the min, max and average value once every logInterval.
 */
void print_complete_stats(stats *stats, int64_t value)
{
    int64_t avgValue = 0;
    int64_t curr_time = av_gettime();

    if (stats == NULL) {
        return;
    }

    pthread_mutex_lock(&stats->stats_lock);
    stats->nrOfSamples++;
    stats->totalValue += value;
    if (stats->maxValue < value) {
        stats->maxValue = value;
    }

    if (stats->minValue > value || stats->minValue == 0) {
        stats->minValue = value;
    }

    if (stats->lastLog == 0) {
        stats->lastLog = curr_time;
    }

    if (curr_time - stats->lastLog > stats->logInterval) {
        stats->lastLog = curr_time;
        avgValue = stats->totalValue / stats->nrOfSamples;

        av_log(NULL, AV_LOG_INFO, "complete_stats name: %s, min: %"PRId64", max: %"PRId64", avg: %"PRId64", time: %"PRId64"\n",
            stats->name,
            stats->minValue,
            stats->maxValue,
            avgValue,
            curr_time);

        stats->minValue = 0;
        stats->maxValue = 0;
        stats->totalValue = 0;
        stats->nrOfSamples = 0;
    }
    pthread_mutex_unlock(&stats->stats_lock);
}

/**
 * Call his method with a value and it will print the total value once every logInterval.
 */
void print_total_stats(stats *stats, int64_t value)
{
    int64_t curr_time = av_gettime();

    if (stats == NULL) {
        return;
    }

    pthread_mutex_lock(&stats->stats_lock);
    stats->totalValue += value;

    if (stats->lastLog == 0) {
        stats->lastLog = curr_time;
    }

    if (curr_time - stats->lastLog > stats->logInterval) {
        stats->lastLog = curr_time;

        av_log(NULL, AV_LOG_INFO, "%s: %"PRId64", time: %"PRId64"\n",
            stats->name,
            stats->totalValue,
            curr_time);

        stats->totalValue = 0;
    }
    pthread_mutex_unlock(&stats->stats_lock);
}

stats *init_stats(const char *name, int logInterval)
{
    stats *stats = av_mallocz(sizeof(struct stats));
    stats->logInterval = logInterval;
    av_strlcpy(stats->name, name, sizeof(stats->name));
    pthread_mutex_init(&stats->stats_lock, NULL);
    return stats;
}

stats *init_stats_prefix(const char *name, const char *prefix, const int logInterval) {
    stats *result_stats = NULL;

    char * const stats_name = av_asprintf("%s.%s", prefix, name);
    if (stats_name == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to allocate stats name+prefix");
        return NULL;
    }

    result_stats = init_stats(stats_name, logInterval);
    av_free(stats_name);
    if (result_stats == NULL) {
        av_log(NULL, AV_LOG_ERROR, "Failed to allocate stats context");
    }

    return result_stats;
}

void free_stats(stats *stats)
{
    if (!stats) {
        return;
    }

    pthread_mutex_destroy(&stats->stats_lock);
    av_free(stats);
}

static inline const char *get_flow_string(const int64_t value) {
    return value == LONG_MAX ? "Overflow" : "Underflow";
}

/**
 * Return the timestamp (in microseconds) that was added when the frame has entered FFmpeg.
 */
int64_t get_init_time(const AVPacket *pkt) {
    size_t size = 0;
    AVDictionary *dict = NULL;
    AVDictionaryEntry* timeEntry = NULL;
    int ret = 0;
    const char key[] = "init_time";

    const uint8_t *side_data = av_packet_get_side_data(pkt, AV_PKT_DATA_STRINGS_METADATA, &size);
    if (!side_data || !size) {
        av_log(NULL, AV_LOG_ERROR, "Packet doesn't contain AV_PKT_DATA_STRINGS_METADATA, pts: %ld", pkt->pts);
        return AVERROR(ENOENT);
    }

    ret = av_packet_unpack_dictionary(side_data, size, &dict);
    if (ret < 0) {
        av_log(NULL, AV_LOG_ERROR, "Failed to unpack side_data dictionary, packet pts: %ld", pkt->pts);
        return ret;
    }

    timeEntry = av_dict_get(dict, key, NULL, 0);
    if (timeEntry) {
        int64_t init_time = 0;
        errno = 0;
        init_time = strtoll(timeEntry->value, NULL, kDecimalBase);
        av_dict_free(&dict);
        if ((init_time == LONG_MAX || init_time == LONG_MIN) && errno == ERANGE) {
            av_log(NULL, AV_LOG_ERROR, "%s during extracting %s from the packet with pts %ld, sd value: %s", get_flow_string(init_time), key, pkt->pts, timeEntry->value);
            return AVERROR(ERANGE);
        }

        return init_time;
    }

    av_dict_free(&dict);
    av_log(NULL, AV_LOG_ERROR, "Failed to find %s, packet pts: %ld", key, pkt->pts);
    return AVERROR(ENOENT);
}
