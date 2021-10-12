#include "dashenc_stats.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libavutil/time.h"
#include "libavutil/avstring.h"

/**
 * Call his method with a value and it will print the min, max and average value once every logInterval.
 */
void print_complete_stats(stats *stats, int64_t value)
{
    int64_t avgValue;
    int64_t curr_time = av_gettime_relative();

    if (stats == NULL) {
        return;
    }

    pthread_mutex_lock(&stats->stats_lock);
    stats->nrOfSamples++;
    stats->totalValue += value;
    if (stats->maxValue < value)
        stats->maxValue = value;

    if (stats->minValue > value || stats->minValue == 0)
        stats->minValue = value;

    if (stats->lastLog == 0)
        stats->lastLog = curr_time;

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
    int64_t curr_time = av_gettime_relative();

    if (stats == NULL) {
        return;
    }

    pthread_mutex_lock(&stats->stats_lock);
    stats->totalValue += value;

    if (stats->lastLog == 0)
        stats->lastLog = curr_time;

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
    stats *stats = calloc(1, sizeof(struct stats));
    stats->logInterval = logInterval;
    av_strlcpy(stats->name, name, sizeof(stats->name));
    pthread_mutex_init(&stats->stats_lock, NULL);
    return stats;
}

void free_stats(stats *stats)
{
    pthread_mutex_destroy(&stats->stats_lock);
    free(stats);
}
