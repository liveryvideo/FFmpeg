#include "dashenc_stats.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libavutil/time.h"

void print_time_stats(stats *stats, int64_t value)
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

        av_log(NULL, AV_LOG_INFO, "%s min: %"PRId64", max: %"PRId64", avg: %"PRId64", time: %"PRId64"\n",
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

stats *init_time_stats(const char *name, int logInterval)
{
    stats *stats = calloc(1, sizeof(struct stats));
    stats->logInterval = logInterval;
    stats->name = name;
    pthread_mutex_init(&stats->stats_lock, NULL);
    return stats;
}

void free_time_stats(stats *stats)
{
    pthread_mutex_destroy(&stats->stats_lock);
}
