#ifndef AVFORMAT_DASH_STATS_H
#define AVFORMAT_DASH_STATS_H

#include <pthread.h>
#include "avformat.h"

typedef struct stats {
    int64_t lastLog;
    int64_t maxValue;
    int64_t minValue;
    int64_t totalValue;
    int64_t nrOfSamples;
    int logInterval;
    const char *name;
    pthread_mutex_t stats_lock;
} stats;

void print_time_stats(stats *stats, int64_t value);
stats *init_time_stats(const char *name, int logInterval);
void free_time_stats(stats *stats);

#endif /* AVFORMAT_DASH_HTTP_H */