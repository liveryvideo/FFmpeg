#pragma once

#define UNUSED(x) (void)(x)

enum TimeConstants {
    kOneMicrosecond = 1000,
    kOneMillisecond = 1000,
    kOneSecond = kOneMicrosecond * kOneMillisecond
};

#define US_TO_MS(x) ((x) / kOneMicrosecond)
#define US_TO_S(x) ((x) / kOneSecond)
#define MS_TO_S(x) ((x) / kOneMillisecond)
#define S_TO_US(x) (((int64_t)x) * kOneSecond)
#define S_TO_MS(x) (((int64_t)x) * kOneMillisecond)

enum MiscConstants {
    kDefaultStatsTime = 5 * kOneSecond,
};
