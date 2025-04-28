#include "LogicallyOrderedRangeCache.h"

LogicallyOrderedRangeCache::LogicallyOrderedRangeCache(int max_size)
    : max_size(max_size), full_hit_count(0), full_query_count(0), current_size(0), 
      hit_size(0), query_size(0), enable_statistic(false), cache_statistic(CacheStatistic()) {
}

LogicallyOrderedRangeCache::~LogicallyOrderedRangeCache() {
}

bool LogicallyOrderedRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedRangeCache::setEnableStatistic(bool enable_statistic) {
    this->enable_statistic = enable_statistic;
}

CacheStatistic& LogicallyOrderedRangeCache::getCacheStatistic() {
    return cache_statistic;
}

void LogicallyOrderedRangeCache::increaseFullHitCount() {
    full_hit_count++;
}

void LogicallyOrderedRangeCache::increaseFullQueryCount() {
    full_query_count++;
}

void LogicallyOrderedRangeCache::increaseHitSize(int size) {
    hit_size += size;
}

void LogicallyOrderedRangeCache::increaseQuerySize(int size) {
    query_size += size;
}


double LogicallyOrderedRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double LogicallyOrderedRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
