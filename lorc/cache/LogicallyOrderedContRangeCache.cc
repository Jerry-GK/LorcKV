#include "LogicallyOrderedContRangeCache.h"

LogicallyOrderedContRangeCache::LogicallyOrderedContRangeCache(int max_size)
    : max_size(max_size), full_hit_count(0), full_query_count(0), current_size(0), 
      hit_size(0), query_size(0), enable_statistic(false), cache_statistic(CacheStatistic()) {
}

LogicallyOrderedContRangeCache::~LogicallyOrderedContRangeCache() {
}

bool LogicallyOrderedContRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedContRangeCache::setEnableStatistic(bool enable_statistic) {
    this->enable_statistic = enable_statistic;
}

CacheStatistic& LogicallyOrderedContRangeCache::getCacheStatistic() {
    return cache_statistic;
}

void LogicallyOrderedContRangeCache::increaseFullHitCount() {
    full_hit_count++;
}

void LogicallyOrderedContRangeCache::increaseFullQueryCount() {
    full_query_count++;
}

void LogicallyOrderedContRangeCache::increaseHitSize(int size) {
    hit_size += size;
}

void LogicallyOrderedContRangeCache::increaseQuerySize(int size) {
    query_size += size;
}


double LogicallyOrderedContRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double LogicallyOrderedContRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
