#include "LogicallyOrderedVecRangeCache.h"

LogicallyOrderedVecRangeCache::LogicallyOrderedVecRangeCache(int max_size)
    : max_size(max_size), full_hit_count(0), full_query_count(0), current_size(0), 
      hit_size(0), query_size(0), enable_statistic(false), cache_statistic(CacheStatistic()) {
}

LogicallyOrderedVecRangeCache::~LogicallyOrderedVecRangeCache() {
}

bool LogicallyOrderedVecRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedVecRangeCache::setEnableStatistic(bool enable_statistic) {
    this->enable_statistic = enable_statistic;
}

CacheStatistic& LogicallyOrderedVecRangeCache::getCacheStatistic() {
    return cache_statistic;
}

void LogicallyOrderedVecRangeCache::increaseFullHitCount() {
    full_hit_count++;
}

void LogicallyOrderedVecRangeCache::increaseFullQueryCount() {
    full_query_count++;
}

void LogicallyOrderedVecRangeCache::increaseHitSize(int size) {
    hit_size += size;
}

void LogicallyOrderedVecRangeCache::increaseQuerySize(int size) {
    query_size += size;
}


double LogicallyOrderedVecRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double LogicallyOrderedVecRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
