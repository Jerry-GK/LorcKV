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

CacheStatistic LogicallyOrderedRangeCache::getCacheStatistic() const {
    return cache_statistic;
}
