#include "rocksdb/lorc.h"

namespace ROCKSDB_NAMESPACE {

LogicallyOrderedRangeCache::LogicallyOrderedRangeCache(size_t capacity_, LorcLogger::Level logger_level_, PhysicalRangeType physical_range_type_)
    : capacity(capacity_), logger(LorcLogger(logger_level_)), physical_range_type(physical_range_type_),
    ranges_view(LogicalRangesView()), current_size(0), total_range_length(0), enable_statistic(false), cache_statistic(CacheStatistic()), 
    full_hit_count(0), full_query_count(0), hit_size(0), query_size(0) {
}

LogicallyOrderedRangeCache::~LogicallyOrderedRangeCache() {
}

bool LogicallyOrderedRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedRangeCache::setEnableStatistic(bool enable_statistic_) {
    this->enable_statistic = enable_statistic_;
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

}  // namespace ROCKSDB_NAMESPACE
