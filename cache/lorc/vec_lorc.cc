#include "rocksdb/vec_lorc.h"

namespace ROCKSDB_NAMESPACE {

LogicallyOrderedSliceVecRangeCache::LogicallyOrderedSliceVecRangeCache(size_t capacity_, bool enable_logger_)
    : capacity(capacity_), current_size(0), enable_statistic(false), cache_statistic(CacheStatistic()), logger(LorcLogger(LorcLogger::Level::DEBUG, enable_logger_)), 
    full_hit_count(0), full_query_count(0), hit_size(0), query_size(0) {
}

LogicallyOrderedSliceVecRangeCache::~LogicallyOrderedSliceVecRangeCache() {
}

bool LogicallyOrderedSliceVecRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedSliceVecRangeCache::setEnableStatistic(bool enable_statistic_) {
    this->enable_statistic = enable_statistic_;
}

CacheStatistic& LogicallyOrderedSliceVecRangeCache::getCacheStatistic() {
    return cache_statistic;
}

void LogicallyOrderedSliceVecRangeCache::increaseFullHitCount() {
    full_hit_count++;
}

void LogicallyOrderedSliceVecRangeCache::increaseFullQueryCount() {
    full_query_count++;
}

void LogicallyOrderedSliceVecRangeCache::increaseHitSize(int size) {
    hit_size += size;
}

void LogicallyOrderedSliceVecRangeCache::increaseQuerySize(int size) {
    query_size += size;
}


double LogicallyOrderedSliceVecRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double LogicallyOrderedSliceVecRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}

}  // namespace ROCKSDB_NAMESPACE
