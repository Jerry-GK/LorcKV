#include "rocksdb/lorc.h"

namespace ROCKSDB_NAMESPACE {

LogicalOrderedRangeCache::LogicalOrderedRangeCache(size_t capacity_, LorcLogger::Level logger_level_, PhysicalRangeType physical_range_type_)
    : capacity(capacity_), logger(LorcLogger(logger_level_)), physical_range_type(physical_range_type_),
    ranges_view(LogicalRangesView()), current_size(0), total_range_length(0), range_cache_seq_num(kMinUnCommittedSeq), enable_statistic(false), cache_statistic(CacheStatistic()), 
    full_hit_count(0), full_query_count(0), hit_size(0), query_size(0) {
}

LogicalOrderedRangeCache::~LogicalOrderedRangeCache() {
}

bool LogicalOrderedRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicalOrderedRangeCache::setEnableStatistic(bool enable_statistic_) {
    this->enable_statistic = enable_statistic_;
}

CacheStatistic& LogicalOrderedRangeCache::getCacheStatistic() {
    return cache_statistic;
}

void LogicalOrderedRangeCache::increaseFullHitCount() {
    full_hit_count++;
}

void LogicalOrderedRangeCache::increaseFullQueryCount() {
    full_query_count++;
}

void LogicalOrderedRangeCache::increaseHitSize(int size) {
    hit_size += size;
}

void LogicalOrderedRangeCache::increaseQuerySize(int size) {
    query_size += size;
}


double LogicalOrderedRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double LogicalOrderedRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}

}  // namespace ROCKSDB_NAMESPACE
