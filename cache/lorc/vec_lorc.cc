#include "rocksdb/vec_lorc.h"

namespace ROCKSDB_NAMESPACE {
namespace lorc {

LogicallyOrderedSliceVecRangeCache::LogicallyOrderedSliceVecRangeCache(int max_size)
    : max_size(max_size), full_hit_count(0), full_query_count(0), current_size(0), 
      hit_size(0), query_size(0), enable_statistic(false), cache_statistic(CacheStatistic()) {
}

LogicallyOrderedSliceVecRangeCache::~LogicallyOrderedSliceVecRangeCache() {
}

bool LogicallyOrderedSliceVecRangeCache::enableStatistic() const {
    return enable_statistic;
}

void LogicallyOrderedSliceVecRangeCache::setEnableStatistic(bool enable_statistic) {
    this->enable_statistic = enable_statistic;
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

}  // namespace lorc
}  // namespace ROCKSDB_NAMESPACE
