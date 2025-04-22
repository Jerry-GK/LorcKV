#include "LogicallyOrderedRangeCache.h"

LogicallyOrderedRangeCache::LogicallyOrderedRangeCache(int max_size)
    : max_size(max_size), hit_count(0), query_count(0), current_size(0) {
}

LogicallyOrderedRangeCache::~LogicallyOrderedRangeCache() {
}
