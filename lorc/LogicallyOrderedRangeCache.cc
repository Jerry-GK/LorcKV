#include "LogicallyOrderedRangeCache.h"

LogicallyOrderedRangeCache::LogicallyOrderedRangeCache(int max_size)
    : max_size(max_size), full_hit_count(0), full_query_count(0), current_size(0), 
      hit_size(0), query_size(0) {
}

LogicallyOrderedRangeCache::~LogicallyOrderedRangeCache() {
}
