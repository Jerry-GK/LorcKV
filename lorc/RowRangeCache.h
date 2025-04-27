#pragma once

#include "Range.h"
#include <vector>
#include <set>
#include <map>
#include "LogicallyOrderedRangeCache.h"
#include "logger/Logger.h"

/**
 * RowRangeCache: A cache implementation using a map to store range data
 * No LRU implementation, only used for testing (victim randomly)
 */
class RowRangeCache : public LogicallyOrderedRangeCache {
public:
    RowRangeCache(int max_size);
    ~RowRangeCache() override;

    void putRange(Range&& range) override;
    // TODO(jr): RowRangeCache::gerRange is not clear and should be deperecated
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    
private:
    std::map<std::string, std::string> row_map;
};
