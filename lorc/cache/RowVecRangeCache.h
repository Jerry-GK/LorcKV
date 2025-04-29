#pragma once

#include <vector>
#include <set>
#include <map>
#include "LogicallyOrderedVecRangeCache.h"
#include "../range/VecRange.h"
#include "../logger/Logger.h"

/**
 * RowRangeCache: A cache implementation using a map to store VecRange data
 * No LRU implementation, only used for testing (victim randomly)
 */
class RowVecRangeCache : public LogicallyOrderedVecRangeCache {
public:
    RowVecRangeCache(int max_size);
    ~RowVecRangeCache() override;

    void putRange(VecRange&& VecRange) override;
    // TODO(jr): RowRangeCache::gerRange is not clear and should be deperecated
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    
private:
    std::map<std::string, std::string> row_map;
};
