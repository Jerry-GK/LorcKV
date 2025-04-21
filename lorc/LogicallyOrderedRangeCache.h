#pragma once

#include <unordered_map>
#include <string>
#include "Range.h"

// This is a cache for ranges. Key-Value Ranges is the smallest unit of data that can be cached.
class LogicallyOrderedRangeCache {
public:
    LogicallyOrderedRangeCache();
    virtual ~LogicallyOrderedRangeCache();

    // insert a key-value range into the cache, update overlapped exsisting ranges
    virtual void putRange(const Range& range) = 0;

    // lookup a key-value range in the cache
    virtual Range getRange(const std::string& start_key, const std::string& end_key) = 0;

    virtual double hitRate() const = 0;


protected:
    int hit_count;
    int query_count;
};