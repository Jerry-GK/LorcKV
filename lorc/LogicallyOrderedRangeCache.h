#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include "Range.h"

class CacheResult {
public:
    CacheResult(bool full_hit, bool partial_hit, const Range& full_hit_range,
                const std::vector<Range>& partial_hit_ranges)
        : full_hit(full_hit), partial_hit(partial_hit),
          full_hit_range(full_hit_range), partial_hit_ranges(partial_hit_ranges) {}

    bool isFullHit() const { return full_hit; }
    bool isPartialHit() const { return partial_hit; }
    const Range& getFullHitRange() const { return full_hit_range; }
    const std::vector<Range>& getPartialHitRanges() const { return partial_hit_ranges; }
private:
    bool full_hit;
    bool partial_hit;
    Range full_hit_range;
    std::vector<Range> partial_hit_ranges;
};

// This is a cache for ranges. Key-Value Ranges is the smallest unit of data that can be cached.
class LogicallyOrderedRangeCache {
public:
    LogicallyOrderedRangeCache(int max_size);
    virtual ~LogicallyOrderedRangeCache();

    // insert a key-value range into the cache, update overlapped exsisting ranges
    virtual void putRange(const Range& range) = 0;

    // lookup a key-value range in the cache
    virtual CacheResult getRange(const std::string& start_key, const std::string& end_key) = 0;

    // victim
    virtual void victim() = 0;

    // pin a range in the cache to implement LRU
    virtual void pinRange(int index) = 0;

    virtual double fullHitRate() const = 0;

    virtual double hitSizeRate() const = 0;

protected:
    int max_size;
    int current_size;
    int full_hit_count;
    int full_query_count;
    int hit_size;
    int query_size;
};