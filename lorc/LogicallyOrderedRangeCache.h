#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include "Range.h"

/**
 * Class that represents the result of a cache lookup operation.
 * It can indicate either a full hit, partial hit, or miss,
 * and contains the relevant range data.
 */
class CacheResult {
public:
    // Deprecated constructor that used references instead of move semantics
    // CacheResult(bool full_hit, bool partial_hit, Range& full_hit_range,
    //             const std::vector<Range>& partial_hit_ranges)
    //     : full_hit(full_hit), partial_hit(partial_hit),
    //       full_hit_range(full_hit_range), 
    //       partial_hit_ranges(partial_hit_ranges) {}

    // Constructor using move semantics for better performance
    CacheResult(bool full_hit, bool partial_hit, Range&& full_hit_range,
                std::vector<Range>&& partial_hit_ranges)
        : full_hit(full_hit), partial_hit(partial_hit),
          full_hit_range(std::move(full_hit_range)), 
          partial_hit_ranges(std::move(partial_hit_ranges)) {}

    // Move constructor
    CacheResult(CacheResult&& other) noexcept
        : full_hit(other.full_hit),
          partial_hit(other.partial_hit),
          full_hit_range(std::move(other.full_hit_range)),
          partial_hit_ranges(std::move(other.partial_hit_ranges)) {}

    // Move assignment operator
    CacheResult& operator=(CacheResult&& other) noexcept {
        if (this != &other) {
            full_hit = other.full_hit;
            partial_hit = other.partial_hit;
            full_hit_range = std::move(other.full_hit_range);
            partial_hit_ranges = std::move(other.partial_hit_ranges);
        }
        return *this;
    }

    // Disable copy constructor and copy assignment
    CacheResult(const CacheResult&) = delete;
    CacheResult& operator=(const CacheResult&) = delete;

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

    /**
     * Add a new range to the cache, merging with existing overlapping ranges.
     * The new range's data takes precedence over existing data in overlapping regions.
     */
    virtual void putRange(Range&& range) = 0;

    /**
     * Retrieve a range from the cache between start_key and end_key.
     * Returns a CacheResult indicating full hit, partial hit, or miss.
     */
    virtual CacheResult getRange(const std::string& start_key, const std::string& end_key) = 0;

    /**
     * Remove or truncate entries to maintain cache size within limits.
     */
    virtual void victim() = 0;

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