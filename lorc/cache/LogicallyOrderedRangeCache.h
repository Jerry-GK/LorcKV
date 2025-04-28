#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include "../range/Range.h"
#include "../iterator/RangeCacheIterator.h"

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

class CacheStatistic {
public:
    CacheStatistic() : putRangeNum(0), getRangeNum(0), putRangeTotalTime(0), getRangeTotalTime(0) {}
    CacheStatistic(int putRangeNum, int getRangeNum, int64_t putRangeTotalTime, int64_t getRangeTotalTime)
        : putRangeNum(putRangeNum), getRangeNum(getRangeNum),
          putRangeTotalTime(putRangeTotalTime), getRangeTotalTime(getRangeTotalTime) {}

    int getPutRangeNum() const { return putRangeNum; }

    int getGetRangeNum() const { return getRangeNum; }

    int64_t getAvgPutRangeTime() const {
        return putRangeNum == 0 ? 0 : putRangeTotalTime / putRangeNum;
    }

    int64_t getAvgGetRangeTime() const {
        return getRangeNum == 0 ? 0 : getRangeTotalTime / getRangeNum;
    }

    void increasePutRangeNum() {
        putRangeNum++;
    }

    void increaseGetRangeNum() {
        getRangeNum++;
    }

    void increasePutRangeTotalTime(int64_t time) {
        putRangeTotalTime += time;
    }

    void increaseGetRangeTotalTime(int64_t time) {
        getRangeTotalTime += time;
    }

private:
    int putRangeNum;
    int getRangeNum;
    int64_t putRangeTotalTime;  // microseconds(us)
    int64_t getRangeTotalTime;  // microseconds(us)
};

// This is a cache for ranges. Key-Value Ranges is the smallest unit of data that can be cached.
class RangeCacheIterator;

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

    /**
     * Get enableStatistic
     */
    bool enableStatistic() const;

    /**
     * Set enableStatistic
     */
    void setEnableStatistic(bool enable_statistic);

    /**
     * Get cache statistics
     */
    CacheStatistic& getCacheStatistic();

    void increaseFullHitCount();

    void increaseFullQueryCount();

    void increaseHitSize(int size);

    void increaseQuerySize(int size);

    /**
     * Calculate the full hit rate of range queries.
     */
    double fullHitRate() const;

    /**
     * Calculate the hit size rate, which is the ratio of hit size to query size.
     */
    double hitSizeRate() const;

    /**
     * Get a new range cache iterator.
     */
    virtual RangeCacheIterator* newRangeCacheIterator() const = 0;

protected:
    friend class RangeCacheIterator;

    int max_size;
    int current_size;

    bool enable_statistic; // initialize to false
    CacheStatistic cache_statistic;

private:
    int full_hit_count;
    int full_query_count;
    int hit_size;
    int query_size;
};