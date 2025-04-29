#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include "../range/ContRange.h"
#include "../iterator/ContRangeCacheIterator.h"

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
class ContRangeCacheIterator;

class LogicallyOrderedContRangeCache {
public:
    LogicallyOrderedContRangeCache(int max_size);
    virtual ~LogicallyOrderedContRangeCache();

    /**
     * Add a new ContRange to the cache, merging with existing overlapping ranges.
     * The new ContRange's data takes precedence over existing data in overlapping regions.
     */
    virtual void putRange(ContRange&& ContRange) = 0;

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
     * Calculate the full hit rate of ContRange queries.
     */
    double fullHitRate() const;

    /**
     * Calculate the hit size rate, which is the ratio of hit size to query size.
     */
    double hitSizeRate() const;

    /**
     * Get a new ContRange cache iterator.
     */
    virtual ContRangeCacheIterator* newRangeCacheIterator() const = 0;

protected:
    friend class ContRangeCacheIterator;

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