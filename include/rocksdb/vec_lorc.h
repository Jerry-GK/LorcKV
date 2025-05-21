#pragma once

#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {
class LorcLogger {
public:
    enum Level {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };

private:
    std::string getLevelString(Level level) {
        switch (level) {
            case DEBUG: return "DEBUG";
            case INFO:  return "INFO";
            case WARN:  return "WARN";
            case ERROR: return "ERROR";
            default:    return "UNKNOWN";
        }
    }
    
    void log(Level level, const std::string& message) {
        if (!enabled || level < currentLevel) return;
        
        auto now = std::time(nullptr);
        auto tm = *std::localtime(&now);
        
        char time_buf[24];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm);
        
        std::cout << "[LorcLogger]-"
                  << "[" << getLevelString(level) << "]-"
                  << "[" << time_buf << "]: " 
                  << message << std::endl;
    }

public:
    LorcLogger(Level level, bool enabled_) : currentLevel(level), enabled(enabled_) {}
    LorcLogger() : currentLevel(Level::DEBUG), enabled(true) {}
    ~LorcLogger() = default;

    void debug(const std::string& message) {
        log(DEBUG, message);
    }
    
    void info(const std::string& message) {
        log(INFO, message);
    }
    
    void warn(const std::string& message) {
        log(WARN, message);
    }
    
    void error(const std::string& message) {
        log(ERROR, message);
    }

private:
    Level currentLevel;
    bool enabled;
};

class CacheStatistic {
public:
    CacheStatistic() : putRangeNum(0), getRangeNum(0), putRangeTotalTime(0), getRangeTotalTime(0) {}
    CacheStatistic(int putRangeNum_, int getRangeNum_, int64_t putRangeTotalTime_, int64_t getRangeTotalTime_)
        : putRangeNum(putRangeNum_), getRangeNum(getRangeNum_),
          putRangeTotalTime(putRangeTotalTime_), getRangeTotalTime(getRangeTotalTime_) {}

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
class SliceVecRangeCacheIterator;

class Arena;

class LogicallyOrderedSliceVecRangeCache {
public:
    LogicallyOrderedSliceVecRangeCache(size_t capacity_);
    virtual ~LogicallyOrderedSliceVecRangeCache();

    /**
     * Add a new SliceVecRange to the cache, merging with existing overlapping ranges.
     * The new SliceVecRange's data takes precedence over existing data in overlapping regions.
     */
    virtual void putRange(SliceVecRange&& SliceVecRange) = 0;

    /**
     * Update an entry in existing ranges
     * Return false if the key is not found
     */
    virtual bool updateEntry(const Slice& key, const Slice& value) = 0;

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
     * Calculate the full hit rate of SliceVecRange queries.
     */
    double fullHitRate() const;

    /**
     * Calculate the hit size rate, which is the ratio of hit size to query size.
     */
    double hitSizeRate() const;

    /**
     * Get a new SliceVecRange cache iterator.
     */
    virtual SliceVecRangeCacheIterator* newSliceVecRangeCacheIterator(Arena* arena) const = 0;

    /**
     * Get the value memory pool for the range cache.
     */
    virtual std::pmr::monotonic_buffer_resource* getRangePool() { assert(false); return nullptr; };

protected:
    friend class SliceVecRangeCacheIterator;

    size_t capacity;
    size_t current_size;

    bool enable_statistic; // initialize to false
    CacheStatistic cache_statistic;

    LorcLogger logger;

private:
    int full_hit_count;
    int full_query_count;
    int hit_size;
    int query_size;
};

}  // namespace ROCKSDB_NAMESPACE
