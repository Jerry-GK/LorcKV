#pragma once

#include <vector>
#include "LogicallyOrderedVecRangeCache.h"
#include "../logger/Logger.h"

class SegmentedRangeCacheEntry;

/**
 * A cache implementation that stores and manages ranges in segments.
 * It supports VecRange-based operations and maintains logical ordering of entries.
 */
class SegmentedVecRangeCache : public LogicallyOrderedVecRangeCache {
public:
    SegmentedVecRangeCache(int max_size);
    ~SegmentedVecRangeCache() override;

    void putRange(VecRange&& VecRange) override;

    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;

    void victim() override;

    /**
     * Update the timestamp of a VecRange to mark it as recently used.
     */
    void pinRange(int index);

private:
    std::vector<SegmentedRangeCacheEntry> entries;
    int cache_timestamp;  // Tracks the most recent access time
};

/**
 * Represents a single entry in the segmented VecRange cache.
 * Each entry contains a VecRange and its associated timestamp.
 */
class SegmentedRangeCacheEntry {
public:
    SegmentedRangeCacheEntry(const VecRange& range, int timestamp)
        : vec_range(range), timestamp(timestamp) {};
    ~SegmentedRangeCacheEntry() {};

    const VecRange& getRange() const {
        return vec_range;
    }
    int getTimestamp() const {
        return timestamp;
    }

    friend class SegmentedVecRangeCache;

private:
    VecRange vec_range;  // ¸ÄÃûÎª vec_range
    int timestamp;
};