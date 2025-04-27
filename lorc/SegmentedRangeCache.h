#pragma once

#include <vector>
#include "LogicallyOrderedRangeCache.h"
#include "logger/Logger.h"

class SegmentedRangeCacheEntry;

/**
 * A cache implementation that stores and manages ranges in segments.
 * It supports range-based operations and maintains logical ordering of entries.
 */
class SegmentedRangeCache : public LogicallyOrderedRangeCache {
public:
    SegmentedRangeCache(int max_size);
    ~SegmentedRangeCache() override;

    void putRange(Range&& range) override;

    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;

    void victim() override;

    /**
     * Update the timestamp of a range to mark it as recently used.
     */
    void pinRange(int index);

private:
    std::vector<SegmentedRangeCacheEntry> entries;
    int cache_timestamp;  // Tracks the most recent access time
};

/**
 * Represents a single entry in the segmented range cache.
 * Each entry contains a range and its associated timestamp.
 */
class SegmentedRangeCacheEntry {
public:
    SegmentedRangeCacheEntry(const Range& range, int timestamp)
        : range(range), timestamp(timestamp) {};
    ~SegmentedRangeCacheEntry() {};

    const Range& getRange() const {
        return range;
    }
    int getTimestamp() const {
        return timestamp;
    }

    friend class SegmentedRangeCache;

private:
    Range range;
    int timestamp;
};