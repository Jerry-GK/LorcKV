#pragma once

#include <vector>
#include "LogicallyOrderedRangeCache.h"
#include "Logger.h"

class SegmentedRangeCacheEntry;

class SegmentedRangeCache : public LogicallyOrderedRangeCache {
public:
    SegmentedRangeCache(int max_size);
    ~SegmentedRangeCache() override;

    void putRange(Range&& range) override;
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    double fullHitRate() const override;
    double hitSizeRate() const override;

    void pinRange(int index);

private:
    std::vector<SegmentedRangeCacheEntry> entries;
    int cache_timestamp;
};

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