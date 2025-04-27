#pragma once

#include <set>
#include <map>
#include <string>
#include "LogicallyOrderedRangeCache.h" // Ensure this header provides the full definition of LogicallyOrderedRangeCache
#include "Range.h"
#include "logger/Logger.h"

class RBTreeRangeCacheIterator;
class RangeCacheIterator;

/**
 * RBTreeRangeCache: A cache implementation using Red-Black Tree to store range data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeRangeCache : public LogicallyOrderedRangeCache {
public:
    RBTreeRangeCache(int max_size);
    ~RBTreeRangeCache() override;

    void putRange(Range&& range) override;
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    
    RangeCacheIterator* newRangeCacheIterator() const override;

    /**
     * Update the timestamp of a range to mark it as recently used.
     */
    void pinRange(std::string startKey);
    
private:
    friend class RBTreeRangeCacheIterator;
    std::set<Range> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
};
