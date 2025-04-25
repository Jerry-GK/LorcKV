#pragma once

#include "Range.h"
#include <vector>
#include <set>
#include <map>
#include "LogicallyOrderedRangeCache.h"
#include "Logger.h"

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
    double fullHitRate() const override;
    double hitSizeRate() const override;

    /**
     * Update the timestamp of a range to mark it as recently used.
     */
    void pinRange(std::string startKey);
    
private:
    std::set<Range> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    int cache_timestamp;          // Timestamp for LRU-like functionality
};
