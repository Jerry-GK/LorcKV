#pragma once

#include <set>
#include <map>
#include <string>
#include "LogicallyOrderedVecRangeCache.h" // Ensure this header provides the full definition of LogicallyOrderedRangeCache
#include "../range/VecRange.h"
#include "../logger/Logger.h"

class RBTreeRangeCacheIterator;
class VecRangeCacheIterator;

/**
 * RBTreeRangeCache: A cache implementation using Red-Black Tree to store VecRange data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeRangeCache : public LogicallyOrderedVecRangeCache {
public:
    RBTreeRangeCache(int max_size);
    ~RBTreeRangeCache() override;

    void putRange(VecRange&& VecRange) override;
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    
    VecRangeCacheIterator* newRangeCacheIterator() const override;

    /**
     * Update the timestamp of a VecRange to mark it as recently used.
     */
    void pinRange(std::string startKey);
    
private:
    friend class RBTreeVecRangeCacheIterator;
    std::set<VecRange> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
};
