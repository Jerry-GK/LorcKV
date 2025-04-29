#pragma once

#include <set>
#include <map>
#include <string>
#include "LogicallyOrderedContRangeCache.h" // Ensure this header provides the full definition of LogicallyOrderedRangeCache
#include "../range/ContRange.h"
#include "../logger/Logger.h"

class RBTreeContRangeCacheIterator;
class ContRangeCacheIterator;

/**
 * RBTreeContRangeCache: A cache implementation using Red-Black Tree to store ContRange data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeContRangeCache : public LogicallyOrderedContRangeCache {
public:
    RBTreeContRangeCache(int max_size);
    ~RBTreeContRangeCache() override;

    void putRange(ContRange&& ContRange) override;
    void victim() override;
    
    ContRangeCacheIterator* newContRangeCacheIterator() const override;

    /**
     * Update the timestamp of a ContRange to mark it as recently used.
     */
    void pinRange(std::string startKey);
    
private:
    friend class RBTreeContRangeCacheIterator;
    std::set<ContRange> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
};
