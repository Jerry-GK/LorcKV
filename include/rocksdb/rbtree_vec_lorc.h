#pragma once

#include <set>
#include <map>
#include <string>
#include "rocksdb/vec_lorc.h"
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

class RBTreeRangeCacheIterator;
class SliceVecRangeCacheIterator;

/**
 * RBTreeSliceVecRangeCache: A cache implementation using Red-Black Tree to store SliceVecRange data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeSliceVecRangeCache : public LogicallyOrderedSliceVecRangeCache {
public:
    RBTreeSliceVecRangeCache(int max_size);
    ~RBTreeSliceVecRangeCache() override;

    void putRange(SliceVecRange&& SliceVecRange) override;
    bool updateEntry(const Slice& key, const Slice& value) override;
    void victim() override;
    
    SliceVecRangeCacheIterator* newSliceVecRangeCacheIterator() const override;

    /**
     * Update the timestamp of a SliceVecRange to mark it as recently used.
     */
    void pinRange(std::string startKey);
    
private:
    friend class RBTreeSliceVecRangeCacheIterator;
    std::set<SliceVecRange> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
};

}  // namespace ROCKSDB_NAMESPACE
