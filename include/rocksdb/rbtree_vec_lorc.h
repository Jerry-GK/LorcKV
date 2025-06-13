#pragma once

#include <set>
#include <map>
#include <string>
#include <shared_mutex>
#include "rocksdb/vec_lorc.h"
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

class RBTreeRangeCacheIterator;
class SliceVecRangeCacheIterator;
class Arena;

/**
 * RBTreeSliceVecRangeCache: A cache implementation using Red-Black Tree to store SliceVecRange data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeSliceVecRangeCache : public LogicallyOrderedSliceVecRangeCache {
public:
    RBTreeSliceVecRangeCache(size_t capacity, bool enable_logger_ = false);
    ~RBTreeSliceVecRangeCache() override;

    void putOverlappingRefRange(ReferringSliceVecRange&& newRefRange) override;
    void putActualGapRange(SliceVecRange&& newRange, bool leftConcat, bool rightConcat, bool emptyConcat, std::string emptyConcatLeftKey, std::string emptyConcatRightKey) override;
    bool updateEntry(const Slice& key, const Slice& value) override;
    void victim() override;
    void tryVictim() override;
    
    SliceVecRangeCacheIterator* newSliceVecRangeCacheIterator(Arena* arena) const override;

    /**
     * Update the timestamp of a SliceVecRange to mark it as recently used.
     */
    void pinRange(std::string startKey);

    void printAllRangesWithKeys() const override;
        
    void printAllPhysicalRanges() const override;

    void printAllLogicalRanges() const override;

    std::vector<LogicalRange> divideLogicalRange(const Slice& start_key, size_t len, const Slice& end_key) const override;

private:
    size_t getLengthInRangeCache(const Slice& start_key, const Slice& end_key, size_t remaining_length) const;

    friend class RBTreeSliceVecRangeCacheIterator;
    std::set<SliceVecRange> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
    mutable std::shared_mutex cache_mutex_;

    static const bool concatContinuousRanges = false; // Whether to concatenate continuous ranges at putRange
};

}  // namespace ROCKSDB_NAMESPACE
