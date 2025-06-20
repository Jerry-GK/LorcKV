#pragma once

#include <set>
#include <map>
#include <string>
#include <shared_mutex>
#include "rocksdb/continuous_physical_range.h"
#include "rocksdb/lorc.h"
#include "rocksdb/physical_range.h"
#include "rocksdb/vec_physical_range.h"

namespace ROCKSDB_NAMESPACE {

class RBTreeRangeCacheIterator;
class LogicallyOrderedRangeCacheIterator;
class Arena;

// enum


// Custom comparator for heterogeneous lookup
struct PhysicalRangeComparator {
    using is_transparent = void; // Enable heterogeneous lookup
    
    bool operator()(const std::unique_ptr<PhysicalRange>& lhs, const std::unique_ptr<PhysicalRange>& rhs) const {
        return lhs->startUserKey() < rhs->startUserKey();
    }
    
    bool operator()(const std::unique_ptr<PhysicalRange>& lhs, const Slice& rhs) const {
        return lhs->startUserKey() < rhs;
    }
    
    bool operator()(const Slice& lhs, const std::unique_ptr<PhysicalRange>& rhs) const {
        return lhs < rhs->startUserKey();
    }
    
    bool operator()(const std::unique_ptr<PhysicalRange>& lhs, const std::string& rhs) const {
        return lhs->startUserKey() < Slice(rhs);
    }
    
    bool operator()(const std::string& lhs, const std::unique_ptr<PhysicalRange>& rhs) const {
        return Slice(lhs) < rhs->startUserKey();
    }
};

/**
 * RBTreeLogicallyOrderedRangeCache: A cache implementation using Red-Black Tree to store PhysicalRange data
 * Uses ordered containers to maintain ranges sorted by their start keys and lengths
 */
class RBTreeLogicallyOrderedRangeCache : public LogicallyOrderedRangeCache {
public:
    RBTreeLogicallyOrderedRangeCache(size_t capacity, LorcLogger::Level logger_level_ = LorcLogger::Level::DISABLE, PhysicalRangeType physical_range_type_ = PhysicalRangeType::VEC);
    ~RBTreeLogicallyOrderedRangeCache() override;

    void putOverlappingRefRange(ReferringRange&& newRefRange) override;
    void putGapPhysicalRange(ReferringRange&& newRefRange, bool leftConcat, bool rightConcat, bool emptyConcat, std::string emptyConcatLeftKey, std::string emptyConcatRightKey) override;
    bool updateEntry(const Slice& key, const Slice& value) override;
    void victim() override;
    void tryVictim() override;
    
    LogicallyOrderedRangeCacheIterator* newLogicallyOrderedRangeCacheIterator(Arena* arena) const override;

    /**
     * Update the timestamp of a PhysicalRange to mark it as recently used.
     */
    void pinRange(std::string startKey);

    void printAllRangesWithKeys() const override;
        
    void printAllPhysicalRanges() const override;

    void printAllLogicalRanges() const override;

    std::vector<LogicalRange> divideLogicalRange(const Slice& start_key, size_t len, const Slice& end_key) const override;

private:
    // Downward estimate data can be read from range cache (to avoid pre-division too many ranges)
    size_t downwardEstimateLengthInRangeCache(const Slice& start_key, const Slice& end_key, size_t remaining_length) const;

    friend class RBTreeLogicallyOrderedRangeCacheIterator;
    std::set<std::unique_ptr<PhysicalRange>, PhysicalRangeComparator> orderedRanges;     // Container for ranges sorted by start key
    std::multimap<int, std::string> lengthMap;  // Container for ranges sorted by length (for victim selection)
    uint64_t cache_timestamp;          // Timestamp for LRU-like functionality
    mutable std::shared_mutex cache_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
