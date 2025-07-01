#pragma once

#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include "rocksdb/physical_range.h"
#include "rocksdb/ref_range.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief ContinuousPhysicalRange class represents a sorted key-value range in memory
 * with optimized continuous memory storage
 */
class ContinuousPhysicalRange : public PhysicalRange {
private:
    struct RangeData {
        // Continuous storage for keys and values
        std::unique_ptr<char[]> keys_buffer;
        std::unique_ptr<char[]> values_buffer;
        size_t keys_buffer_size;
        size_t values_buffer_size;
        
        // Offset arrays to track position and size of each entry
        std::vector<size_t> key_offsets;
        std::vector<size_t> key_sizes;
        std::vector<size_t> value_offsets;
        std::vector<size_t> value_sizes;
        std::vector<size_t> original_value_sizes; // Track original value sizes for overflow management
        
        // For values that exceed original size, store as separate strings
        std::vector<std::unique_ptr<std::string>> overflow_values;
        std::vector<bool> is_overflow; // Track which values are overflow
        
        // Slice vectors for fast access
        std::vector<Slice> internal_key_slices;
        std::vector<Slice> user_key_slices;
        std::vector<Slice> value_slices;
    };
    std::shared_ptr<RangeData> data;
    mutable std::shared_mutex physical_range_mutex_;

private:
    ContinuousPhysicalRange(std::shared_ptr<RangeData> data, size_t length);
    
    // Helper functions for continuous storage management
    void initializeFromReferringRange(const ReferringRange& refRange);
    void initializeFromReferringRangeSubset(const ReferringRange& refRange, int start_pos, int end_pos);
    void emplaceInternal(const Slice& internal_key, const Slice& value, size_t index);
    void updateValueAt(size_t index, const Slice& new_value) const;
    void rebuildSlices() const;
    void rebuildSlicesAt(size_t index) const;
    
    // Internal functions without locking for internal use
    const Slice& userKeyAtInternal(size_t index) const;
    int findInternal(const Slice& key) const;

public:
    ContinuousPhysicalRange(bool valid = false);
    ~ContinuousPhysicalRange() override;
    
    // Copy and move constructors/operators
    ContinuousPhysicalRange(const ContinuousPhysicalRange& other);
    ContinuousPhysicalRange(ContinuousPhysicalRange&& other) noexcept;
    ContinuousPhysicalRange& operator=(const ContinuousPhysicalRange& other);
    ContinuousPhysicalRange& operator=(ContinuousPhysicalRange&& other) noexcept;

    // Static factory functions
    static std::unique_ptr<ContinuousPhysicalRange> buildFromReferringRange(const ReferringRange& refRange);

    // Override pure virtual functions from PhysicalRange
    const Slice& startUserKey() const override;
    const Slice& endUserKey() const override;
    const Slice& startInternalKey() const override;
    const Slice& endInternalKey() const override;
    const Slice& internalKeyAt(size_t index) const override;
    const Slice& userKeyAt(size_t index) const override;
    const Slice& valueAt(size_t index) const override;
    PhysicalRangeUpdateResult update(const Slice& internal_key, const Slice& value) const override;
    int find(const Slice& key) const override;
    void reserve(size_t len) override;
    std::string toString() const override;
};

}  // namespace ROCKSDB_NAMESPACE
