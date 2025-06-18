#pragma once

#include <string>
#include <vector>
#include <memory>
#include "rocksdb/ref_vec_range.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief SliceVecRange class represents a sorted key-value SliceVecRange in memory
 * It supports SliceVecRange operations like subrange and concatenation
 * with optimized memory management using shared_ptr
 */
class SliceVecRange {
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
    mutable size_t range_length; // size in length
    mutable size_t byte_size; // size in bytes
    mutable bool valid;
    mutable int timestamp;

private:
    SliceVecRange(std::shared_ptr<RangeData> data, size_t length);
    
    // Helper functions for continuous storage management
    void initializeFromReferringRange(const ReferringSliceVecRange& refRange);
    void initializeFromReferringRangeSubset(const ReferringSliceVecRange& refRange, int start_pos, int end_pos);
    void emplaceInternal(const Slice& internal_key, const Slice& value, size_t index);
    void updateValueAt(size_t index, const Slice& new_value) const;
    void rebuildSlices() const;
    void rebuildSlicesAt(size_t index) const;

public:
    SliceVecRange(bool valid = false);
    ~SliceVecRange();      
    // create from string vector
    // DEPRECATED: create a SliceVecRange with a single key-value pair, used for seekx
    // SliceVecRange(Slice startUserKey);
    // deep copy
    SliceVecRange(const SliceVecRange& other);
    // move copy
    SliceVecRange(SliceVecRange&& other) noexcept;
    SliceVecRange& operator=(const SliceVecRange& other);
    SliceVecRange& operator=(SliceVecRange&& other) noexcept;

    // build a SliceVecRange from ReferringSliceVecRange
    static SliceVecRange buildFromReferringSliceVecRange(const ReferringSliceVecRange& refRange);

    // materialize the subrange (startKey, endKey)  to SliceVecRange
    // it's ensured that this->startKey() <= startKey <= endKey <= this->endKey()
    static SliceVecRange dumpSubRangeFromReferringSliceVecRange(const ReferringSliceVecRange& refRange, const Slice& startKey, const Slice& endKey, bool leftIncluded, bool rightIncluded);

    // reserve the range
    void reserve(size_t len);

    const Slice& startUserKey() const; // return in user key format slice, for range cache location
    const Slice& endUserKey() const; // return in user key format slice, for range cache location

    const Slice& startInternalKey() const;
    const Slice& endInternalKey() const;

    const Slice& internalKeyAt(size_t index) const;
    const Slice& userKeyAt(size_t index) const;
    const Slice& valueAt(size_t index) const;

    size_t length() const;
    size_t byteSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    bool update(const Slice& internal_key, const Slice& value) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& key) const;

    std::string toString() const;

    static std::string ToStringPlain(std::string s);

    // DEPRECATED: Merge ordered and non-overlapping ranges without deep copy
    // static SliceVecRange concatRangesMoved(std::vector<SliceVecRange>& ranges);

    // Define comparison operator, sort by startUserKey in ascending order
    bool operator<(const SliceVecRange& other) const {
        return startUserKey() < other.startUserKey();
    }

    static const int internal_key_extra_bytes = 8;
    static const bool enable_async_release = false;
};

}  // namespace ROCKSDB_NAMESPACE
