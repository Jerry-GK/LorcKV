#pragma once

#include <string>
#include <vector>
#include <memory>
#include "rocksdb/slice.h"
#include "rocksdb/vec_range.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief ReferringSliceVecRange class is a range representation of key-value pairs, which inner storage
 * is slice which refers to the original data. 
 * Diffenrent from SliceVecRange, it doesn't own the data. So building a ReferringSliceVecRange does not need data copy.
 * It's only used for generating a range view during range query for LORC to put range.
 * Slices of non-overlapping subranges will called ToString() to dump to SliceVecRange and called SliceVecRange::concatRangesMoved() to merge.
 */
class ReferringSliceVecRange {
private:
    struct SliceRangeData {
        std::vector<Slice> slice_keys;  // user keys, dump with seq_num to internal keys in dumpSubRange()
        std::vector<Slice> slice_values;
    };
    std::shared_ptr<SliceRangeData> slice_data;
    mutable size_t range_length; // size in length
    mutable bool valid;
    mutable SequenceNumber seq_num;

public:
    ReferringSliceVecRange(bool valid_, SequenceNumber seq_num_);
    ~ReferringSliceVecRange();      
    // create from string vector
    // deep copy
    ReferringSliceVecRange(const ReferringSliceVecRange& other);
    // move copy
    ReferringSliceVecRange(ReferringSliceVecRange&& other) noexcept;
    ReferringSliceVecRange& operator=(const ReferringSliceVecRange& other);
    ReferringSliceVecRange& operator=(ReferringSliceVecRange&& other) noexcept;

    // reserve the range
    void reserve(size_t len);

    // empalce a key-value pair Slice copy
    void emplace(const Slice& key, const Slice& value);

    Slice startKey() const;
    Slice endKey() const;

    Slice keyAt(size_t index) const;
    Slice valueAt(size_t index) const;

    size_t length() const;
    bool isValid() const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& key) const;

    // materialize the subrange (startKey, endKey)  to SliceVecRange
    // it's ensured that this->startKey() <= startKey <= endKey <= this->endKey()
    SliceVecRange dumpSubRange(const Slice& startKey, const Slice& endKey, bool leftIncluded, bool rightIncluded) const;

    std::string toString() const;

    static std::string ToStringPlain(std::string s);
};

}  // namespace ROCKSDB_NAMESPACE
