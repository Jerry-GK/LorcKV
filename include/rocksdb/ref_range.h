#pragma once

#include <string>
#include <vector>
#include <memory>
#include "rocksdb/slice.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief ReferringRange class is a range representation of key-value pairs, which inner storage
 * is slice which refers to the original data. 
 * Diffenrent from PhysicalRange, it doesn't own the data. So building a ReferringRange does not need data copy.
 * It's only used for generating a range view during range query for LORC to put range.
 * Slices of non-overlapping subranges will called ToString() to dump to PhysicalRange and called PhysicalRange::concatRangesMoved() to merge.
 */
class ReferringRange {
private:
    struct SliceRangeData {
        std::vector<Slice> slice_keys;  // user keys, dump with seq_num to internal keys in dumpSubRange()
        std::vector<Slice> slice_values;
    };
    std::shared_ptr<SliceRangeData> slice_data;
    mutable size_t range_length; // size in length
    mutable size_t keys_byte_size; // size in bytes (key size virtually expanded to internal key size)
    mutable size_t values_byte_size; // size in bytes (value size)
    mutable bool valid;
    mutable SequenceNumber seq_num;

public:
    ReferringRange(bool valid_, SequenceNumber seq_num_);
    ~ReferringRange();      
    // create from string vector
    // deep copy
    ReferringRange(const ReferringRange& other);
    // move copy
    ReferringRange(ReferringRange&& other) noexcept;
    ReferringRange& operator=(const ReferringRange& other);
    ReferringRange& operator=(ReferringRange&& other) noexcept;

    // reserve the range
    void reserve(size_t len);

    // empalce a key-value pair Slice copy
    void emplace(const Slice& key, const Slice& value);

    Slice startKey() const;
    Slice endKey() const;

    Slice keyAt(size_t index) const;
    Slice valueAt(size_t index) const;

    size_t length() const;
    size_t keysByteSize() const;
    size_t valuesByteSize() const;
    bool isValid() const;

    SequenceNumber getSeqNum() const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& key) const;

    std::string toString() const;

    static std::string ToStringPlain(std::string s);
};

}  // namespace ROCKSDB_NAMESPACE
