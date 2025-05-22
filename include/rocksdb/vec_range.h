#pragma once

#include <string>
#include <vector>
#include <memory>
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
        std::vector<std::string> keys;
        std::vector<std::string> values;
    };
    std::shared_ptr<RangeData> data;
    mutable size_t range_length; // size in length
    mutable size_t byte_size; // size in bytes
    mutable bool valid;
    mutable int timestamp;

    // Start position for subrange operations (default -1). If >= 0, indicates this is a subrange view, and only used in concatRangesMoved
    int subrange_view_start_pos; 

    private:
    // construct by data pointer and subrange_view_start_pos, it's a subrange view if subrange_view_start_pos >= 0
    SliceVecRange(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t length);

public:
    SliceVecRange(bool valid = false);
    ~SliceVecRange();      
    // create from string vector
    // create a SliceVecRange with a single key-value pair, used for seekx
    SliceVecRange(Slice startKey);
    // deep copy
    SliceVecRange(const SliceVecRange& other);
    // move copy
    SliceVecRange(SliceVecRange&& other) noexcept;
    SliceVecRange& operator=(const SliceVecRange& other);
    SliceVecRange& operator=(SliceVecRange&& other) noexcept;

    // reserve the range
    void reserve(size_t len);

    // empalce a key-value pair Slice copy
    void emplace(const Slice& key, const Slice& value);

    // empalce a key-value pair string in a moved pattern
    void emplaceMoved(std::string& key, std::string& value);

    Slice startKey() const;
    Slice endKey() const;

    Slice keyAt(size_t index) const;
    Slice valueAt(size_t index) const;

    size_t length() const;
    size_t byteSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    bool update(const Slice& key, const Slice& value) const;

    // the origin SliceVecRange will be invalid after this operation (turn to a temp subrange view only for concatRangesMoved)
    SliceVecRange subRangeView(size_t start_index, size_t end_index) const;

    // truncate in place
    void truncate(int length) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& key) const;

    std::string toString() const;

    static std::string ToStringPlain(std::string s);

    // Merge ordered and non-overlapping ranges without deep copy
    static SliceVecRange concatRangesMoved(std::vector<SliceVecRange>& ranges);

    // Define comparison operator, sort by startKey in ascending order
    bool operator<(const SliceVecRange& other) const {
        return startKey() < other.startKey();
    }
};

}  // namespace ROCKSDB_NAMESPACE
