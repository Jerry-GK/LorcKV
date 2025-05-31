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
        std::vector<std::string> internal_keys;  // internal keys actually, but internal seq number is not participated in comparasion
        std::vector<std::string> values;
    };
    std::shared_ptr<RangeData> data;
    mutable size_t range_length; // size in length
    mutable size_t byte_size; // size in bytes
    mutable bool valid;
    mutable int timestamp;

    private:
    SliceVecRange(std::shared_ptr<RangeData> data, size_t length);

public:
    SliceVecRange(bool valid = false);
    ~SliceVecRange();      
    // create from string vector
    // create a SliceVecRange with a single key-value pair, used for seekx
    SliceVecRange(Slice startUserKey);
    // deep copy
    SliceVecRange(const SliceVecRange& other);
    // move copy
    SliceVecRange(SliceVecRange&& other) noexcept;
    SliceVecRange& operator=(const SliceVecRange& other);
    SliceVecRange& operator=(SliceVecRange&& other) noexcept;

    // reserve the range
    void reserve(size_t len);

    // empalce a key-value pair Slice copy
    void emplace(const Slice& internal_key, const Slice& value);

    // empalce a key-value pair string in a moved pattern
    void emplaceMoved(std::string& internal_key, std::string& value);

    Slice startUserKey() const; // return in user key format slice, for range cache location
    Slice endUserKey() const; // return in user key format slice, for range cache location

    Slice startInternalKey() const;
    Slice endInternalKey() const;

    Slice internalKeyAt(size_t index) const;
    Slice userKeyAt(size_t index) const;
    Slice valueAt(size_t index) const;

    size_t length() const;
    size_t byteSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    bool update(const Slice& internal_key, const Slice& value) const;

    // truncate in place
    void truncate(int length) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& internal_key) const;

    std::string toString() const;

    static std::string ToStringPlain(std::string s);

    // Merge ordered and non-overlapping ranges without deep copy
    static SliceVecRange concatRangesMoved(std::vector<SliceVecRange>& ranges);

    // Define comparison operator, sort by startUserKey in ascending order
    bool operator<(const SliceVecRange& other) const {
        return startUserKey() < other.startUserKey();
    }

    static const int internal_key_extra_bytes = 8;
    static const bool enable_async_release = false;
    static const size_t kAsyncReleaseThreshold = 16 * 1024 * 1024; // Threshold for asynchronous resource release (in bytes)
};

}  // namespace ROCKSDB_NAMESPACE
