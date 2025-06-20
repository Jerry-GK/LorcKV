#pragma once

#include <string>
#include <vector>
#include <memory>
#include "rocksdb/ref_range.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

enum class PhysicalRangeType {
    CONTINUOUS,
    VEC
};

enum class PhysicalRangeUpdateResult {
    UPDATED,
    INSERTED,
    UNABLE_TO_INSERT,
    OUT_OF_RANGE,
    ERROR
};

/**
 * @brief PhysicalRange abstract base class for sorted key-value ranges in memory
 */
class PhysicalRange {
protected:
    mutable bool valid;
    mutable size_t range_length; // size in length  
    mutable size_t byte_size; // size in bytes (internal keys + values)
    mutable int timestamp;
    
public:
    PhysicalRange(bool valid_ = false) : valid(valid_), range_length(0), byte_size(0), timestamp(0) {}
    virtual ~PhysicalRange() = default;
    
    virtual const Slice& startUserKey() const = 0;
    virtual const Slice& endUserKey() const = 0;
    virtual const Slice& startInternalKey() const = 0;
    virtual const Slice& endInternalKey() const = 0;
    virtual const Slice& internalKeyAt(size_t index) const = 0;
    virtual const Slice& userKeyAt(size_t index) const = 0;
    virtual const Slice& valueAt(size_t index) const = 0;
    virtual PhysicalRangeUpdateResult update(const Slice& internal_key, const Slice& value) const = 0;
    virtual int find(const Slice& key) const = 0;   // find the first index of the key >= the target key, -1 if no such key
    virtual void reserve(size_t len) = 0;
    virtual std::string toString() const = 0;
    
    static std::string ToStringPlain(std::string s) {
        // Only for debug (internal internal_keys)
        std::string result;
        result.reserve(s.size());
        for (size_t i = 0; i < s.size(); ++i) {
            unsigned char c = static_cast<unsigned char>(s[i]);
            if (c < 32 || c > 126) {
                // ignore the non-printable characters
            } else {
                result.push_back(s[i]);
            }
        }
        return result;
    }

    size_t length() const { return range_length; }
    size_t byteSize() const { return byte_size; }
    bool isValid() const { return valid; }
    int getTimestamp() const { return timestamp; }
    void setTimestamp(int timestamp_) const { this->timestamp = timestamp_; }

    // Define comparison operator, sort by startUserKey in ascending order
    bool operator<(const PhysicalRange& other) const {
        return startUserKey() < other.startUserKey();
    }

    static const int internal_key_extra_bytes = 8;
    static const bool enable_async_release = false;
};

}  // namespace ROCKSDB_NAMESPACE
