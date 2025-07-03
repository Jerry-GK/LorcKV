#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "rocksdb/ref_range.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

// Only for debug (internal keys)
std::string ReferringRange::ToStringPlain(std::string s) {
    std::string result;
    result.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        if (c < 32 || c > 126) {
            // ignore the non-printable characters
            // char hex[5];
            // snprintf(hex, sizeof(hex), "\\x%02x", c);
            // result.append(hex);
        } else {
            result.push_back(s[i]);
        }
    }
    return result;
}

ReferringRange::ReferringRange(bool valid_, SequenceNumber seq_num_) {
    this->slice_data = std::make_shared<SliceRangeData>();
    this->valid = valid_;
    this->seq_num = seq_num_;
    this->range_length = 0;
    this->keys_byte_size = 0;
    this->values_byte_size = 0;
    if (valid) {
        slice_data = std::make_shared<SliceRangeData>();
    }
}

ReferringRange::~ReferringRange() {
    slice_data.reset();
}

ReferringRange::ReferringRange(const ReferringRange& other) {
    // assert(false); // (it should not be called in RBTreeReferringLogicalOrderedRangeCache)
    this->valid = other.valid;
    this->seq_num = other.seq_num;
    this->range_length = other.range_length;
    this->keys_byte_size = other.keys_byte_size;
    this->values_byte_size = other.values_byte_size;
    
    if (other.valid && other.slice_data) {
        this->slice_data = std::make_shared<SliceRangeData>();
        this->slice_data->slice_keys = other.slice_data->slice_keys;
        this->slice_data->slice_values = other.slice_data->slice_values;
    }
}

ReferringRange::ReferringRange(ReferringRange&& other) noexcept {
    slice_data = std::move(other.slice_data);
    valid = other.valid;
    seq_num = other.seq_num;
    range_length = other.range_length;
    keys_byte_size = other.keys_byte_size;
    values_byte_size = other.values_byte_size;

    other.valid = false;
    other.range_length = 0;
}

ReferringRange& ReferringRange::operator=(const ReferringRange& other) {
    // RBTreeReferringLogicalOrderedRangeCache should never call this
    // assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->seq_num = other.seq_num;
        this->range_length = other.range_length;
        this->keys_byte_size = other.keys_byte_size;
        this->values_byte_size = other.values_byte_size;

        if (other.valid && other.slice_data) {
            this->slice_data = std::make_shared<SliceRangeData>();
            this->slice_data->slice_keys = other.slice_data->slice_keys;
            this->slice_data->slice_values = other.slice_data->slice_values;
        } else {
            this->slice_data.reset();
        }
    }
    return *this;
}

ReferringRange& ReferringRange::operator=(ReferringRange&& other) noexcept {
    if (this != &other) {
        // Clean up current object's resources
        slice_data.reset();
        
        // Move resources from other
        slice_data = std::move(other.slice_data);
        valid = other.valid;
        seq_num = other.seq_num;
        range_length = other.range_length;
        keys_byte_size = other.keys_byte_size;
        values_byte_size = other.values_byte_size;
        
        // Reset other object's state
        other.valid = false;
        other.range_length = 0;
        other.seq_num = 0;
        other.keys_byte_size = 0;
        other.values_byte_size = 0;
    }
    return *this;
}

Slice ReferringRange::startKey() const {
    assert(valid && range_length > 0 && slice_data->slice_keys.size() > 0);
    return Slice(slice_data->slice_keys[0]);
}

Slice  ReferringRange::endKey() const {
    assert(valid && range_length > 0 && slice_data->slice_keys.size() > 0);
    return Slice(slice_data->slice_keys[range_length - 1]);
}   

Slice ReferringRange::keyAt(size_t index) const {
    assert(valid && range_length > index);
    return Slice(slice_data->slice_keys[index]);
}

Slice ReferringRange::valueAt(size_t index) const {
    assert(valid && range_length > index);
    return Slice(slice_data->slice_values[index]);
}

size_t ReferringRange::length() const {
    return range_length;
}   

size_t ReferringRange::keysByteSize() const {
    return keys_byte_size;
}

size_t ReferringRange::valuesByteSize() const {
    return values_byte_size;
}

bool ReferringRange::isValid() const {
    return valid;
}

SequenceNumber ReferringRange::getSeqNum() const {
    return seq_num;
}

int ReferringRange::find(const Slice& key) const {
    assert(valid && range_length > 0);
    if (!valid || range_length == 0) {
        return -1;
    }
    
    int left = 0;
    int right = range_length - 1;
    
    while (left <= right) {
        int mid = left + (right - left) / 2;
        Slice mid_key = keyAt(mid);
        if (mid_key == key) {
            return mid;
        } else if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    
    if ((size_t)left >= range_length) {
        return -1;
    }
    return left;
}

std::string ReferringRange::toString() const {
    std::string str = "< " + ToStringPlain(this->startKey().ToString()) + " -> " + ToStringPlain(this->endKey().ToString()) + " >";
    return str;
}

void ReferringRange::reserve(size_t len) {
    assert(valid);
    slice_data->slice_keys.reserve(len);
    slice_data->slice_values.reserve(len);
}

void ReferringRange::emplace(const Slice& key, const Slice& value) {
    assert(valid);
    slice_data->slice_keys.emplace_back(key);
    slice_data->slice_values.emplace_back(value);
    range_length++;
    keys_byte_size += key.size() + 8; // key size virtually expanded to internal key size (user key size + 8)
    values_byte_size += value.size();
}

}  // namespace ROCKSDB_NAMESPACE
