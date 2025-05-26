#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "rocksdb/ref_vec_range.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

// Only for debug (internal keys)
std::string ReferringSliceVecRange::ToStringPlain(std::string s) {
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

ReferringSliceVecRange::ReferringSliceVecRange(bool valid_, SequenceNumber seq_num_) {
    this->slice_data = std::make_shared<SliceRangeData>();
    this->valid = valid_;
    this->seq_num = seq_num_;
    this->range_length = 0;
    if (valid) {
        slice_data = std::make_shared<SliceRangeData>();
    }
}

ReferringSliceVecRange::~ReferringSliceVecRange() {
    slice_data.reset();
}

ReferringSliceVecRange::ReferringSliceVecRange(const ReferringSliceVecRange& other) {
    // assert(false); // (it should not be called in RBTreeReferringSliceVecRangeCache)
    this->valid = other.valid;
    this->seq_num = other.seq_num;
    this->range_length = other.range_length;
    
    if (other.valid && other.slice_data) {
        this->slice_data = std::make_shared<SliceRangeData>();
        this->slice_data->slice_keys = other.slice_data->slice_keys;
        this->slice_data->slice_values = other.slice_data->slice_values;
    }
}

ReferringSliceVecRange::ReferringSliceVecRange(ReferringSliceVecRange&& other) noexcept {
    slice_data = std::move(other.slice_data);
    valid = other.valid;
    seq_num = other.seq_num;
    range_length = other.range_length;

    other.valid = false;
    other.range_length = 0;
}

ReferringSliceVecRange& ReferringSliceVecRange::operator=(const ReferringSliceVecRange& other) {
    // RBTreeReferringSliceVecRangeCache should never call this
    // assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->seq_num = other.seq_num;
        this->range_length = other.range_length;
        
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

ReferringSliceVecRange& ReferringSliceVecRange::operator=(ReferringSliceVecRange&& other) noexcept {
    if (this != &other) {
        // Clean up current object's resources
        slice_data.reset();
        
        // Move resources from other
        slice_data = std::move(other.slice_data);
        valid = other.valid;
        seq_num = other.seq_num;
        range_length = other.range_length;
        
        // Reset other object's state
        other.valid = false;
        other.range_length = 0;
    }
    return *this;
}

Slice ReferringSliceVecRange::startKey() const {
    assert(valid && range_length > 0 && slice_data->slice_keys.size() > 0);
    return Slice(slice_data->slice_keys[0]);
}

Slice  ReferringSliceVecRange::endKey() const {
    assert(valid && range_length > 0 && slice_data->slice_keys.size() > 0);
    return Slice(slice_data->slice_keys[range_length - 1]);
}   

Slice ReferringSliceVecRange::keyAt(size_t index) const {
    assert(valid && range_length > index);
    return Slice(slice_data->slice_keys[index]);
}

Slice ReferringSliceVecRange::valueAt(size_t index) const {
    assert(valid && range_length > index);
    return Slice(slice_data->slice_values[index]);
}

size_t ReferringSliceVecRange::length() const {
    return range_length;
}   

bool ReferringSliceVecRange::isValid() const {
    return valid;
}

int ReferringSliceVecRange::find(const Slice& key) const {
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

std::string ReferringSliceVecRange::toString() const {
    std::string str = "< " + ToStringPlain(this->startKey().ToString()) + " -> " + ToStringPlain(this->endKey().ToString()) + " >";
    return str;
}

void ReferringSliceVecRange::reserve(size_t len) {
    assert(valid);
    slice_data->slice_keys.reserve(len);
    slice_data->slice_values.reserve(len);
}

void ReferringSliceVecRange::emplace(const Slice& key, const Slice& value) {
    assert(valid);
    slice_data->slice_keys.emplace_back(key);
    slice_data->slice_values.emplace_back(value);
    range_length++;
}

SliceVecRange ReferringSliceVecRange::dumpSubRange(const Slice& startKey, const Slice& endKey, bool leftIncluded, bool rightIncluded) const {
    assert(valid && range_length > 0 && this->startKey() <= startKey && startKey <= endKey && endKey <= this->endKey());
    
    // find the start position of the subrange
    int start_pos = find(startKey);
    if (start_pos < 0 || (size_t)start_pos >= range_length) {
        assert(false);
        return SliceVecRange(false);
    }
    
    if (keyAt(start_pos) == startKey && !leftIncluded) {
        start_pos++;
    }
    
    int end_pos = find(endKey);
    if (end_pos < 0) {
        assert(false);
        return SliceVecRange(false);
    }
    
    if (keyAt(end_pos) == endKey) {
        if (!rightIncluded) {
            end_pos--;
        }
    } else {
        end_pos--;
    }
    
    if (start_pos > end_pos || (size_t)start_pos >= range_length || end_pos < 0) {
        assert(false);
        return SliceVecRange(false);
    }
    
    SliceVecRange result(true);
    size_t num_elements = end_pos - start_pos + 1;
    result.reserve(num_elements);
    
    // actually copy the subrange data
    for (int i = start_pos; i <= end_pos; i++) {
        std::string internal_key_str = InternalKey(keyAt(i), seq_num, kTypeValue).Encode().ToString();
        result.emplace(std::move(internal_key_str), valueAt(i));
    }
    
    return result;
}

}  // namespace ROCKSDB_NAMESPACE
