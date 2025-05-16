#include <cassert>
#include <algorithm> 
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

SliceVecRange::SliceVecRange(bool valid_) {
    this->data = std::make_shared<RangeData>();
    this->valid = valid;
    this->range_length = 0;
    this->byte_size = 0;
    this->timestamp = 0;
    this->subrange_view_start_pos = -1;
    if (valid) {
        data = std::make_shared<RangeData>();
    }
}

SliceVecRange::~SliceVecRange() {
    data.reset();
}

SliceVecRange::SliceVecRange(const SliceVecRange& other) {
    // assert(false); // (it should not be called in RBTreeSliceVecRangeCache)
    this->valid = other.valid;
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;
    this->subrange_view_start_pos = other.subrange_view_start_pos;
    
    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        this->data->keys = other.data->keys;
        this->data->values = other.data->values;
    }
}

SliceVecRange::SliceVecRange(SliceVecRange&& other) noexcept {
    data = std::move(other.data);
    valid = other.valid;
    range_length = other.range_length;
    byte_size = other.byte_size;
    subrange_view_start_pos = other.subrange_view_start_pos;
    timestamp = other.timestamp;
    
    other.valid = false;
    other.range_length = 0;
    other.subrange_view_start_pos = -1;
    other.timestamp = 0;
}

// SliceVecRange::SliceVecRange(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t range_length) {
//     data = std::make_shared<RangeData>();
//     data->keys = std::move(keys);
//     data->values = std::move(values);
//     this->subrange_view_start_pos = -1;
//     this->valid = true;
//     this->range_length = range_length;
//     this->timestamp = 0;
// }

SliceVecRange::SliceVecRange(Slice startKey) {
    data = std::make_shared<RangeData>();
    data->keys.push_back(startKey.ToString());
    data->values.push_back("");
    this->subrange_view_start_pos = -1;
    this->valid = true;
    this->range_length = 1;
    this->byte_size = startKey.size();
    this->timestamp = 0;
}

SliceVecRange::SliceVecRange(std::shared_ptr<RangeData> data_, int subrange_view_start_pos_, size_t size_) {
    this->data = data_;
    this->subrange_view_start_pos = subrange_view_start_pos_;
    this->valid = true;
    this->range_length = size_;
    this->byte_size = 0; // it's no meaning to calculate the byte size in a subrange view
    this->timestamp = 0;
}

SliceVecRange& SliceVecRange::operator=(const SliceVecRange& other) {
    // RBTreeSliceVecRangeCache should never call this
    // assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->range_length = other.range_length;
        this->byte_size = other.byte_size;
        this->timestamp = other.timestamp;
        this->subrange_view_start_pos = other.subrange_view_start_pos;
        
        if (other.valid && other.data) {
            this->data = std::make_shared<RangeData>();
            this->data->keys = other.data->keys;
            this->data->values = other.data->values;
        } else {
            this->data.reset();
        }
    }
    return *this;
}

SliceVecRange& SliceVecRange::operator=(SliceVecRange&& other) noexcept {
    if (this != &other) {
        // Clean up current object's resources
        data.reset();
        
        // Move resources from other
        data = std::move(other.data);
        valid = other.valid;
        range_length = other.range_length;
        byte_size = other.byte_size;
        subrange_view_start_pos = other.subrange_view_start_pos;
        timestamp = other.timestamp;
        
        // Reset other object's state
        other.valid = false;
        other.range_length = 0;
        other.subrange_view_start_pos = -1;
        other.timestamp = 0;
    }
    return *this;
}

Slice SliceVecRange::startKey() const {
    assert(valid && range_length > 0 && data->keys.length() > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys[0]);
}

Slice  SliceVecRange::endKey() const {
    assert(valid && range_length > 0 && data->keys.length() > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys[range_length - 1]);
}   

Slice SliceVecRange::keyAt(size_t index) const {
    assert(valid && range_length > index && subrange_view_start_pos == -1);
    return Slice(data->keys[index]);
}

Slice SliceVecRange::valueAt(size_t index) const {
    assert(valid && range_length > index && subrange_view_start_pos == -1);
    return Slice(data->values[index]);
}

size_t SliceVecRange::length() const {
    return range_length;
}   

size_t SliceVecRange::byteSize() const {
    return byte_size;
}   

bool SliceVecRange::isValid() const {
    return valid;
}

int SliceVecRange::getTimestamp() const {
    return timestamp;
}

void SliceVecRange::setTimestamp(int timestamp_) const {
    this->timestamp = timestamp_;
}

bool SliceVecRange::update(const Slice& key, const Slice& value) const {
    assert(valid && range_length > 0 && subrange_view_start_pos == -1);    
    int index = find(key);
    if (index >= 0 && keyAt(index) == key) {
        data->values[index] = value.ToString();
        return true;
    }
    return false;
}

SliceVecRange SliceVecRange::subRangeView(size_t start_index, size_t end_index) const {
    assert(valid && range_length > end_index && start_index >= 0 && subrange_view_start_pos == -1);
    // the only code to create non-negative subrange_view_start_pos
    return SliceVecRange(data, start_index, end_index - start_index + 1);
}

void SliceVecRange::truncate(int targetLength) const {
    assert(valid && range_length > 0 && data->keys.length() > 0 && subrange_view_start_pos == -1);
    if (targetLength < 0 || (size_t)targetLength > range_length) {
        assert(false);
        return;
    }
    
    data->keys.resize(targetLength);
    data->values.resize(targetLength);
    range_length = targetLength;
    byte_size = 0;
    for (size_t i = 0; i < range_length; i++) {
        byte_size += data->keys[i].size();
    }
}

int SliceVecRange::find(const Slice& key) const {
    assert(valid && range_length > 0 && subrange_view_start_pos == -1);
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

std::string SliceVecRange::toString() const {
    std::string str = "< " + this->startKey().ToString() + " -> " + this->endKey().ToString() + " >";
    return str;
}

// Function to combine ordered and non-overlapping ranges with move semantics
SliceVecRange SliceVecRange::concatRangesMoved(std::vector<SliceVecRange>& ranges) {
    if (ranges.empty()) {
        return SliceVecRange(false);
    }
    
    // TODO(jr): estimate the total size more accurately
    size_t total_length = 0;
    for (const auto& sliceVecRange : ranges) {
        total_length += sliceVecRange.length();
    }
    
    auto new_data = std::make_shared<RangeData>();
    new_data->keys.reserve(total_length);
    new_data->values.reserve(total_length);

    // Move data from each SliceVecRange into the new combined SliceVecRange
    for (auto& sliceVecRange : ranges) {
        auto src_data = sliceVecRange.data; 
        int subrange_view_start_pos = sliceVecRange.subrange_view_start_pos;
        int range_start = 0;
        if (subrange_view_start_pos != -1) {
            range_start = subrange_view_start_pos;
        }
        size_t len = sliceVecRange.length();
        
        // Using std::make_move_iterator to move strings from source containers to new container
        // This operation "steals" content from source strings, leaving them as empty strings
        // Even if source SliceVecRange objects are destroyed later, the moved data is safely transferred
        // This avoids data copying, improves performance, and ensures proper data lifetime management
        new_data->keys.insert(new_data->keys.end(),
            std::make_move_iterator(src_data->keys.begin() + range_start),
            std::make_move_iterator(src_data->keys.begin() + range_start + len));
        new_data->values.insert(new_data->values.end(),
            std::make_move_iterator(src_data->values.begin() + range_start),
            std::make_move_iterator(src_data->values.begin() + range_start + len));
    }

    return SliceVecRange(new_data, -1, total_length);
}

// build methods
void SliceVecRange::reserve(size_t len) {
    assert(valid);
    data->keys.reserve(len);
    data->values.reserve(len);
}

void SliceVecRange::emplace(const Slice& key, const Slice& value) {
    assert(valid && subrange_view_start_pos == -1);
    data->keys.emplace_back(key.ToString());
    data->values.emplace_back(value.ToString());
    range_length++;
    byte_size += key.size() + value.size();
}

void SliceVecRange::emplaceMoved(std::string& key, std::string& value) {
    assert(valid && subrange_view_start_pos == -1);
    data->keys.emplace_back(std::move(key));
    data->values.emplace_back(std::move(value));
    range_length++;
    byte_size += data->keys.back().size() + data->values.back().size();
}

}  // namespace ROCKSDB_NAMESPACE
