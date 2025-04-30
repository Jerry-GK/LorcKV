#include <iostream>
#include <cassert>
#include <algorithm> 
#include "SliceVecRange.h"
#include "../logger/Logger.h"

SliceVecRange::SliceVecRange(bool valid) {
    this->valid = valid;
    this->size = 0;
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
    this->size = other.size;
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
    size = other.size;
    subrange_view_start_pos = other.subrange_view_start_pos;
    timestamp = other.timestamp;
    
    other.valid = false;
    other.size = 0;
    other.subrange_view_start_pos = -1;
    other.timestamp = 0;
}

// SliceVecRange::SliceVecRange(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size) {
//     data = std::make_shared<RangeData>();
//     data->keys = std::move(keys);
//     data->values = std::move(values);
//     this->subrange_view_start_pos = -1;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

SliceVecRange::SliceVecRange(Slice startKey) {
    data = std::make_shared<RangeData>();
    data->keys.push_back(startKey.ToString());
    data->values.push_back("");
    this->subrange_view_start_pos = -1;
    this->valid = true;
    this->size = 1;
    this->timestamp = 0;
}

SliceVecRange::SliceVecRange(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size) {
    this->data = data;
    this->subrange_view_start_pos = subrange_view_start_pos;
    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

SliceVecRange& SliceVecRange::operator=(const SliceVecRange& other) {
    // RBTreeSliceVecRangeCache should never call this
    // assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->size = other.size;
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
        size = other.size;
        subrange_view_start_pos = other.subrange_view_start_pos;
        timestamp = other.timestamp;
        
        // Reset other object's state
        other.valid = false;
        other.size = 0;
        other.subrange_view_start_pos = -1;
        other.timestamp = 0;
    }
    return *this;
}

Slice SliceVecRange::startKey() const {
    assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys[0]);
}

Slice  SliceVecRange::endKey() const {
    assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys[size - 1]);
}   

Slice SliceVecRange::keyAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    return Slice(data->keys[index]);
}

Slice SliceVecRange::valueAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    return Slice(data->values[index]);
}

size_t SliceVecRange::getSize() const {
    return size;
}   

bool SliceVecRange::isValid() const {
    return valid;
}

int SliceVecRange::getTimestamp() const {
    return timestamp;
}

void SliceVecRange::setTimestamp(int timestamp) const {
    this->timestamp = timestamp;
}

bool SliceVecRange::update(const Slice& key, const Slice& value) const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);    
    int index = find(key);
    if (index >= 0 && keyAt(index) == key) {
        data->values[index] = value.ToString();
        return true;
    }
    return false;
}

SliceVecRange SliceVecRange::subRangeView(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index && start_index >= 0 && subrange_view_start_pos == -1);
    // the only code to create non-negative subrange_view_start_pos
    return SliceVecRange(data, start_index, end_index - start_index + 1);
}

void SliceVecRange::truncate(int length) const {
    assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
    if (length < 0 || length > size) {
        Logger::error("Invalid length to truncate");
        return;
    }
    
    data->keys.resize(length);
    data->values.resize(length);
    size = length;
}

int SliceVecRange::find(const Slice& key) const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    if (!valid || size == 0) {
        return -1;
    }
    
    int left = 0;
    int right = size - 1;
    
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
    
    if (left >= size) {
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
    
    size_t total_size = 0;
    for (const auto& SliceVecRange : ranges) {
        total_size += SliceVecRange.getSize();
    }

    auto new_data = std::make_shared<RangeData>();
    new_data->keys.reserve(total_size);
    new_data->values.reserve(total_size);

    // Move data from each SliceVecRange into the new combined SliceVecRange
    for (auto& SliceVecRange : ranges) {
        auto src_data = SliceVecRange.data; 
        int subrange_view_start_pos = SliceVecRange.subrange_view_start_pos;
        int range_start = 0;
        if (subrange_view_start_pos != -1) {
            range_start = subrange_view_start_pos;
        }
        size_t len = SliceVecRange.getSize();
        
        new_data->keys.insert(new_data->keys.end(),
            std::make_move_iterator(src_data->keys.begin() + range_start),
            std::make_move_iterator(src_data->keys.begin() + range_start + len));
        new_data->values.insert(new_data->values.end(),
            std::make_move_iterator(src_data->values.begin() + range_start),
            std::make_move_iterator(src_data->values.begin() + range_start + len));
    }

    return SliceVecRange(new_data, -1, total_size);
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
    size++;
}