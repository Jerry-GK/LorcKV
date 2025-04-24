#include "Range.h"
#include "Logger.h"
#include <iostream>
#include <cassert>
#include <algorithm> 

Range::Range(bool valid) {
    this->valid = valid;
    this->size = 0;
    this->timestamp = 0;
}

Range::Range(const Range& other) {
    // assert(false);
    this->valid = other.valid;
    this->size = other.size;
    this->timestamp = other.timestamp;
    this->start_pos = other.start_pos;
    
    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        this->data->keys = other.data->keys;
        this->data->values = other.data->values;
    }
}

Range::Range(Range&& other) noexcept {
    data = std::move(other.data);
    valid = other.valid;
    size = other.size;
    start_pos = other.start_pos;
    timestamp = other.timestamp;
    
    other.valid = false;
    other.size = 0;
    other.start_pos = 0;
    other.timestamp = 0;
}

// Range::Range(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size) {
//     data = std::make_shared<RangeData>();
//     data->keys = keys;
//     data->values = values;
//     this->start_pos = 0;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

Range::Range(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size) {
    data = std::make_shared<RangeData>();
    data->keys = std::move(keys);
    data->values = std::move(values);
    this->start_pos = 0;
    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

Range::Range(std::shared_ptr<RangeData> data, size_t start, size_t size) {
    this->data = data;
    this->start_pos = start;
    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

Range::~Range() {
    data.reset();
}

Range& Range::operator=(const Range& other) {
    // RBTreeRangeCache should never call this
    assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->size = other.size;
        this->timestamp = other.timestamp;
        this->start_pos = other.start_pos;
        
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

Range& Range::operator=(Range&& other) noexcept {
    if (this != &other) {
        // 确保清理当前对象的资源
        data.reset();
        
        // 移动资源
        data = std::move(other.data);
        valid = other.valid;
        size = other.size;
        start_pos = other.start_pos;
        timestamp = other.timestamp;
        
        // 重置其他对象状态
        other.valid = false;
        other.size = 0;
        other.start_pos = 0;
        other.timestamp = 0;
    }
    return *this;
}

std::string& Range::startKey() const {
    assert(valid && size > 0 && data->keys.size() > 0 && start_pos == 0);
    return data->keys[start_pos];
}

std::string& Range::endKey() const {
    assert(valid && size > 0 && data->keys.size() > 0 && start_pos == 0);
    return data->keys[start_pos + size - 1];
}   

std::string& Range::keyAt(size_t index) const {
    assert(valid && size > index && start_pos == 0);
    return data->keys[start_pos + index];
}

std::string& Range::valueAt(size_t index) const {
    assert(valid && size > index && start_pos == 0);
    return data->values[start_pos + index];
}

std::vector<std::string>& Range::getKeys() const {
    assert(valid && size > 0 && data->keys.size() > 0 && start_pos == 0);
    return data->keys;
}

std::vector<std::string>& Range::getValues() const {
    assert(valid && size > 0 && data->values.size() > 0 && start_pos == 0);
    return data->values;
}

size_t Range::getSize() const {
    return size;
}   

bool Range::isValid() const {
    return valid;
}

int Range::getTimestamp() const {
    return timestamp;
}

void Range::setTimestamp(int timestamp) const {
    this->timestamp = timestamp;
}

Range Range::subRange(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index && data->keys.size() - start_pos > end_index && data->values.size() - start_pos > end_index);
    
    // a subrange copy of the underlying data
    std::vector<std::string> sub_keys(
        data->keys.begin() + start_index,
        data->keys.begin() + end_index + 1
    );
    std::vector<std::string> sub_values(
        data->values.begin() + start_index,
        data->values.begin() + end_index + 1
    );
    
    return Range(std::move(sub_keys), std::move(sub_values), end_index - start_index + 1);
}

// the origin range will be invalid after this operation (turn to a temp subrange)
Range Range::subRangeMoved(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index);
    return Range(data, start_pos + start_index, end_index - start_index + 1);
}

// return the largest subrange in [start_key, end_key]
Range Range::subRange(std::string start_key, std::string end_key) const {
    // use binary search to find the start and end index
    assert(valid && size > 0 && data->keys.size() > 0);
    // auto start_it = std::lower_bound(keys.begin(), keys.end(), start_key);
    // auto end_it = std::upper_bound(keys.begin(), keys.end(), end_key);

    size_t start_index = this->find(start_key);
    size_t end_index = this->find(end_key);
    if (end_index != -1 && data->keys[end_index] > end_key) {
        end_index--;
    }
    if (start_index == -1 || end_index == -1) {
        return Range(false);
    }

    return this->subRange(start_index, end_index);
}

void Range::truncate(int length) const {
    assert(valid && size > 0 && data->keys.size() > 0);
    if (length < 0 || length > size || start_pos > 0) {
        Logger::error("Invalid length to truncate");
        return;
    }
    
    data->keys.resize(length);
    data->values.resize(length);
    size = length;
}

// return the first element index whose key is greater than or equal to key
int Range::find(std::string key) const {
    if (!valid || size == 0) {
        return -1;
    }
    
    int left = 0;
    int right = size - 1;
    
    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (data->keys[start_pos + mid] == key) {
            return start_pos + mid;
        } else if (data->keys[start_pos + mid] < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    
    if (left >= size) {
        return -1;
    }
    return start_pos + left;
}

std::string Range::toString() const {
    std::string str = "< " + this->startKey() + " -> " + this->endKey() + " >";
    return str;
}

Range Range::concatRanges(std::vector<Range>& ranges) {
    if (ranges.empty()) {
        return Range(false);
    }
    
    std::vector<std::string> mergedKeys;
    std::vector<std::string> mergedValues;
    
    for (const auto& range : ranges) {
        auto keys = range.getKeys();
        auto values = range.getValues();
        mergedKeys.insert(mergedKeys.end(), keys.begin(), keys.end());
        mergedValues.insert(mergedValues.end(), values.begin(), values.end());
    }
    
    return Range(std::move(mergedKeys), std::move(mergedValues), mergedKeys.size());
}

Range Range::concatRangesMoved(std::vector<Range>& ranges) {
    if (ranges.empty()) {
        return Range(false);
    }
    
    size_t total_size = 0;
    for (const auto& range : ranges) {
        total_size += range.getSize();
    }

    auto new_data = std::make_shared<RangeData>();
    new_data->keys.reserve(total_size);
    new_data->values.reserve(total_size);

    for (auto& range : ranges) {
        auto src_data = range.getData(); 
        size_t start = range.getStartPos();
        size_t len = range.getSize();
        
        new_data->keys.insert(new_data->keys.end(),
            std::make_move_iterator(src_data->keys.begin() + start),
            std::make_move_iterator(src_data->keys.begin() + start + len));
        new_data->values.insert(new_data->values.end(),
            std::make_move_iterator(src_data->values.begin() + start),
            std::make_move_iterator(src_data->values.begin() + start + len));
    }

    return Range(new_data, 0, total_size);
}