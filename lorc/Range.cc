#include "Range.h"
#include "Logger.h"
#include <iostream>
#include <cassert>
#include <algorithm>    // Ìí¼Ó´ËÐÐ

Range::Range(bool valid) {
    this->valid = valid;
    this->size = 0;
    this->timestamp = 0;
}

Range::Range(std::vector<std::string> keys, std::vector<std::string> values, size_t size) {
    // vector is deep copied
    this->keys = keys;
    this->values = values;

    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

Range::~Range() {

}

std::string Range::startKey() const {
    assert(valid && size > 0 && keys.size() > 0);
    if (keys.size() == 0) {
        Logger::warn("keys is empty");
    }
    return keys[0];
}

std::string Range::endKey() const {
    assert(valid && size > 0 && keys.size() > 0);
    return keys[size - 1];
}   

std::string Range::keyAt(size_t index) const {
    assert(valid && size > index && keys.size() > index);
    return keys[index];
}

std::string Range::valueAt(size_t index) const {
    assert(valid && size > index && values.size() > index);
    return values[index];
}

std::vector<std::string> Range::getKeys() const {
    assert(valid && size > 0 && keys.size() > 0);
    return keys;
}

std::vector<std::string> Range::getValues() const {
    assert(valid && size > 0 && values.size() > 0);
    return values;
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
    assert(valid && size > end_index && keys.size() > end_index && values.size() > end_index);
    std::vector<std::string> sub_keys;
    std::vector<std::string> sub_values;
    for (size_t i = start_index; i <= end_index; i++) {
        sub_keys.push_back(keys[i]);
        sub_values.push_back(values[i]);
    }
    return Range(sub_keys, sub_values, end_index - start_index + 1);
}

// Range Range::subRange(std::string start_key, std::string end_key) const {
//     assert(valid && size > 0 && keys.size() > 0);
//     auto start_it = std::lower_bound(keys.begin(), keys.end(), start_key);
//     auto end_it = std::upper_bound(keys.begin(), keys.end(), end_key);

//     size_t start_index = std::distance(keys.begin(), start_it);
//     size_t end_index = std::distance(keys.begin(), end_it) - 1;

//     return this->subRange(start_index, end_index);
// }

// return the largest subrange in [start_key, end_key]
Range Range::subRange(std::string start_key, std::string end_key) const {
    // use binary search to find the start and end index
    assert(valid && size > 0 && keys.size() > 0);
    // auto start_it = std::lower_bound(keys.begin(), keys.end(), start_key);
    // auto end_it = std::upper_bound(keys.begin(), keys.end(), end_key);

    size_t start_index = this->find(start_key);
    size_t end_index = this->find(end_key);
    if (end_index != -1 && keys[end_index] > end_key) {
        end_index--;
    }
    if (start_index == -1 || end_index == -1) {
        return Range(false);
    }

    return this->subRange(start_index, end_index);
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
        if (keys[mid] == key) {
            return mid;
        } else if (keys[mid] < key) {
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
    
    return Range(mergedKeys, mergedValues, mergedKeys.size());
}