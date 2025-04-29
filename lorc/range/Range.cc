#include <iostream>
#include <cassert>
#include <algorithm> 
#include "Range.h"
#include <string.h>
#include <string_view>
#include "../logger/Logger.h"

Range::Range(bool valid) {
    this->valid = valid;
    this->size = 0;
    this->timestamp = 0;
    this->subrange_view_start_pos = -1;
}

Range::~Range() {
    if (data && data.use_count() == 1) {
        delete[] data->keys_buffer;
        delete[] data->values_buffer;
        delete[] data->key_views;
        delete[] data->value_views;
    }
    data.reset();
}

Range::Range(const Range& other) {
    this->valid = other.valid;
    this->size = other.size;
    this->timestamp = other.timestamp;
    this->subrange_view_start_pos = other.subrange_view_start_pos;
    
    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        
        // 复制 buffer 数据
        this->data->keys_buffer_size = other.data->keys_buffer_size;
        this->data->values_buffer_size = other.data->values_buffer_size;
        
        this->data->keys_buffer = new char[other.data->keys_buffer_size];
        this->data->values_buffer = new char[other.data->values_buffer_size];
        
        memcpy(this->data->keys_buffer, other.data->keys_buffer, other.data->keys_buffer_size);
        memcpy(this->data->values_buffer, other.data->values_buffer, other.data->values_buffer_size);
        
        // 复制 view 数据
        this->data->key_views = new StringView[size];
        this->data->value_views = new StringView[size];
        
        memcpy(this->data->key_views, other.data->key_views, size * sizeof(StringView));
        memcpy(this->data->value_views, other.data->value_views, size * sizeof(StringView));
    }
}

Range::Range(Range&& other) noexcept {
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

// Range::Range(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size) {
//     data = std::make_shared<RangeData>();
//     data->keys = keys;
//     data->values = values;
//     this->subrange_view_start_pos = -1;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

Range::Range(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size) {
    data = std::make_shared<RangeData>();
    
    // 计算keys和values分别需要的buffer大小
    size_t total_keys_size = 0;
    size_t total_values_size = 0;
    for (size_t i = 0; i < size; i++) {
        total_keys_size += keys[i].length();
        total_values_size += values[i].length();
    }
    
    // 分配内存
    data->keys_buffer = new char[total_keys_size];
    data->values_buffer = new char[total_values_size];
    data->keys_buffer_size = total_keys_size;
    data->values_buffer_size = total_values_size;
    data->key_views = new StringView[size];
    data->value_views = new StringView[size];
    
    // 复制keys数据
    size_t keys_offset = 0;
    for (size_t i = 0; i < size; i++) {
        data->key_views[i].offset = keys_offset;
        data->key_views[i].length = keys[i].length();
        memcpy(data->keys_buffer + keys_offset, keys[i].c_str(), keys[i].length());
        keys_offset += keys[i].length();
    }
    
    // 复制values数据
    size_t values_offset = 0;
    for (size_t i = 0; i < size; i++) {
        data->value_views[i].offset = values_offset;
        data->value_views[i].length = values[i].length();
        memcpy(data->values_buffer + values_offset, values[i].c_str(), values[i].length());
        values_offset += values[i].length();
    }
    
    this->size = size;
    this->valid = true;
    this->timestamp = 0;
    this->subrange_view_start_pos = -1;
}

Range::Range(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size) {
    this->data = data;
    this->subrange_view_start_pos = subrange_view_start_pos;
    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

Range& Range::operator=(const Range& other) {
    if (this != &other) {
        // 清理当前资源
        if (data && data.use_count() == 1) {
            delete[] data->keys_buffer;
            delete[] data->values_buffer;
            delete[] data->key_views;
            delete[] data->value_views;
        }
        data.reset();
        
        this->valid = other.valid;
        this->size = other.size;
        this->timestamp = other.timestamp;
        this->subrange_view_start_pos = other.subrange_view_start_pos;
        
        if (other.valid && other.data) {
            this->data = std::make_shared<RangeData>();
            
            // 复制 buffer 数据
            this->data->keys_buffer_size = other.data->keys_buffer_size;
            this->data->values_buffer_size = other.data->values_buffer_size;
            
            this->data->keys_buffer = new char[other.data->keys_buffer_size];
            this->data->values_buffer = new char[other.data->values_buffer_size];
            
            memcpy(this->data->keys_buffer, other.data->keys_buffer, other.data->keys_buffer_size);
            memcpy(this->data->values_buffer, other.data->values_buffer, other.data->values_buffer_size);
            
            // 复制 view 数据
            this->data->key_views = new StringView[size];
            this->data->value_views = new StringView[size];
            
            memcpy(this->data->key_views, other.data->key_views, size * sizeof(StringView));
            memcpy(this->data->value_views, other.data->value_views, size * sizeof(StringView));
        }
    }
    return *this;
}

Range& Range::operator=(Range&& other) noexcept {
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

std::string Range::getStringFromView(const char* buffer, const StringView& view) const {
    std::string_view sv(buffer + view.offset, view.length);
    return std::string(buffer + view.offset, view.length);
}

std::string& Range::startKey() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    static std::string temp;
    temp = getStringFromView(data->keys_buffer, data->key_views[0]);
    return temp;
}

std::string& Range::endKey() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    static std::string temp;
    temp = getStringFromView(data->keys_buffer, data->key_views[size - 1]);
    return temp;
}   

std::string& Range::keyAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    static std::string temp;
    temp = getStringFromView(data->keys_buffer, data->key_views[index]);
    return temp;
}

std::string& Range::valueAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    static std::string temp;
    temp = getStringFromView(data->values_buffer, data->value_views[index]);
    return temp;
}

std::vector<std::string>& Range::getKeys() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    static std::vector<std::string> keys;
    keys.clear();
    for(size_t i = 0; i < size; i++) {
        keys.push_back(getStringFromView(data->keys_buffer, data->key_views[i]));
    }
    return keys;
}

std::vector<std::string>& Range::getValues() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    static std::vector<std::string> values;
    values.clear();
    for(size_t i = 0; i < size; i++) {
        values.push_back(getStringFromView(data->values_buffer, data->value_views[i]));
    }
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

Range Range::subRangeView(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index && start_index >= 0 && subrange_view_start_pos == -1);
    // the only code to create non-negative subrange_view_start_pos
    return Range(data, start_index, end_index - start_index + 1);
}

Range Range::subRange(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index && subrange_view_start_pos == -1);
    
    std::vector<std::string> sub_keys;
    std::vector<std::string> sub_values;
    
    for(size_t i = start_index; i <= end_index; i++) {
        sub_keys.push_back(getStringFromView(data->keys_buffer, data->key_views[i]));
        sub_values.push_back(getStringFromView(data->values_buffer, data->value_views[i]));
    }
    
    return Range(std::move(sub_keys), std::move(sub_values), end_index - start_index + 1);
}

Range Range::subRange(std::string start_key, std::string end_key) const {
    // use binary search to find the start and end index
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    
    // 二分查找获取start_index
    size_t start_index = this->find(start_key);
    if (start_index == -1) {
        return Range(false);
    }
    
    // 二分查找获取end_index
    size_t end_index = this->find(end_key);
    if (end_index == -1) {
        return Range(false);
    }
    
    // 检查end_key是否大于找到位置的key
    std::string found_key = getStringFromView(data->keys_buffer, data->key_views[end_index]);
    if (found_key > end_key) {
        if (end_index == 0) {
            return Range(false);
        }
        end_index--;
    }
    
    return this->subRange(start_index, end_index);
}

void Range::truncate(int length) const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    if (length < 0 || length > size) {
        Logger::error("Invalid length to truncate");
        return;
    }
    
    // 因为我们使用的是StringView,truncate只需要更新size即可
    // buffer中的数据可以保持不变,因为StringView控制了可访问范围
    size = length;
}

int Range::find(std::string key) const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    if (!valid || size == 0) {
        return -1;
    }
    
    int left = 0;
    int right = size - 1;
    
    while (left <= right) {
        int mid = left + (right - left) / 2;
        std::string mid_key = getStringFromView(data->keys_buffer, data->key_views[mid]);
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

// Function to combine ordered and non-overlapping ranges with move semantics
Range Range::concatRangesMoved(std::vector<Range>& ranges) {
    if (ranges.empty()) {
        return Range(false);
    }
    
    // 计算总大小
    size_t total_size = 0;
    size_t total_keys_buffer_size = 0;
    size_t total_values_buffer_size = 0;
    
    for (const auto& range : ranges) {
        total_size += range.getSize();
        if (range.subrange_view_start_pos == -1) {
            total_keys_buffer_size += range.data->keys_buffer_size;
            total_values_buffer_size += range.data->values_buffer_size;
        } else {
            // 对于子视图，需要计算实际大小
            for (size_t i = 0; i < range.size; i++) {
                total_keys_buffer_size += range.data->key_views[range.subrange_view_start_pos + i].length;
                total_values_buffer_size += range.data->value_views[range.subrange_view_start_pos + i].length;
            }
        }
    }

    // 创建新的Range
    auto new_data = std::make_shared<RangeData>();
    new_data->keys_buffer = new char[total_keys_buffer_size];
    new_data->values_buffer = new char[total_values_buffer_size];
    new_data->key_views = new StringView[total_size];
    new_data->value_views = new StringView[total_size];
    
    // 合并数据
    size_t current_pos = 0;
    size_t keys_offset = 0;
    size_t values_offset = 0;
    
    for (auto& range : ranges) {
        size_t range_start = range.subrange_view_start_pos == -1 ? 0 : range.subrange_view_start_pos;
        for (size_t i = 0; i < range.size; i++) {
            const StringView& key_view = range.data->key_views[range_start + i];
            const StringView& value_view = range.data->value_views[range_start + i];
            
            // 复制key数据
            new_data->key_views[current_pos].offset = keys_offset;
            new_data->key_views[current_pos].length = key_view.length;
            memcpy(new_data->keys_buffer + keys_offset,
                   range.data->keys_buffer + key_view.offset,
                   key_view.length);
            keys_offset += key_view.length;
            
            // 复制value数据
            new_data->value_views[current_pos].offset = values_offset;
            new_data->value_views[current_pos].length = value_view.length;
            memcpy(new_data->values_buffer + values_offset,
                   range.data->values_buffer + value_view.offset,
                   value_view.length);
            values_offset += value_view.length;
            
            current_pos++;
        }
    }
    
    new_data->keys_buffer_size = total_keys_buffer_size;
    new_data->values_buffer_size = total_values_buffer_size;

    Logger::debug("Merged Range: " + std::to_string(total_size) + " keys, " + std::to_string(total_keys_buffer_size) + " bytes");
    Logger::debug("Merged Range: " + std::to_string(total_size) + " values, " + std::to_string(total_values_buffer_size) + " bytes");
    
    return Range(new_data, -1, total_size);
}