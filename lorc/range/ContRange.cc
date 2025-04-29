#include <iostream>
#include <cassert>
#include <algorithm> 
#include "ContRange.h"
#include <string.h>
#include <string_view>
#include "../logger/Logger.h"

ContRange::ContRange(bool valid) {
    this->valid = valid;
    this->size = 0;
    this->timestamp = 0;
    this->subrange_view_start_pos = -1;
}

ContRange::~ContRange() {
    if (data && data.use_count() == 1) {
        delete[] data->keys_buffer;
        delete[] data->values_buffer;
        delete[] data->key_views;
        delete[] data->value_views;
    }
    data.reset();
}

ContRange::ContRange(const ContRange& other) {
    this->valid = other.valid;
    this->size = other.size;
    this->timestamp = other.timestamp;
    this->subrange_view_start_pos = other.subrange_view_start_pos;
    
    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        
        // ���� buffer ����
        this->data->keys_buffer_size = other.data->keys_buffer_size;
        this->data->values_buffer_size = other.data->values_buffer_size;
        
        this->data->keys_buffer = new char[other.data->keys_buffer_size];
        this->data->values_buffer = new char[other.data->values_buffer_size];
        
        memcpy(this->data->keys_buffer, other.data->keys_buffer, other.data->keys_buffer_size);
        memcpy(this->data->values_buffer, other.data->values_buffer, other.data->values_buffer_size);
        
        // ���� view ����
        this->data->key_views = new StringView[size];
        this->data->value_views = new StringView[size];
        
        memcpy(this->data->key_views, other.data->key_views, size * sizeof(StringView));
        memcpy(this->data->value_views, other.data->value_views, size * sizeof(StringView));
    }
}

ContRange::ContRange(ContRange&& other) noexcept {
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

// ContRange::ContRange(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size) {
//     data = std::make_shared<RangeData>();
//     data->keys = keys;
//     data->values = values;
//     this->subrange_view_start_pos = -1;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

ContRange::ContRange(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size) {
    data = std::make_shared<RangeData>();
    
    // ����keys��values�ֱ���Ҫ��buffer��С
    size_t total_keys_size = 0;
    size_t total_values_size = 0;
    for (size_t i = 0; i < size; i++) {
        total_keys_size += keys[i].length();
        total_values_size += values[i].length();
    }
    
    // �����ڴ�
    data->keys_buffer = new char[total_keys_size];
    data->values_buffer = new char[total_values_size];
    data->keys_buffer_size = total_keys_size;
    data->values_buffer_size = total_values_size;
    data->key_views = new StringView[size];
    data->value_views = new StringView[size];
    
    // ����keys����
    size_t keys_offset = 0;
    for (size_t i = 0; i < size; i++) {
        data->key_views[i].offset = keys_offset;
        data->key_views[i].length = keys[i].length();
        memcpy(data->keys_buffer + keys_offset, keys[i].c_str(), keys[i].length());
        keys_offset += keys[i].length();
    }
    
    // ����values����
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

ContRange::ContRange(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size) {
    this->data = data;
    this->subrange_view_start_pos = subrange_view_start_pos;
    this->valid = true;
    this->size = size;
    this->timestamp = 0;
}

ContRange& ContRange::operator=(const ContRange& other) {
    if (this != &other) {
        // ����ǰ��Դ
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
            
            // ���� buffer ����
            this->data->keys_buffer_size = other.data->keys_buffer_size;
            this->data->values_buffer_size = other.data->values_buffer_size;
            
            this->data->keys_buffer = new char[other.data->keys_buffer_size];
            this->data->values_buffer = new char[other.data->values_buffer_size];
            
            memcpy(this->data->keys_buffer, other.data->keys_buffer, other.data->keys_buffer_size);
            memcpy(this->data->values_buffer, other.data->values_buffer, other.data->values_buffer_size);
            
            // ���� view ����
            this->data->key_views = new StringView[size];
            this->data->value_views = new StringView[size];
            
            memcpy(this->data->key_views, other.data->key_views, size * sizeof(StringView));
            memcpy(this->data->value_views, other.data->value_views, size * sizeof(StringView));
        }
    }
    return *this;
}

ContRange& ContRange::operator=(ContRange&& other) noexcept {
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

// std::string ContRange::getStringFromView(const char* buffer, const StringView& view) const {
//     std::string_view sv(buffer + view.offset, view.length);
//     return std::string(buffer + view.offset, view.length);
// }

Slice ContRange::startKey() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys_buffer + data->key_views[0].offset, data->key_views[0].length);
}

Slice ContRange::endKey() const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys_buffer + data->key_views[size - 1].offset, data->key_views[size - 1].length);
}   

Slice ContRange::keyAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    return Slice(data->keys_buffer + data->key_views[index].offset, data->key_views[index].length);
}

Slice ContRange::valueAt(size_t index) const {
    assert(valid && size > index && subrange_view_start_pos == -1);
    return Slice(data->values_buffer + data->value_views[index].offset, data->value_views[index].length);
}

size_t ContRange::getSize() const {
    return size;
}   

bool ContRange::isValid() const {
    return valid;
}

int ContRange::getTimestamp() const {
    return timestamp;
}

void ContRange::setTimestamp(int timestamp) const {
    this->timestamp = timestamp;
}

ContRange ContRange::subRangeView(size_t start_index, size_t end_index) const {
    assert(valid && size > end_index && start_index >= 0 && subrange_view_start_pos == -1);
    // the only code to create non-negative subrange_view_start_pos
    return ContRange(data, start_index, end_index - start_index + 1);
}

void ContRange::truncate(int length) const {
    assert(valid && size > 0 && subrange_view_start_pos == -1);
    if (length < 0 || length > size) {
        Logger::error("Invalid length to truncate");
        return;
    }
    
    // ��Ϊ����ʹ�õ���StringView,truncateֻ��Ҫ����size����
    // buffer�е����ݿ��Ա��ֲ���,��ΪStringView�����˿ɷ��ʷ�Χ
    size = length;
}

int ContRange::find(Slice key) const {
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

std::string ContRange::toString() const {
    std::string str = "< " + this->startKey().ToString() + " -> " + this->endKey().ToString() + " >";
    return str;
}

// Function to combine ordered and non-overlapping ranges with move semantics
ContRange ContRange::concatRangesMoved(std::vector<ContRange>& ranges) {
    if (ranges.empty()) {
        return ContRange(false);
    }
    
    // �����ܴ�С
    size_t total_size = 0;
    size_t total_keys_buffer_size = 0;
    size_t total_values_buffer_size = 0;
    
    for (const auto& ContRange : ranges) {
        total_size += ContRange.getSize();
        if (ContRange.subrange_view_start_pos == -1) {
            total_keys_buffer_size += ContRange.data->keys_buffer_size;
            total_values_buffer_size += ContRange.data->values_buffer_size;
        } else {
            // ��������ͼ����Ҫ����ʵ�ʴ�С
            for (size_t i = 0; i < ContRange.size; i++) {
                total_keys_buffer_size += ContRange.data->key_views[ContRange.subrange_view_start_pos + i].length;
                total_values_buffer_size += ContRange.data->value_views[ContRange.subrange_view_start_pos + i].length;
            }
        }
    }

    // �����µ�Range
    auto new_data = std::make_shared<RangeData>();
    new_data->keys_buffer = new char[total_keys_buffer_size];
    new_data->values_buffer = new char[total_values_buffer_size];
    new_data->key_views = new StringView[total_size];
    new_data->value_views = new StringView[total_size];
    
    // �ϲ�����
    size_t current_pos = 0;
    size_t keys_offset = 0;
    size_t values_offset = 0;
    
    for (auto& ContRange : ranges) {
        size_t range_start = ContRange.subrange_view_start_pos == -1 ? 0 : ContRange.subrange_view_start_pos;
        for (size_t i = 0; i < ContRange.size; i++) {
            const StringView& key_view = ContRange.data->key_views[range_start + i];
            const StringView& value_view = ContRange.data->value_views[range_start + i];
            
            // ����key����
            new_data->key_views[current_pos].offset = keys_offset;
            new_data->key_views[current_pos].length = key_view.length;
            memcpy(new_data->keys_buffer + keys_offset,
                   ContRange.data->keys_buffer + key_view.offset,
                   key_view.length);
            keys_offset += key_view.length;
            
            // ����value����
            new_data->value_views[current_pos].offset = values_offset;
            new_data->value_views[current_pos].length = value_view.length;
            memcpy(new_data->values_buffer + values_offset,
                   ContRange.data->values_buffer + value_view.offset,
                   value_view.length);
            values_offset += value_view.length;
            
            current_pos++;
        }
    }
    
    new_data->keys_buffer_size = total_keys_buffer_size;
    new_data->values_buffer_size = total_values_buffer_size;

    Logger::debug("Merged ContRange: " + std::to_string(total_size) + " keys, " + std::to_string(total_keys_buffer_size) + " bytes");
    Logger::debug("Merged ContRange: " + std::to_string(total_size) + " values, " + std::to_string(total_values_buffer_size) + " bytes");
    
    return ContRange(new_data, -1, total_size);
}