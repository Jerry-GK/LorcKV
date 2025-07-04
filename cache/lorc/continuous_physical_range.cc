#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "db/dbformat.h"
#include "rocksdb/continuous_physical_range.h"

namespace ROCKSDB_NAMESPACE {

std::string ContinuousPhysicalRange::toString() const {
    std::shared_lock<std::shared_mutex> lock(physical_range_mutex_);
    std::string str = "< " + ToStringPlain(this->startUserKey().ToString()) + " -> " + ToStringPlain(this->endUserKey().ToString()) + " >"
        + " ( len = " + std::to_string(this->length()) + " )";
    return str;
}

// ContinuousPhysicalRange implementation
ContinuousPhysicalRange::ContinuousPhysicalRange(bool valid_) : PhysicalRange(valid_) {
    this->data = std::make_shared<RangeData>();
    if (valid) {
        data = std::make_shared<RangeData>();
        data->keys_buffer_size = 0;
        data->values_buffer_size = 0;
    }
}

ContinuousPhysicalRange::~ContinuousPhysicalRange() {
    data.reset();
}

ContinuousPhysicalRange::ContinuousPhysicalRange(const ContinuousPhysicalRange& other) : PhysicalRange(other.valid) {
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;
    
    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        
        // Copy continuous buffers
        if (other.data->keys_buffer_size > 0) {
            this->data->keys_buffer = std::make_unique<char[]>(other.data->keys_buffer_size);
            memcpy(this->data->keys_buffer.get(), other.data->keys_buffer.get(), other.data->keys_buffer_size);
            this->data->keys_buffer_size = other.data->keys_buffer_size;
        }
        
        if (other.data->values_buffer_size > 0) {
            this->data->values_buffer = std::make_unique<char[]>(other.data->values_buffer_size);
            memcpy(this->data->values_buffer.get(), other.data->values_buffer.get(), other.data->values_buffer_size);
            this->data->values_buffer_size = other.data->values_buffer_size;
        }
        
        // Copy offset and size arrays
        this->data->key_offsets = other.data->key_offsets;
        this->data->key_sizes = other.data->key_sizes;
        this->data->value_offsets = other.data->value_offsets;
        this->data->value_sizes = other.data->value_sizes;
        this->data->original_value_sizes = other.data->original_value_sizes;
        this->data->is_overflow = other.data->is_overflow;
        
        // Copy overflow values
        this->data->overflow_values.resize(other.data->overflow_values.size());
        for (size_t i = 0; i < other.data->overflow_values.size(); i++) {
            if (other.data->overflow_values[i]) {
                this->data->overflow_values[i] = std::make_unique<std::string>(*other.data->overflow_values[i]);
            }
        }
        
        // Rebuild slices
        rebuildSlices();
    }
}

ContinuousPhysicalRange::ContinuousPhysicalRange(ContinuousPhysicalRange&& other) noexcept : PhysicalRange(other.valid) {
    this->data = std::move(other.data);
    this->valid = other.valid;
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;

    other.valid = false;
    other.range_length = 0;
    other.timestamp = 0;
}

ContinuousPhysicalRange::ContinuousPhysicalRange(std::shared_ptr<RangeData> data_, size_t range_length_) : PhysicalRange(true) {
    this->data = data_;
    this->range_length = range_length_;
    this->byte_size = 0;
    for (size_t i = 0; i < range_length_; i++) {
        this->byte_size += data_->key_sizes[i];
        this->byte_size += data_->value_sizes[i];
    }
}

ContinuousPhysicalRange& ContinuousPhysicalRange::operator=(const ContinuousPhysicalRange& other) {
    if (this != &other) {
        this->valid = other.valid;
        this->range_length = other.range_length;
        this->byte_size = other.byte_size;
        this->timestamp = other.timestamp;
        
        if (other.valid && other.data) {
            this->data = std::make_shared<RangeData>();
            
            // Copy continuous buffers
            if (other.data->keys_buffer_size > 0) {
                this->data->keys_buffer = std::make_unique<char[]>(other.data->keys_buffer_size);
                memcpy(this->data->keys_buffer.get(), other.data->keys_buffer.get(), other.data->keys_buffer_size);
                this->data->keys_buffer_size = other.data->keys_buffer_size;
            }
            
            if (other.data->values_buffer_size > 0) {
                this->data->values_buffer = std::make_unique<char[]>(other.data->values_buffer_size);
                memcpy(this->data->values_buffer.get(), other.data->values_buffer.get(), other.data->values_buffer_size);
                this->data->values_buffer_size = other.data->values_buffer_size;
            }
            
            // Copy offset and size arrays
            this->data->key_offsets = other.data->key_offsets;
            this->data->key_sizes = other.data->key_sizes;
            this->data->value_offsets = other.data->value_offsets;
            this->data->value_sizes = other.data->value_sizes;
            this->data->original_value_sizes = other.data->original_value_sizes;
            this->data->is_overflow = other.data->is_overflow;
            
            // Copy overflow values
            this->data->overflow_values.resize(other.data->overflow_values.size());
            for (size_t i = 0; i < other.data->overflow_values.size(); i++) {
                if (other.data->overflow_values[i]) {
                    this->data->overflow_values[i] = std::make_unique<std::string>(*other.data->overflow_values[i]);
                }
            }
            
            // Rebuild slices
            rebuildSlices();
        } else {
            this->data.reset();
        }
    }
    return *this;
}

ContinuousPhysicalRange& ContinuousPhysicalRange::operator=(ContinuousPhysicalRange&& other) noexcept {
    if (this != &other) {
        // Clean up current object's resources
        this->data.reset();
        
        // Move resources from other
        this->data = std::move(other.data);
        this->valid = other.valid;
        this->range_length = other.range_length;
        this->byte_size = other.byte_size;
        this->timestamp = other.timestamp;

        // Reset other object's state
        other.valid = false;
        other.range_length = 0;
        other.timestamp = 0;
    }
    return *this;
}

std::unique_ptr<ContinuousPhysicalRange> ContinuousPhysicalRange::buildFromReferringRange(const ReferringRange& refRange) {
    auto newRange = std::make_unique<ContinuousPhysicalRange>(true);
    newRange->initializeFromReferringRange(refRange);
    
    for (size_t i = 0; i < refRange.length(); i++) {
        Slice user_key = refRange.keyAt(i);
        std::string internal_key_str = InternalKey(user_key, refRange.getSeqNum(), kTypeRangeCacheValue).Encode().ToString();
        Slice value = refRange.valueAt(i);
        newRange->emplaceInternal(Slice(internal_key_str), value, i);
    }
    
    newRange->range_length = refRange.length();
    newRange->byte_size = refRange.keysByteSize() + refRange.valuesByteSize();
    newRange->rebuildSlices();
    return newRange;
}

// Override virtual functions
// key methods in continuous physical range does NOT need be read locked since key vector is immutable after initialization (no random insertion)
const Slice& ContinuousPhysicalRange::startUserKey() const {
    assert(valid && range_length > 0 && data->key_sizes.size() > 0 && data->key_sizes[0] > internal_key_extra_bytes);
    return this->data->user_key_slices[0];
}

const Slice& ContinuousPhysicalRange::endUserKey() const {
    assert(valid && range_length > 0 && data->key_sizes.size() > 0 && data->key_sizes[range_length - 1] > internal_key_extra_bytes);
    return this->data->user_key_slices[range_length - 1];
}

const Slice& ContinuousPhysicalRange::startInternalKey() const {
    assert(valid && range_length > 0 && data->key_sizes.size() > 0);
    return this->data->internal_key_slices[0];
}

const Slice& ContinuousPhysicalRange::endInternalKey() const {
    assert(valid && range_length > 0 && data->key_sizes.size() > 0);
    return this->data->internal_key_slices[range_length - 1];
}

const Slice& ContinuousPhysicalRange::internalKeyAt(size_t index) const {
    assert(valid && range_length > index);
    return this->data->internal_key_slices[index];
}

const Slice& ContinuousPhysicalRange::userKeyAt(size_t index) const {
    return userKeyAtInternal(index);
}

const Slice& ContinuousPhysicalRange::userKeyAtInternal(size_t index) const {
    assert(valid && range_length > index && data->key_sizes[index] > internal_key_extra_bytes);
    return this->data->user_key_slices[index];
}

const Slice& ContinuousPhysicalRange::valueAt(size_t index) const {
    std::shared_lock<std::shared_mutex> lock(physical_range_mutex_);
    assert(valid && range_length > index);
    return this->data->value_slices[index];
}

PhysicalRangeUpdateResult ContinuousPhysicalRange::update(const Slice& internal_key, const Slice& value) const {
    std::unique_lock<std::shared_mutex> lock(physical_range_mutex_);
    assert(valid && range_length > 0 && internal_key.size() > internal_key_extra_bytes);    
    Slice user_key = Slice(internal_key.data(), internal_key.size() - PhysicalRange::internal_key_extra_bytes);
    int index = findInternal(user_key);
    if (index < 0) {
        assert(false);
        return PhysicalRangeUpdateResult::OUT_OF_RANGE;
    }

    // Update sequence number in key (in-place since size doesn't change)
    ParsedInternalKey parsed_internal_key;
    Status s = ParseInternalKey(internal_key, &parsed_internal_key, false);
    if (!s.ok()) {
        return PhysicalRangeUpdateResult::ERROR;
    }
    SequenceNumber seq_num = parsed_internal_key.sequence;
    // Create new internal key with correct type for range cache
    ValueType type_in_range_cache;
    bool is_delete_entry = (parsed_internal_key.type == kTypeDeletion || parsed_internal_key.type == kTypeSingleDeletion || parsed_internal_key.type == kTypeDeletionWithTimestamp);
    if (is_delete_entry) {
        // reserve deletion types for range cache
        type_in_range_cache = parsed_internal_key.type; 
    } else {
        // parsed_internal_key.type should be kTypeValue here
        // TODO(jr): is there any other type that should be considered in range cache?
        type_in_range_cache = kTypeRangeCacheValue;  
    }
    std::string new_internal_key_str = InternalKey(user_key, seq_num, type_in_range_cache).Encode().ToString();


    if (userKeyAtInternal(index) == user_key) {
        // Update key in continuous buffer (same size, so in-place)
        memcpy(data->keys_buffer.get() + data->key_offsets[index], 
               new_internal_key_str.data(), new_internal_key_str.size());
        
        // Update value
        updateValueAt(index, value);
        
        // Rebuild affected slices
        rebuildSlicesAt(index);

        if (is_delete_entry) {
            delete_length++;
        }
        return PhysicalRangeUpdateResult::UPDATED;
    } else if (userKeyAtInternal(index) != user_key) {
        // continuous physical range does NOT support random insertion
        if (is_delete_entry) {
            delete_length++;
        }
        return PhysicalRangeUpdateResult::UNABLE_TO_INSERT;
    }
    
    return PhysicalRangeUpdateResult::ERROR;
}

int ContinuousPhysicalRange::find(const Slice& key) const {
    std::shared_lock<std::shared_mutex> lock(physical_range_mutex_);
    return findInternal(key);
}

int ContinuousPhysicalRange::findInternal(const Slice& key) const {
    assert(valid && range_length > 0 && key.size() > 0);

    if (!valid || range_length == 0) {
        return -1;
    }
    
    int left = 0;
    int right = range_length - 1;
    
    while (left <= right) {
        int mid = left + (right - left) / 2;
        Slice mid_user_key = userKeyAtInternal(mid);
        if (mid_user_key == key) {
            return mid;
        } else if (mid_user_key < key) {
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

void ContinuousPhysicalRange::reserve(size_t len) {
    std::unique_lock<std::shared_mutex> lock(physical_range_mutex_);
    assert(valid);
    data->key_offsets.reserve(len);
    data->key_sizes.reserve(len);
    data->value_offsets.reserve(len);
    data->value_sizes.reserve(len);
    data->original_value_sizes.reserve(len);
    data->is_overflow.reserve(len);
    data->overflow_values.reserve(len);
    data->internal_key_slices.reserve(len);
    data->user_key_slices.reserve(len);
    data->value_slices.reserve(len);
}

void ContinuousPhysicalRange::initializeFromReferringRange(const ReferringRange& refRange) {
    size_t keys_total_size = refRange.keysByteSize();
    size_t values_total_size = refRange.valuesByteSize();
    
    // Allocate continuous buffers
    data->keys_buffer = std::make_unique<char[]>(keys_total_size);
    data->values_buffer = std::make_unique<char[]>(values_total_size);
    data->keys_buffer_size = keys_total_size;
    data->values_buffer_size = values_total_size;
    
    // Reserve vectors
    reserve(refRange.length());
}

void ContinuousPhysicalRange::initializeFromReferringRangeSubset(const ReferringRange& refRange, int start_pos, int end_pos) {
    size_t num_elements = end_pos - start_pos + 1;
    size_t keys_total_size = 0;
    size_t values_total_size = 0;
    
    for (int i = start_pos; i <= end_pos; i++) {
        keys_total_size += refRange.keyAt(i).size() + internal_key_extra_bytes;
        values_total_size += refRange.valueAt(i).size();
    }
    
    data->keys_buffer = std::make_unique<char[]>(keys_total_size);
    data->values_buffer = std::make_unique<char[]>(values_total_size);
    data->keys_buffer_size = keys_total_size;
    data->values_buffer_size = values_total_size;
    
    reserve(num_elements);
}

void ContinuousPhysicalRange::emplaceInternal(const Slice& internal_key, const Slice& value, size_t index) {
    assert(valid);
    // Calculate offsets
    size_t key_offset = (index == 0) ? 0 : data->key_offsets[index - 1] + data->key_sizes[index - 1];
    size_t value_offset = (index == 0) ? 0 : data->value_offsets[index - 1] + data->value_sizes[index - 1];
    
    // Store key
    memcpy(data->keys_buffer.get() + key_offset, internal_key.data(), internal_key.size());
    data->key_offsets.emplace_back(key_offset);
    data->key_sizes.emplace_back(internal_key.size());
    
    // Store value
    memcpy(data->values_buffer.get() + value_offset, value.data(), value.size());
    data->value_offsets.emplace_back(value_offset);
    data->value_sizes.emplace_back(value.size());
    data->original_value_sizes.emplace_back(value.size()); // Record original size
    data->is_overflow.emplace_back(false);
    data->overflow_values.emplace_back(nullptr);
}

void ContinuousPhysicalRange::updateValueAt(size_t index, const Slice& new_value) const {
    size_t original_size = data->original_value_sizes[index]; // Use original size
    
    if (new_value.size() <= original_size) {
        // Can fit in original space
        if (data->is_overflow[index]) {
            // Move back from overflow to continuous storage
            data->overflow_values[index].reset();
            data->is_overflow[index] = false;
        }
        
        // Update in continuous buffer
        memcpy(data->values_buffer.get() + data->value_offsets[index], 
               new_value.data(), new_value.size());
        data->value_sizes[index] = new_value.size();
    } else {
        // Need overflow storage
        if (!data->is_overflow[index]) {
            data->overflow_values[index] = std::make_unique<std::string>();
            data->is_overflow[index] = true;
        }
        *data->overflow_values[index] = new_value.ToString();
        data->value_sizes[index] = new_value.size();
    }
}

void ContinuousPhysicalRange::rebuildSlices() const {
    data->internal_key_slices.clear();
    data->user_key_slices.clear();
    data->value_slices.clear();
    
    for (size_t i = 0; i < range_length; i++) {
        data->internal_key_slices.emplace_back(data->keys_buffer.get() + data->key_offsets[i], data->key_sizes[i]);
        data->user_key_slices.emplace_back(data->keys_buffer.get() + data->key_offsets[i], data->key_sizes[i] - internal_key_extra_bytes);
        
        if (data->is_overflow[i]) {
            data->value_slices.emplace_back(*data->overflow_values[i]);
        } else {
            data->value_slices.emplace_back(data->values_buffer.get() + data->value_offsets[i], data->value_sizes[i]);
        }
    }
}

void ContinuousPhysicalRange::rebuildSlicesAt(size_t index) const {
    data->internal_key_slices[index] = Slice(data->keys_buffer.get() + data->key_offsets[index], data->key_sizes[index]);
    data->user_key_slices[index] = Slice(data->keys_buffer.get() + data->key_offsets[index], data->key_sizes[index] - internal_key_extra_bytes);
    
    if (data->is_overflow[index]) {
        data->value_slices[index] = Slice(*data->overflow_values[index]);
    } else {
        data->value_slices[index] = Slice(data->values_buffer.get() + data->value_offsets[index], data->value_sizes[index]);
    }
}

}  // namespace ROCKSDB_NAMESPACE
