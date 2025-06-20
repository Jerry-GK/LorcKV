#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "db/dbformat.h"
#include "rocksdb/vec_physical_range.h"

namespace ROCKSDB_NAMESPACE {

std::string VecPhysicalRange::toString() const {
    std::string str = "< " + ToStringPlain(this->startUserKey().ToString()) + " -> " + ToStringPlain(this->endUserKey().ToString()) + " >"
        + " ( len = " + std::to_string(this->length()) + " )";
    return str;
}

VecPhysicalRange::VecPhysicalRange(bool valid_) {
    this->data = std::make_shared<RangeData>();
    this->valid = valid_;
    this->range_length = 0;
    this->byte_size = 0;
    this->timestamp = 0;
    if (valid) {
        data = std::make_shared<RangeData>();
    }
}

// TODO(jr): find out is it necessary to deconstruct VecPhysicalRange asynchronously
VecPhysicalRange::~VecPhysicalRange() {
    data.reset();
}

VecPhysicalRange::VecPhysicalRange(const VecPhysicalRange& other) : PhysicalRange(other.valid) {
    // assert(false); // (it should not be called in RBTreeVecPhysicalRangeCache)
    this->valid = other.valid;
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;

    if (other.valid && other.data) {
        this->data = std::make_shared<RangeData>();
        this->data->internal_keys = other.data->internal_keys;
        this->data->values = other.data->values;
        for (size_t i = 0; i < this->range_length; i++) {
            this->data->internal_key_slices.emplace_back(this->data->internal_keys[i].data(), this->data->internal_keys[i].size() - internal_key_extra_bytes);
            this->data->user_key_slices.emplace_back(this->data->internal_keys[i].data(), this->data->internal_keys[i].size() - internal_key_extra_bytes);
            this->data->value_slices.emplace_back(this->data->values[i]);
        }
    }
}

VecPhysicalRange::VecPhysicalRange(VecPhysicalRange&& other) noexcept : PhysicalRange(other.valid) {
    this->data = std::move(other.data);
    this->valid = other.valid;
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;

    other.valid = false;
    other.range_length = 0;
    other.timestamp = 0;
}

VecPhysicalRange::VecPhysicalRange(std::shared_ptr<RangeData> data_, size_t range_length_) {
    this->data = data_;
    this->valid = true;
    this->range_length = range_length_;
    // TODO(jr): avoid calculating byte_size in the constructor
    this->byte_size = 0;
    for (size_t i = 0; i < range_length_; i++) {
        this->byte_size += data_->internal_keys[i].size();
        this->byte_size += data_->values[i].size();
    }
    this->timestamp = 0;
}

VecPhysicalRange& VecPhysicalRange::operator=(const VecPhysicalRange& other) {
    // RBTreeVecPhysicalRangeCache should never call this
    // assert(false);
    if (this != &other) {
        this->valid = other.valid;
        this->range_length = other.range_length;
        this->byte_size = other.byte_size;
        this->timestamp = other.timestamp;

        if (other.valid && other.data) {
            this->data = std::make_shared<RangeData>();
            this->data->internal_keys = other.data->internal_keys;
            this->data->values = other.data->values;
            for (size_t i = 0; i < this->range_length; i++) {
                this->data->internal_key_slices.emplace_back(this->data->internal_keys[i].data(), this->data->internal_keys[i].size() - internal_key_extra_bytes);
                this->data->user_key_slices.emplace_back(this->data->internal_keys[i].data(), this->data->internal_keys[i].size() - internal_key_extra_bytes);
                this->data->value_slices.emplace_back(this->data->values[i]);
            }
        } else {
            this->data.reset();
        }
    }
    return *this;
}

VecPhysicalRange& VecPhysicalRange::operator=(VecPhysicalRange&& other) noexcept {
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

std::unique_ptr<VecPhysicalRange> VecPhysicalRange::buildFromReferringRange(const ReferringRange& refRange) {
    auto newRange = std::make_unique<VecPhysicalRange>(true);
    newRange->reserve(refRange.length());

    for (size_t i = 0; i < refRange.length(); i++) {
        Slice internal_key = refRange.keyAt(i);
        std::string internal_key_str = InternalKey(internal_key, refRange.getSeqNum(), kTypeRangeCacheValue).Encode().ToString();
        Slice value = refRange.valueAt(i);
        newRange->emplaceInternal(Slice(internal_key_str), value);
    }

    return newRange;
}

std::unique_ptr<VecPhysicalRange> VecPhysicalRange::dumpSubRangeFromReferringRange(const ReferringRange& refRange, const Slice& startKey, const Slice& endKey, bool leftIncluded, bool rightIncluded) {
    assert(refRange.isValid() && refRange.length() > 0 && refRange.startKey() <= startKey && startKey <= endKey && endKey <= refRange.endKey());

    // find the start position of the subrange
    int start_pos = refRange.find(startKey);
    if (start_pos < 0 || (size_t)start_pos >= refRange.length()) {
        assert(false);
        return std::make_unique<VecPhysicalRange>(false);
    }

    if (refRange.keyAt(start_pos) == startKey && !leftIncluded) {
        start_pos++;
    }

    int end_pos = refRange.find(endKey);
    if (end_pos < 0) {
        assert(false);
        return std::make_unique<VecPhysicalRange>(false);
    }

    if (refRange.keyAt(end_pos) == endKey) {
        if (!rightIncluded) {
            end_pos--;
        }
    } else {
        end_pos--;
    }

    if (start_pos > end_pos || (size_t)start_pos >= refRange.length() || end_pos < 0) {
        // cant find entries in the range
        return std::make_unique<VecPhysicalRange>(false);
    }

    auto result = std::make_unique<VecPhysicalRange>(true);
    size_t num_elements = end_pos - start_pos + 1;
    result->reserve(num_elements);

    // actually copy the subrange data
    for (int i = start_pos; i <= end_pos; i++) {
        // use kTypeRangeCacheValue as the value type for internal keys of range cache
        std::string internal_key_str = InternalKey(refRange.keyAt(i), refRange.getSeqNum(), kTypeRangeCacheValue).Encode().ToString();
        result->emplaceInternal(Slice(internal_key_str), refRange.valueAt(i));
    }

    return result;
}

const Slice& VecPhysicalRange::startUserKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0 && data->internal_keys[0].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[0].data(), data->internal_keys[0].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[0];
}

const Slice& VecPhysicalRange::endUserKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0 && data->internal_keys[range_length - 1].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[range_length - 1].data(), data->internal_keys[range_length - 1].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[range_length - 1];
}  

const Slice& VecPhysicalRange::startInternalKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0);
    // return Slice(data->internal_keys[0]);
    return this->data->internal_key_slices[0];
}

const Slice& VecPhysicalRange::endInternalKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0);
    // return Slice(data->internal_keys[range_length - 1]);
    return this->data->internal_key_slices[range_length - 1];
}   

const Slice& VecPhysicalRange::internalKeyAt(size_t index) const {
    assert(valid && range_length > index);
    // return Slice(data->internal_keys[index]);
    return this->data->internal_key_slices[index];
}

const Slice& VecPhysicalRange::userKeyAt(size_t index) const {
    assert(valid && range_length > index && data->internal_keys[index].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[index].data(), data->internal_keys[index].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[index];
}

const Slice& VecPhysicalRange::valueAt(size_t index) const {
    assert(valid && range_length > index);
    // return Slice(data->values[index]);
    return this->data->value_slices[index];
}

PhysicalRangeUpdateResult VecPhysicalRange::update(const Slice& internal_key, const Slice& value) const {
    assert(valid && range_length > 0 && internal_key.size() > internal_key_extra_bytes);    
    Slice user_key = Slice(internal_key.data(), internal_key.size() - VecPhysicalRange::internal_key_extra_bytes);
    int index = find(user_key);
    // index == -1 indicates an tail insertion of middle physical range

    // Parse the internal key to get sequence number
    ParsedInternalKey parsed_internal_key;
    Status s = ParseInternalKey(internal_key, &parsed_internal_key, false);
    if (!s.ok()) {
        return PhysicalRangeUpdateResult::ERROR;
    }
    SequenceNumber seq_num = parsed_internal_key.sequence;
    // Create new internal key with kTypeRangeCacheValue type
    std::string new_internal_key_str = InternalKey(user_key, seq_num, kTypeRangeCacheValue).Encode().ToString();

    // dont use internal_key for comparison (the last bit may be kTypeValue in Range Cache and kTypeBlobIndex in LSM)
    if (index >= 0 && userKeyAt(index) == user_key) {
        data->internal_keys[index] = new_internal_key_str;
        // update the value
        data->values[index] = value.ToString();

        return PhysicalRangeUpdateResult::UPDATED;
    } else if (index == -1 || userKeyAt(index) != user_key) {
        assert(index == -1 || userKeyAt(index) > user_key);
        if (index == -1) {
            index = range_length; // tail insertion
        }

        // random insert in vec physical range
        data->internal_keys.insert(data->internal_keys.begin() + index, new_internal_key_str);
        data->values.insert(data->values.begin() + index, value.ToString());
        
        // Insert corresponding slices at the same position
        data->internal_key_slices.insert(data->internal_key_slices.begin() + index, 
            Slice(data->internal_keys[index].data(), data->internal_keys[index].size()));
        data->user_key_slices.insert(data->user_key_slices.begin() + index,
            Slice(data->internal_keys[index].data(), data->internal_keys[index].size() - internal_key_extra_bytes));
        data->value_slices.insert(data->value_slices.begin() + index,
            Slice(data->values[index].data(), data->values[index].size()));
        
        range_length++;
        byte_size += new_internal_key_str.size() + value.size();
        
        return PhysicalRangeUpdateResult::INSERTED;
    }
    
    return PhysicalRangeUpdateResult::ERROR;
}

int VecPhysicalRange::find(const Slice& key) const {
    assert(valid && range_length > 0 && key.size() > 0);

    if (!valid || range_length == 0) {
        return -1;
    }

    int left = 0;
    int right = range_length - 1;

    while (left <= right) {
        int mid = left + (right - left) / 2;
        Slice mid_user_key = userKeyAt(mid); // use user key for inner comparison
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

void VecPhysicalRange::reserve(size_t len) {
    assert(valid);
    data->internal_keys.reserve(len);
    data->values.reserve(len);
    data->internal_key_slices.reserve(len);
    data->user_key_slices.reserve(len);
    data->value_slices.reserve(len);
}

void VecPhysicalRange::emplaceInternal(const Slice& internal_key, const Slice& value) {
    assert(valid);
    range_length++;
    byte_size += internal_key.size() + value.size();
    data->internal_keys.emplace_back(internal_key.data(), internal_key.size());
    data->values.emplace_back(value.data(), value.size());
    data->internal_key_slices.emplace_back(data->internal_keys.back().data(), data->internal_keys.back().size());
    data->user_key_slices.emplace_back(data->internal_keys.back().data(), data->internal_keys.back().size() - internal_key_extra_bytes);
    data->value_slices.emplace_back(data->values.back().data(), data->values.back().size());
}

}  // namespace ROCKSDB_NAMESPACE