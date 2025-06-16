#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "db/dbformat.h"
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

// Only for debug (internal internal_keys)
std::string SliceVecRange::ToStringPlain(std::string s) {
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

// Global resource cleanup thread pool
class ResourceCleanupThreadPool {
public:
    static ResourceCleanupThreadPool& GetInstance() {
        static ResourceCleanupThreadPool instance;
        return instance;
    }

    void Schedule(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(std::move(task));
        }
        condition_.notify_one();
    }

    ~ResourceCleanupThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        condition_.notify_all();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

private:
    ResourceCleanupThreadPool(size_t thread_count = 1) : stop_(false) {
        for (size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        condition_.wait(lock, [this] { 
                            return stop_ || !tasks_.empty(); 
                        });
                        
                        if (stop_ && tasks_.empty()) {
                            return;
                        }
                        
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool stop_;
};

SliceVecRange::SliceVecRange(bool valid_) {
    this->data = std::make_shared<RangeData>();
    this->valid = valid_;
    this->range_length = 0;
    this->byte_size = 0;
    this->timestamp = 0;
    if (valid) {
        data = std::make_shared<RangeData>();
    }
}

// TODO(jr): find out is it necessary to deconstruct SliceVecRange asynchronously
SliceVecRange::~SliceVecRange() {
    // Check if there's enough data that needs asynchronous cleanup
    if (enable_async_release && data && data.use_count() == 1 && byte_size > kAsyncReleaseThreshold) {
        // Create a copy of the data for asynchronous release
        auto data_to_release = std::move(data);
        
        // Schedule an asynchronous task to release the resources
        ResourceCleanupThreadPool::GetInstance().Schedule([data_to_release]() {
            // data_to_release will be automatically released in the background thread
        });
    } else {
        // For small data volumes or shared references (count > 1), release directly in the current thread
        data.reset();
    }
}

SliceVecRange::SliceVecRange(const SliceVecRange& other) {
    // assert(false); // (it should not be called in RBTreeSliceVecRangeCache)
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

SliceVecRange::SliceVecRange(SliceVecRange&& other) noexcept {
    this->data = std::move(other.data);
    this->valid = other.valid;
    this->range_length = other.range_length;
    this->byte_size = other.byte_size;
    this->timestamp = other.timestamp;

    other.valid = false;
    other.range_length = 0;
    other.timestamp = 0;
}

// SliceVecRange::SliceVecRange(std::vector<std::string>&& internal_keys, std::vector<std::string>&& values, size_t range_length) {
//     data = std::make_shared<RangeData>();
//     data->internal_keys = std::move(internal_keys);
//     data->values = std::move(values);
//     this->valid = true;
//     this->range_length = range_length;
//     this->timestamp = 0;
// }

// DEPRECATED: used to generate a temp range to compare
// SliceVecRange::SliceVecRange(Slice startUserKey) {
//     data = std::make_shared<RangeData>();
//     data->internal_keys.push_back(startUserKey.ToString());
//     data->internal_keys[0].append(internal_key_extra_bytes, '\0');
//     this->valid = true;
//     this->range_length = 1;
//     this->byte_size = data->internal_keys[0].size();
//     this->timestamp = 0;
// }

SliceVecRange::SliceVecRange(std::shared_ptr<RangeData> data_, size_t range_length_) {
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

SliceVecRange& SliceVecRange::operator=(const SliceVecRange& other) {
    // RBTreeSliceVecRangeCache should never call this
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

SliceVecRange& SliceVecRange::operator=(SliceVecRange&& other) noexcept {
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

const Slice& SliceVecRange::startUserKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0 && data->internal_keys[0].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[0].data(), data->internal_keys[0].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[0];
}

const Slice& SliceVecRange::endUserKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0 && data->internal_keys[range_length - 1].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[range_length - 1].data(), data->internal_keys[range_length - 1].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[range_length - 1];
}  

const Slice& SliceVecRange::startInternalKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0);
    // return Slice(data->internal_keys[0]);
    return this->data->internal_key_slices[0];
}

const Slice& SliceVecRange::endInternalKey() const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0);
    // return Slice(data->internal_keys[range_length - 1]);
    return this->data->internal_key_slices[range_length - 1];
}   

const Slice& SliceVecRange::internalKeyAt(size_t index) const {
    assert(valid && range_length > index);
    // return Slice(data->internal_keys[index]);
    return this->data->internal_key_slices[index];
}

const Slice& SliceVecRange::userKeyAt(size_t index) const {
    assert(valid && range_length > index && data->internal_keys[index].size() > internal_key_extra_bytes);
    // return Slice(data->internal_keys[index].data(), data->internal_keys[index].size() - internal_key_extra_bytes);
    return this->data->user_key_slices[index];
}

const Slice& SliceVecRange::valueAt(size_t index) const {
    assert(valid && range_length > index);
    // return Slice(data->values[index]);
    return this->data->value_slices[index];
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

bool SliceVecRange::update(const Slice& internal_key, const Slice& value) const {
    assert(valid && range_length > 0 && internal_key.size() > internal_key_extra_bytes);    
    Slice user_key = Slice(internal_key.data(), internal_key.size() - SliceVecRange::internal_key_extra_bytes);
    int index = find(user_key);
    // dont use internal_key for comparison (the last bit may be kTypeValue in Range Cache and kTypeBlobIndex in LSM)
    if (index >= 0 && userKeyAt(index) == user_key) {
        // update the sequence number of the internal key, the type is turned to kTypeRangeCacheValue
        ParsedInternalKey parsed_internal_key;
        Status s = ParseInternalKey(internal_key, &parsed_internal_key, false);
        SequenceNumber seq_num = parsed_internal_key.sequence;
        if (!s.ok()) {
            return false;
        }
        std::string new_internal_key_str = InternalKey(user_key, seq_num, kTypeRangeCacheValue).Encode().ToString();

        ParsedInternalKey old_parsed_internal_key;
        s = ParseInternalKey(Slice(data->internal_keys[index]), &old_parsed_internal_key, false);
        if (!s.ok()) {
            return false;
        }
        SequenceNumber old_seq_num = old_parsed_internal_key.sequence;

        data->internal_keys[index] = new_internal_key_str;
        
        // update the value
        data->values[index] = value.ToString();
        return true;
    }
    return false;
}

void SliceVecRange::truncate(int targetLength) const {
    assert(valid && range_length > 0 && data->internal_keys.size() > 0);
    if (targetLength < 0 || (size_t)targetLength > range_length) {
        assert(false);
        return;
    }
    
    data->internal_keys.resize(targetLength);
    data->values.resize(targetLength);
    range_length = targetLength;
    byte_size = 0;
    for (size_t i = 0; i < range_length; i++) {
        byte_size += data->internal_keys[i].size();
    }
}

int SliceVecRange::find(const Slice& key) const {
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

std::string SliceVecRange::toString() const {
    std::string str = "< " + ToStringPlain(this->startUserKey().ToString()) + " -> " + ToStringPlain(this->endUserKey().ToString()) + " >"
        + " ( len = " + std::to_string(this->length()) + " )";
    // if ((int)this->length() != std::stoi(ToStringPlain(this->endUserKey().ToString())) - std::stoi(ToStringPlain(this->startUserKey().ToString())) + 1) {
    //     str += " (warning: length mismatch)";
    //     assert(false);
    // }
    return str;
}

// Function to combine ordered and non-overlapping ranges with move semantics
SliceVecRange SliceVecRange::concatRangesMoved(std::vector<SliceVecRange>& ranges) {
    if (ranges.empty()) {
        return SliceVecRange(false);
    }
    if (ranges.size() == 1) {
        return ranges[0];
    }
    
    // find the longest SliceVecRange
    int max_length_index = 0;
    size_t max_length = 0;
    size_t total_length = 0;
    
    for (int i = 0; i < (int)ranges.size(); i++) {
        size_t current_length = ranges[i].length();
        total_length += current_length;
        if (current_length > max_length) {
            max_length = current_length;
            max_length_index = i;
        }
    }
    
    // take the longest range as the base
    auto base_data = ranges[max_length_index].data;
    base_data->internal_keys.reserve(total_length);
    base_data->values.reserve(total_length);

    for (int i = max_length_index -1; i >= 0; i--) {
        base_data->internal_keys.insert(base_data->internal_keys.begin(),
            std::make_move_iterator(ranges[i].data->internal_keys.begin()),
            std::make_move_iterator(ranges[i].data->internal_keys.end()));
        base_data->values.insert(base_data->values.begin(),
            std::make_move_iterator(ranges[i].data->values.begin()),
            std::make_move_iterator(ranges[i].data->values.end()));
        base_data->internal_key_slices.insert(base_data->internal_key_slices.begin(),
            std::make_move_iterator(ranges[i].data->internal_key_slices.begin()),
            std::make_move_iterator(ranges[i].data->internal_key_slices.end()));
        base_data->user_key_slices.insert(base_data->user_key_slices.begin(),
            std::make_move_iterator(ranges[i].data->user_key_slices.begin()),
            std::make_move_iterator(ranges[i].data->user_key_slices.end()));
        base_data->value_slices.insert(base_data->value_slices.begin(),
            std::make_move_iterator(ranges[i].data->value_slices.begin()),
            std::make_move_iterator(ranges[i].data->value_slices.end()));
    }

    for (int i = max_length_index + 1; i < (int)ranges.size(); i++) {
        base_data->internal_keys.insert(base_data->internal_keys.end(),
            std::make_move_iterator(ranges[i].data->internal_keys.begin()),
            std::make_move_iterator(ranges[i].data->internal_keys.end()));
        base_data->values.insert(base_data->values.end(),
            std::make_move_iterator(ranges[i].data->values.begin()),
            std::make_move_iterator(ranges[i].data->values.end()));
        base_data->internal_key_slices.insert(base_data->internal_key_slices.end(),
            std::make_move_iterator(ranges[i].data->internal_key_slices.begin()),
            std::make_move_iterator(ranges[i].data->internal_key_slices.end()));
        base_data->user_key_slices.insert(base_data->user_key_slices.end(),
            std::make_move_iterator(ranges[i].data->user_key_slices.begin()),
            std::make_move_iterator(ranges[i].data->user_key_slices.end()));
        base_data->value_slices.insert(base_data->value_slices.end(),
            std::make_move_iterator(ranges[i].data->value_slices.begin()),
            std::make_move_iterator(ranges[i].data->value_slices.end()));   
    }
    
    return SliceVecRange(base_data, total_length);
}

void SliceVecRange::reserve(size_t len) {
    assert(valid);
    data->internal_keys.reserve(len);
    data->values.reserve(len);
    data->internal_key_slices.reserve(len);
    data->user_key_slices.reserve(len);
    data->value_slices.reserve(len);
}

void SliceVecRange::emplace(const Slice& internal_key, const Slice& value) {
    assert(valid);
    range_length++;
    byte_size += internal_key.size() + value.size();
    data->internal_keys.emplace_back(internal_key.data(), internal_key.size());
    data->values.emplace_back(value.data(), value.size());
    data->internal_key_slices.emplace_back(data->internal_keys.back().data(), data->internal_keys.back().size());
    data->user_key_slices.emplace_back(data->internal_keys.back().data(), data->internal_keys.back().size() - internal_key_extra_bytes);
    data->value_slices.emplace_back(data->values.back().data(), data->values.back().size());
}

// void SliceVecRange::emplaceMoved(std::string& internal_key, std::string& value) {
//     assert(valid);
//     data->internal_keys.emplace_back(std::move(internal_key));
//     data->values.emplace_back(std::move(value));
//     range_length++;
//     byte_size += data->internal_keys.back().size() + data->values.back().size();
// }

}  // namespace ROCKSDB_NAMESPACE
