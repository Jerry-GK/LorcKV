#include <cassert>
#include <algorithm> 
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

// Only for debug (internal keys)
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

// Threshold for asynchronous resource release (in bytes)
static const size_t kAsyncReleaseThreshold = 16 * 1024 * 1024; // 16MB

SliceVecRange::SliceVecRange(bool valid_) {
    this->data = std::make_shared<RangeData>();
    this->valid = valid_;
    this->range_length = 0;
    this->byte_size = 0;
    this->timestamp = 0;
    this->subrange_view_start_pos = -1;
    if (valid) {
        data = std::make_shared<RangeData>();
    }
}

SliceVecRange::~SliceVecRange() {
    // Check if there's enough data that needs asynchronous cleanup
    if (data && data.use_count() == 1 && byte_size > kAsyncReleaseThreshold) {
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

SliceVecRange::SliceVecRange(std::shared_ptr<RangeData> data_, int subrange_view_start_pos_, size_t range_length_) {
    this->data = data_;
    this->subrange_view_start_pos = subrange_view_start_pos_;
    this->valid = true;
    this->range_length = range_length_;
    // TODO(jr): avoid calculating byte_size in the constructor
    this->byte_size = 0;
    for (size_t i = 0; i < range_length_; i++) {
        this->byte_size += data_->keys[i].size();
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
    assert(valid && range_length > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
    return Slice(data->keys[0]);
}

Slice  SliceVecRange::endKey() const {
    assert(valid && range_length > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
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
    assert(valid && range_length > end_index && subrange_view_start_pos == -1);
    // the only code to create non-negative subrange_view_start_pos
    return SliceVecRange(data, start_index, end_index - start_index + 1);
}

void SliceVecRange::truncate(int targetLength) const {
    assert(valid && range_length > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
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
    std::string str = "< " + ToStringPlain(this->startKey().ToString()) + " -> " + ToStringPlain(this->endKey().ToString()) + " >";
    return str;
}

// Function to combine ordered and non-overlapping ranges with move semantics
SliceVecRange SliceVecRange::concatRangesMoved(std::vector<SliceVecRange>& ranges) {
    if (ranges.empty()) {
        return SliceVecRange(false);
    }
    
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

void SliceVecRange::reserve(size_t len) {
    assert(valid);
    data->keys.reserve(len);
    data->values.reserve(len);
}

void SliceVecRange::emplace(const Slice& key, const Slice& value) {
    assert(valid && subrange_view_start_pos == -1);
    data->keys.emplace_back(key.data(), key.size());
    data->values.emplace_back(value.data(), value.size());
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
