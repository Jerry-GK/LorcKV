// #include <iostream>
// #include <cassert>
// #include <algorithm> 
// #include "Range.h"
// #include "../logger/Logger.h"

// Range::Range(bool valid) {
//     this->valid = valid;
//     this->size = 0;
//     this->timestamp = 0;
//     this->subrange_view_start_pos = -1;
// }

// Range::~Range() {
//     data.reset();
// }

// Range::Range(const Range& other) {
//     // assert(false); // (it should not be called in RBTreeRangeCache)
//     this->valid = other.valid;
//     this->size = other.size;
//     this->timestamp = other.timestamp;
//     this->subrange_view_start_pos = other.subrange_view_start_pos;
    
//     if (other.valid && other.data) {
//         this->data = std::make_shared<RangeData>();
//         this->data->keys = other.data->keys;
//         this->data->values = other.data->values;
//     }
// }

// Range::Range(Range&& other) noexcept {
//     data = std::move(other.data);
//     valid = other.valid;
//     size = other.size;
//     subrange_view_start_pos = other.subrange_view_start_pos;
//     timestamp = other.timestamp;
    
//     other.valid = false;
//     other.size = 0;
//     other.subrange_view_start_pos = -1;
//     other.timestamp = 0;
// }

// // Range::Range(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size) {
// //     data = std::make_shared<RangeData>();
// //     data->keys = keys;
// //     data->values = values;
// //     this->subrange_view_start_pos = -1;
// //     this->valid = true;
// //     this->size = size;
// //     this->timestamp = 0;
// // }

// Range::Range(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size) {
//     data = std::make_shared<RangeData>();
//     data->keys = std::move(keys);
//     data->values = std::move(values);
//     this->subrange_view_start_pos = -1;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

// Range::Range(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size) {
//     this->data = data;
//     this->subrange_view_start_pos = subrange_view_start_pos;
//     this->valid = true;
//     this->size = size;
//     this->timestamp = 0;
// }

// Range& Range::operator=(const Range& other) {
//     // RBTreeRangeCache should never call this
//     // assert(false);
//     if (this != &other) {
//         this->valid = other.valid;
//         this->size = other.size;
//         this->timestamp = other.timestamp;
//         this->subrange_view_start_pos = other.subrange_view_start_pos;
        
//         if (other.valid && other.data) {
//             this->data = std::make_shared<RangeData>();
//             this->data->keys = other.data->keys;
//             this->data->values = other.data->values;
//         } else {
//             this->data.reset();
//         }
//     }
//     return *this;
// }

// Range& Range::operator=(Range&& other) noexcept {
//     if (this != &other) {
//         // Clean up current object's resources
//         data.reset();
        
//         // Move resources from other
//         data = std::move(other.data);
//         valid = other.valid;
//         size = other.size;
//         subrange_view_start_pos = other.subrange_view_start_pos;
//         timestamp = other.timestamp;
        
//         // Reset other object's state
//         other.valid = false;
//         other.size = 0;
//         other.subrange_view_start_pos = -1;
//         other.timestamp = 0;
//     }
//     return *this;
// }

// std::string& Range::startKey() const {
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     return data->keys[0];
// }

// std::string& Range::endKey() const {
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     return data->keys[size - 1];
// }   

// std::string& Range::keyAt(size_t index) const {
//     assert(valid && size > index && subrange_view_start_pos == -1);
//     return data->keys[index];
// }

// std::string& Range::valueAt(size_t index) const {
//     assert(valid && size > index && subrange_view_start_pos == -1);
//     return data->values[index];
// }

// std::vector<std::string>& Range::getKeys() const {
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     return data->keys;
// }

// std::vector<std::string>& Range::getValues() const {
//     assert(valid && size > 0 && data->values.size() > 0 && subrange_view_start_pos == -1);
//     return data->values;
// }

// size_t Range::getSize() const {
//     return size;
// }   

// bool Range::isValid() const {
//     return valid;
// }

// int Range::getTimestamp() const {
//     return timestamp;
// }

// void Range::setTimestamp(int timestamp) const {
//     this->timestamp = timestamp;
// }

// Range Range::subRangeView(size_t start_index, size_t end_index) const {
//     assert(valid && size > end_index && start_index >= 0 && subrange_view_start_pos == -1);
//     // the only code to create non-negative subrange_view_start_pos
//     return Range(data, start_index, end_index - start_index + 1);
// }

// Range Range::subRange(size_t start_index, size_t end_index) const {
//     assert(valid && size > end_index && subrange_view_start_pos == -1);
    
//     // a subrange copy of the underlying data
//     std::vector<std::string> sub_keys(
//         data->keys.begin() + start_index,
//         data->keys.begin() + end_index + 1
//     );
//     std::vector<std::string> sub_values(
//         data->values.begin() + start_index,
//         data->values.begin() + end_index + 1
//     );
    
//     return Range(std::move(sub_keys), std::move(sub_values), end_index - start_index + 1);
// }

// Range Range::subRange(std::string start_key, std::string end_key) const {
//     // use binary search to find the start and end index
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     // auto start_it = std::lower_bound(keys.begin(), keys.end(), start_key);
//     // auto end_it = std::upper_bound(keys.begin(), keys.end(), end_key);

//     size_t start_index = this->find(start_key);
//     size_t end_index = this->find(end_key);
//     if (end_index != -1 && data->keys[end_index] > end_key) {
//         end_index--;
//     }
//     if (start_index == -1 || end_index == -1) {
//         return Range(false);
//     }

//     return this->subRange(start_index, end_index);
// }

// void Range::truncate(int length) const {
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     if (length < 0 || length > size) {
//         Logger::error("Invalid length to truncate");
//         return;
//     }
    
//     data->keys.resize(length);
//     data->values.resize(length);
//     size = length;
// }

// int Range::find(std::string key) const {
//     assert(valid && size > 0 && data->keys.size() > 0 && subrange_view_start_pos == -1);
//     if (!valid || size == 0) {
//         return -1;
//     }
    
//     int left = 0;
//     int right = size - 1;
    
//     while (left <= right) {
//         int mid = left + (right - left) / 2;
//         if (data->keys[mid] == key) {
//             return mid;
//         } else if (data->keys[mid] < key) {
//             left = mid + 1;
//         } else {
//             right = mid - 1;
//         }
//     }
    
//     if (left >= size) {
//         return -1;
//     }
//     return left;
// }

// std::string Range::toString() const {
//     std::string str = "< " + this->startKey() + " -> " + this->endKey() + " >";
//     return str;
// }

// Range Range::concatRanges(std::vector<Range>& ranges) {
//     if (ranges.empty()) {
//         return Range(false);
//     }
    
//     std::vector<std::string> mergedKeys;
//     std::vector<std::string> mergedValues;
    
//     for (const auto& range : ranges) {
//         auto keys = range.getKeys();
//         auto values = range.getValues();
//         mergedKeys.insert(mergedKeys.end(), keys.begin(), keys.end());
//         mergedValues.insert(mergedValues.end(), values.begin(), values.end());
//     }
    
//     return Range(std::move(mergedKeys), std::move(mergedValues), mergedKeys.size());
// }

// // Function to combine ordered and non-overlapping ranges with move semantics
// Range Range::concatRangesMoved(std::vector<Range>& ranges) {
//     if (ranges.empty()) {
//         return Range(false);
//     }
    
//     size_t total_size = 0;
//     for (const auto& range : ranges) {
//         total_size += range.getSize();
//     }

//     auto new_data = std::make_shared<RangeData>();
//     new_data->keys.reserve(total_size);
//     new_data->values.reserve(total_size);

//     // Move data from each range into the new combined range
//     for (auto& range : ranges) {
//         auto src_data = range.data; 
//         int subrange_view_start_pos = range.subrange_view_start_pos;
//         int range_start = 0;
//         if (subrange_view_start_pos != -1) {
//             range_start = subrange_view_start_pos;
//         }
//         size_t len = range.getSize();
        
//         new_data->keys.insert(new_data->keys.end(),
//             std::make_move_iterator(src_data->keys.begin() + range_start),
//             std::make_move_iterator(src_data->keys.begin() + range_start + len));
//         new_data->values.insert(new_data->values.end(),
//             std::make_move_iterator(src_data->values.begin() + range_start),
//             std::make_move_iterator(src_data->values.begin() + range_start + len));
//     }

//     return Range(new_data, -1, total_size);
// }