#pragma once

#include <vector>
#include <memory>
#include <string>
#include "../logger/Logger.h"
#include "Slice.h"

/**
 * @brief ContRange class represents a sorted key-value ContRange in memory
 * It supports ContRange operations like subrange and concatenation
 * with optimized memory management using shared_ptr
 */
class ContRange {
private:
    struct StringView {
        size_t offset;
        size_t length;
    };

    struct RangeData {
        char* keys_buffer;         // keys的连续存储区
        char* values_buffer;       // values的连续存储区
        size_t keys_buffer_size;   // keys buffer总大小
        size_t values_buffer_size; // values buffer总大小
        StringView* key_views;     // 存储每个key的位置和长度
        StringView* value_views;   // 存储每个value的位置和长度
    };
    
    std::shared_ptr<RangeData> data;
    mutable size_t size;          // key-value对的数量
    mutable bool valid;
    mutable int timestamp;
    int subrange_view_start_pos;  

private:
    ContRange(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size);

public:
ContRange(bool valid = false);
    ~ContRange();      
    // create from vector
    // ContRange::ContRange(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size);
    ContRange(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size);
    // create a ContRange with a single key-value pair, used for seekx
    ContRange(Slice startKey);
    // deep copy
    ContRange(const ContRange& other);
    // move copy
    ContRange(ContRange&& other) noexcept;
    ContRange& operator=(const ContRange& other);
    ContRange& operator=(ContRange&& other) noexcept;

    // reserve the range
    void reserve(size_t keys_buffer_size, size_t values_buffer_size, size_t len);
    
    // empalce a key-value pair copy
    void emplace(const Slice& key, const Slice& value);

    Slice startKey() const;
    Slice endKey() const;

    Slice keyAt(size_t index) const;
    Slice valueAt(size_t index) const;

    size_t getSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    // the origin ContRange will be invalid after this operation (turn to a temp subrange view only for concatRangesMoved)
    ContRange subRangeView(size_t start_index, size_t end_index) const;

    // truncate in place
    void truncate(int length) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(const Slice& key) const;

    std::string toString() const;

    // Merge ordered and non-overlapping ranges
    static ContRange concatRangesMoved(std::vector<ContRange>& ranges);

    bool operator<(const ContRange& other) const {
        return this->startKey() < other.startKey();
    }
};