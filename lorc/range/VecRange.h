#pragma once

#include <string>
#include <vector>
#include <memory>

/**
 * @brief VecRange class represents a sorted key-value VecRange in memory
 * It supports VecRange operations like subrange and concatenation
 * with optimized memory management using shared_ptr
 */
class VecRange {
private:
    struct RangeData {
        std::vector<std::string> keys;
        std::vector<std::string> values;
    };
    std::shared_ptr<RangeData> data;
    mutable size_t size;
    mutable bool valid;
    mutable int timestamp;

    // Start position for subrange operations (default -1). If >= 0, indicates this is a subrange view, and only used in concatRangesMoved
    int subrange_view_start_pos;  

private:
    // construct by data pointer and subrange_view_start_pos, it's a subrange view if subrange_view_start_pos >= 0
    VecRange(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size);

public:
    VecRange(bool valid = false);
    ~VecRange();      
    // create from vector
    // VecRange::VecRange(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size);
    VecRange(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size);
    // deep copy
    VecRange(const VecRange& other);
    // move copy
    VecRange(VecRange&& other) noexcept;
    VecRange& operator=(const VecRange& other);
    VecRange& operator=(VecRange&& other) noexcept;

    std::string& startKey() const;
    std::string& endKey() const;

    std::string& keyAt(size_t index) const;
    std::string& valueAt(size_t index) const;

    std::vector<std::string>& getKeys() const;
    std::vector<std::string>& getValues() const;

    size_t getSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    // the origin VecRange will be invalid after this operation (turn to a temp subrange view only for concatRangesMoved)
    VecRange subRangeView(size_t start_index, size_t end_index) const;

    // get a subrange copy of the underlying data
    VecRange subRange(size_t start_index, size_t end_index) const;

    // get a subrange(the largest subrange in [start_key, end_key]) copy of the underlying data 
    VecRange subRange(std::string start_key, std::string end_key) const;

    // truncate in place
    void truncate(int length) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(std::string key) const;

    std::string toString() const;

    // Merge ordered and non-overlapping ranges
    static VecRange concatRanges(std::vector<VecRange>& ranges);

    static VecRange concatRangesMoved(std::vector<VecRange>& ranges);

    // Define comparison operator, sort by startKey in ascending order
    bool operator<(const VecRange& other) const {
        return startKey() < other.startKey();
    }
};