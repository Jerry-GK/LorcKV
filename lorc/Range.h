#pragma once

#include <string>
#include <vector>
#include <memory>

/**
 * @brief Range class represents a sorted key-value range in memory
 * It supports range operations like subrange and concatenation
 * with optimized memory management using shared_ptr
 */
class Range {
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
    Range(std::shared_ptr<RangeData> data, int subrange_view_start_pos, size_t size);

public:
    Range(bool valid = false);
    ~Range();      
    // create from vector
    // Range::Range(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size);
    Range(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size);
    // deep copy
    Range(const Range& other);
    // move copy
    Range(Range&& other) noexcept;
    Range& operator=(const Range& other);
    Range& operator=(Range&& other) noexcept;

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

    // the origin range will be invalid after this operation (turn to a temp subrange view only for concatRangesMoved)
    Range subRangeView(size_t start_index, size_t end_index) const;

    // get a subrange copy of the underlying data
    Range subRange(size_t start_index, size_t end_index) const;

    // get a subrange(the largest subrange in [start_key, end_key]) copy of the underlying data 
    Range subRange(std::string start_key, std::string end_key) const;

    // truncate in place
    void truncate(int length) const;

    // return the first element index whose key is < greater than or equal > to key
    int find(std::string key) const;

    std::string toString() const;

    // Merge ordered and non-overlapping ranges
    static Range concatRanges(std::vector<Range>& ranges);

    static Range concatRangesMoved(std::vector<Range>& ranges);

    // Define comparison operator, sort by startKey in ascending order
    bool operator<(const Range& other) const {
        return startKey() < other.startKey();
    }
};