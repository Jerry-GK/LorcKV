#pragma once

#include <string>
#include <vector>
#include <memory>

class Range {
private:
    struct RangeData {
        std::vector<std::string> keys;
        std::vector<std::string> values;
    };
    std::shared_ptr<RangeData> data;
    // subrange start (if it's not 0, it means this range is a subrange of the original range, and is only used for range concat)
    size_t start_pos;  
    mutable size_t size;
    mutable bool valid;
    mutable int timestamp;

public:
    Range(bool valid = false);
    // create from vector
    // Range::Range(std::vector<std::string>& keys, std::vector<std::string>& values, size_t size);
    Range(std::vector<std::string>&& keys, std::vector<std::string>&& values, size_t size);
    // deep copy
    Range(const Range& other);
    // move copy
    Range(Range&& other) noexcept;
    // moved construction
    Range(std::shared_ptr<RangeData> data, size_t start, size_t size);
    ~Range();      
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

    Range subRange(size_t start_index, size_t end_index) const;
    Range subRangeMoved(size_t start_index, size_t end_index) const;
    Range subRange(std::string start_key, std::string end_key) const;

    void truncate(int length) const;

    int find(std::string key) const;

    std::string toString() const;

    // concat ranges that is ordered and not overlapping
    static Range concatRanges(std::vector<Range>& ranges);


    static Range concatRangesMoved(std::vector<Range>& ranges);

    // 定义比较运算符，按startKey从小到大排序
    bool operator<(const Range& other) const {
        return startKey() < other.startKey();
    }

    // 添加新方法来获取原始数据的引用
    std::shared_ptr<RangeData> getData() const { return data; }
    size_t getStartPos() const { return start_pos; }
};