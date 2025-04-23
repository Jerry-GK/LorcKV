#pragma once

#include <string>
#include <vector>

class Range {
public:
    Range(bool valid);
    // create from vector
    Range(std::vector<std::string> keys, std::vector<std::string> values, size_t size);
    ~Range();      

    std::string startKey() const;
    std::string endKey() const;

    std::string keyAt(size_t index) const;
    std::string valueAt(size_t index) const;

    std::vector<std::string> getKeys() const;
    std::vector<std::string> getValues() const;
    size_t getSize() const;
    bool isValid() const;
    int getTimestamp() const;
    void setTimestamp(int timestamp) const;

    Range subRange(size_t start_index, size_t end_index) const;
    Range subRange(std::string start_key, std::string end_key) const;

    int find(std::string key) const;

    std::string toString() const;

    // concat ranges that is ordered and not overlapping
    static Range concatRanges(std::vector<Range>& ranges);

    // 定义比较运算符，按startKey从小到大排序
    bool operator<(const Range& other) const {
        return startKey() < other.startKey();
    }

private:
    std::vector<std::string> keys;
    std::vector<std::string> values;
    size_t size;
    bool valid;
    mutable int timestamp;
};