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

    Range subRange(size_t start_index, size_t end_index) const;

private:
    std::vector<std::string> keys;
    std::vector<std::string> values;
    size_t size;
    bool valid;
};