#include <fstream>
#include <sstream>
#include <iostream>
#include "KVMap.h"
#include "KVMapIterator.h"

bool KVMap::Put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = value;
    return true;
}

bool KVMap::PutBatch(const std::vector<std::string>& keys,
                     const std::vector<std::string>& values) {
    if (keys.size() != values.size()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        data_[keys[i]] = values[i];
    }
    return true;
}

bool KVMap::InitFromCSV(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    data_.clear();
    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string key, value;
        if (std::getline(ss, key, ',') && std::getline(ss, value)) {
            data_[key] = value;
        }
    }
    return true;
}

std::string KVMap::Get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end()) {
        return ""; 
    }
    return it->second;
}

std::pair<std::vector<std::string>, std::vector<std::string>>
KVMap::Scan(const std::string& startKey, const std::string& endKey) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys, values;
    auto it = data_.lower_bound(startKey);
    while (it != data_.end() && it->first <= endKey) {
        keys.push_back(it->first);
        values.push_back(it->second);
        ++it;
    }
    return {keys, values};
}

std::pair<std::vector<std::string>, std::vector<std::string>>
KVMap::Scan(const std::string& startKey, size_t len) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys, values;
    auto it = data_.lower_bound(startKey);
    while (it != data_.end() && keys.size() < len) {
        keys.push_back(it->first);
        values.push_back(it->second);
        ++it;
    }
    return {keys, values};
}

bool KVMap::Validate(const std::vector<std::string>& keys,
                    const std::vector<std::string>& values) const {
    if (keys.size() != values.size()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (size_t i = 0; i < keys.size(); ++i) {
        auto it = data_.find(keys[i]);
        if (it == data_.end()) {
            std::cout << "Key not found: " << keys[i] << std::endl;
            return false;
        }
        if (it->second != values[i]) {
            std::cout << "Value mismatch for key " << keys[i] 
                     << ": expected " << values[i] 
                     << ", got " << it->second << std::endl;
            return false;
        }
    }
    return true;
}

bool KVMap::DumpToCSV(const std::string& filename) const {
    std::ofstream file(filename);
    if (!file.is_open()) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, value] : data_) {
        file << key << "," << value << "\n";
    }
    return true;
}

size_t KVMap::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
}

size_t KVMap::MemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = 0;
    for (const auto& [key, value] : data_) {
        total += key.capacity() + value.capacity();
    }
    return total;
}

// KVMap iterator methods
KVMapIterator* KVMap::newKVMapIterator() const {
    return new KVMapIterator(this, data_.begin(), false);
}
