#pragma once

#include <map>
#include <string>
#include <vector>
#include <mutex>  

class KVMapIterator;

class KVMap {
public:
    KVMap() = default;
    ~KVMap() = default;

    // write
    bool Put(const std::string& key, const std::string& value);
    bool PutBatch(const std::vector<std::string>& keys, 
                  const std::vector<std::string>& values);
    bool InitFromCSV(const std::string& filename);
    bool DumpToCSV(const std::string& filename) const;

    // read
    std::string Get(const std::string& key) const; 
    std::pair<std::vector<std::string>, std::vector<std::string>> 
    Scan(const std::string& startKey, const std::string& endKey) const;
    std::pair<std::vector<std::string>, std::vector<std::string>> 
    Scan(const std::string& startKey, size_t len) const;
    bool Validate(const std::vector<std::string>& keys,
                 const std::vector<std::string>& values) const;
    size_t Size() const;
    size_t MemoryUsage() const;

    KVMapIterator* newKVMapIterator() const;

private:
    friend class KVMapIterator;
    mutable std::mutex mutex_; 
    std::map<std::string, std::string> data_;
};
