#pragma once

#include <set>
#include <string>
#include "RangeCacheIterator.h"

class RBTreeRangeCache;
class Range;

class RBTreeRangeCacheIterator : public RangeCacheIterator {
public:
    explicit RBTreeRangeCacheIterator(const RBTreeRangeCache* cache);
    
    bool Valid() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const std::string& target) override;
    void SeekForPrev(const std::string& target) override;
    void Next() override;
    void Prev() override;
    std::string key() const override;
    std::string value() const override;
    std::string status() const override;

private:
    const RBTreeRangeCache* cache;
    std::set<Range>::iterator current;
    std::set<Range>::iterator current_range;
    int current_index;
    std::string error_status;
    bool valid;
};
