#pragma once

#include <set>
#include <string>
#include "VecRangeCacheIterator.h"

class RBTreeVecRangeCache;
class VecRange;

class RBTreeVecRangeCacheIterator : public VecRangeCacheIterator {
public:
    explicit RBTreeVecRangeCacheIterator(const RBTreeVecRangeCache* cache);
    
    ~RBTreeVecRangeCacheIterator() override;
    bool Valid() const override;
    bool HasNextInRange() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const std::string& target) override;
    void SeekForPrev(const std::string& target) override;
    void Next() override;
    void Prev() override;
    const std::string& key() const override;
    const std::string& value() const override;
    std::string status() const override;

private:
    const RBTreeVecRangeCache* cache;
    std::set<VecRange>::const_iterator current_range;
    int current_index;
    std::string status_str;
    bool valid;
};
