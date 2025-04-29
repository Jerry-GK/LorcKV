#pragma once

#include <set>
#include <string>
#include "ContRangeCacheIterator.h"

class RBTreeContRangeCache;
class ContRange;

class RBTreeContRangeCacheIterator : public ContRangeCacheIterator {
public:
    explicit RBTreeContRangeCacheIterator(const RBTreeContRangeCache* cache);
    
    ~RBTreeContRangeCacheIterator() override;
    bool Valid() const override;
    bool HasNextInRange() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target) override;
    void SeekForPrev(const Slice& target) override;
    void Next() override;
    void Prev() override;
    const Slice key() const override;
    const Slice value() const override;
    std::string status() const override;

private:
    const RBTreeContRangeCache* cache;
    std::set<ContRange>::const_iterator current_range;
    int current_index;
    std::string status_str;
    bool valid;
};
