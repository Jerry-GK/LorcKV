#pragma once

#include <set>
#include <string>
#include "rocksdb/lorc_iter.h"

namespace ROCKSDB_NAMESPACE {

class RBTreeLogicalOrderedRangeCache;
class PhysicalRange;

class RBTreeLogicalOrderedRangeCacheIterator : public LogicalOrderedRangeCacheIterator {
public:
    explicit RBTreeLogicalOrderedRangeCacheIterator(const RBTreeLogicalOrderedRangeCache* cache);
    
    ~RBTreeLogicalOrderedRangeCacheIterator() override;
    bool Valid() const override;
    bool HasNextInRange() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target_internal_key) override;
    void SeekForPrev(const Slice& target_internal_key) override;
    void Next() override;
    void Prev() override;
    Slice key() const override;
    Slice userKey() const override;
    Slice value() const override;
    Status status() const override;

private:
    const RBTreeLogicalOrderedRangeCache* cache;
    std::set<std::unique_ptr<PhysicalRange>>::const_iterator current_range;
    int current_index;
    Status iter_status;
    bool valid;
};

}  // namespace ROCKSDB_NAMESPACE
