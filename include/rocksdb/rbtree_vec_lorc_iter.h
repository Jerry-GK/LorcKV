#pragma once

#include <set>
#include <string>
#include "rocksdb/vec_lorc_iter.h"

namespace ROCKSDB_NAMESPACE {

class RBTreeSliceVecRangeCache;
class SliceVecRange;

class RBTreeSliceVecRangeCacheIterator : public SliceVecRangeCacheIterator {
public:
    explicit RBTreeSliceVecRangeCacheIterator(const RBTreeSliceVecRangeCache* cache);
    
    ~RBTreeSliceVecRangeCacheIterator() override;
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
    const RBTreeSliceVecRangeCache* cache;
    std::set<SliceVecRange>::const_iterator current_range;
    int current_index;
    Status iter_status;
    bool valid;
};

}  // namespace ROCKSDB_NAMESPACE
