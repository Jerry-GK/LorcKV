#pragma once

#include <string>
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class LogicallyOrderedSliceVecRangeCache;

class SliceVecRangeCacheIterator : public InternalIterator {
public:
    SliceVecRangeCacheIterator() = default;
    // No copying allowed
    SliceVecRangeCacheIterator(const SliceVecRangeCacheIterator&) = delete;
    void operator=(const SliceVecRangeCacheIterator&) = delete;

    virtual ~SliceVecRangeCacheIterator() = default;

    virtual bool Valid() const override = 0;
    virtual void SeekToFirst() override = 0;
    virtual void SeekToLast() override = 0;
    virtual void Seek(const Slice& target_internal_key) override = 0;
    virtual void SeekForPrev(const Slice& target_internal_key) override = 0;
    virtual void Next() override = 0;
    virtual void Prev() override = 0;
    virtual Slice key() const override = 0;
    virtual Slice value() const override = 0;
    virtual Status status() const override = 0;

    virtual bool HasNextInRange() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
