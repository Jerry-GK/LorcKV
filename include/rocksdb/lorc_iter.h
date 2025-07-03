#pragma once

#include <string>
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class LogicalOrderedRangeCache;

class LogicalOrderedRangeCacheIterator : public InternalIterator {
public:
    LogicalOrderedRangeCacheIterator() = default;
    // No copying allowed
    LogicalOrderedRangeCacheIterator(const LogicalOrderedRangeCacheIterator&) = delete;
    void operator=(const LogicalOrderedRangeCacheIterator&) = delete;

    virtual ~LogicalOrderedRangeCacheIterator() = default;

    virtual bool Valid() const override = 0;
    virtual void SeekToFirst() override = 0;
    virtual void SeekToLast() override = 0;
    virtual void Seek(const Slice& target_internal_key) override = 0;
    virtual void SeekForPrev(const Slice& target_internal_key) override = 0;
    virtual void Next() override = 0;
    virtual void Prev() override = 0;
    virtual Slice key() const override = 0;
    virtual Slice userKey() const = 0;
    virtual Slice value() const override = 0;
    virtual Status status() const override = 0;

    virtual bool HasNextInRange() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
