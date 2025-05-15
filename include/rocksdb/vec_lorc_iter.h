#pragma once

#include <string>
#include "rocksdb/vec_range.h"

namespace ROCKSDB_NAMESPACE {

class LogicallyOrderedSliceVecRangeCache;

class SliceVecRangeCacheIterator {
public:
    SliceVecRangeCacheIterator() = default;
    // No copying allowed
    SliceVecRangeCacheIterator(const SliceVecRangeCacheIterator&) = delete;
    void operator=(const SliceVecRangeCacheIterator&) = delete;

    virtual ~SliceVecRangeCacheIterator() = default;

    // An RangeCacheIterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the RangeCacheIterator is valid.
    // Always returns false if !status().ok().
    virtual bool Valid() const = 0;

    // Returns true if the RangeCacheIterator has more keys in the SliceVecRange
    virtual bool HasNextInRange() const = 0;

    // Position at the first key in the source.  The RangeCacheIterator is Valid()
    // after this call iff the source is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last key in the source.  The RangeCacheIterator is
    // Valid() after this call iff the source is not empty.
    // Currently incompatible with user timestamp.
    virtual void SeekToLast() = 0;

    // Position at the first key in the source that at or past target.
    // The RangeCacheIterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    // All Seek*() methods clear any error status() that the RangeCacheIterator had prior to
    // the call; after the seek, status() indicates only the error (if any) that
    // happened during the seek, not any past errors.
    // Target does not contain timestamp.
    virtual void Seek(const Slice& target) = 0;

    // Position at the last key in the source that at or before target.
    // The RangeCacheIterator is Valid() after this call iff the source contains
    // an entry that comes at or before target.
    // Currently incompatible with user timestamp.
    virtual void SeekForPrev(const Slice& target) = 0;

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the RangeCacheIterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the RangeCacheIterator was not positioned at the first entry in source.
    // Currently incompatible with user timestamp.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Return the key for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the RangeCacheIterator.
    // REQUIRES: Valid()
    virtual const Slice key() const = 0;

    // Return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the RangeCacheIterator.
    // REQUIRES: Valid()
    virtual const Slice value() const = 0;

    // If an error has occurred, return it.  Else return an ok status.
    // If non-blocking IO is requested and this operation cannot be
    // satisfied without doing some IO, then this returns Status::Incomplete().
    virtual std::string status() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
