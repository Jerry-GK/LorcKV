#pragma once

#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include "rocksdb/physical_range.h"
#include "rocksdb/ref_range.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief VecPhysicalRange class represents a sorted key-value range in memory.Cleanable
 * with vector-based storage
 */
class VecPhysicalRange : public PhysicalRange {
private:
    struct RangeData {
        std::vector<std::string> internal_keys;
        std::vector<std::string> values;
        std::vector<Slice> internal_key_slices;
        std::vector<Slice> user_key_slices;
        std::vector<Slice> value_slices;
    };
    Slice start_user_key_slice;
    std::shared_ptr<RangeData> data;
    mutable std::shared_mutex physical_range_mutex_;

private:
    VecPhysicalRange(std::shared_ptr<RangeData> data, size_t length);
    
    // Helper functions for vec storage management
    void emplaceInternal(const Slice& internal_key, const Slice& value);
    
    // Internal functions without locking for internal use
    const Slice& userKeyAtInternal(size_t index) const;
    int findInternal(const Slice& key) const;

public:
    VecPhysicalRange(bool valid = false);
    ~VecPhysicalRange() override;
    
    // Copy and move constructors/operators
    VecPhysicalRange(const VecPhysicalRange& other);
    VecPhysicalRange(VecPhysicalRange&& other) noexcept;
    VecPhysicalRange& operator=(const VecPhysicalRange& other);
    VecPhysicalRange& operator=(VecPhysicalRange&& other) noexcept;

    // Static factory functions
    static std::unique_ptr<VecPhysicalRange> buildFromReferringRange(const ReferringRange& refRange);

    // Override pure virtual functions from PhysicalRange
    const Slice& startUserKey() const override;
    const Slice& endUserKey() const override;
    const Slice& startInternalKey() const override;
    const Slice& endInternalKey() const override;
    const Slice& internalKeyAt(size_t index) const override;
    const Slice& userKeyAt(size_t index) const override;
    const Slice& valueAt(size_t index) const override;
    PhysicalRangeUpdateResult update(const Slice& internal_key, const Slice& value) const override;
    int find(const Slice& key) const override;
    void reserve(size_t len) override;
    std::string toString() const override;
};

}  // namespace ROCKSDB_NAMESPACE
