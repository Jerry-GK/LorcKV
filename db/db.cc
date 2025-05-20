#include "rocksdb/db.h"
#include "db/dbformat.h"

// TODO(jr): remove this file

namespace ROCKSDB_NAMESPACE {

std::string DB::GetInternalKeyOfValueTypeStr(const Slice& _user_key, SequenceNumber s) const {
    return InternalKey(_user_key, s, kTypeValue).Encode().ToString();
}

} // namespace ROCKSDB_NAMESPACE