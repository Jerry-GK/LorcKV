#include "KVMapIterator.h"

// KVMapIterator implementation
void KVMapIterator::Seek(const std::string& key) {
    std::lock_guard<std::mutex> lock(map_->mutex_);
    it_ = map_->data_.lower_bound(key);
    valid_ = it_ != map_->data_.end();
}

void KVMapIterator::Next() {
    std::lock_guard<std::mutex> lock(map_->mutex_);
    if (it_ != map_->data_.end()) {
        ++it_;
    }
    valid_ = it_ != map_->data_.end();
}

KVMapIterator KVMapIterator::operator+(int n) {
    std::lock_guard<std::mutex> lock(map_->mutex_);
    auto result = *this;
    for (int i = 0; i < n && result.Valid(); ++i) {
        ++result.it_;
    }
    valid_ = it_ != map_->data_.end();
    return result;
}