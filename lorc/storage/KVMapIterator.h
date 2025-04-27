#include "KVMap.h"

class KVMapIterator {
    public:
        KVMapIterator(const KVMap* map, std::map<std::string, std::string>::const_iterator it, bool valid)
            : map_(map), it_(it), valid_(valid) {}

        void Seek(const std::string& key);
        void Next();
        bool Valid() const { return valid_; }
        const std::string& key() const { return it_->first; }
        const std::string& value() const { return it_->second; }

        KVMapIterator& operator++() { Next(); return *this; }
        KVMapIterator operator++(int) { KVMapIterator tmp = *this; Next(); return tmp; }
        KVMapIterator operator+(int n);
        bool operator==(const KVMapIterator& other) const { return it_ == other.it_; }
        bool operator!=(const KVMapIterator& other) const { return it_ != other.it_; }

    private:
        const KVMap* map_;
        std::map<std::string, std::string>::const_iterator it_;    
        bool valid_;
};