#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

int main() {
    std::string db_path = "./db/1000w_db";
    // delete the database
    rocksdb::Options options;
    rocksdb::Status status = rocksdb::DestroyDB(db_path, options);

    if (!status.ok()) {
        std::cerr << "Failed to delete database: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Deleted database " << db_path << std::endl;
    return 0;
}