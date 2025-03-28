#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

int main() {
    std::string db_path = "./db/demo_db";
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    std::string key = "my_key";
    std::string value = "my_value";
    status = db->Put(rocksdb::WriteOptions(), key, value);

    if (!status.ok()) {
        std::cerr << "Failed to insert key-value pair: " << status.ToString() << std::endl;
        delete db;
        return 1;
    }

    std::string retrieved_value;
    status = db->Get(rocksdb::ReadOptions(), key, &retrieved_value);

    if (!status.ok()) {
        std::cerr << "Failed to retrieve value: " << status.ToString() << std::endl;
        delete db;
        return 1;
    }

    std::cout << "Retrieved value: " << retrieved_value << std::endl;

    delete db;
    return 0;
}