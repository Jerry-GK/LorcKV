#include <iostream>
#include <string>
#include <chrono>
#include <random>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "rocksdb/lorc.h"
#include "rocksdb/physical_range.h"
#include "rocksdb/ref_range.h"

using namespace std;
using namespace rocksdb;
using namespace chrono;

#define KEY_LEN 24
#define VALUE_LEN 4096

bool do_update = true;

bool enable_blob = true;
bool enable_blob_cache = false;
bool enable_lorc = false;
bool enable_timer = true;

// size_t block_cache_size = 0; // 0MB
// size_t block_cache_size = 32 * 1024 * 1024; // 32MB
// size_t block_cache_size = 1024 * 1024 * 1024; // 1GB
size_t block_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t blob_cache_size = 0; // 0MB
// size_t blob_cache_size = 8 * 1024 * 1024; // 32MB
// size_t blob_cache_size = 1024 * 1024 * 1024; // 1GB
size_t blob_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

size_t range_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

const int start_key = 100000; // Start key for range
const int end_key = 999999;   // End key for range

const double update_ratio = 0.50;

Status status;

std::string generateRandomString(int len) {
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, 9);
    std::string result(len, '0');

    for (int i = 0; i < len; ++i) {
        result[i] = '0' + dist(gen);
    }
    
    // int rand = std::rand();
    // std::string result = to_string(rand);
    // result += std::string(len - result.length(), '0');

    return result;
}

// function gen key
std::string gen_key(int key) {
    std::string key_str = std::to_string(key);
    int prefix_len = KEY_LEN - key_str.length();
    assert(prefix_len >= 0);
    key_str.insert(0, prefix_len, '0');
    return key_str;
}

// function gen value
std::string gen_value(int key) {
    std::string key_str = gen_key(key);
    int prefix_len = VALUE_LEN - key_str.length();
    assert(prefix_len >= 0);
    // gen a rand string of length prefix_len
    std::string value_str = generateRandomString(prefix_len) + key_str;
    return value_str;
}

void execute_update(DB* db, Options options, std::vector<std::pair<std::string, std::string>> keyValuePairs) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    // Insert key-value pairs into database
    for (const auto& pair : keyValuePairs) {
        const std::string& key = pair.first;
        const std::string& value = pair.second;

        rocksdb::WriteOptions writeOptions;
        writeOptions.disableWAL = true;
        rocksdb::Status status = db->Put(writeOptions, key, value);

        if (!status.ok()) {
            std::cerr << "Failed to insert key: " << key << ", error: " << status.ToString() << std::endl;
            break;
        }
    }
    
    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Updating " << keyValuePairs.size() << " entries costs time: " << time_span.count() << " seconds" << endl;
    }
}

int main() {
    // Set database path based on blob flag
    int precentage = update_ratio > 0.0 ? (int)(update_ratio * 100) : 0;
    string precentage_str = to_string(precentage) + "%";
    string db_path = enable_blob ? "./db/test_db_blobdb_" : "./db/test_db_rocksdb_";
    db_path += "_";
    db_path += precentage_str + "update";

    // Configure database options
    Options options;
    options.create_if_missing = false;

    // Configure blob settings if enabled
    options.enable_blob_files = enable_blob;
    std::shared_ptr<Cache> blob_cache = nullptr;
    if (enable_blob) {
        options.min_blob_size = 512;                
        options.blob_file_size = 64 * 1024 * 1024;  
        options.enable_blob_garbage_collection = false;
        options.blob_garbage_collection_age_cutoff = 9999;
        if (enable_blob_cache) {
            blob_cache = rocksdb::NewLRUCache(blob_cache_size); 
            options.blob_cache = blob_cache;
        }
    }

    // Configure LORC if enabled
    std::shared_ptr<LogicallyOrderedRangeCache> lorc = nullptr;
    if (enable_lorc) {
        lorc = rocksdb::NewRBTreeLogicallyOrderedRangeCache(range_cache_size);
        options.range_cache = lorc;
    }

    // Configure block cache
    auto block_cache = NewLRUCache(block_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = block_cache;
    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    // Open database
    DB* db;
    cout << " Opening database at " << db_path << endl;
    status = DB::Open(options, db_path, &db);

    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }

    // Use random generator for key order
    std::random_device rd;
    std::mt19937 rng(rd());
    
    // Create and populate database if required
    if (do_update) {
        std::vector<std::pair<std::string, std::string>> keyValuePairs;
        int update_size = (int)(update_ratio * (end_key - start_key + 1));
        keyValuePairs.reserve(update_size);
        std::uniform_int_distribution<> dist(start_key, end_key);
        for (int i = 0; i < update_size; ++i) {
            int key = dist(rng);
            std::string key_str = gen_key(key);
            std::string value_str = gen_value(key);
            keyValuePairs.emplace_back(key_str, value_str);
        }

        execute_update(db, options, keyValuePairs);
    }

    // Clean up
    delete db;

    return 0;
}