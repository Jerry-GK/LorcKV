#include <iostream>
#include <string>
#include <chrono>
#include <random>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "rocksdb/vec_lorc.h"
#include "rocksdb/vec_range.h"

using namespace std;
using namespace rocksdb;
using namespace chrono;

#define KEY_LEN 24
#define VALUE_LEN 4096

bool do_create = false;
bool do_scan = true;
bool do_get = false;
bool do_update = false;

bool enable_blob = true;
bool enable_lorc = true;

// size_t block_cache_size = 0; // 0MB
// size_t block_cache_size = 32 * 1024 * 1024; // 32MB
// size_t block_cache_size = 1024 * 1024 * 1024; // 1GB
size_t block_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

size_t blob_cache_size = 0; // 0MB
// size_t blob_cache_size = 8 * 1024 * 1024; // 32MB
// size_t blob_cache_size = 1024 * 1024 * 1024; // 1GB
// size_t blob_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

size_t range_cache_size = (size_t)2 * 4096 * 1024 * 1024; // 4GB

const int start_key = 100000; // Start key for range
const int end_key = 999999;   // End key for range

std::vector<std::pair<std::string, std::string>> keyValuePairs;
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

void execute_insert(DB* db, Options options) {
    auto start = high_resolution_clock::now();
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
    
    auto end = high_resolution_clock::now();
    duration<double> time_span = duration_cast<duration<double>>(end - start);
    cout << "Insert time: " << time_span.count() << " seconds" << endl;
}

void execute_scan_without_lorc(DB* db, std::string scan_start_key = "" , int len = -1) {
    auto start = high_resolution_clock::now();
    if (len == -1) {
        len = end_key - start_key + 1;
    }
    const Snapshot* snapshot = db->GetSnapshot();
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    Iterator* it = db->NewIterator(read_options);
    // Time tracking for ToString operations
    duration<double> toString_time(0);

    int i = 0;
    if (scan_start_key != "" && !scan_start_key.empty()) {
        it->Seek(scan_start_key);
    } else {
        it->SeekToFirst();
    }

    if (!it->Valid()) {
        std::cout << "Warning: Iterator is not valid after seek the first key." << std::endl;
    }

    for (; it->Valid(); it->Next()) {
        Slice key_slice = it->key();
        auto toString_start = high_resolution_clock::now();
        std::string value = it->value().ToString();
        auto toString_end = high_resolution_clock::now();

        // Accumulate timing
        toString_time += duration_cast<duration<double>>(toString_end - toString_start);
    }

    delete it;
    auto end = high_resolution_clock::now();
    duration<double> time_span = duration_cast<duration<double>>(end - start);
    cout << "Scan time: " << time_span.count() << " seconds" << endl;
    cout << "ToString time: " << toString_time.count() << " seconds" << endl;
}

void execute_scan_with_lorc(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc, std::string scan_start_key = "" , int len = -1) {
    auto start = high_resolution_clock::now();
    if (len == -1) {
        len = end_key - start_key + 1;
    }
    const Snapshot* snapshot = db->GetSnapshot();
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    Iterator* it = db->NewIterator(read_options);
    SequenceNumber seq_num = snapshot->GetSequenceNumber();

    SliceVecRange slice_vec_range(true, lorc->getRangePool());
    slice_vec_range.reserve(len);
    
    // Time tracking for ToString operations
    duration<double> toString_time(0);

    int i = 0;
    if (scan_start_key != "" && !scan_start_key.empty()) {
        it->Seek(scan_start_key);
    } else {
        it->SeekToFirst();
    }

    if (!it->Valid()) {
        std::cout << "Warning: Iterator is not valid after seek the first key." << std::endl;
    }

    int printStrAddrNum = len;
    int stringBlockNum = 1;
    long long diffSum = 0;
    const char* last_str_addr = nullptr;
    for (; it->Valid(); it->Next()) {
        Slice key_slice = it->key();
        // TODO(jr): avoid internal key representation outside
        std::string internal_key_str = db->GetInternalKeyOfValueTypeStr(key_slice, seq_num);
        
        // Start timing
        auto toString_start = high_resolution_clock::now();
        Slice value_slice = it->value();
        // std::string value = it->value().ToString();
        if (enable_lorc) {
            slice_vec_range.emplace(Slice(internal_key_str), value_slice);
        }
        auto toString_end = high_resolution_clock::now();

        // print the first 10 value str address
        if (printStrAddrNum-- > 0) {
            // std::cout << "key = " << internal_key_str << std::endl;
            const char* str_addr = it->value().GetDataAddress();
            // std::cout << "  iter value str addr: " << (void*)str_addr << std::endl;
            if (last_str_addr != nullptr ) {
                // print the diff of the address
                std::ptrdiff_t addr_diff = str_addr - last_str_addr;
                diffSum += addr_diff - VALUE_LEN;
                if (addr_diff > 4200) {
                    stringBlockNum++;
                }
            }
            last_str_addr = str_addr;
        }

        // Accumulate timing
        toString_time += duration_cast<duration<double>>(toString_end - toString_start);
        
        // std::cout << i << ": " << key  << std::endl;
        if (++i >= len) {
            break;
        }
    }

    // print stringBlockNum and diffSum
    std::cout << "  stringBlockNum: " << stringBlockNum << std::endl;
    std::cout << "  avg diffSum: " << diffSum / len << " bytes" << std::endl;

    if (enable_lorc && slice_vec_range.length() != (size_t)len) {
        std::cout << "slice_vec_range.size = " << slice_vec_range.length() << std::endl;
        assert(false);
    }

    // lorc put range
    if (enable_lorc) {
        lorc->putRange(std::move(slice_vec_range)); 
    }

    delete it;
    auto end = high_resolution_clock::now();
    duration<double> time_span = duration_cast<duration<double>>(end - start);
    cout << "Scan time: " << time_span.count() << " seconds" << endl;
    cout << "ToString and add to range time: " << toString_time.count() << " seconds" << endl;
}

void execute_get(DB* db, Options options) {
    auto start = high_resolution_clock::now();
    for (const auto& pair : keyValuePairs) {
        const std::string &key = pair.first;
        std::string value = pair.second;
        status = db->Get(ReadOptions(), key, &value);
        if (!status.ok()) {
            cerr << "Failed to get key: " << key << ", error: " << status.ToString() << endl;
        }
    }
    auto end = high_resolution_clock::now();
    duration<double> time_span = duration_cast<duration<double>>(end - start);
    cout << "Get time: " << time_span.count() << " seconds" << endl;
}

// for test
void execute_scan_cold(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting cold scan..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

// void execute_scan_hot(DB* db, Options options) {
//     std::cout << "\nExecuting hot scan..." << std::endl;
//     execute_scan_with_lorc(db, options);
// }

void execute_scan_hot_1(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting hot scan 1..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

void execute_scan_hot_2(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting hot scan 2..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

void execute_scan_hot_3(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting hot scan 3..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

void execute_scan_hot_4(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting hot scan 4..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

void execute_scan_hot_5(DB* db, std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc) {
    std::cout << "\nExecuting hot scan 5..." << std::endl;
    execute_scan_with_lorc(db, lorc);
}

int main() {
    // Set database path based on blob flag
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";

    // Configure database options
    Options options;
    options.create_if_missing = do_create;

    // Configure blob settings if enabled
    options.enable_blob_files = enable_blob;
    std::shared_ptr<Cache> blob_cache = nullptr;
    if (enable_blob) {
        options.min_blob_size = 512;                
        options.blob_file_size = 64 * 1024 * 1024;  
        options.enable_blob_garbage_collection = true;
        options.blob_garbage_collection_age_cutoff = 0.25;
        blob_cache = rocksdb::NewLRUCache(blob_cache_size); 
        options.blob_cache = blob_cache;
    }

    // Configure LORC if enabled
    std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc = nullptr;
    if (enable_lorc) {
        lorc = rocksdb::NewRBTreeSliceVecRangeCache(range_cache_size);
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
    status = DB::Open(options, db_path, &db);

    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }

    // Use random generator for key order
    std::random_device rd;
    std::mt19937 rng(rd());
    
    // Create and populate database if required
    if (do_create) {
        // Generate key-value pairs and store in vector
        for (int i = start_key; i <= end_key; ++i) {
            std::string key = gen_key(i);
            // value = 10 * 
            std::string value = gen_value(i);
            keyValuePairs.emplace_back(key, value);
        }

        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);

        execute_insert(db, options);
    }

    // Execute operations based on flags
    if (do_scan) {
        execute_scan_cold(db, lorc);
        execute_scan_hot_1(db, lorc);
        execute_scan_hot_2(db, lorc);
        execute_scan_hot_3(db, lorc);
        execute_scan_hot_4(db, lorc);
        execute_scan_hot_5(db, lorc);
    }

    if (do_get) {
        // shuffle again to get random keys for querying
        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);
        execute_get(db, options);
    }

    // Clean up
    delete db;

    return 0;
}