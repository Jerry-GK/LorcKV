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
#include "rocksdb/ref_vec_range.h"

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
bool enable_blob_cache = false;
bool enable_lorc = true;
bool enable_timer = true;

const int start_key = 100000; // Start key for range
const int end_key = 999999;   // End key for range
const int total_len = end_key - start_key + 1;
const int max_range_query_len = total_len * 0.01; // 10% of total length
const int min_range_query_len = total_len * 0.01;
const int num_range_queries = 1000;


// size_t block_cache_size = 0; // 0MB
size_t block_cache_size = (size_t)64 * 1024 * 1024; // 64MB
// size_t block_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t block_cache_size = (size_t)2048 * 1024 * 1024; // 2GB
// size_t block_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t blob_cache_size = 0; // 0MB
// size_t blob_cache_size = (size_t)64 * 1024 * 1024; // 64MB
size_t blob_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t blob_cache_size = (size_t)2048  * 1024 * 1024; // 2GB
// size_t blob_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t range_cache_size = 0; // 0MB
// size_t range_cache_size = (size_t)64 * 1024 * 1024; // 64MB
size_t range_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t range_cache_size = (size_t)2048 * 1024 * 1024; // 2GB
// size_t range_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

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
        cout << "Insert time: " << time_span.count() << " seconds" << endl;
    }
}

void execute_scan(DB* db, Options options, std::string scan_start_key = "" , int len = -1) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    if (len == -1) {
        len = end_key - start_key + 1;
    }

    const Snapshot* snapshot = db->GetSnapshot();
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    Iterator* it = db->NewIterator(read_options);
    SequenceNumber seq_num = snapshot->GetSequenceNumber();

    std::vector<std::string> userkey_str_vec;
    std::vector<std::string> value_str_vec;
    ReferringSliceVecRange ref_slice_vec_range(true, seq_num);

    ref_slice_vec_range.reserve(len);
    userkey_str_vec.reserve(len);
    value_str_vec.reserve(len);
    
    // Time tracking for ToString operations
    duration<double> toString_time(0);
    duration<double> next_time(0); // Time tracking for Next operations
    duration<double> emplace_time(0); // Time tracking for ref_slice_vec_range.emplace

    int i = 0;
    if (scan_start_key != "" && !scan_start_key.empty()) {
        it->Seek(scan_start_key);
    } else {
        it->SeekToFirst();
    }

    if (!it->Valid()) {
        std::cout << "Warning: Iterator is not valid after seek the first key." << std::endl;
    }

    int printStrAddrNum = 0;
    int stringBlockNum = 1;
    long long diffSum = 0;
    const char* last_str_addr = nullptr;
    int lastPrintedPercent = -1;
    for (; it->Valid();) {
        // TODO(jr): avoid internal key representation outside
        // Start timing
        auto toString_start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
        userkey_str_vec.emplace_back(it->key().ToString());
        value_str_vec.emplace_back(it->value().ToString());

        // std::string value_str = value.ToString();
        if (enable_lorc) {
            auto emplace_start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
            ref_slice_vec_range.emplace(Slice(userkey_str_vec.back()), Slice(value_str_vec.back()));
            if (enable_timer) {
                auto emplace_end = high_resolution_clock::now();
                emplace_time += duration_cast<duration<double>>(emplace_end - emplace_start);
            }
        }
        if (enable_timer) {
            auto toString_end = high_resolution_clock::now();
            toString_time += duration_cast<duration<double>>(toString_end - toString_start);
        }

        // print the first 10 value str address
        // if (printStrAddrNum-- > 0) {
        //     std::cout << "key = " << userkey_str_vec.back() << std::endl;
        //     const char* str_addr = it->value().GetDataAddress();
        //     const char* scan_value_addr = value.data();
        //     std::cout << "  iter value str addr: " << (void*)str_addr << std::endl;
        //     std::cout << "  scan value str addr: " << (void*)scan_value_addr << std::endl;
        //     if (last_str_addr != nullptr ) {
        //         // print the diff of the address
        //         std::ptrdiff_t addr_diff = str_addr - last_str_addr;
        //         std::cout << "  Diff: " << addr_diff << " bytes" << std::endl;
        //         diffSum += addr_diff - VALUE_LEN;
        //         if (addr_diff > 4200) {
        //             stringBlockNum++;
        //         }
        //     }
        //     last_str_addr = str_addr;
        // }
        
        // print in % (i/len) every 1%
        // int percent = 100 * ((double)i / len);
        // if ((percent != lastPrintedPercent) && (percent % 1 == 0)) {
        //     std::cout << "Progress: " << percent << "%" << std::endl;
        //     lastPrintedPercent = percent;
        // }
        if (++i >= len) {
            break;
        }

        // Time tracking for Next operations
        auto next_start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
        it->Next();
        if (enable_timer) {
            auto next_end = high_resolution_clock::now();
            // Accumulate Next operation time
            next_time += duration_cast<duration<double>>(next_end - next_start);
        }
    }

    // print stringBlockNum and diffSum
    if (printStrAddrNum > 0) {
        std::cout << "\tstringBlockNum: " << stringBlockNum << std::endl;
        std::cout << "\tdiffSum: " << diffSum << " bytes" << std::endl;
    }

    if (enable_lorc && ref_slice_vec_range.length() != (size_t)len) {
        std::cout << "\tslice_vec_range.size = " << ref_slice_vec_range.length() << std::endl;
        assert(false);
    }

    // lorc put range
    // set to 0
    duration<double> putRange_time{0};
    if (enable_lorc) {
        auto putRange_start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
        options.range_cache->putRange(std::move(ref_slice_vec_range)); 
        if (enable_timer) {
            auto putRange_end = high_resolution_clock::now();
            putRange_time = duration_cast<duration<double>>(putRange_end - putRange_start);
        }
    }

    delete it;

    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "\tScan total time: " << time_span.count() << " seconds" << endl;
        cout << "\t\tToString(and add to range if needed) total time: " << toString_time.count() << " seconds" << endl;
        cout << "\t\tNext() of iterator total time: " << next_time.count() << " seconds" << endl;
        if (enable_lorc) {
            duration<double> lorc_write_total_time = emplace_time + putRange_time;
            cout << "\t\tLORC write total time: " << lorc_write_total_time.count() << " seconds" << endl;
            cout << "\t\t\tSlice emplace total time: " << emplace_time.count() << " seconds" << endl;
            cout << "\t\t\tLorc putRange time: " << putRange_time.count() << " seconds" << endl;
        }
    }
}

void execute_get(DB* db, Options options) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    for (const auto& pair : keyValuePairs) {
        const std::string &key = pair.first;
        std::string value = pair.second;
        status = db->Get(ReadOptions(), key, &value);
        if (!status.ok()) {
            cerr << "Failed to get key: " << key << ", error: " << status.ToString() << endl;
        }
    }
    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Get time: " << time_span.count() << " seconds" << endl;
    }
}

// for test
void execute_scan_cold(DB* db, Options options) {
    std::cout << "\nExecuting cold scan..." << std::endl;
    execute_scan(db, options);
}

// void execute_scan_hot(DB* db, Options options) {
//     std::cout << "\nExecuting hot scan..." << std::endl;
//     execute_scan(db, options);
// }

void execute_scan_hot_1(DB* db, Options options) {
    std::cout << "\nExecuting hot scan 1..." << std::endl;
    execute_scan(db, options);
}

void execute_scan_hot_2(DB* db, Options options) {
    std::cout << "\nExecuting hot scan 2..." << std::endl;
    execute_scan(db, options);
}

void execute_scan_hot_3(DB* db, Options options) {
    std::cout << "\nExecuting hot scan 3..." << std::endl;
    execute_scan(db, options);
}

void execute_scan_hot_4(DB* db, Options options) {
    std::cout << "\nExecuting hot scan 4..." << std::endl;
    execute_scan(db, options);
}

void execute_scan_hot_5(DB* db, Options options) {
    std::cout << "\nExecuting hot scan 5..." << std::endl;
    execute_scan(db, options);
}

int main() {
    // Set database path based on blob flag
    string postfix = "_25%_update";
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";
    // db_path += postfix;

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
        if (enable_blob_cache) {
            blob_cache = rocksdb::NewLRUCache(blob_cache_size); 
            options.blob_cache = blob_cache;
        }
    }

    // Configure LORC if enabled
    std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc = nullptr;
    if (enable_lorc) {
        lorc = rocksdb::NewRBTreeSliceVecRangeCache(range_cache_size, false);
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
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> range_len_dist(min_range_query_len, max_range_query_len);
        
        double total_query_time = 0.0;
        
        std::cout << "Executing " << num_range_queries << " random range queries..." << std::endl;
        std::cout << "Range length between " << min_range_query_len << " and " << max_range_query_len << std::endl;
        
        for (int i = 0; i < num_range_queries; i++) {
            // 随机生成范围长度
            int query_length = range_len_dist(gen);
            
            // 随机生成起始键，确保不会超出有效范围
            int max_start = end_key - query_length;
            std::uniform_int_distribution<> start_key_dist(start_key, max_start);
            int query_start_key = start_key_dist(gen);
            
            std::string start_key_str = gen_key(query_start_key);
            
            std::cout << "Query " << (i+1) << ": start key = " << query_start_key 
                      << ", length = " << query_length << std::endl;
                      
            // 开始计时
            auto query_start = high_resolution_clock::now();
            
            // 执行范围查询
            execute_scan(db, options, start_key_str, query_length);
            
            // 结束计时
            auto query_end = high_resolution_clock::now();
            duration<double> query_time = duration_cast<duration<double>>(query_end - query_start);
            
            total_query_time += query_time.count();
            std::cout << "Query " << (i+1) << " time: " << query_time.count() << 
                " seconds, avg time = " << total_query_time / (i + 1) << "seconds" <<  std::endl;
        }
        
        // 输出平均查询时间
        double avg_query_time = total_query_time / num_range_queries;
        std::cout << "\nAverage query time for " << num_range_queries 
                  << " random range queries: " << avg_query_time << " seconds\n" << std::endl;
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