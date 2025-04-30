#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cstring>
#include <memory>
#include <iomanip>
#include <algorithm>
#include <cstdlib>  // 为aligned_alloc添加头文件

// Continuous storage structure for strings
class ContinuousStorage {
private:
    static constexpr size_t ALIGNMENT = 64;  // 64字节对齐
    alignas(64) char* data;  // 64字节对齐，匹配常见的缓存行大小
    size_t total_size;
    std::vector<size_t> offsets;
    std::vector<size_t> lengths;

public:
    ContinuousStorage() : data(nullptr), total_size(0) {}
    
    ~ContinuousStorage() {
        if (data) std::free(data);  // 使用free替代delete[]
    }
    
    // Calculate total required size and allocate memory
    void reserve(const std::vector<std::string>& strings) {
        total_size = 0;
        offsets.clear();
        lengths.clear();
        
        offsets.reserve(strings.size());
        lengths.reserve(strings.size());
        
        for (const auto& s : strings) {
            offsets.push_back(total_size);
            lengths.push_back(s.length());
            total_size += s.length();
        }
        
        // 计算对齐后的总大小
        size_t aligned_size = (total_size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
        // 使用aligned_alloc分配对齐内存
        data = static_cast<char*>(aligned_alloc(ALIGNMENT, aligned_size));
        if (!data) {
            throw std::bad_alloc();
        }
    }
    
    // Copy strings into continuous storage
    void load(const std::vector<std::string>& strings) {
        for (size_t i = 0; i < strings.size(); ++i) {
            memcpy(data + offsets[i], strings[i].c_str(), lengths[i]);
        }
    }
    
    // Get pointer to a specific string
    const char* get_string(size_t index) const {
        return data + offsets[index];
    }
    
    // Get length of a specific string
    size_t get_length(size_t index) const {
        return lengths[index];
    }
    
    size_t size() const {
        return offsets.size();
    }

    // 获取底层数据指针
    const char* get_data() const {
        return data;
    }
    
    // 获取总数据大小
    size_t get_total_size() const {
        return total_size;
    }
};

// Test scenario functions
double test_noncontiguous_to_noncontiguous(const std::vector<std::string>& source, std::vector<std::string>& target) {
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < source.size(); ++i) {
        // target[i].resize(source[i].length());
        memcpy(&target[i][0], source[i].c_str(), source[i].length());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

double test_contiguous_to_noncontiguous(const ContinuousStorage& source, std::vector<std::string>& target) {
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < source.size(); ++i) {
        // target[i].resize(source.get_length(i));
        memcpy(&target[i][0], source.get_string(i), source.get_length(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

double test_noncontiguous_to_contiguous(const std::vector<std::string>& source, ContinuousStorage& target) {
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < source.size(); ++i) {
        memcpy(const_cast<char*>(target.get_string(i)), source[i].c_str(), source[i].length());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

double test_contiguous_to_contiguous(const ContinuousStorage& source, ContinuousStorage& target) {
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < source.size(); ++i) {
        memcpy(const_cast<char*>(target.get_string(i)), source.get_string(i), source.get_length(i));
    }

    // 直接复制整块连续内存
    // memcpy(const_cast<char*>(target.get_data()), source.get_data(), source.get_total_size());
    
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

int main() {
    const size_t STRING_SIZE = 4096;
    const size_t NUM_STRINGS = 200000;
    const size_t TOTAL_DATA_MB = (STRING_SIZE * NUM_STRINGS) / (1024 * 1024);
    
    std::cout << "Preparing test data..." << std::endl;
    std::cout << "Number of strings: " << NUM_STRINGS << std::endl;
    std::cout << "String size: " << STRING_SIZE << " bytes" << std::endl;
    std::cout << "Total data size: " << TOTAL_DATA_MB << " MB" << std::endl;
    
    // Generate random strings
    std::vector<std::string> source_strings;
    source_strings.reserve(NUM_STRINGS);
    
    for (size_t i = 0; i < NUM_STRINGS; ++i) {
        std::string s(STRING_SIZE, 'A');
        // Add some variance to prevent optimization
        for (size_t j = 0; j < STRING_SIZE; j += 64) {
            s[j] = 'A' + (i + j) % 26;
        }
        source_strings.push_back(s);
    }
    
    // Prepare continuous storage
    ContinuousStorage cont_source;
    cont_source.reserve(source_strings);
    cont_source.load(source_strings);
    
    // Prepare target spaces
    bool run_test1 = true;  // Non-contiguous to non-contiguous
    bool run_test2 = true;  // Contiguous to non-contiguous
    bool run_test3 = true;  // Non-contiguous to contiguous
    bool run_test4 = true;  // Contiguous to contiguous

    std::vector<std::string> target_strings;
    ContinuousStorage cont_target;

    if (run_test1 || run_test2) {
        target_strings.resize(NUM_STRINGS);
        for (auto& s : target_strings) {
            s.resize(STRING_SIZE);
        }
    }

    if (run_test3 || run_test4) {
        cont_target.reserve(source_strings);
    }

    // Run tests
    std::cout << "\nRunning tests...\n" << std::endl;
    
    double time1 = 0, time2 = 0, time3 = 0, time4 = 0;
    double throughput1 = 0, throughput2 = 0, throughput3 = 0, throughput4 = 0;

    if (run_test1) {
        time1 = test_noncontiguous_to_noncontiguous(source_strings, target_strings);
        throughput1 = TOTAL_DATA_MB / time1;
    }

    if (run_test2) {
        time2 = test_contiguous_to_noncontiguous(cont_source, target_strings);
        throughput2 = TOTAL_DATA_MB / time2;
    }

    if (run_test3) {
        time3 = test_noncontiguous_to_contiguous(source_strings, cont_target);
        throughput3 = TOTAL_DATA_MB / time3;
    }

    if (run_test4) {
        time4 = test_contiguous_to_contiguous(cont_source, cont_target);
        throughput4 = TOTAL_DATA_MB / time4;
    }

    // Display results
    std::cout << "Test Results:" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "--------------------------------------------------------------" << std::endl;
    std::cout << "| Scenario                       | Time (s) | Throughput (MB/s) |" << std::endl;
    std::cout << "--------------------------------------------------------------" << std::endl;
    
    if (run_test1) {
        std::cout << "| Non-contiguous -> Non-contiguous | " << std::setw(8) << time1 << " | " << std::setw(16) << throughput1 << " |" << std::endl;
    }
    if (run_test2) {
        std::cout << "| Contiguous -> Non-contiguous     | " << std::setw(8) << time2 << " | " << std::setw(16) << throughput2 << " |" << std::endl;
    }
    if (run_test3) {
        std::cout << "| Non-contiguous -> Contiguous     | " << std::setw(8) << time3 << " | " << std::setw(16) << throughput3 << " |" << std::endl;
    }
    if (run_test4) {
        std::cout << "| Contiguous -> Contiguous         | " << std::setw(8) << time4 << " | " << std::setw(16) << throughput4 << " |" << std::endl;
    }
    std::cout << "--------------------------------------------------------------" << std::endl;

    // Calculate improvements
    if (run_test1) {
        std::cout << "\nPerformance improvements:" << std::endl;
        if (run_test2) {
            double improvement1 = (throughput2 / throughput1 - 1.0) * 100;
            std::cout << "- Contiguous source improvement: " << improvement1 << "%" << std::endl;
        }
        if (run_test3) {
            double improvement2 = (throughput3 / throughput1 - 1.0) * 100;
            std::cout << "- Contiguous target improvement: " << improvement2 << "%" << std::endl;
        }
        if (run_test4) {
            double improvement3 = (throughput4 / throughput1 - 1.0) * 100;
            std::cout << "- Both contiguous improvement: " << improvement3 << "%" << std::endl;
        }
    }
    
    return 0;
}