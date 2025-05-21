#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cstring>
#include <memory>
#include <iomanip>
#include <algorithm>
#include <memory_resource> // 添加 PMR 头文件

// Continuous storage structure for strings
class ContinuousStorage {
private:
    char* data;
    size_t total_size;
    std::vector<size_t> offsets;
    std::vector<size_t> lengths;

public:
    ContinuousStorage() : data(nullptr), total_size(0) {}
    
    ~ContinuousStorage() {
        if (data) delete[] data;
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
        
        data = new char[total_size];
        // preheat
        memset(data, 0, total_size);
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

// 新增 PMR 测试函数
double test_noncontiguous_to_pmr(const std::vector<std::string>& source, std::pmr::vector<std::pmr::string>& target) {
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < source.size(); ++i) {
        memcpy(&target[i][0], source[i].c_str(), source[i].length());
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

    // memset test
    // memset(const_cast<char*>(target.get_data()), 6, source.get_total_size());
    
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

int main() {
    const size_t STRING_SIZE = 4096;
    const size_t NUM_STRINGS = 200000 * 5;
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
    bool run_test5 = true;  // Non-contiguous to PMR vector

    std::vector<std::string> target_strings;
    ContinuousStorage cont_target;
    
    // 准备 PMR 内存资源和容器
    std::unique_ptr<char[]> buffer;
    std::pmr::vector<std::pmr::string> pmr_target;
    std::pmr::monotonic_buffer_resource* pmr_resource = nullptr;
    
    if (run_test5) {
        size_t buffer_size = NUM_STRINGS * (STRING_SIZE + sizeof(std::pmr::string) + 32);  // 分配足够的缓冲空间
        buffer = std::make_unique<char[]>(buffer_size);
        pmr_resource = new std::pmr::monotonic_buffer_resource(buffer.get(), buffer_size);
        
        // 创建使用pmr内存资源的pmr分配器
        std::pmr::polymorphic_allocator<std::pmr::string> alloc(pmr_resource);
        pmr_target = std::pmr::vector<std::pmr::string>(alloc);
        pmr_target.reserve(NUM_STRINGS);
        
        // 正确创建pmr字符串
        for (size_t i = 0; i < NUM_STRINGS; ++i) {
            // 创建一个使用相同内存资源的字符串
            std::pmr::string str(STRING_SIZE, ' ', pmr_resource);
            pmr_target.push_back(std::move(str));
        }
    }

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
    
    double time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0;
    double throughput1 = 0, throughput2 = 0, throughput3 = 0, throughput4 = 0, throughput5 = 0;

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

    if (run_test5) {
        time5 = test_noncontiguous_to_pmr(source_strings, pmr_target);
        throughput5 = TOTAL_DATA_MB / time5;
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
    if (run_test5) {
        std::cout << "| Non-contiguous -> PMR            | " << std::setw(8) << time5 << " | " << std::setw(16) << throughput5 << " |" << std::endl;
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
        if (run_test5) {
            double improvement4 = (throughput5 / throughput1 - 1.0) * 100;
            std::cout << "- PMR vector improvement: " << improvement4 << "%" << std::endl;
        }
    }
    
    // 清理 PMR 资源
    if (pmr_resource) {
        delete pmr_resource;
    }
    
    return 0;
}