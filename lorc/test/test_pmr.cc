#include <iostream>
#include <memory_resource>
#include <vector>
#include <string>
#include <chrono>
#include <iomanip>
#include <memory>
#include <cstring> // For memset

// Helper function: Generate a string with specified size
std::string generate_string(size_t size) {
    return std::string(size, 'a');
}

// Helper function: Display current time and memory usage
void print_status(const std::string& message, 
                  std::pmr::memory_resource* mr, 
                  const auto& start_time) {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
    
    std::cout << message 
              << " - Time: " << std::fixed << std::setprecision(2) << duration / 1000.0 
              << "s" << std::endl;
}

// Initialize the first vector
std::pmr::vector<std::pmr::string> initialize_first_vector(
    std::pmr::memory_resource* pool,
    size_t num_strings,
    size_t string_size,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& start_time) {
    
    std::cout << "Creating first 4GB vector..." << std::endl;
    std::pmr::vector<std::pmr::string> vector(pool);
    vector.reserve(num_strings);
    
    for (size_t i = 0; i < num_strings; ++i) {
        vector.push_back(std::pmr::string(generate_string(string_size), pool));
        if (i % (num_strings / 10) == 0) {
            std::cout << "Filled " << (i * 100 / num_strings) << "%" << std::endl;
        }
    }
    
    print_status("First vector creation completed", pool, start_time);
    return vector;
}

// Run test cycles
void run_test_cycles(
    std::pmr::memory_resource* pool,
    std::pmr::vector<std::pmr::string>& old_vector,
    size_t num_strings,
    size_t string_size,
    int num_iterations,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& start_time) {
    
    std::pmr::polymorphic_allocator<char> alloc(pool);
    for (int iter = 0; iter < num_iterations; ++iter) {
        std::cout << "\n===== Iteration " << (iter + 1) << " =====" << std::endl;
        
        // Create second vector (new one)
        std::cout << "Creating new vector..." << std::endl;
        std::pmr::vector<std::pmr::string> new_vector(alloc);
        new_vector.reserve(num_strings);
        
        for (size_t i = 0; i < num_strings; ++i) {
            new_vector.push_back(std::pmr::string(generate_string(string_size), pool));
            if (i % (num_strings / 10) == 0 && i > 0) {
                std::cout << "Filled " << (i * 100 / num_strings) << "%" << std::endl;
            }
        }
        
        print_status("New vector creation completed", pool, start_time);
        
        // Clear old vector
        std::cout << "Clearing old vector..." << std::endl;
        // old_vector.~vector();
        // Old points to new
        old_vector = std::move(new_vector);

        print_status("Old vector clearing completed", pool, start_time);
        
        print_status("Vector swap completed", pool, start_time);
    }
}

int main() {
    const size_t G = 1024 * 1024 * 1024;
    const size_t POOL_SIZE = 9 * G;  // 9GB memory pool
    const size_t STRING_SIZE = 4 * 1024;  // 4KB strings
    const size_t NUM_STRINGS = 1024 * 1024;  // 1M strings
    const int NUM_ITERATIONS = 5;  // Number of test cycles
    
    std::cout << "Creating " << (POOL_SIZE / (1024 * 1024 * 1024)) << "GB memory pool..." << std::endl;
    
    // Create memory pool
    char* buffer = new char[POOL_SIZE];
    // memset
    memset(buffer, 0, POOL_SIZE);

    std::pmr::monotonic_buffer_resource fixed_size_pool(buffer, POOL_SIZE);
    // 创建线程安全的池化资源，绑定到固定大小的缓冲区
    std::pmr::pool_options options;
    options.largest_required_pool_block = 8 * 1024;
    options.max_blocks_per_chunk = 8;   
    std::pmr::unsynchronized_pool_resource pool(&fixed_size_pool);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::cout << "Starting test cycle..." << std::endl;
    
    // Initialize first vector
    auto old_vector = initialize_first_vector(&pool, NUM_STRINGS, STRING_SIZE, start_time);
    
    // Run test cycles
    run_test_cycles(&pool, old_vector, NUM_STRINGS, STRING_SIZE, NUM_ITERATIONS, start_time);
    
    // Clean up the last vector and memory pool
    old_vector.clear();
    
    std::cout << "\nTest completed!" << std::endl;
    
    delete[] buffer;
    return 0;
}
