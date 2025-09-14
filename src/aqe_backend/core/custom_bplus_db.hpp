#pragma once

#include <vector>
#include <memory>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <cstdint>
#include <string>

/**
 * Custom B+ Tree Database optimized for parallel approximate queries.
 * Designed specifically for high-performance analytical workloads.
 */

struct Record {
    int64_t id;
    double amount;
    int32_t region;
    int32_t product_id;
    int64_t timestamp;
    
    Record() : id(0), amount(0.0), region(0), product_id(0), timestamp(0) {}
    Record(int64_t i, double a, int32_t r, int32_t p, int64_t t) 
        : id(i), amount(a), region(r), product_id(p), timestamp(t) {}
};

class BPlusTreeNode {
public:
    static const int MAX_KEYS = 255;  // Optimized for cache lines
    
    bool is_leaf;
    int key_count;
    size_t subtree_record_count;  // Total records in this subtree
    std::vector<int64_t> keys;
    std::vector<Record> records;  // Only for leaf nodes
    std::vector<std::shared_ptr<BPlusTreeNode>> children;  // Only for internal nodes
    std::shared_ptr<BPlusTreeNode> next_leaf;  // For leaf node chaining
    
    BPlusTreeNode(bool leaf = false);
    
    // Core B+ tree operations
    void insert_record(const Record& record);
    std::vector<Record> search_range(int64_t start_id, int64_t end_id);
    std::shared_ptr<BPlusTreeNode> split();
    
    // Parallel-friendly operations
    std::vector<Record> get_all_records() const;
    size_t get_record_count() const;
    void update_subtree_counts();  // Update record counts up the tree
    
private:
    std::mutex node_mutex;  // Fine-grained locking
};

class CustomBPlusDB {
public:
    CustomBPlusDB();
    ~CustomBPlusDB();
    
    // Database operations
    bool create_database(const std::string& db_path);
    bool open_database(const std::string& db_path);
    void close_database();
    
    // Record operations
    bool insert_record(const Record& record);
    bool insert_batch(const std::vector<Record>& records);
    
    // Query operations - exact
    double sum_amount();
    double avg_amount();
    size_t count_records();
    double sum_amount_where(double min_amount, double max_amount);
    
    // Query operations - parallel approximate
    double parallel_sum_sample(double sample_percent, int num_threads = 4);
    double parallel_avg_sample(double sample_percent, int num_threads = 4);
    size_t parallel_count_sample(double sample_percent, int num_threads = 4);
    double parallel_sum_where_sample(double min_amount, double max_amount, 
                                    double sample_percent, int num_threads = 4);
    
    // Database statistics
    size_t get_total_records() const;
    size_t get_tree_height() const;
    size_t get_node_count() const;
    
    // Parallel sampling utilities
    std::vector<Record> sample_records(double sample_percent);
    std::vector<Record> optimized_sequential_sample(double sample_percent);  // True sequential sampling
    std::vector<std::vector<Record>> partition_records_for_threads(
        const std::vector<Record>& records, int num_threads);
    
    // Native pointer-based sampling methods
    std::vector<Record> fast_pointer_sample(double sample_percent, int step_size = 2);
    std::vector<Record> slow_pointer_sample(double sample_percent);
    std::vector<Record> dual_pointer_sample(double sample_percent);
    std::vector<Record> parallel_pointer_sample(double sample_percent, int num_threads = 4);
    std::vector<Record> random_pointer_sample(double sample_percent, unsigned int seed = 42);
    
    // Advanced multithreaded fast/slow pointer with CLT validation
    std::vector<Record> clt_validated_dual_pointer_sample(double sample_percent, 
                                                         double confidence_level = 0.95,
                                                         int check_interval = 10,
                                                         int num_threads = 4,
                                                         double max_error_percent = 2.0);
    
    // Optimized high-performance CLT sampling with minimal overhead
    std::vector<Record> optimized_clt_sample(double sample_percent,
                                            double confidence_level = 0.95,
                                            int check_interval = 20,
                                            int num_threads = 4,
                                            double max_error_percent = 2.0);
    
    // Intelligent tree-based sampling using balanced structure
    std::vector<Record> index_based_sample(double sample_percent);  // Use index positions
    std::vector<Record> node_skip_sample(double sample_percent, int skip_factor = 2);  // Skip nodes
    std::vector<Record> balanced_tree_sample(double sample_percent);  // Use tree balance
    std::vector<Record> direct_access_sample(double sample_percent);  // Direct node access
    
    // Ultra-fast direct memory address arithmetic sampling
    std::vector<Record> byte_offset_sample(double sample_percent);  // Direct byte offset calculation
    std::vector<Record> random_start_nth_sample(double sample_percent, int nth = 10);  // Random start + nth sampling
    std::vector<Record> memory_stride_sample(double sample_percent, size_t stride_bytes = 0);  // Memory stride access
    std::vector<Record> address_arithmetic_sample(double sample_percent);  // Pure address arithmetic
    std::vector<Record> optimized_address_arithmetic_sample(double sample_percent);  // Pre-allocated mmap
    
    // Advanced memory stride with your optimizations
    std::vector<Record> random_start_memory_stride_sample(double sample_percent, size_t stride_bytes = 0);
    std::vector<Record> multithreaded_memory_stride_sample(double sample_percent, int num_threads = 4);
    double fast_aggregated_memory_stride_sum(double sample_percent, int num_threads = 4);
    
    // Signal-based CLT validation with fast/slow pointer coordination
    std::vector<Record> signal_based_clt_sample(double sample_percent, int check_interval = 10);
    
    // Block/Page-based sampling methods
    std::vector<Record> block_sample(double sample_percent, size_t block_size = 1000);
    std::vector<Record> page_sample(double sample_percent, size_t page_size = 4096);
    std::vector<Record> parallel_block_sample(double sample_percent, size_t block_size = 1000, int num_threads = 4);
    std::vector<Record> adaptive_block_sample(double sample_percent, size_t min_block_size = 500, size_t max_block_size = 2000);
    std::vector<Record> stratified_block_sample(double sample_percent, size_t block_size = 1000, int strata_count = 4);
    
    // File I/O operations
    bool save_to_file(const std::string& file_path);
    bool load_from_file(const std::string& file_path);
    
private:
    std::shared_ptr<BPlusTreeNode> root;
    std::atomic<size_t> total_records;
    std::atomic<size_t> tree_height;
    std::string db_path_;
    
    // Memory layout information for direct addressing
    size_t record_size_;  // Fixed size of each record in bytes
    void* tree_start_address_;  // Starting memory address of tree data
    std::vector<void*> leaf_addresses_;  // Cache of leaf node addresses
    bool memory_mapped_;  // Track if memory mapping is initialized
    std::vector<Record> cached_records_;  // Pre-allocated record cache for mmap
    
    // Thread-safe operations
    mutable std::shared_mutex db_mutex;
    
    // Helper methods
    bool insert_into_node(std::shared_ptr<BPlusTreeNode> node, const Record& record);
    std::vector<Record> collect_all_records() const;
    std::vector<Record> collect_records_from_subtree(std::shared_ptr<BPlusTreeNode> node) const;
    std::vector<Record> collect_leaf_records() const;
    
    // Memory address arithmetic helpers
    void initialize_memory_layout();  // Cache addresses and calculate record size
    void update_leaf_addresses();  // Update cached leaf node addresses
    Record* get_record_at_offset(size_t byte_offset);  // Direct memory access
    bool is_valid_record_address(void* address);  // Validate memory bounds
    std::vector<Record> get_records_by_indices(const std::vector<size_t>& indices);  // Efficient index-based access
    
    // Serialization helpers
    void serialize_node(std::ofstream& file, std::shared_ptr<BPlusTreeNode> node);
    std::shared_ptr<BPlusTreeNode> deserialize_node(std::ifstream& file);
};