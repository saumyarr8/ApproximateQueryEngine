// Native C++ pointer-based traversal methods for B+ tree
// These methods operate directly on the tree structure for maximum performance

#include "custom_bplus_db.hpp"
#include <vector>
#include <random>
#include <algorithm>
#include <thread>
#include <future>

/**
 * Native pointer-based traversal methods
 */
class BTreePointerTraversal {
public:
    /**
     * Fast pointer method: Skip nodes with configurable step size
     */
    static std::vector<Record> fast_pointer_sample(
        std::shared_ptr<BPlusTreeNode> root, 
        double sample_percent,
        int step_size = 2) {
        
        std::vector<Record> samples;
        if (!root) return samples;
        
        // Calculate target sample count
        auto all_records = collect_leaf_records(root);
        int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
        
        // Fast pointer traversal with step_size
        int step = std::max(1, static_cast<int>(all_records.size() / target_count));
        step *= step_size; // Fast pointer multiplier
        
        for (size_t i = 0; i < all_records.size() && samples.size() < target_count; i += step) {
            samples.push_back(all_records[i]);
        }
        
        return samples;
    }
    
    /**
     * Slow pointer method: Systematic sampling with small steps
     */
    static std::vector<Record> slow_pointer_sample(
        std::shared_ptr<BPlusTreeNode> root,
        double sample_percent) {
        
        std::vector<Record> samples;
        if (!root) return samples;
        
        auto all_records = collect_leaf_records(root);
        int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
        
        // Slow pointer: smaller, more systematic steps
        int step = std::max(1, static_cast<int>(all_records.size() / target_count));
        
        for (size_t i = 0; i < all_records.size() && samples.size() < target_count; i += step) {
            samples.push_back(all_records[i]);
        }
        
        return samples;
    }
    
    /**
     * Dual pointer method: Fast and slow pointers working together
     */
    static std::vector<Record> dual_pointer_sample(
        std::shared_ptr<BPlusTreeNode> root,
        double sample_percent) {
        
        std::vector<Record> samples;
        if (!root) return samples;
        
        auto all_records = collect_leaf_records(root);
        int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
        
        // Split target between fast and slow pointers
        int fast_target = target_count / 3;
        int slow_target = target_count - fast_target;
        
        // Fast pointer (bigger steps)
        int fast_step = std::max(1, static_cast<int>(all_records.size() / fast_target));
        fast_step *= 3; // Fast multiplier
        
        for (size_t i = 0; i < all_records.size() && samples.size() < fast_target; i += fast_step) {
            samples.push_back(all_records[i]);
        }
        
        // Slow pointer (smaller steps, offset start)
        int slow_step = std::max(1, static_cast<int>(all_records.size() / slow_target));
        size_t offset = fast_step / 2; // Offset to avoid overlap
        
        for (size_t i = offset; i < all_records.size() && samples.size() < target_count; i += slow_step) {
            samples.push_back(all_records[i]);
        }
        
        return samples;
    }
    
    /**
     * Parallel pointer method: Multiple threads with different starting positions
     */
    static std::vector<Record> parallel_pointer_sample(
        std::shared_ptr<BPlusTreeNode> root,
        double sample_percent,
        int num_threads = 4) {
        
        std::vector<Record> samples;
        if (!root) return samples;
        
        auto all_records = collect_leaf_records(root);
        int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
        
        // Divide work among threads
        int samples_per_thread = target_count / num_threads;
        std::vector<std::future<std::vector<Record>>> futures;
        
        for (int t = 0; t < num_threads; ++t) {
            futures.push_back(std::async(std::launch::async, [&, t]() {
                std::vector<Record> thread_samples;
                
                // Each thread starts at a different offset
                size_t start_offset = (all_records.size() / num_threads) * t;
                int step = std::max(1, static_cast<int>(all_records.size() / target_count));
                
                for (size_t i = start_offset; 
                     i < all_records.size() && thread_samples.size() < samples_per_thread; 
                     i += step) {
                    thread_samples.push_back(all_records[i]);
                }
                
                return thread_samples;
            }));
        }
        
        // Collect results from all threads
        for (auto& future : futures) {
            auto thread_samples = future.get();
            samples.insert(samples.end(), thread_samples.begin(), thread_samples.end());
        }
        
        return samples;
    }
    
    /**
     * Random pointer method: Random positions for unbiased sampling
     */
    static std::vector<Record> random_pointer_sample(
        std::shared_ptr<BPlusTreeNode> root,
        double sample_percent,
        unsigned int seed = 42) {
        
        std::vector<Record> samples;
        if (!root) return samples;
        
        auto all_records = collect_leaf_records(root);
        int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
        
        // Generate random positions
        std::mt19937 rng(seed);
        std::uniform_int_distribution<size_t> dist(0, all_records.size() - 1);
        
        std::set<size_t> selected_positions;
        while (selected_positions.size() < target_count && selected_positions.size() < all_records.size()) {
            selected_positions.insert(dist(rng));
        }
        
        // Collect samples at random positions
        for (size_t pos : selected_positions) {
            samples.push_back(all_records[pos]);
        }
        
        return samples;
    }

private:
    /**
     * Helper: Collect all records from leaf nodes (left-to-right traversal)
     */
    static std::vector<Record> collect_leaf_records(std::shared_ptr<BPlusTreeNode> root) {
        std::vector<Record> records;
        
        if (!root) return records;
        
        // Find leftmost leaf
        auto current = root;
        while (!current->is_leaf && !current->children.empty()) {
            current = current->children[0];
        }
        
        // Traverse all leaf nodes
        while (current) {
            for (int i = 0; i < current->key_count; ++i) {
                records.push_back(current->records[i]);
            }
            current = current->next_leaf;
        }
        
        return records;
    }
};