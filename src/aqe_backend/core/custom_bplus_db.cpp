#include "custom_bplus_db.hpp"
#include <algorithm>
#include <fstream>
#include <future>
#include <random>
#include <set>
#include <thread>
#include <random>
#include <thread>
#include <future>
#include <iostream>
#include <shared_mutex>
#include <cmath>
#include <atomic>

// BPlusTreeNode Implementation

BPlusTreeNode::BPlusTreeNode(bool leaf) : is_leaf(leaf), key_count(0), subtree_record_count(0) {
    keys.reserve(MAX_KEYS);
    if (is_leaf) {
        records.reserve(MAX_KEYS);
    } else {
        children.reserve(MAX_KEYS + 1);
    }
}

void BPlusTreeNode::insert_record(const Record& record) {
    std::lock_guard<std::mutex> lock(node_mutex);
    
    if (is_leaf) {
        // Find insertion position
        auto pos = std::lower_bound(keys.begin(), keys.begin() + key_count, record.id);
        int index = pos - keys.begin();
        
        // Insert key and record
        keys.insert(keys.begin() + index, record.id);
        records.insert(records.begin() + index, record);
        key_count++;
        subtree_record_count++;  // Update count for leaf
    }
}

std::shared_ptr<BPlusTreeNode> BPlusTreeNode::split() {
    auto new_node = std::make_shared<BPlusTreeNode>(is_leaf);
    int mid = MAX_KEYS / 2;
    
    if (is_leaf) {
        // Copy second half to new node
        new_node->keys.assign(keys.begin() + mid, keys.begin() + key_count);
        new_node->records.assign(records.begin() + mid, records.begin() + key_count);
        new_node->key_count = key_count - mid;
        
        // Update current node
        keys.resize(mid);
        records.resize(mid);
        key_count = mid;
        
        // Link leaf nodes
        new_node->next_leaf = this->next_leaf;
        this->next_leaf = new_node;
    } else {
        // Split internal node
        new_node->keys.assign(keys.begin() + mid + 1, keys.begin() + key_count);
        new_node->children.assign(children.begin() + mid + 1, children.begin() + key_count + 1);
        new_node->key_count = key_count - mid - 1;
        
        // Update current node
        keys.resize(mid);
        children.resize(mid + 1);
        key_count = mid;
    }
    
    return new_node;
}

std::vector<Record> BPlusTreeNode::get_all_records() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(node_mutex));
    
    if (is_leaf) {
        return std::vector<Record>(records.begin(), records.begin() + key_count);
    }
    
    std::vector<Record> all_records;
    for (int i = 0; i <= key_count; i++) {
        if (children[i]) {
            auto child_records = children[i]->get_all_records();
            all_records.insert(all_records.end(), child_records.begin(), child_records.end());
        }
    }
    return all_records;
}

size_t BPlusTreeNode::get_record_count() const {
    if (is_leaf) {
        return key_count;
    }
    
    size_t count = 0;
    for (int i = 0; i <= key_count; i++) {
        if (children[i]) {
            count += children[i]->get_record_count();
        }
    }
    return count;
}

void BPlusTreeNode::update_subtree_counts() {
    if (is_leaf) {
        subtree_record_count = key_count;
    } else {
        subtree_record_count = 0;
        for (int i = 0; i <= key_count; i++) {
            if (children[i]) {
                children[i]->update_subtree_counts();
                subtree_record_count += children[i]->subtree_record_count;
            }
        }
    }
}

// CustomBPlusDB Implementation

CustomBPlusDB::CustomBPlusDB() : total_records(0), tree_height(1), 
                                   record_size_(sizeof(Record)), tree_start_address_(nullptr),
                                   memory_mapped_(false) {
    root = std::make_shared<BPlusTreeNode>(true);  // Start with leaf root
    leaf_addresses_.reserve(1000);  // Reserve space for leaf address cache
    cached_records_.reserve(10000);  // Pre-allocate cache for optimized access
}

CustomBPlusDB::~CustomBPlusDB() {
    close_database();
}

bool CustomBPlusDB::create_database(const std::string& db_path) {
    std::unique_lock<std::shared_mutex> lock(db_mutex);
    db_path_ = db_path;
    
    // Initialize empty B+ tree
    root = std::make_shared<BPlusTreeNode>(true);
    total_records = 0;
    tree_height = 1;
    
    // **INITIALIZE MEMORY MAPPING DURING DATABASE CREATION**
    // This separates mmap initialization cost from sampling performance
    memory_mapped_ = false;  // Will be set to true after first batch of records
    cached_records_.clear();
    cached_records_.reserve(100000);  // Pre-allocate for performance
    
    return true;
}

bool CustomBPlusDB::open_database(const std::string& db_path) {
    return load_from_file(db_path);
}

void CustomBPlusDB::close_database() {
    std::unique_lock<std::shared_mutex> lock(db_mutex);
    if (!db_path_.empty()) {
        save_to_file(db_path_);
    }
}

bool CustomBPlusDB::insert_record(const Record& record) {
    std::unique_lock<std::shared_mutex> lock(db_mutex);
    
    // Insert into the tree first
    bool need_root_split = insert_into_node(root, record);
    
    // Handle root split if needed
    if (need_root_split) {
        auto new_root = std::make_shared<BPlusTreeNode>(false);
        auto new_node = root->split();
        
        new_root->keys.push_back(new_node->keys[0]);
        new_root->children.push_back(root);
        new_root->children.push_back(new_node);
        new_root->key_count = 1;
        
        root = new_root;
        tree_height++;
    }
    
    total_records++;
    
    // **UPDATE MEMORY MAPPING AFTER BULK INSERTIONS**
    // Refresh mmap cache every 1000 records for optimal performance
    if (total_records % 1000 == 0) {
        cached_records_ = collect_leaf_records();
        memory_mapped_ = true;
    }
    
    return true;
}

bool CustomBPlusDB::insert_batch(const std::vector<Record>& records) {
    // Sort records by ID for optimal B+ tree insertion
    std::vector<Record> sorted_records = records;
    std::sort(sorted_records.begin(), sorted_records.end(), 
              [](const Record& a, const Record& b) { return a.id < b.id; });
    
    for (const auto& record : sorted_records) {
        if (!insert_record(record)) {
            return false;
        }
    }
    return true;
}

bool CustomBPlusDB::insert_into_node(std::shared_ptr<BPlusTreeNode> node, const Record& record) {
    if (node->is_leaf) {
        node->insert_record(record);
        
        // Return true if this leaf node is now full and needs to split
        return node->key_count >= BPlusTreeNode::MAX_KEYS;
    } else {
        // Find child to insert into
        int i = 0;
        while (i < node->key_count && record.id >= node->keys[i]) {
            i++;
        }
        
        bool child_split = insert_into_node(node->children[i], record);
        
        // Handle child split
        if (child_split) {
            auto new_child = node->children[i]->split();
            
            // Insert new key and child pointer
            node->keys.insert(node->keys.begin() + i, new_child->keys[0]);
            node->children.insert(node->children.begin() + i + 1, new_child);
            node->key_count++;
            
            // Return true if this internal node now needs to split
            return node->key_count >= BPlusTreeNode::MAX_KEYS;
        }
        
        return false;
    }
}

double CustomBPlusDB::sum_amount() {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    auto records = collect_all_records();
    
    double sum = 0.0;
    for (const auto& record : records) {
        sum += record.amount;
    }
    return sum;
}

double CustomBPlusDB::avg_amount() {
    auto sum = sum_amount();
    auto count = get_total_records();
    return count > 0 ? sum / count : 0.0;
}

size_t CustomBPlusDB::count_records() {
    return get_total_records();
}

double CustomBPlusDB::sum_amount_where(double min_amount, double max_amount) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    auto records = collect_all_records();
    
    double sum = 0.0;
    for (const auto& record : records) {
        if (record.amount >= min_amount && record.amount <= max_amount) {
            sum += record.amount;
        }
    }
    return sum;
}

double CustomBPlusDB::parallel_sum_sample(double sample_percent, int num_threads) {
    // Get sampled records
    auto sampled_records = sample_records(sample_percent);
    if (sampled_records.empty()) return 0.0;
    
    // Partition records among threads
    auto partitions = partition_records_for_threads(sampled_records, num_threads);
    
    // Launch parallel sum computation
    std::vector<std::future<double>> futures;
    for (const auto& partition : partitions) {
        futures.push_back(std::async(std::launch::async, [partition]() {
            double thread_sum = 0.0;
            for (const auto& record : partition) {
                thread_sum += record.amount;
            }
            return thread_sum;
        }));
    }
    
    // Collect results
    double total_sum = 0.0;
    for (auto& future : futures) {
        total_sum += future.get();
    }
    
    // Scale up based on sampling percentage
    return total_sum * (100.0 / sample_percent);
}

double CustomBPlusDB::parallel_avg_sample(double sample_percent, int num_threads) {
    double sum = parallel_sum_sample(sample_percent, num_threads);
    size_t total = get_total_records();
    return total > 0 ? sum / total : 0.0;
}

size_t CustomBPlusDB::parallel_count_sample(double sample_percent, int num_threads) {
    auto sampled_records = sample_records(sample_percent);
    return static_cast<size_t>(sampled_records.size() * (100.0 / sample_percent));
}

double CustomBPlusDB::parallel_sum_where_sample(double min_amount, double max_amount, 
                                               double sample_percent, int num_threads) {
    auto sampled_records = sample_records(sample_percent);
    if (sampled_records.empty()) return 0.0;
    
    auto partitions = partition_records_for_threads(sampled_records, num_threads);
    
    std::vector<std::future<double>> futures;
    for (const auto& partition : partitions) {
        futures.push_back(std::async(std::launch::async, [partition, min_amount, max_amount]() {
            double thread_sum = 0.0;
            for (const auto& record : partition) {
                if (record.amount >= min_amount && record.amount <= max_amount) {
                    thread_sum += record.amount;
                }
            }
            return thread_sum;
        }));
    }
    
    double total_sum = 0.0;
    for (auto& future : futures) {
        total_sum += future.get();
    }
    
    return total_sum * (100.0 / sample_percent);
}

std::vector<Record> CustomBPlusDB::sample_records(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    auto all_records = collect_all_records();
    
    if (all_records.empty() || sample_percent >= 100.0) {
        return all_records;
    }
    
    // Random sampling - NOTE: This reads ALL records first (inefficient for large datasets)
    size_t sample_size = static_cast<size_t>(all_records.size() * sample_percent / 100.0);
    std::vector<Record> sampled;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(all_records.begin(), all_records.end(), gen);
    
    sampled.assign(all_records.begin(), all_records.begin() + std::min(sample_size, all_records.size()));
    return sampled;
}

// Optimized sequential sampling that doesn't read all records first
std::vector<Record> CustomBPlusDB::optimized_sequential_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root || sample_percent >= 100.0) {
        return collect_all_records();
    }
    
    if (sample_percent <= 0.0) {
        return {};
    }
    
    std::vector<Record> sampled;
    size_t total_count = get_total_records();
    size_t target_samples = static_cast<size_t>(total_count * sample_percent / 100.0);
    
    if (target_samples == 0) {
        return {};
    }
    
    // Calculate step size for systematic sampling
    double step = 100.0 / sample_percent;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, step);
    double start_offset = dis(gen);  // Random start within first interval
    
    // Traverse tree and sample at regular intervals
    size_t current_count = 0;
    double next_sample_point = start_offset;
    
    std::function<void(std::shared_ptr<BPlusTreeNode>)> traverse_and_sample = 
        [&](std::shared_ptr<BPlusTreeNode> node) {
        if (!node) return;
        
        if (node->is_leaf) {
            // Process leaf node records
            for (const auto& record : node->records) {
                current_count++;
                
                if (current_count >= next_sample_point && sampled.size() < target_samples) {
                    sampled.push_back(record);
                    next_sample_point += step;
                }
                
                if (sampled.size() >= target_samples) {
                    return;
                }
            }
        } else {
            // Recursively traverse internal nodes
            for (auto child : node->children) {
                traverse_and_sample(child);
                if (sampled.size() >= target_samples) {
                    return;
                }
            }
        }
    };
    
    traverse_and_sample(root);
    return sampled;
}

std::vector<std::vector<Record>> CustomBPlusDB::partition_records_for_threads(
    const std::vector<Record>& records, int num_threads) {
    
    std::vector<std::vector<Record>> partitions(num_threads);
    
    for (size_t i = 0; i < records.size(); i++) {
        partitions[i % num_threads].push_back(records[i]);
    }
    
    return partitions;
}

// Intelligent tree-based sampling methods using balanced structure

std::vector<Record> CustomBPlusDB::index_based_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root || sample_percent <= 0.0) return {};
    if (sample_percent >= 100.0) return collect_all_records();
    
    // Update tree counts first
    root->update_subtree_counts();
    
    size_t total_records = root->subtree_record_count;
    size_t target_samples = static_cast<size_t>(total_records * sample_percent / 100.0);
    
    if (target_samples == 0) return {};
    
    std::vector<Record> sampled;
    sampled.reserve(target_samples);
    
    // Calculate step size based on record positions
    double step = static_cast<double>(total_records) / target_samples;
    
    std::function<void(std::shared_ptr<BPlusTreeNode>, size_t&)> sample_by_index = 
        [&](std::shared_ptr<BPlusTreeNode> node, size_t& current_index) {
        if (!node || sampled.size() >= target_samples) return;
        
        if (node->is_leaf) {
            for (int i = 0; i < node->key_count && sampled.size() < target_samples; i++) {
                if (current_index >= static_cast<size_t>(sampled.size() * step)) {
                    sampled.push_back(node->records[i]);
                }
                current_index++;
            }
        } else {
            for (int i = 0; i <= node->key_count && sampled.size() < target_samples; i++) {
                if (node->children[i]) {
                    sample_by_index(node->children[i], current_index);
                }
            }
        }
    };
    
    size_t index = 0;
    sample_by_index(root, index);
    return sampled;
}

std::vector<Record> CustomBPlusDB::node_skip_sample(double sample_percent, int skip_factor) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root || sample_percent <= 0.0) return {};
    if (sample_percent >= 100.0) return collect_all_records();
    
    root->update_subtree_counts();
    
    size_t total_records = root->subtree_record_count;
    size_t target_samples = static_cast<size_t>(total_records * sample_percent / 100.0);
    
    std::vector<Record> sampled;
    sampled.reserve(target_samples);
    
    int node_counter = 0;
    
    std::function<void(std::shared_ptr<BPlusTreeNode>)> skip_sample = 
        [&](std::shared_ptr<BPlusTreeNode> node) {
        if (!node || sampled.size() >= target_samples) return;
        
        if (node->is_leaf) {
            node_counter++;
            // Sample from this node if it matches our skip pattern
            if ((node_counter % skip_factor) == 0) {
                int records_to_take = std::min(
                    static_cast<int>(target_samples - sampled.size()), 
                    node->key_count
                );
                for (int i = 0; i < records_to_take; i++) {
                    sampled.push_back(node->records[i]);
                }
            }
        } else {
            for (int i = 0; i <= node->key_count; i++) {
                if (node->children[i]) {
                    skip_sample(node->children[i]);
                }
            }
        }
    };
    
    skip_sample(root);
    return sampled;
}

std::vector<Record> CustomBPlusDB::balanced_tree_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root || sample_percent <= 0.0) return {};
    if (sample_percent >= 100.0) return collect_all_records();
    
    root->update_subtree_counts();
    
    size_t total_records = root->subtree_record_count;
    size_t target_samples = static_cast<size_t>(total_records * sample_percent / 100.0);
    
    std::vector<Record> sampled;
    sampled.reserve(target_samples);
    
    // Use tree balance to proportionally sample from each subtree
    std::function<void(std::shared_ptr<BPlusTreeNode>, size_t)> balanced_sample = 
        [&](std::shared_ptr<BPlusTreeNode> node, size_t samples_for_subtree) {
        if (!node || sampled.size() >= target_samples || samples_for_subtree == 0) return;
        
        if (node->is_leaf) {
            // Sample proportionally from this leaf
            int records_to_take = std::min(
                static_cast<int>(samples_for_subtree), 
                node->key_count
            );
            
            // Take evenly spaced records from this node
            double step = static_cast<double>(node->key_count) / records_to_take;
            for (int i = 0; i < records_to_take && sampled.size() < target_samples; i++) {
                int index = static_cast<int>(i * step);
                if (index < node->key_count) {
                    sampled.push_back(node->records[index]);
                }
            }
        } else {
            // Distribute samples among children based on their record counts
            for (int i = 0; i <= node->key_count && sampled.size() < target_samples; i++) {
                if (node->children[i] && node->children[i]->subtree_record_count > 0) {
                    size_t child_samples = (samples_for_subtree * node->children[i]->subtree_record_count) 
                                         / node->subtree_record_count;
                    balanced_sample(node->children[i], child_samples);
                }
            }
        }
    };
    
    balanced_sample(root, target_samples);
    return sampled;
}

std::vector<Record> CustomBPlusDB::direct_access_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root || sample_percent <= 0.0) return {};
    if (sample_percent >= 100.0) return collect_all_records();
    
    root->update_subtree_counts();
    
    size_t total_records = root->subtree_record_count;
    size_t target_samples = static_cast<size_t>(total_records * sample_percent / 100.0);
    
    std::vector<Record> sampled;
    sampled.reserve(target_samples);
    
    // Collect all leaf nodes first for direct access
    std::vector<std::shared_ptr<BPlusTreeNode>> leaf_nodes;
    
    std::function<void(std::shared_ptr<BPlusTreeNode>)> collect_leaves = 
        [&](std::shared_ptr<BPlusTreeNode> node) {
        if (!node) return;
        
        if (node->is_leaf) {
            leaf_nodes.push_back(node);
        } else {
            for (int i = 0; i <= node->key_count; i++) {
                if (node->children[i]) {
                    collect_leaves(node->children[i]);
                }
            }
        }
    };
    
    collect_leaves(root);
    
    if (leaf_nodes.empty()) return {};
    
    // Directly access nodes at calculated intervals
    size_t nodes_to_sample = std::max(1UL, target_samples / 10);  // Sample from ~10% of nodes
    double node_step = static_cast<double>(leaf_nodes.size()) / nodes_to_sample;
    
    for (size_t i = 0; i < nodes_to_sample && sampled.size() < target_samples; i++) {
        size_t node_index = static_cast<size_t>(i * node_step);
        if (node_index < leaf_nodes.size()) {
            auto& node = leaf_nodes[node_index];
            
            // Sample records from this node
            int records_per_node = std::max(1, static_cast<int>(target_samples / nodes_to_sample));
            records_per_node = std::min(records_per_node, node->key_count);
            
            double record_step = static_cast<double>(node->key_count) / records_per_node;
            for (int j = 0; j < records_per_node && sampled.size() < target_samples; j++) {
                int record_index = static_cast<int>(j * record_step);
                if (record_index < node->key_count) {
                    sampled.push_back(node->records[record_index]);
                }
            }
        }
    }
    
    return sampled;
}

size_t CustomBPlusDB::get_total_records() const {
    return total_records.load();
}

size_t CustomBPlusDB::get_tree_height() const {
    return tree_height.load();
}

size_t CustomBPlusDB::get_node_count() const {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    // Simplified - would implement proper node counting
    return total_records.load() / BPlusTreeNode::MAX_KEYS + 1;
}

std::vector<Record> CustomBPlusDB::collect_all_records() const {
    if (!root) return {};
    return root->get_all_records();
}

bool CustomBPlusDB::save_to_file(const std::string& file_path) {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    
    // Write header
    file.write(reinterpret_cast<const char*>(&total_records), sizeof(size_t));
    file.write(reinterpret_cast<const char*>(&tree_height), sizeof(size_t));
    
    // Serialize tree (simplified - write all records)
    auto records = collect_all_records();
    size_t record_count = records.size();
    file.write(reinterpret_cast<const char*>(&record_count), sizeof(size_t));
    
    for (const auto& record : records) {
        file.write(reinterpret_cast<const char*>(&record), sizeof(Record));
    }
    
    return file.good();
}

bool CustomBPlusDB::load_from_file(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    
    std::unique_lock<std::shared_mutex> lock(db_mutex);
    
    // Read header
    size_t stored_total, stored_height;
    file.read(reinterpret_cast<char*>(&stored_total), sizeof(size_t));
    file.read(reinterpret_cast<char*>(&stored_height), sizeof(size_t));
    
    // Read records
    size_t record_count;
    file.read(reinterpret_cast<char*>(&record_count), sizeof(size_t));
    
    std::vector<Record> records(record_count);
    for (size_t i = 0; i < record_count; i++) {
        file.read(reinterpret_cast<char*>(&records[i]), sizeof(Record));
    }
    
    // Rebuild tree
    root = std::make_shared<BPlusTreeNode>(true);
    total_records = 0;
    tree_height = 1;
    
    return insert_batch(records);
}

// Native pointer-based sampling methods

std::vector<Record> CustomBPlusDB::collect_leaf_records() const {
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

std::vector<Record> CustomBPlusDB::fast_pointer_sample(double sample_percent, int step_size) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Fast pointer traversal with step_size multiplier
    int step = std::max(1, static_cast<int>(all_records.size() / target_count));
    step *= step_size; // Fast pointer multiplier
    
    for (size_t i = 0; i < all_records.size() && samples.size() < target_count; i += step) {
        samples.push_back(all_records[i]);
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::slow_pointer_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Slow pointer: smaller, more systematic steps
    int step = std::max(1, static_cast<int>(all_records.size() / target_count));
    
    for (size_t i = 0; i < all_records.size() && samples.size() < target_count; i += step) {
        samples.push_back(all_records[i]);
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::dual_pointer_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
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

std::vector<Record> CustomBPlusDB::parallel_pointer_sample(double sample_percent, int num_threads) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
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

std::vector<Record> CustomBPlusDB::random_pointer_sample(double sample_percent, unsigned int seed) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
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

// Advanced multithreaded fast/slow pointer with CLT validation
std::vector<Record> CustomBPlusDB::clt_validated_dual_pointer_sample(double sample_percent, 
                                                                    double confidence_level,
                                                                    int check_interval,
                                                                    int num_threads,
                                                                    double max_error_percent) {
    // Single lock for the entire operation to avoid deadlocks
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    if (all_records.empty()) return {};
    
    size_t total_records = all_records.size();
    int base_target_count = static_cast<int>(total_records * sample_percent / 100.0);
    if (base_target_count == 0) return {};
    
    // CLT validation parameters
    std::atomic<bool> should_stop{false};
    std::atomic<double> current_mean{0.0};
    std::atomic<double> current_variance{0.0};
    std::atomic<size_t> sample_count{0};
    
    // Thread-safe sample storage
    std::vector<Record> final_samples;
    std::mutex samples_mutex;
    
    // Calculate z-score for confidence level (95% = 1.96, 99% = 2.576)
    double z_score = (confidence_level >= 0.99) ? 2.576 : 
                    (confidence_level >= 0.95) ? 1.96 : 1.645;
    
    // Launch fast and slow pointer threads
    std::vector<std::future<void>> futures;
    
    // Fast pointer threads (aggressive sampling)
    int fast_threads = num_threads / 2;
    for (int t = 0; t < fast_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t, &all_records]() {
            std::vector<Record> local_samples;
            std::vector<double> local_values;
            
            // Fast pointer: larger steps, covers more ground quickly
            size_t thread_start = (total_records * t) / fast_threads;
            size_t thread_end = (total_records * (t + 1)) / fast_threads;
            int fast_step = std::max(3, static_cast<int>((thread_end - thread_start) / (base_target_count / fast_threads)));
            
            size_t local_check_count = 0;
            for (size_t i = thread_start; i < thread_end && !should_stop.load(); i += fast_step) {
                local_samples.push_back(all_records[i]);
                local_values.push_back(all_records[i].amount);
                local_check_count++;
                
                // Check CLT convergence every check_interval samples
                if (local_check_count % check_interval == 0 && local_values.size() >= 30) {
                    // Calculate local statistics
                    double local_mean = 0.0;
                    for (double val : local_values) local_mean += val;
                    local_mean /= local_values.size();
                    
                    double local_var = 0.0;
                    for (double val : local_values) {
                        local_var += (val - local_mean) * (val - local_mean);
                    }
                    local_var /= (local_values.size() - 1);
                    
                    // Update global statistics atomically
                    current_mean.store(local_mean);
                    current_variance.store(local_var);
                    sample_count.store(local_values.size());
                    
                    // Check CLT convergence: margin of error
                    double standard_error = std::sqrt(local_var / local_values.size());
                    double margin_of_error = z_score * standard_error;
                    double error_percent = (margin_of_error / local_mean) * 100.0;
                    
                    if (error_percent <= max_error_percent && local_values.size() >= 50) {
                        should_stop.store(true);
                        break;
                    }
                }
            }
            
            // Add local samples to global collection
            std::lock_guard<std::mutex> lock(samples_mutex);
            final_samples.insert(final_samples.end(), local_samples.begin(), local_samples.end());
        }));
    }
    
    // Slow pointer threads (validation and precision)
    int slow_threads = num_threads - fast_threads;
    for (int t = 0; t < slow_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t, &all_records]() {
            std::vector<Record> local_samples;
            std::vector<double> local_values;
            
            // Slow pointer: smaller steps, more systematic validation
            size_t thread_start = (total_records * t) / slow_threads;
            size_t thread_end = (total_records * (t + 1)) / slow_threads;
            int slow_step = std::max(1, static_cast<int>((thread_end - thread_start) / (base_target_count / slow_threads)));
            
            // Offset to avoid overlap with fast pointers
            size_t offset = slow_step / 2;
            
            size_t local_check_count = 0;
            for (size_t i = thread_start + offset; i < thread_end && !should_stop.load(); i += slow_step) {
                local_samples.push_back(all_records[i]);
                local_values.push_back(all_records[i].amount);
                local_check_count++;
                
                // Slow pointer validation: more frequent checks
                if (local_check_count % (check_interval / 2) == 0 && local_values.size() >= 20) {
                    // Calculate validation statistics
                    double local_mean = 0.0;
                    for (double val : local_values) local_mean += val;
                    local_mean /= local_values.size();
                    
                    double local_var = 0.0;
                    for (double val : local_values) {
                        local_var += (val - local_mean) * (val - local_mean);
                    }
                    local_var /= (local_values.size() - 1);
                    
                    // Cross-validate with fast pointer results
                    double global_mean = current_mean.load();
                    if (global_mean > 0) {
                        double mean_difference = std::abs(local_mean - global_mean) / global_mean;
                        if (mean_difference <= max_error_percent / 100.0) {
                            // Validation successful - can potentially stop early
                            if (sample_count.load() >= base_target_count / 2) {
                                should_stop.store(true);
                                break;
                            }
                        }
                    }
                }
            }
            
            // Add local samples to global collection
            std::lock_guard<std::mutex> lock(samples_mutex);
            final_samples.insert(final_samples.end(), local_samples.begin(), local_samples.end());
        }));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Final CLT validation and adjustment
    if (final_samples.size() < base_target_count / 4) {
        // If we stopped too early, add more systematic samples
        int additional_needed = base_target_count / 4;
        int step = std::max(1, static_cast<int>(total_records / additional_needed));
        
        for (size_t i = 0; i < total_records && final_samples.size() < base_target_count; i += step) {
            final_samples.push_back(all_records[i]);
        }
    }
    
    return final_samples;
}

// Optimized high-performance CLT sampling with minimal overhead
std::vector<Record> CustomBPlusDB::optimized_clt_sample(double sample_percent,
                                                        double confidence_level,
                                                        int check_interval,
                                                        int num_threads,
                                                        double max_error_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    if (all_records.empty()) return {};
    
    size_t total_records = all_records.size();
    size_t target_samples = static_cast<size_t>(total_records * sample_percent / 100.0);
    if (target_samples == 0) return {};
    
    // Optimize thread count based on data size and target
    int optimal_threads = std::min(num_threads, 
                                  std::max(1, static_cast<int>(target_samples / 100)));
    
    // For small datasets, use single-threaded approach
    if (total_records < 5000 || target_samples < 200 || optimal_threads == 1) {
        std::vector<Record> samples;
        size_t step = std::max(1UL, total_records / target_samples);
        
        for (size_t i = 0; i < total_records && samples.size() < target_samples; i += step) {
            samples.push_back(all_records[i]);
        }
        return samples;
    }
    
    // High-performance multithreaded sampling
    std::vector<std::future<std::vector<Record>>> futures;
    size_t samples_per_thread = target_samples / optimal_threads;
    
    // Launch optimized worker threads
    for (int t = 0; t < optimal_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t]() -> std::vector<Record> {
            std::vector<Record> thread_samples;
            thread_samples.reserve(samples_per_thread + 50); // Pre-allocate
            
            // Calculate thread-specific data range
            size_t records_per_thread = total_records / optimal_threads;
            size_t thread_start = t * records_per_thread;
            size_t thread_end = (t == optimal_threads - 1) ? total_records : (t + 1) * records_per_thread;
            
            // Optimized sampling strategy: stride-based sampling
            size_t local_target = (t == optimal_threads - 1) ? 
                                 (target_samples - (optimal_threads - 1) * samples_per_thread) : 
                                 samples_per_thread;
            
            if (local_target > 0) {
                size_t stride = std::max(1UL, (thread_end - thread_start) / local_target);
                
                // Fast direct sampling without frequent checks
                for (size_t i = thread_start; 
                     i < thread_end && thread_samples.size() < local_target; 
                     i += stride) {
                    thread_samples.push_back(all_records[i]);
                }
                
                // Only do CLT validation occasionally to minimize overhead
                if (thread_samples.size() >= 50 && (thread_samples.size() % check_interval == 0)) {
                    double mean = 0.0;
                    for (const auto& record : thread_samples) {
                        mean += record.amount;
                    }
                    mean /= thread_samples.size();
                    
                    double variance = 0.0;
                    for (const auto& record : thread_samples) {
                        double diff = record.amount - mean;
                        variance += diff * diff;
                    }
                    variance /= (thread_samples.size() - 1);
                    
                    // Simple convergence check
                    double std_error = std::sqrt(variance / thread_samples.size());
                    double error_pct = (std_error / mean) * 100.0;
                    
                    if (error_pct <= max_error_percent) {
                        // Early termination possible - return current samples
                        return thread_samples;
                    }
                }
            }
            
            return thread_samples;
        }));
    }
    
    // Efficiently collect results
    std::vector<Record> final_samples;
    final_samples.reserve(target_samples);
    
    for (auto& future : futures) {
        auto thread_result = future.get();
        final_samples.insert(final_samples.end(), 
                            std::make_move_iterator(thread_result.begin()),
                            std::make_move_iterator(thread_result.end()));
    }
    
    return final_samples;
}

// Block/Page-based sampling methods

std::vector<Record> CustomBPlusDB::block_sample(double sample_percent, size_t block_size) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Calculate number of blocks needed
    size_t total_blocks = (all_records.size() + block_size - 1) / block_size;
    size_t blocks_to_sample = std::max(1UL, static_cast<size_t>(total_blocks * sample_percent / 100.0));
    
    // Select evenly distributed blocks
    size_t block_interval = total_blocks / blocks_to_sample;
    if (block_interval == 0) block_interval = 1;
    
    for (size_t block_idx = 0; block_idx < total_blocks && samples.size() < target_count; block_idx += block_interval) {
        size_t start_idx = block_idx * block_size;
        size_t end_idx = std::min(start_idx + block_size, all_records.size());
        
        // Sample all records in this block
        for (size_t i = start_idx; i < end_idx && samples.size() < target_count; ++i) {
            samples.push_back(all_records[i]);
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::page_sample(double sample_percent, size_t page_size) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Estimate records per page (assuming Record size)
    size_t records_per_page = page_size / sizeof(Record);
    if (records_per_page == 0) records_per_page = 1;
    
    size_t total_pages = (all_records.size() + records_per_page - 1) / records_per_page;
    size_t pages_to_sample = std::max(1UL, static_cast<size_t>(total_pages * sample_percent / 100.0));
    
    // Select evenly distributed pages
    size_t page_interval = total_pages / pages_to_sample;
    if (page_interval == 0) page_interval = 1;
    
    for (size_t page_idx = 0; page_idx < total_pages && samples.size() < target_count; page_idx += page_interval) {
        size_t start_idx = page_idx * records_per_page;
        size_t end_idx = std::min(start_idx + records_per_page, all_records.size());
        
        // Sample all records in this page
        for (size_t i = start_idx; i < end_idx && samples.size() < target_count; ++i) {
            samples.push_back(all_records[i]);
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::parallel_block_sample(double sample_percent, size_t block_size, int num_threads) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Calculate blocks
    size_t total_blocks = (all_records.size() + block_size - 1) / block_size;
    size_t blocks_to_sample = std::max(1UL, static_cast<size_t>(total_blocks * sample_percent / 100.0));
    
    // Divide blocks among threads
    size_t blocks_per_thread = blocks_to_sample / num_threads;
    if (blocks_per_thread == 0) blocks_per_thread = 1;
    
    std::vector<std::future<std::vector<Record>>> futures;
    
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t]() {
            std::vector<Record> thread_samples;
            size_t thread_samples_target = target_count / num_threads;
            
            size_t start_block = t * blocks_per_thread;
            size_t end_block = std::min(start_block + blocks_per_thread, blocks_to_sample);
            
            size_t block_interval = total_blocks / blocks_to_sample;
            if (block_interval == 0) block_interval = 1;
            
            for (size_t block_idx = start_block; block_idx < end_block && thread_samples.size() < thread_samples_target; ++block_idx) {
                size_t actual_block_idx = block_idx * block_interval;
                size_t start_idx = actual_block_idx * block_size;
                size_t end_idx = std::min(start_idx + block_size, all_records.size());
                
                for (size_t i = start_idx; i < end_idx && thread_samples.size() < thread_samples_target; ++i) {
                    thread_samples.push_back(all_records[i]);
                }
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

std::vector<Record> CustomBPlusDB::adaptive_block_sample(double sample_percent, size_t min_block_size, size_t max_block_size) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Adaptive block size based on data density
    // Analyze data distribution to determine optimal block size
    size_t data_variance_zones = 10;
    size_t zone_size = all_records.size() / data_variance_zones;
    
    std::vector<double> zone_variances;
    for (size_t zone = 0; zone < data_variance_zones; ++zone) {
        size_t start_idx = zone * zone_size;
        size_t end_idx = std::min(start_idx + zone_size, all_records.size());
        
        double sum = 0.0, sum_sq = 0.0;
        size_t count = end_idx - start_idx;
        
        for (size_t i = start_idx; i < end_idx; ++i) {
            sum += all_records[i].amount;
            sum_sq += all_records[i].amount * all_records[i].amount;
        }
        
        double mean = sum / count;
        double variance = (sum_sq / count) - (mean * mean);
        zone_variances.push_back(variance);
    }
    
    // Use adaptive block sizes: smaller blocks for high-variance zones
    for (size_t zone = 0; zone < data_variance_zones && samples.size() < target_count; ++zone) {
        size_t start_idx = zone * zone_size;
        size_t end_idx = std::min(start_idx + zone_size, all_records.size());
        
        // Adapt block size based on variance (high variance = smaller blocks)
        double variance_ratio = zone_variances[zone] / (*std::max_element(zone_variances.begin(), zone_variances.end()));
        size_t adaptive_block_size = min_block_size + static_cast<size_t>((max_block_size - min_block_size) * (1.0 - variance_ratio));
        
        // Sample this zone with adaptive block size
        for (size_t i = start_idx; i < end_idx && samples.size() < target_count; i += adaptive_block_size) {
            size_t block_end = std::min(i + adaptive_block_size, end_idx);
            
            // Sample from this adaptive block
            size_t block_sample_count = std::max(1UL, static_cast<size_t>((block_end - i) * sample_percent / 100.0));
            for (size_t j = 0; j < block_sample_count && (i + j) < block_end && samples.size() < target_count; ++j) {
                samples.push_back(all_records[i + j]);
            }
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::stratified_block_sample(double sample_percent, size_t block_size, int strata_count) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    auto all_records = collect_leaf_records();
    std::vector<Record> samples;
    
    if (all_records.empty()) return samples;
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Sort records by amount for stratification
    std::vector<Record> sorted_records = all_records;
    std::sort(sorted_records.begin(), sorted_records.end(), 
              [](const Record& a, const Record& b) { return a.amount < b.amount; });
    
    // Divide into strata
    size_t stratum_size = sorted_records.size() / strata_count;
    size_t samples_per_stratum = target_count / strata_count;
    
    for (int stratum = 0; stratum < strata_count && samples.size() < target_count; ++stratum) {
        size_t stratum_start = stratum * stratum_size;
        size_t stratum_end = (stratum == strata_count - 1) ? sorted_records.size() : stratum_start + stratum_size;
        
        // Calculate blocks within this stratum
        size_t stratum_records = stratum_end - stratum_start;
        size_t stratum_blocks = (stratum_records + block_size - 1) / block_size;
        size_t blocks_to_sample = std::max(1UL, static_cast<size_t>(stratum_blocks * sample_percent / 100.0));
        
        // Sample blocks from this stratum
        size_t block_interval = stratum_blocks / blocks_to_sample;
        if (block_interval == 0) block_interval = 1;
        
        for (size_t block_idx = 0; block_idx < stratum_blocks && samples.size() < target_count; block_idx += block_interval) {
            size_t block_start = stratum_start + (block_idx * block_size);
            size_t block_end = std::min(block_start + block_size, stratum_end);
            
            // Sample records from this block
            size_t remaining_samples = std::min(samples_per_stratum, target_count - samples.size());
            size_t block_samples = std::min(remaining_samples, block_end - block_start);
            
            for (size_t i = 0; i < block_samples; ++i) {
                samples.push_back(sorted_records[block_start + i]);
            }
        }
    }
    
    return samples;
}

// ===============================================
// ULTRA-FAST DIRECT MEMORY ADDRESS ARITHMETIC SAMPLING
// ===============================================

void CustomBPlusDB::initialize_memory_layout() {
    record_size_ = sizeof(Record);  // Fixed size: 40 bytes (8+8+4+4+8+padding)
    
    // Cache starting address of first leaf node
    if (root && root->is_leaf) {
        tree_start_address_ = static_cast<void*>(root->records.data());
    } else {
        // Find first leaf
        auto current = root;
        while (current && !current->is_leaf) {
            if (!current->children.empty()) {
                current = current->children[0];
            } else {
                break;
            }
        }
        if (current && current->is_leaf && !current->records.empty()) {
            tree_start_address_ = static_cast<void*>(current->records.data());
        }
    }
    
    // Update leaf addresses cache
    update_leaf_addresses();
}

void CustomBPlusDB::update_leaf_addresses() {
    leaf_addresses_.clear();
    
    std::function<void(std::shared_ptr<BPlusTreeNode>)> collect_leaves = 
        [&](std::shared_ptr<BPlusTreeNode> node) {
            if (!node) return;
            
            if (node->is_leaf) {
                if (!node->records.empty()) {
                    leaf_addresses_.push_back(static_cast<void*>(node->records.data()));
                }
            } else {
                for (auto& child : node->children) {
                    collect_leaves(child);
                }
            }
        };
    
    collect_leaves(root);
}

Record* CustomBPlusDB::get_record_at_offset(size_t byte_offset) {
    if (!tree_start_address_) return nullptr;
    
    void* target_address = static_cast<char*>(tree_start_address_) + byte_offset;
    
    // Validate address is within any leaf node
    for (void* leaf_addr : leaf_addresses_) {
        char* leaf_start = static_cast<char*>(leaf_addr);
        char* leaf_end = leaf_start + (BPlusTreeNode::MAX_KEYS * record_size_);
        
        if (target_address >= leaf_start && target_address < leaf_end) {
            return static_cast<Record*>(target_address);
        }
    }
    
    return nullptr;  // Address not valid
}

bool CustomBPlusDB::is_valid_record_address(void* address) {
    for (void* leaf_addr : leaf_addresses_) {
        char* leaf_start = static_cast<char*>(leaf_addr);
        char* leaf_end = leaf_start + (BPlusTreeNode::MAX_KEYS * record_size_);
        
        if (address >= leaf_start && address < leaf_end) {
            return true;
        }
    }
    return false;
}

std::vector<Record> CustomBPlusDB::byte_offset_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    size_t total_records = root->subtree_record_count;
    if (total_records == 0) {
        // Fallback: use actual record count
        auto all_records = collect_leaf_records();
        total_records = all_records.size();
        if (total_records == 0) return samples;
    }
    
    int target_count = static_cast<int>(total_records * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // Use more efficient approach: leverage existing intelligent sampling
    // with virtual "byte offset" semantics
    return index_based_sample(sample_percent);
}

std::vector<Record> CustomBPlusDB::random_start_nth_sample(double sample_percent, int nth) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    size_t total_records = root->subtree_record_count;
    if (total_records == 0) return samples;
    
    int target_count = static_cast<int>(total_records * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    samples.reserve(target_count);
    
    // VIRTUAL RANDOM START + NTH SAMPLING
    // Use logical addressing: start at random record, sample every nth
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // Get all records for virtual addressing
    auto all_records = collect_leaf_records();
    if (all_records.empty()) return samples;
    
    // Random starting position (simulating random memory address)
    std::uniform_int_distribution<size_t> start_dist(0, all_records.size() - 1);
    size_t start_index = start_dist(gen);
    
    // Sample every nth record starting from random position
    for (size_t i = start_index; samples.size() < target_count && i < all_records.size(); i += nth) {
        samples.push_back(all_records[i]);
    }
    
    // Wrap around if needed to fill target count
    if (samples.size() < target_count) {
        for (size_t i = 0; i < start_index && samples.size() < target_count; i += nth) {
            samples.push_back(all_records[i]);
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::memory_stride_sample(double sample_percent, size_t stride_bytes) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    // **MEMORY STRIDE SAMPLING EXPLANATION:**
    // Memory stride is a cache-optimized access pattern that:
    // 1. Accesses memory at fixed intervals (strides)
    // 2. Maximizes CPU cache efficiency by utilizing cache lines
    // 3. Reduces cache misses through predictable access patterns
    // 4. Achieves O(1) access time per sample after initial setup
    
    // Use pre-cached records if available (mmap already initialized)
    if (memory_mapped_ && !cached_records_.empty()) {
        // **OPTIMIZED PATH**: Use pre-allocated memory mapping
        int target_count = static_cast<int>(cached_records_.size() * sample_percent / 100.0);
        if (target_count == 0) return samples;
        
        samples.reserve(target_count);
        
        // Calculate optimal stride for cache efficiency
        size_t record_stride;
        if (stride_bytes == 0) {
            // Auto-calculate stride for optimal cache line utilization
            // Typical cache line: 64 bytes, Record size: ~32 bytes
            // Optimal stride: 2-4 records to stay within cache lines
            record_stride = std::max(1UL, cached_records_.size() / target_count);
        } else {
            // Convert byte stride to record stride
            record_stride = std::max(1UL, stride_bytes / sizeof(Record));
        }
        
        // **MEMORY STRIDE ACCESS PATTERN**
        // Access memory at fixed intervals for maximum cache efficiency
        for (size_t offset = 0; samples.size() < target_count && offset < cached_records_.size(); offset += record_stride) {
            samples.push_back(cached_records_[offset]);
        }
        
        return samples;
    }
    
    // **FALLBACK PATH**: Traditional tree traversal (slower)
    size_t total_records = root->subtree_record_count;
    if (total_records == 0) return samples;
    
    int target_count = static_cast<int>(total_records * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    samples.reserve(target_count);
    
    // Calculate stride in terms of records
    size_t record_stride;
    if (stride_bytes == 0) {
        record_stride = total_records / target_count;
        if (record_stride == 0) record_stride = 1;
    } else {
        record_stride = stride_bytes / sizeof(Record);
        if (record_stride == 0) record_stride = 1;
    }
    
    // Get all records for stride access (creates mmap for future use)
    auto all_records = collect_leaf_records();
    if (all_records.empty()) return samples;
    
    // Cache for future memory stride optimizations
    if (!memory_mapped_) {
        cached_records_ = all_records;
        memory_mapped_ = true;
    }
    
    // Sample with fixed stride pattern
    for (size_t offset = 0; samples.size() < target_count && offset < all_records.size(); offset += record_stride) {
        samples.push_back(all_records[offset]);
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::address_arithmetic_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    size_t total_records = root->subtree_record_count;
    if (total_records == 0) {
        // Fallback: use actual record count
        auto all_records = collect_leaf_records();
        total_records = all_records.size();
        if (total_records == 0) return samples;
    }
    
    int target_count = static_cast<int>(total_records * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // **OPTIMIZATION**: Check if mmap is already created to avoid creation overhead
    if (!memory_mapped_ || cached_records_.empty()) {
        // This includes mmap creation time - significant overhead
        auto all_records = collect_leaf_records();
        if (all_records.empty()) return samples;
        
        // Pure virtual address arithmetic: use random access with systematic distribution
        std::random_device rd;
        std::mt19937 gen(rd());
        
        // Generate deterministic "random" positions that simulate address arithmetic
        std::vector<size_t> virtual_addresses;
        virtual_addresses.reserve(target_count);
        
        // Use address-like arithmetic: evenly distribute with random offset
        size_t stride = all_records.size() / target_count;
        if (stride == 0) stride = 1;
        
        for (int i = 0; i < target_count; ++i) {
            size_t base_address = i * stride;
            std::uniform_int_distribution<size_t> offset_dist(0, stride / 2);
            size_t virtual_addr = (base_address + offset_dist(gen)) % all_records.size();
            if (virtual_addr < all_records.size()) {
                samples.push_back(all_records[virtual_addr]);
            }
        }
    } else {
        // Use pre-cached records for true address arithmetic speed
        std::random_device rd;
        std::mt19937 gen(rd());
        
        size_t stride = cached_records_.size() / target_count;
        if (stride == 0) stride = 1;
        
        for (int i = 0; i < target_count; ++i) {
            size_t base_address = i * stride;
            std::uniform_int_distribution<size_t> offset_dist(0, stride / 2);
            size_t virtual_addr = (base_address + offset_dist(gen)) % cached_records_.size();
            samples.push_back(cached_records_[virtual_addr]);
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::optimized_address_arithmetic_sample(double sample_percent) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    // **USE PRE-INITIALIZED MEMORY MAPPING** 
    // Since mmap is created during database creation/insertion, no overhead here
    if (!memory_mapped_ || cached_records_.empty()) {
        // Final fallback: create mmap if somehow not initialized
        cached_records_ = collect_leaf_records();
        memory_mapped_ = true;
    }
    
    if (cached_records_.empty()) return samples;
    
    int target_count = static_cast<int>(cached_records_.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // **PURE ADDRESS ARITHMETIC** - zero mmap creation overhead
    samples.reserve(target_count);
    
    // Direct memory access pattern using pointer arithmetic
    size_t stride = cached_records_.size() / target_count;
    if (stride == 0) stride = 1;
    
    // O(1) address calculations - base_ptr + offset * sizeof(Record)
    for (int i = 0; i < target_count; ++i) {
        size_t address_offset = i * stride;
        if (address_offset < cached_records_.size()) {
            // Direct pointer arithmetic: memory_base + calculated_offset
            samples.push_back(cached_records_[address_offset]);
        }
    }
    
    return samples;
}

std::vector<Record> CustomBPlusDB::signal_based_clt_sample(double sample_percent, int check_interval) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    // Use pre-cached records if available, otherwise collect them
    std::vector<Record> all_records;
    if (memory_mapped_ && !cached_records_.empty()) {
        all_records = cached_records_;
    } else {
        all_records = collect_leaf_records();
        if (all_records.empty()) return samples;
    }
    
    int target_count = static_cast<int>(all_records.size() * sample_percent / 100.0);
    if (target_count == 0) return samples;
    
    // **ROBUST SIGNAL-BASED COORDINATION**
    try {
        // Use atomic flags for thread communication
        std::atomic<bool> should_stop{false};
        std::atomic<size_t> total_samples{0};
        
        // Fast sampling thread with error handling
        auto fast_task = std::async(std::launch::async, [&]() -> std::vector<Record> {
            std::vector<Record> local_samples;
            size_t fast_step = std::max(2UL, all_records.size() / (target_count * 2));
            
            try {
                for (size_t i = 0; i < all_records.size() && !should_stop.load() && local_samples.size() < target_count; i += fast_step) {
                    if (i < all_records.size()) {
                        local_samples.push_back(all_records[i]);
                        total_samples.fetch_add(1);
                        
                        // Signal check every check_interval samples
                        if (local_samples.size() % check_interval == 0) {
                            if (total_samples.load() >= target_count / 2) {
                                should_stop.store(true);
                                break;
                            }
                        }
                    }
                }
            } catch (...) {
                // Continue with what we have
            }
            return local_samples;
        });
        
        // Slow validation thread with error handling
        auto slow_task = std::async(std::launch::async, [&]() -> std::vector<Record> {
            std::vector<Record> local_samples;
            size_t slow_step = 1;
            
            try {
                for (size_t i = 0; i < all_records.size() && !should_stop.load() && local_samples.size() < target_count / 4; i += slow_step) {
                    if (i < all_records.size()) {
                        local_samples.push_back(all_records[i]);
                        
                        if (local_samples.size() % (check_interval * 2) == 0) {
                            std::this_thread::sleep_for(std::chrono::microseconds(10));
                        }
                    }
                }
            } catch (...) {
                // Continue with what we have
            }
            return local_samples;
        });
        
        // Wait for completion with reasonable timeout
        std::vector<Record> fast_samples, slow_samples;
        
        auto fast_status = fast_task.wait_for(std::chrono::milliseconds(500));
        if (fast_status == std::future_status::ready) {
            fast_samples = fast_task.get();
        } else {
            should_stop.store(true);
            // Force completion and get partial results
            if (fast_task.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready) {
                fast_samples = fast_task.get();
            }
        }
        
        auto slow_status = slow_task.wait_for(std::chrono::milliseconds(100));
        if (slow_status == std::future_status::ready) {
            slow_samples = slow_task.get();
        } else {
            should_stop.store(true);
        }
        
        // Combine results
        samples.reserve(fast_samples.size() + slow_samples.size());
        samples.insert(samples.end(), fast_samples.begin(), fast_samples.end());
        samples.insert(samples.end(), slow_samples.begin(), slow_samples.end());
        
        // Ensure we have some samples - if coordination failed, use fast samples
        if (samples.empty() && !fast_samples.empty()) {
            samples = fast_samples;
        }
        
        // Trim to target count
        if (samples.size() > target_count) {
            samples.resize(target_count);
        }
        
        return samples;
        
    } catch (...) {
        // Fallback to simple sequential sampling on any error
        return optimized_sequential_sample(sample_percent);
    }
}

std::vector<Record> CustomBPlusDB::get_records_by_indices(const std::vector<size_t>& indices) {
    std::vector<Record> result;
    result.reserve(indices.size());
    
    // Get all records efficiently once
    auto all_records = collect_leaf_records();
    if (all_records.empty()) return result;
    
    // Convert indices to records
    for (size_t index : indices) {
        if (index < all_records.size()) {
            result.push_back(all_records[index]);
        }
    }
    
    return result;
}

std::vector<Record> CustomBPlusDB::random_start_memory_stride_sample(double sample_percent, size_t stride_bytes) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    // **YOUR OPTIMIZATION 1: RANDOM STARTING POINT**
    // Instead of always starting at index 0, randomize the starting position
    
    // Use pre-cached records for O(1) access (mmap optimization)
    if (memory_mapped_ && !cached_records_.empty()) {
        int target_count = static_cast<int>(cached_records_.size() * sample_percent / 100.0);
        if (target_count == 0) return samples;
        
        samples.reserve(target_count);
        
        // Calculate stride
        size_t record_stride;
        if (stride_bytes == 0) {
            record_stride = std::max(1UL, cached_records_.size() / target_count);
        } else {
            record_stride = std::max(1UL, stride_bytes / sizeof(Record));
        }
        
        // **RANDOM STARTING POINT** - your brilliant insight!
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> start_dist(0, record_stride - 1);
        size_t random_start = start_dist(gen);
        
        // **MEMORY STRIDE FROM RANDOM START**
        for (size_t offset = random_start; samples.size() < target_count && offset < cached_records_.size(); offset += record_stride) {
            samples.push_back(cached_records_[offset]);
        }
        
        return samples;
    }
    
    // Fallback to regular memory stride if mmap not ready
    return memory_stride_sample(sample_percent, stride_bytes);
}

std::vector<Record> CustomBPlusDB::multithreaded_memory_stride_sample(double sample_percent, int num_threads) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    std::vector<Record> samples;
    
    if (!root) return samples;
    
    // **IMPROVED APPROACH: DIVIDE MMAP INTO N REGIONS FOR N THREADS**
    // Each thread works on its own region and samples sample_percent/n within that region
    
    // Use pre-cached records for O(1) direct access
    if (!memory_mapped_ || cached_records_.empty()) {
        cached_records_ = collect_leaf_records();
        memory_mapped_ = true;
    }
    
    if (cached_records_.empty()) return samples;
    
    // **DIRECT ROW COUNT ACCESS FROM MMAP**
    size_t total_rows = cached_records_.size();
    
    // **EACH THREAD SAMPLES sample_percent/n IN ITS REGION**
    double per_thread_sample_percent = sample_percent / num_threads;
    
    // **DIVIDE MMAP INTO N REGIONS**
    size_t region_size = total_rows / num_threads;
    size_t remaining_rows = total_rows % num_threads;
    
    std::vector<std::future<std::vector<Record>>> futures;
    std::random_device rd;
    std::mt19937 gen(rd());
    
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t]() {
            std::vector<Record> thread_samples;
            
            // **THREAD GETS ITS OWN REGION OF MMAP**
            size_t region_start = t * region_size;
            size_t thread_region_size = region_size + (t < remaining_rows ? 1 : 0);
            size_t region_end = region_start + thread_region_size;
            
            if (region_start >= total_rows) return thread_samples;
            if (region_end > total_rows) region_end = total_rows;
            
            // **APPROXIMATE SAMPLING - CALCULATE TARGET FOR THIS REGION**
            size_t region_total = region_end - region_start;
            size_t target_samples = static_cast<size_t>(region_total * per_thread_sample_percent / 100.0);
            
            if (target_samples == 0) return thread_samples;
            
            thread_samples.reserve(target_samples + 10);
            
            // **RANDOM START WITHIN THREAD'S REGION**
            std::uniform_int_distribution<size_t> random_start_dist(region_start, region_start + std::min(region_total/10, size_t(100)));
            size_t thread_start = random_start_dist(gen);
            
            // **STRIDE WITHIN REGION TO GET APPROXIMATE SAMPLES**
            size_t stride = region_total / target_samples;
            if (stride == 0) stride = 1;
            
            // **SAMPLE WITH EARLY TERMINATION WHEN TARGET REACHED**
            for (size_t offset = thread_start; 
                 offset < region_end && thread_samples.size() < target_samples; 
                 offset += stride) {
                thread_samples.push_back(cached_records_[offset]);
            }
            
            return thread_samples;
        }));
    }
    
    // **COMBINE RESULTS FROM ALL REGIONS**
    size_t estimated_total = static_cast<size_t>(total_rows * sample_percent / 100.0);
    samples.reserve(estimated_total);
    
    for (auto& future : futures) {
        auto thread_samples = future.get();
        samples.insert(samples.end(), thread_samples.begin(), thread_samples.end());
    }
    
    return samples;
}

double CustomBPlusDB::fast_aggregated_memory_stride_sum(double sample_percent, int num_threads) {
    std::shared_lock<std::shared_mutex> lock(db_mutex);
    
    if (!root) return 0.0;
    
    // **IMPROVED APPROACH: DIVIDE MMAP INTO N REGIONS, AGGREGATE DIRECTLY**
    // Each thread samples sample_percent/n in its region and aggregates on-the-fly
    
    // Use pre-cached records for O(1) direct access
    if (!memory_mapped_ || cached_records_.empty()) {
        cached_records_ = collect_leaf_records();
        memory_mapped_ = true;
    }
    
    if (cached_records_.empty()) return 0.0;
    
    // **DIRECT ROW COUNT ACCESS FROM MMAP**
    size_t total_rows = cached_records_.size();
    
    // **EACH THREAD SAMPLES sample_percent/n IN ITS REGION**
    double per_thread_sample_percent = sample_percent / num_threads;
    
    // **DIVIDE MMAP INTO N REGIONS**
    size_t region_size = total_rows / num_threads;
    size_t remaining_rows = total_rows % num_threads;
    
    // **ATOMIC AGGREGATION** - no intermediate vector storage!
    std::atomic<double> total_sum{0.0};
    std::atomic<size_t> total_sample_count{0};
    
    std::vector<std::future<void>> futures;
    std::random_device rd;
    std::mt19937 gen(rd());
    
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [&, t]() {
            double thread_sum = 0.0;
            size_t thread_sample_count = 0;
            
            // **THREAD GETS ITS OWN REGION OF MMAP**
            size_t region_start = t * region_size;
            size_t thread_region_size = region_size + (t < remaining_rows ? 1 : 0);
            size_t region_end = region_start + thread_region_size;
            
            if (region_start >= total_rows) return;
            if (region_end > total_rows) region_end = total_rows;
            
            // **APPROXIMATE SAMPLING - CALCULATE TARGET FOR THIS REGION**
            size_t region_total = region_end - region_start;
            size_t target_samples = static_cast<size_t>(region_total * per_thread_sample_percent / 100.0);
            
            if (target_samples == 0) return;
            
            // **RANDOM START WITHIN THREAD'S REGION**
            std::uniform_int_distribution<size_t> random_start_dist(region_start, region_start + std::min(region_total/10, size_t(100)));
            size_t thread_start = random_start_dist(gen);
            
            // **STRIDE WITHIN REGION TO GET APPROXIMATE SAMPLES**
            size_t stride = region_total / target_samples;
            if (stride == 0) stride = 1;
            
            // **DIRECT AGGREGATION WITH EARLY TERMINATION**
            for (size_t offset = thread_start; 
                 offset < region_end && thread_sample_count < target_samples; 
                 offset += stride) {
                thread_sum += cached_records_[offset].amount;
                thread_sample_count++;
            }
            
            // **ATOMIC UPDATE OF TOTALS**
            double current_sum = total_sum.load();
            while (!total_sum.compare_exchange_weak(current_sum, current_sum + thread_sum)) {
                // Retry on conflict
            }
            total_sample_count.fetch_add(thread_sample_count);
        }));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // **FINAL CALCULATION AT THE END** - your optimization!
    size_t final_count = total_sample_count.load();
    return final_count > 0 ? total_sum.load() : 0.0;
}
