#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include <cstdint>

/**
 * Direct SQLite file format reader that bypasses the SQLite engine
 * and reads B-tree pages directly from the database file.
 * 
 * This enables true parallel processing by reading different file regions
 * in parallel threads without SQLite's single-threaded constraints.
 */
class DirectDBReader {
public:
    struct Record {
        int64_t id;
        double amount;
        int32_t region;
        int32_t product_id;
        int64_t timestamp;
    };
    
    struct PageInfo {
        uint32_t page_number;
        uint64_t file_offset;
        uint16_t cell_count;
        std::vector<uint16_t> cell_offsets;
    };
    
    explicit DirectDBReader(const std::string& db_path);
    ~DirectDBReader();
    
    // Initialize and read database metadata
    bool initialize();
    
    // Get total record count without full scan
    size_t get_estimated_record_count() const;
    
    // Read records from specific page range (for parallel processing)
    std::vector<Record> read_page_range(uint32_t start_page, uint32_t end_page);
    
    // Parallel sampling methods
    double parallel_sum_sampling(const std::string& column, double sample_percent, int num_threads);
    double parallel_avg_sampling(const std::string& column, double sample_percent, int num_threads);
    size_t parallel_count_sampling(double sample_percent, int num_threads);
    
    // Direct file-level operations
    std::vector<Record> sample_records_direct(double sample_percent);
    
    // Get database file size and page count
    size_t get_file_size() const;
    uint32_t get_page_count() const;
    
private:
    std::string db_path_;
    std::ifstream file_;
    
    // SQLite file format constants
    static const uint16_t SQLITE_PAGE_SIZE = 4096;
    static const uint16_t SQLITE_HEADER_SIZE = 100;
    
    // Database metadata
    uint32_t page_size_;
    uint32_t page_count_;
    uint32_t first_freelist_page_;
    size_t file_size_;
    
    // Schema information
    int64_t table_root_page_;
    std::vector<int> column_types_;
    
    // Internal methods
    bool read_file_header();
    bool find_table_schema();
    PageInfo read_page_header(uint32_t page_number);
    std::vector<Record> parse_page_records(uint32_t page_number);
    Record parse_record_from_cell(const uint8_t* cell_data, size_t cell_size);
    
    // Low-level file operations
    bool read_bytes(uint64_t offset, uint8_t* buffer, size_t size);
    uint32_t read_uint32_be(uint64_t offset);
    uint16_t read_uint16_be(uint64_t offset);
    uint8_t read_uint8(uint64_t offset);
    
    // Varint decoding (SQLite format)
    std::pair<uint64_t, size_t> read_varint(const uint8_t* data);
    
    // Thread-safe page reading
    std::vector<Record> read_page_thread_safe(uint32_t page_number);
};