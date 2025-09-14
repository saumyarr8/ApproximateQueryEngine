#include "direct_reader.hpp"
#include <iostream>
#include <algorithm>
#include <random>
#include <thread>
#include <mutex>
#include <future>
#include <cstring>

DirectDBReader::DirectDBReader(const std::string& db_path) 
    : db_path_(db_path), page_size_(SQLITE_PAGE_SIZE), page_count_(0), 
      first_freelist_page_(0), file_size_(0), table_root_page_(0) {
}

DirectDBReader::~DirectDBReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool DirectDBReader::initialize() {
    file_.open(db_path_, std::ios::binary);
    if (!file_.is_open()) {
        std::cerr << "Failed to open database file: " << db_path_ << std::endl;
        return false;
    }
    
    // Get file size
    file_.seekg(0, std::ios::end);
    file_size_ = file_.tellg();
    file_.seekg(0, std::ios::beg);
    
    if (!read_file_header()) {
        std::cerr << "Failed to read SQLite file header" << std::endl;
        return false;
    }
    
    if (!find_table_schema()) {
        std::cerr << "Failed to find table schema" << std::endl;
        return false;
    }
    
    std::cout << "📊 DirectDBReader initialized:" << std::endl;
    std::cout << "   File size: " << (file_size_ / 1024 / 1024) << " MB" << std::endl;
    std::cout << "   Page size: " << page_size_ << " bytes" << std::endl;
    std::cout << "   Page count: " << page_count_ << std::endl;
    std::cout << "   Table root page: " << table_root_page_ << std::endl;
    
    return true;
}

bool DirectDBReader::read_file_header() {
    uint8_t header[SQLITE_HEADER_SIZE];
    if (!read_bytes(0, header, SQLITE_HEADER_SIZE)) {
        return false;
    }
    
    // Check SQLite magic string
    if (std::memcmp(header, "SQLite format 3\000", 16) != 0) {
        std::cerr << "Not a valid SQLite database file" << std::endl;
        return false;
    }
    
    // Read page size (bytes 16-17)
    page_size_ = (header[16] << 8) | header[17];
    if (page_size_ == 1) page_size_ = 65536; // Special case
    
    // Read page count (bytes 28-31)
    page_count_ = (header[28] << 24) | (header[29] << 16) | (header[30] << 8) | header[31];
    
    // Read first freelist page (bytes 32-35)
    first_freelist_page_ = (header[32] << 24) | (header[33] << 16) | (header[34] << 8) | header[35];
    
    return true;
}

bool DirectDBReader::find_table_schema() {
    // For simplicity, assume we know the table structure and it's on page 2
    // In a real implementation, we'd parse the schema from the sqlite_master table
    table_root_page_ = 2;
    
    // Assume our table has: id INTEGER, amount REAL, region INTEGER, product_id INTEGER, timestamp INTEGER
    column_types_ = {1, 2, 1, 1, 1}; // 1=INTEGER, 2=REAL
    
    return true;
}

DirectDBReader::PageInfo DirectDBReader::read_page_header(uint32_t page_number) {
    PageInfo info;
    info.page_number = page_number;
    info.file_offset = (page_number - 1) * page_size_;
    
    uint8_t page_header[12];
    if (!read_bytes(info.file_offset, page_header, 12)) {
        info.cell_count = 0;
        return info;
    }
    
    // Page type (byte 0): 13 = leaf table page, 5 = interior table page
    uint8_t page_type = page_header[0];
    
    // Cell count (bytes 3-4)
    info.cell_count = (page_header[3] << 8) | page_header[4];
    
    // Read cell pointer array
    info.cell_offsets.resize(info.cell_count);
    for (uint16_t i = 0; i < info.cell_count; i++) {
        uint64_t cell_ptr_offset = info.file_offset + 12 + (i * 2);
        uint8_t cell_ptr[2];
        if (read_bytes(cell_ptr_offset, cell_ptr, 2)) {
            info.cell_offsets[i] = (cell_ptr[0] << 8) | cell_ptr[1];
        }
    }
    
    return info;
}

std::vector<DirectDBReader::Record> DirectDBReader::parse_page_records(uint32_t page_number) {
    std::vector<Record> records;
    PageInfo page_info = read_page_header(page_number);
    
    for (uint16_t cell_offset : page_info.cell_offsets) {
        uint64_t cell_address = page_info.file_offset + cell_offset;
        
        // Read cell header to determine size
        uint8_t cell_header[9]; // Max varint size
        if (!read_bytes(cell_address, cell_header, 9)) {
            continue;
        }
        
        // Parse varint for payload size
        auto [payload_size, header_size] = read_varint(cell_header);
        
        // Read the full cell
        std::vector<uint8_t> cell_data(header_size + payload_size);
        if (!read_bytes(cell_address, cell_data.data(), cell_data.size())) {
            continue;
        }
        
        // Parse record from cell
        Record record = parse_record_from_cell(cell_data.data() + header_size, payload_size);
        if (record.id > 0) { // Valid record
            records.push_back(record);
        }
    }
    
    return records;
}

DirectDBReader::Record DirectDBReader::parse_record_from_cell(const uint8_t* cell_data, size_t cell_size) {
    Record record = {0, 0.0, 0, 0, 0};
    
    if (cell_size < 10) return record; // Too small for our record format
    
    size_t offset = 0;
    
    // Parse header length varint
    auto [header_length, header_varint_size] = read_varint(cell_data + offset);
    offset += header_varint_size;
    
    // Parse column type varints
    std::vector<uint64_t> column_types;
    while (offset < header_length) {
        auto [type, type_size] = read_varint(cell_data + offset);
        column_types.push_back(type);
        offset += type_size;
    }
    
    // Parse column data
    if (column_types.size() >= 5) {
        // ID (INTEGER)
        if (column_types[0] == 1) {
            record.id = cell_data[offset];
            offset += 1;
        } else if (column_types[0] == 2) {
            int16_t val = (cell_data[offset] << 8) | cell_data[offset + 1];
            record.id = val;
            offset += 2;
        } else if (column_types[0] == 3) {
            int32_t val = (cell_data[offset] << 16) | (cell_data[offset + 1] << 8) | cell_data[offset + 2];
            record.id = val;
            offset += 3;
        } else if (column_types[0] == 4) {
            int32_t val = (cell_data[offset] << 24) | (cell_data[offset + 1] << 16) | 
                         (cell_data[offset + 2] << 8) | cell_data[offset + 3];
            record.id = val;
            offset += 4;
        }
        
        // Amount (REAL - 8 bytes)
        if (column_types[1] == 7 && offset + 8 <= cell_size) {
            uint64_t bits = 0;
            for (int i = 0; i < 8; i++) {
                bits = (bits << 8) | cell_data[offset + i];
            }
            record.amount = *reinterpret_cast<double*>(&bits);
            offset += 8;
        }
        
        // Region (INTEGER)
        if (column_types[2] == 1 && offset < cell_size) {
            record.region = cell_data[offset];
            offset += 1;
        } else if (column_types[2] == 4 && offset + 4 <= cell_size) {
            record.region = (cell_data[offset] << 24) | (cell_data[offset + 1] << 16) | 
                           (cell_data[offset + 2] << 8) | cell_data[offset + 3];
            offset += 4;
        }
        
        // Product ID and Timestamp (similar parsing)
        // Simplified for demo - assume 4-byte integers
        if (offset + 8 <= cell_size) {
            record.product_id = (cell_data[offset] << 24) | (cell_data[offset + 1] << 16) | 
                               (cell_data[offset + 2] << 8) | cell_data[offset + 3];
            offset += 4;
            
            record.timestamp = (cell_data[offset] << 24) | (cell_data[offset + 1] << 16) | 
                              (cell_data[offset + 2] << 8) | cell_data[offset + 3];
        }
    }
    
    return record;
}

std::pair<uint64_t, size_t> DirectDBReader::read_varint(const uint8_t* data) {
    uint64_t result = 0;
    size_t bytes_read = 0;
    
    for (int i = 0; i < 9; i++) {
        uint8_t byte = data[i];
        bytes_read++;
        
        if (i == 8) {
            result = (result << 8) | byte;
            break;
        } else {
            result = (result << 7) | (byte & 0x7F);
            if ((byte & 0x80) == 0) {
                break;
            }
        }
    }
    
    return {result, bytes_read};
}

bool DirectDBReader::read_bytes(uint64_t offset, uint8_t* buffer, size_t size) {
    if (offset + size > file_size_) {
        return false;
    }
    
    file_.seekg(offset);
    file_.read(reinterpret_cast<char*>(buffer), size);
    return file_.good();
}

size_t DirectDBReader::get_estimated_record_count() const {
    // Estimate based on file size and average record size
    size_t avg_record_size = 32; // Rough estimate for our schema
    size_t data_pages = page_count_ - 1; // Exclude header page
    size_t usable_page_size = page_size_ - 12; // Exclude page header
    return (data_pages * usable_page_size) / avg_record_size;
}

std::vector<DirectDBReader::Record> DirectDBReader::sample_records_direct(double sample_percent) {
    std::vector<Record> sampled_records;
    
    // Calculate number of pages to sample
    uint32_t total_data_pages = page_count_ - 1; // Exclude page 1 (header)
    uint32_t pages_to_sample = std::max(1u, static_cast<uint32_t>(total_data_pages * sample_percent / 100.0));
    
    // Generate random page numbers to sample
    std::vector<uint32_t> page_numbers;
    for (uint32_t i = 2; i <= page_count_; i++) { // Start from page 2
        page_numbers.push_back(i);
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(page_numbers.begin(), page_numbers.end(), gen);
    
    // Sample the selected pages
    for (uint32_t i = 0; i < pages_to_sample && i < page_numbers.size(); i++) {
        auto page_records = parse_page_records(page_numbers[i]);
        sampled_records.insert(sampled_records.end(), page_records.begin(), page_records.end());
    }
    
    return sampled_records;
}

double DirectDBReader::parallel_sum_sampling(const std::string& column, double sample_percent, int num_threads) {
    auto records = sample_records_direct(sample_percent);
    
    if (records.empty()) return 0.0;
    
    // Divide records among threads
    size_t records_per_thread = records.size() / num_threads;
    std::vector<std::future<double>> futures;
    
    for (int t = 0; t < num_threads; t++) {
        size_t start_idx = t * records_per_thread;
        size_t end_idx = (t == num_threads - 1) ? records.size() : (t + 1) * records_per_thread;
        
        futures.push_back(std::async(std::launch::async, [&records, start_idx, end_idx, column]() {
            double thread_sum = 0.0;
            for (size_t i = start_idx; i < end_idx; i++) {
                if (column == "amount") {
                    thread_sum += records[i].amount;
                } else if (column == "id") {
                    thread_sum += records[i].id;
                }
                // Add other columns as needed
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

double DirectDBReader::parallel_avg_sampling(const std::string& column, double sample_percent, int num_threads) {
    double sum = parallel_sum_sampling(column, sample_percent, num_threads);
    size_t estimated_count = get_estimated_record_count();
    return estimated_count > 0 ? sum / estimated_count : 0.0;
}

size_t DirectDBReader::parallel_count_sampling(double sample_percent, int num_threads) {
    auto records = sample_records_direct(sample_percent);
    // Scale up based on sampling percentage
    return static_cast<size_t>(records.size() * (100.0 / sample_percent));
}

size_t DirectDBReader::get_file_size() const {
    return file_size_;
}

uint32_t DirectDBReader::get_page_count() const {
    return page_count_;
}