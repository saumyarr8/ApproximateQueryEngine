#pragma once
#include <string>
#include <vector>
#include <sqlite3.h>

class DB {
public:
    DB(const std::string& db_path);
    ~DB();

    // Add this method
    std::vector<std::vector<std::string>> execute_query(const std::string& query);
    
    // Existing methods
    double execute_sum(const std::string& table, const std::string& column);
    double execute_count(const std::string& table, const std::string& column);
    double execute_avg(const std::string& table, const std::string& column);

private:
    sqlite3* db;
};