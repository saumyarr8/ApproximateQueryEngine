#pragma once
#include <string>

class DB {
public:
    DB(const std::string& path);
    ~DB();
    double execute_sum(const std::string& table, const std::string& column);
    int execute_count(const std::string& table, const std::string& column);
    double execute_avg(const std::string& table, const std::string& column);

private:
    void* db; // sqlite3*
};