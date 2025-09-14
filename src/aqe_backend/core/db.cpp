#include "db.hpp"
#include <sqlite3.h>
#include <stdexcept>

// Callback function for SQLite queries
static int query_callback(void* data, int argc, char** argv, char** azColName) {
    auto* rows = static_cast<std::vector<std::vector<std::string>>*>(data);
    std::vector<std::string> row;
    
    for (int i = 0; i < argc; i++) {
        row.push_back(argv[i] ? argv[i] : "NULL");
    }
    
    rows->push_back(row);
    return 0;
}

DB::DB(const std::string& db_path) {
    if (sqlite3_open(db_path.c_str(), &db)) {
        std::string error = sqlite3_errmsg(db);
        sqlite3_close(db);
        throw std::runtime_error("Cannot open database: " + error);
    }
}

DB::~DB() {
    if (db) {
        sqlite3_close(db);
        db = nullptr;
    }
}

std::vector<std::vector<std::string>> DB::execute_query(const std::string& query) {
    std::vector<std::vector<std::string>> results;
    char* errMsg = nullptr;
    
    int rc = sqlite3_exec(db, query.c_str(), query_callback, &results, &errMsg);
    
    if (rc != SQLITE_OK) {
        std::string error = errMsg;
        sqlite3_free(errMsg);
        throw std::runtime_error("SQL error: " + error);
    }
    
    return results;
}

double DB::execute_sum(const std::string& table, const std::string& column) {
    std::string query = "SELECT SUM(" + column + ") FROM " + table;
    auto results = execute_query(query);
    
    if (results.empty() || results[0].empty() || results[0][0] == "NULL") {
        return 0.0;
    }
    
    return std::stod(results[0][0]);
}

double DB::execute_count(const std::string& table, const std::string& column) {
    std::string query = "SELECT COUNT(" + column + ") FROM " + table;
    auto results = execute_query(query);
    
    if (results.empty() || results[0].empty()) {
        return 0.0;
    }
    
    return std::stod(results[0][0]);
}

double DB::execute_avg(const std::string& table, const std::string& column) {
    std::string query = "SELECT AVG(" + column + ") FROM " + table;
    auto results = execute_query(query);
    
    if (results.empty() || results[0].empty() || results[0][0] == "NULL") {
        return 0.0;
    }
    
    return std::stod(results[0][0]);
}