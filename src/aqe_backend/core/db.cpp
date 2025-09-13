#include "db.hpp"
#include <sqlite3.h>
#include <stdexcept>

DB::DB(const std::string& path) {
    if (sqlite3_open(path.c_str(), (sqlite3**)&db) != SQLITE_OK) {
        throw std::runtime_error("Failed to open database");
    }
}

DB::~DB() {
    if (db) sqlite3_close((sqlite3*)db);
}

double DB::execute_sum(const std::string& table, const std::string& column) {
    std::string sql = "SELECT SUM(" + column + ") FROM " + table + ";";
    sqlite3_stmt* stmt;
    double result = 0.0;

    if (sqlite3_prepare_v2((sqlite3*)db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare query");
    }

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        result = sqlite3_column_double(stmt, 0);
    }

    sqlite3_finalize(stmt);
    return result;
}

int DB::execute_count(const std::string& table, const std::string& column) {
    std::string sql = "SELECT COUNT(" + column + ") FROM " + table + ";";
    sqlite3_stmt* stmt;
    int result = 0;

    if (sqlite3_prepare_v2((sqlite3*)db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare query");
    }

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        result = sqlite3_column_int(stmt, 0);
    }

    sqlite3_finalize(stmt);
    return result;
}

double DB::execute_avg(const std::string& table, const std::string& column) {
    std::string sql = "SELECT AVG(" + column + ") FROM " + table + ";";
    sqlite3_stmt* stmt;
    double result = 0.0;

    if (sqlite3_prepare_v2((sqlite3*)db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare query");
    }

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        result = sqlite3_column_double(stmt, 0);
    }

    sqlite3_finalize(stmt);
    return result;
}