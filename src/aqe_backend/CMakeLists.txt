cmake_minimum_required(VERSION 3.15)
project(aqe_backend)

find_package(pybind11 CONFIG REQUIRED)

add_library(db STATIC core/db.cpp)
target_include_directories(db PUBLIC core)

pybind11_add_module(aqe_backend bindings.cpp)
target_link_libraries(aqe_backend PRIVATE db sqlite3)