cmake_minimum_required(VERSION 3.12)
project(aqe_backend)

set(CMAKE_CXX_STANDARD 17)
find_package(pybind11 REQUIRED)
find_package(SQLite3 REQUIRED)

pybind11_add_module(aqe_backend
    src/aqe_backend/aqe_backend.cpp
    src/aqe_backend/parser.cpp
    src/aqe_backend/executor.cpp
)
target_link_libraries(aqe_backend PRIVATE SQLite::SQLite3)