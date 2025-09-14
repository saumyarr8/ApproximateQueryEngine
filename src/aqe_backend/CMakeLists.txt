cmake_minimum_required(VERSION 3.12)
project(aqe_backend)

set(CMAKE_CXX_STANDARD 17)

find_package(pybind11 REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

pybind11_add_module(aqe_backend 
    bindings/bindings.cpp
    core/custom_bplus_db.cpp
    core/custom_scheduler.cpp
    core/db.cpp
    executor.cpp
    parser.cpp
)

target_include_directories(aqe_backend PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/bindings
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${SQLITE3_INCLUDE_DIRS}
)

target_link_libraries(aqe_backend PRIVATE pthread ${SQLITE3_LIBRARIES})
target_compile_options(aqe_backend PRIVATE ${SQLITE3_CFLAGS_OTHER})