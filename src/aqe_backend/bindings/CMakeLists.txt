cmake_minimum_required(VERSION 3.15)
project(aqe_backend)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

# Include only the core source files that have content
set(CORE_SOURCES
    ../core/db.cpp
)

pybind11_add_module(aqe_backend bindings.cpp ${CORE_SOURCES})

# Link with SQLite3
target_link_libraries(aqe_backend PRIVATE ${SQLITE3_LIBRARIES})
target_include_directories(aqe_backend PRIVATE ${SQLITE3_INCLUDE_DIRS})
target_compile_options(aqe_backend PRIVATE ${SQLITE3_CFLAGS_OTHER})