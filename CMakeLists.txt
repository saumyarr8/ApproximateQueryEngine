cmake_minimum_required(VERSION 3.12)
project(ApproximateQueryEngine)

set(CMAKE_CXX_STANDARD 17)

find_package(pybind11 REQUIRED)

add_subdirectory(src/aqe_backend)