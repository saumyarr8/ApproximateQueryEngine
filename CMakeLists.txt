cmake_minimum_required(VERSION 3.15)
project(aqe_project)

# Fetch pybind11
include(FetchContent)
FetchContent_Declare(
    pybind11
    GIT_REPOSITORY https://github.com/pybind/pybind11
    GIT_TAG v2.10.4
)
FetchContent_MakeAvailable(pybind11)

add_subdirectory(src/aqe_backend/bindings)