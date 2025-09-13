#include <pybind11/pybind11.h>
#include "../core/db.hpp"

namespace py = pybind11;

PYBIND11_MODULE(aqe_backend, m) {
    py::class_<DB>(m, "DB")
        .def(py::init<const std::string&>())
        .def("execute_sum", &DB::execute_sum)
        .def("execute_count", &DB::execute_count)
        .def("execute_avg", &DB::execute_avg);
}