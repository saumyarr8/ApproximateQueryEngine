#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "executor.h"

namespace py = pybind11;

PYBIND11_MODULE(aqe_backend, m) {
    m.doc() = "Approximate Query Engine backend with pybind11";

    m.def("run_query", &execute_query,
          py::arg("sql_query"),
          py::arg("db_path"),
          py::arg("sample_percent")=0,
          "Execute SQL-like query with optional approximate sampling");

    m.def("run_query_groupby", &execute_query_groupby,
          py::arg("sql_query"),
          py::arg("db_path"),
          py::arg("sample_percent")=0,
          py::arg("num_threads")=4,
          "Execute SQL-like GROUP BY query with parallel approximate aggregation");
}