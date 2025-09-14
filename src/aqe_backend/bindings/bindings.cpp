#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include "../core/custom_scheduler.hpp"
#include "../core/custom_bplus_db.hpp"
#include "../executor.h"

namespace py = pybind11;

PYBIND11_MODULE(aqe_backend, m) {
    m.doc() = "ApproximateQueryEngine: Custom B+ Tree Database with Block Sampling";

    // Expose Record struct
    py::class_<Record>(m, "Record")
        .def(py::init<>())
        .def_readwrite("id", &Record::id)
        .def_readwrite("amount", &Record::amount)
        .def_readwrite("region", &Record::region)
        .def_readwrite("product_id", &Record::product_id)
        .def_readwrite("timestamp", &Record::timestamp);
    
    py::enum_<CustomApproximationStatus>(m, "CustomApproximationStatus")
        .value("STABLE", CustomApproximationStatus::STABLE)
        .value("DRIFTING", CustomApproximationStatus::DRIFTING)
        .value("INSUFFICIENT_DATA", CustomApproximationStatus::INSUFFICIENT_DATA)
        .value("ERROR", CustomApproximationStatus::ERROR);
    
    py::class_<CustomValidationResult>(m, "CustomValidationResult")
        .def_readonly("value", &CustomValidationResult::value)
        .def_readonly("status", &CustomValidationResult::status)
        .def_readonly("confidence_level", &CustomValidationResult::confidence_level)
        .def_readonly("error_margin", &CustomValidationResult::error_margin)
        .def_readonly("samples_used", &CustomValidationResult::samples_used)
        .def_readonly("computation_time", &CustomValidationResult::computation_time);

    // Expose QueryResult for confidence intervals
    py::class_<QueryResult>(m, "QueryResult")
        .def_readonly("value", &QueryResult::value)
        .def_readonly("ci_lower", &QueryResult::ci_lower)
        .def_readonly("ci_upper", &QueryResult::ci_upper);

    py::class_<CustomBPlusDB>(m, "CustomBPlusDB")
        .def(py::init<>())
        .def("create_database", &CustomBPlusDB::create_database)
        .def("open_database", &CustomBPlusDB::open_database)
        .def("close_database", &CustomBPlusDB::close_database)
        .def("insert_record", &CustomBPlusDB::insert_record)
        .def("sum_amount", &CustomBPlusDB::sum_amount)
        .def("sum_amount_where", &CustomBPlusDB::sum_amount_where)
        .def("sample_records", &CustomBPlusDB::sample_records)
        .def("optimized_sequential_sample", &CustomBPlusDB::optimized_sequential_sample)
        .def("get_total_records", &CustomBPlusDB::get_total_records)
        .def("get_node_count", &CustomBPlusDB::get_node_count)
        .def("save_to_file", &CustomBPlusDB::save_to_file)
        .def("load_from_file", &CustomBPlusDB::load_from_file)
        .def("fast_pointer_sample", &CustomBPlusDB::fast_pointer_sample, 
             py::arg("sample_percent"), py::arg("step_size") = 2)
        .def("slow_pointer_sample", &CustomBPlusDB::slow_pointer_sample)
        .def("dual_pointer_sample", &CustomBPlusDB::dual_pointer_sample)
        .def("parallel_pointer_sample", &CustomBPlusDB::parallel_pointer_sample,
             py::arg("sample_percent"), py::arg("num_threads") = 4)
        .def("random_pointer_sample", &CustomBPlusDB::random_pointer_sample,
             py::arg("sample_percent"), py::arg("seed") = 42)
        .def("clt_validated_dual_pointer_sample", &CustomBPlusDB::clt_validated_dual_pointer_sample,
             py::arg("sample_percent"), py::arg("confidence_level") = 0.95, 
             py::arg("check_interval") = 10, py::arg("num_threads") = 4, 
             py::arg("max_error_percent") = 2.0)
        .def("optimized_clt_sample", &CustomBPlusDB::optimized_clt_sample,
             py::arg("sample_percent"), py::arg("confidence_level") = 0.95,
             py::arg("check_interval") = 20, py::arg("num_threads") = 4,
             py::arg("max_error_percent") = 2.0)
        .def("block_sample", &CustomBPlusDB::block_sample,
             py::arg("sample_percent"), py::arg("block_size") = 1000)
        .def("page_sample", &CustomBPlusDB::page_sample,
             py::arg("sample_percent"), py::arg("page_size") = 4096)
        .def("parallel_block_sample", &CustomBPlusDB::parallel_block_sample,
             py::arg("sample_percent"), py::arg("block_size") = 1000, py::arg("num_threads") = 4)
        .def("adaptive_block_sample", &CustomBPlusDB::adaptive_block_sample,
             py::arg("sample_percent"), py::arg("min_block_size") = 500, py::arg("max_block_size") = 2000)
        .def("stratified_block_sample", &CustomBPlusDB::stratified_block_sample,
             py::arg("sample_percent"), py::arg("block_size") = 1000, py::arg("strata_count") = 4)
        .def("index_based_sample", &CustomBPlusDB::index_based_sample)
        .def("node_skip_sample", &CustomBPlusDB::node_skip_sample,
             py::arg("sample_percent"), py::arg("skip_factor") = 2)
        .def("balanced_tree_sample", &CustomBPlusDB::balanced_tree_sample)
        .def("direct_access_sample", &CustomBPlusDB::direct_access_sample)
        .def("byte_offset_sample", &CustomBPlusDB::byte_offset_sample)
        .def("random_start_nth_sample", &CustomBPlusDB::random_start_nth_sample,
             py::arg("sample_percent"), py::arg("nth") = 10)
        .def("memory_stride_sample", &CustomBPlusDB::memory_stride_sample,
             py::arg("sample_percent"), py::arg("stride_bytes") = 0)
        .def("address_arithmetic_sample", &CustomBPlusDB::address_arithmetic_sample)
        .def("optimized_address_arithmetic_sample", &CustomBPlusDB::optimized_address_arithmetic_sample)
        .def("random_start_memory_stride_sample", &CustomBPlusDB::random_start_memory_stride_sample,
             py::arg("sample_percent"), py::arg("stride_bytes") = 0)
        .def("multithreaded_memory_stride_sample", &CustomBPlusDB::multithreaded_memory_stride_sample,
             py::arg("sample_percent"), py::arg("num_threads") = 4)
        .def("fast_aggregated_memory_stride_sum", &CustomBPlusDB::fast_aggregated_memory_stride_sum,
             py::arg("sample_percent"), py::arg("num_threads") = 4)
        .def("signal_based_clt_sample", &CustomBPlusDB::signal_based_clt_sample,
             py::arg("sample_percent"), py::arg("check_interval") = 10);

    py::class_<CustomApproximateScheduler>(m, "CustomApproximateScheduler")
        .def(py::init<double>(), py::arg("error_threshold") = 0.05)
        .def("create_database", &CustomApproximateScheduler::create_database)
        .def("open_database", &CustomApproximateScheduler::open_database)
        .def("close_database", &CustomApproximateScheduler::close_database)
        .def("insert_record", &CustomApproximateScheduler::insert_record)
        .def("insert_batch", &CustomApproximateScheduler::insert_batch)
        .def("execute_sum_query", &CustomApproximateScheduler::execute_sum_query,
             py::arg("query"), py::arg("sample_percent") = 10.0, py::arg("num_threads") = 4)
        .def("execute_avg_query", &CustomApproximateScheduler::execute_avg_query,
             py::arg("query"), py::arg("sample_percent") = 10.0, py::arg("num_threads") = 4)
        .def("execute_count_query", &CustomApproximateScheduler::execute_count_query,
             py::arg("query"), py::arg("sample_percent") = 10.0, py::arg("num_threads") = 4)
        .def("execute_exact_sum", &CustomApproximateScheduler::execute_exact_sum)
        .def("execute_exact_avg", &CustomApproximateScheduler::execute_exact_avg)
        .def("execute_exact_count", &CustomApproximateScheduler::execute_exact_count)
        .def("benchmark_query", &CustomApproximateScheduler::benchmark_query,
             py::arg("query_type"), py::arg("sample_percent") = 10.0, py::arg("num_threads") = 4)
        .def("get_total_records", &CustomApproximateScheduler::get_total_records)
        .def("get_tree_height", &CustomApproximateScheduler::get_tree_height)
        .def("get_database_size_mb", &CustomApproximateScheduler::get_database_size_mb);

    // Expose executor functions for SQL query processing with sampling and scaling
    m.def("run_query", &execute_query, "Execute SQL query with sampling and automatic scaling",
          py::arg("sql_query"), py::arg("db_path"), py::arg("sample_percent") = 0);
    
    m.def("run_query_groupby", &execute_query_groupby, "Execute GROUP BY query with sampling",
          py::arg("sql_query"), py::arg("db_path"), py::arg("sample_percent") = 0, py::arg("num_threads") = 4);
    
    m.def("run_query_with_ci", &execute_query_with_ci, "Execute query with confidence intervals",
          py::arg("sql_query"), py::arg("db_path"), py::arg("sample_percent") = 0);
    
    m.def("run_query_groupby_with_ci", &execute_query_groupby_with_ci, "Execute GROUP BY query with confidence intervals",
          py::arg("sql_query"), py::arg("db_path"), py::arg("sample_percent") = 0, py::arg("num_threads") = 4);
}