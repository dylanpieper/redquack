# redquack 0.3.0

## New Features
* Interface redesign with new convience functions:
  - `use_duckdb()` and `close_duckdb()` for database connection management
  - `remove_duckdb()` for cleanup and file removal
  - `tbl_redcap()` for creating table references with dplyr support
  - `collect_labeled()` and `collect_labeled_list()` for data with REDCap labels
  - `collect_list()` for splitting data into instrument-specific tables
  - `list_to_env()` for loading instruments into the global environment
  - `metadata()` and `logs()` for accessing stored metadata and transfer logs
  - `inspect()` for examining table structure for project data
  - `save_parquet()` for efficient data export
* Add REDCap audit logs (`redcap_log()`) with configurable date ranges

## Lifecycle Changes
* Rename `logs()` to `transfer_log()`
* Rename `log_table_name` parameter to `transfer_log_table_name` in `redcap_to_db()`
* Rename `redcap_uri` parameter to `url` in `redcap_to_db()`
* Rename `verbose` to `echo` in `redcap_to_db()` and change from logical to string options: "all" (default), "progress", or "none"
* Add `metadata_table_name` parameter to `redcap_to_db()` for storing REDCap field definitions
* Add `redcap_log_table_name` parameter to `redcap_to_db()` for customizing REDCap audit log table name and schema
* Add `redcap_log_begin_date` and `redcap_log_end_date` parameters to `redcap_to_db()` for configurable REDCap log date ranges
* Remove data export formatting parameters (`raw_or_label`, `raw_or_label_headers`, `export_checkbox_label`) in `redcap_to_db()` as labeling is now handled by collection functions
* Import dplyr, dbplyr, duckdb, labelled, and rlang as new dependencies

## Minor Improvements
* Add S3 methods for dplyr verbs (filter, select, arrange, group_by) that preserve REDCap table attributes
* Automatic record ID preservation across multiple instruments

# redquack 0.2.0

## Lifecycle Changes
* Rename `redcap_to_duckdb()` to `redcap_to_db()` and gain a `conn` argument to support any database connection
* Return a list of processing data as an S3 object (`redcap_transfer_result`) 

## Patches
* If `log_table_name` is NULL, then disable logging
* Remove dependencies on beepr and utils
* Remove `optimize_types` and use connection class to check if DuckDB
* Remove `return_duckdb` to return TRUE (complete) or FALSE (incomplete)
* Bump default `max_retries` from 3 to 10 retries
* Add HTTP 504 transient error to retry
* Rebrand hex logo

# redquack 0.1.1
First release of redquack on CRAN

## Patches
* Improve parameter names and defaults to align with the REDCap API (e.g., `record_id_name` = `record_id`)
* Fix progress tracking messages and audio feedback for an improved user experience
* Improve documentation and use consistent language

# redquack 0.1.0

## New features
* Transfer REDCap data to DuckDB using the `redcap_to_duckdb()` function
