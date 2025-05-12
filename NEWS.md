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
