# redquack (development version)
* Added workflow mode with `return_duckdb` = `FALSE` now returning TRUE/FALSE status for success or failure
* Added automatic retry attempts for partial transfers in workflow mode up to `max_retries`
* Add HTTP 504 transient error to retry
* Rebrand hex logo

# redquack 0.1.1
First release of redquack on CRAN

## Patches
* Improved parameter names and defaults to align with the REDCap API (e.g., `record_id_name` = `record_id`)
* Fixes for progress tracking messages and audio feedback for an improved user experience
* Improved documentation and use of consistent language

# redquack 0.1.0

## New features
* Transfer REDCap data to DuckDB using the `redcap_to_duckdb()` function
