# Transfer 'REDCap' Data to a Database

Transfer REDCap data to a database in chunks of record IDs to minimize
memory usage.

## Usage

``` r
redcap_to_db(
  conn,
  url,
  token,
  data_table_name = "data",
  metadata_table_name = "metadata",
  transfer_log_table_name = "transfer_log",
  redcap_log_table_name = "redcap_log",
  redcap_log_begin_date = Sys.Date() - 6,
  redcap_log_end_date = Sys.Date(),
  export_survey_fields = FALSE,
  export_data_access_groups = FALSE,
  blank_for_gray_form_status = FALSE,
  filter_logic = "",
  datetime_range_begin = as.POSIXct(NA),
  datetime_range_end = as.POSIXct(NA),
  fields = NULL,
  forms = NULL,
  events = NULL,
  record_id_name = "record_id",
  chunk_size = 1000,
  chunk_delay = 0.5,
  max_retries = 10,
  echo = "all",
  beep = TRUE,
  ...
)
```

## Arguments

- conn:

  A DBI connection object to a database.

- url:

  Character string specifying the URI (uniform resource identifier) of
  the REDCap server's API.

- token:

  Character string containing the REDCap API token specific to your
  project. This token is used for authentication and must have export
  permissions.

- data_table_name:

  Character string specifying the name of the table to create or append
  data to. Default is "data". Can include schema name (e.g.
  "schema.table").

- metadata_table_name:

  Character string specifying the name of the table to store REDCap
  metadata. Default is "metadata". Can include schema name (e.g.
  "schema.metadata").

- transfer_log_table_name:

  Character string specifying the name of the table to store transfer
  logs. Default is "transfer_log". Can include schema name (e.g.
  "schema.transfer_log"). Set to NULL to disable logging.

- redcap_log_table_name:

  Character string specifying the name of the table to store REDCap
  audit logs. Default is "redcap_log". Can include schema name (e.g.
  "schema.redcap_log"). Set to NULL to skip REDCap log retrieval.

- redcap_log_begin_date:

  Date/POSIXct specifying the start date for REDCap log retrieval.
  Default is 6 days prior to today.

- redcap_log_end_date:

  Date/POSIXct specifying the end date for REDCap log retrieval. Default
  is today.

- export_survey_fields:

  Logical that specifies whether to export the survey identifier field
  (e.g., 'redcap_survey_identifier') or survey timestamp fields. Default
  is FALSE.

- export_data_access_groups:

  Logical that specifies whether or not to export the
  `redcap_data_access_group` field when data access groups are utilized
  in the project. Default is FALSE.

- blank_for_gray_form_status:

  Logical that specifies whether or not to export blank values for
  instrument complete status fields that have a gray status icon.
  Default is FALSE.

- filter_logic:

  String of logic text (e.g., `[gender] = 'male'`) for filtering the
  data to be returned, where the API will only return records where the
  logic evaluates as TRUE. Default is an empty string.

- datetime_range_begin:

  To return only records that have been created or modified *after* a
  given datetime, provide a POSIXct value. Default is NA (no begin
  time).

- datetime_range_end:

  To return only records that have been created or modified *before* a
  given datetime, provide a POSIXct value. Default is NA (no end time).

- fields:

  Character vector specifying which fields to export. Default is NULL
  (all fields).

- forms:

  Character vector specifying which forms to export. Default is NULL
  (all forms).

- events:

  Character vector specifying which events to export. Default is NULL
  (all events).

- record_id_name:

  Character string specifying the field name that contains record
  identifiers used for chunking requests. Default is "record_id".

- chunk_size:

  Integer specifying the number of record IDs per chunk. Default
  is 1000. Consider decreasing this for projects with many fields.

- chunk_delay:

  Numeric value specifying the delay in seconds between chunked
  requests. Default is 0.5 seconds. Adjust to respect REDCap server
  limits.

- max_retries:

  Integer specifying the maximum number of retry attempts for failed API
  connection or HTTP 504 error. Default is 10.

- echo:

  String to show a progress bar and all status messages ("all"), only a
  progress bar ("progress"), or nothing ("none"). Default is "all".

- beep:

  Logical indicating whether to play sound notifications when the
  process completes or encounters errors. Default is TRUE.

- ...:

  Additional arguments passed to the REDCap API call.

## Value

Returns a list with the following components:

- `success`: Logical if the transfer was completed with no failed
  processing

- `error_chunks`: Vector of chunk numbers that failed processing

- `time_s`: Numeric value for total seconds to transfer and optimize
  data

## Details

This function transfers data from REDCap to any database in chunks,
which helps manage memory usage when dealing with large projects. It
creates up to four tables in the database:

- `data_table_name`: Contains all transferred REDCap records

- `metadata_table_name`: Contains REDCap metadata for field definitions
  and labeling

- `transfer_log_table_name`: Contains timestamped logs of the transfer
  process

- `redcap_log_table_name`: Contains REDCap audit logs (optional)

The function automatically detects existing databases and handles them
in three ways:

- If no table exists, starts a new transfer process

- If a table exists but is incomplete, resumes from the last processed
  record ID

- If a table exists and is complete, returns success without
  reprocessing

The function fetches record IDs, then processes records in chunks. If
any error occurs during processing, the function will continue with
remaining chunks but mark the transfer as incomplete.

If `redcap_log_table_name` is provided, the function will also retrieve
REDCap audit logs and store them in a separate table. The date range for
log retrieval can be controlled with `redcap_log_begin_date` and
`redcap_log_end_date` parameters.

Data is first set to **VARCHAR/TEXT** type for consistent handling
across chunks. For DuckDB, data types are automatically optimized after
data is inserted:

- **INTEGER**: Columns with only whole numbers

- **DOUBLE**: Columns with decimal numbers

- **DATE**: Columns with valid dates

- **TIMESTAMP**: Columns with valid timestamps

- **VARCHAR/TEXT**: All other columns remain as strings

## See also

[`use_duckdb`](https://dylanpieper.github.io/redquack/reference/use_duckdb.md)
for establishing a local duckdb connection
[`close_duckdb`](https://dylanpieper.github.io/redquack/reference/close_duckdb.md)
for closing a local duckdb connection
[`remove_duckdb`](https://dylanpieper.github.io/redquack/reference/remove_duckdb.md)
for closing a local duckdb connection and removing the file
[`collect_labeled`](https://dylanpieper.github.io/redquack/reference/collect_labeled.md)
for collecting a database table into a single data frame with column and
value labels (converts coded values to their text labels by default)
[`collect_list`](https://dylanpieper.github.io/redquack/reference/collect_list.md)
for collecting a database table into a list of instruments
[`collect_labeled_list`](https://dylanpieper.github.io/redquack/reference/collect_list.md)
for collecting a database table into a list of instruments with column
and value labels (converts coded values to their text labels by default)
[`list_to_env`](https://dylanpieper.github.io/redquack/reference/list_to_env.md)
for assigning a list of instruments to the global environment

## Examples

``` r
if (FALSE) { # \dontrun{
library(redquack)
library(dplyr)

conn <- use_duckdb()

result <- redcap_to_db(
  conn,
  url = "https://bbmc.ouhsc.edu/redcap/api/",
  token = "9A81268476645C4E5F03428B8AC3AA7B"
)

data <- tbl_redcap(conn) |>
  collect()

remove_duckdb(conn)
} # }
```
