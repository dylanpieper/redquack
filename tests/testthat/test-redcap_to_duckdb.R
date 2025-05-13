test_that("redcap_to_db creates database with correct tables in DuckDB", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("basic_test.duckdb", db_type = "duckdb")
  on.exit(cleanup_db(db))

  expect_true(DBI::dbExistsTable(db$con, "data"))
  expect_true(DBI::dbExistsTable(db$con, "log"))

  data_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM data")$count
  expect_gt(data_count, 0)

  log_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM log")$count
  expect_gt(log_count, 0)
})

test_that("redcap_to_db creates database with correct tables in SQLite", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("basic_test.db", db_type = "sqlite")
  on.exit(cleanup_db(db))

  expect_true(DBI::dbExistsTable(db$con, "data"))
  expect_true(DBI::dbExistsTable(db$con, "log"))

  data_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM data")$count
  expect_gt(data_count, 0)

  log_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM log")$count
  expect_gt(log_count, 0)
})

test_that("redcap_to_db handles different chunk sizes", {
  skip_on_ci()
  skip_on_cran()

  db_small <- create_test_db("small_chunk.duckdb", db_type = "duckdb", chunk_size = 500)
  on.exit(cleanup_db(db_small))

  db_large <- create_test_db("large_chunk.duckdb", db_type = "duckdb", chunk_size = 5000)
  on.exit(cleanup_db(db_large), add = TRUE)

  small_logs <- DBI::dbGetQuery(db_small$con, "SELECT * FROM log WHERE message LIKE '%Chunk%successfully%'")
  large_logs <- DBI::dbGetQuery(db_large$con, "SELECT * FROM log WHERE message LIKE '%Chunk%successfully%'")

  expect_gt(nrow(small_logs), nrow(large_logs))
})

test_that("optimize_data_types correctly converts column types in DuckDB", {
  skip_on_cran()

  # Create a temporary DuckDB connection
  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  db_path <- file.path(test_dir, "optimize_types_test.duckdb")

  # Connect to the database
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  db <- list(con = con, path = db_path, type = "duckdb")
  on.exit(cleanup_db(db))

  # Create a log table first
  DBI::dbExecute(
    con,
    "CREATE TABLE log (timestamp TIMESTAMP, type VARCHAR(50), message TEXT)"
  )

  # Create sample data with various types (all stored as text initially)
  sample_data <- data.frame(
    integer_col = c("1", "2", "3", "100"),
    decimal_col = c("1.5", "2.7", "-3.14", "0.0"),
    date_col = c("2023-01-01", "2022-12-25", "2020-02-29", "2024-04-30"),
    timestamp_col = c(
      "2023-01-01 12:34:56", "2022-12-25 08:00:00",
      "2020-02-29 23:59:59", "2024-04-30 15:30:45"
    ),
    mixed_integers = c("1", "2", "text", "3"),
    mixed_decimals = c("1.5", "text", "3.14", "0"),
    mixed_dates = c("2023-01-01", "invalid", "2020-02-29", ""),
    text_col = c("text", "more text", "special chars: !@#$%", "123abc"),
    stringsAsFactors = FALSE
  )

  # Create a copy for the non-optimized table
  DBI::dbWriteTable(con, "data_raw", sample_data, overwrite = TRUE)

  # Create another copy for the optimized table
  DBI::dbWriteTable(con, "data_opt", sample_data, overwrite = TRUE)

  # Call the optimize_data_types function on the optimized table only
  optimize_data_types(con, "data_opt", "log", FALSE)

  # Retrieve schemas for both tables
  raw_schema <- DBI::dbGetQuery(con, "PRAGMA table_info(data_raw)")
  opt_schema <- DBI::dbGetQuery(con, "PRAGMA table_info(data_opt)")

  # Create lookup tables for column types
  raw_types <- setNames(raw_schema$type, raw_schema$name)
  opt_types <- setNames(opt_schema$type, opt_schema$name)

  # Check that the non-optimized table has all TEXT/VARCHAR columns
  expect_true(all(raw_types %in% c("VARCHAR", "TEXT", "STRING")),
    label = paste(
      "Expected all raw types to be text-like, got:",
      paste(unique(raw_types), collapse = ", ")
    )
  )

  # Check that the optimized table has multiple types
  expect_gt(length(unique(opt_types)), 1,
    label = paste(
      "Expected multiple types in optimized table, got:",
      paste(unique(opt_types), collapse = ", ")
    )
  )

  # Check specific type conversions using more flexible matching
  integer_type <- opt_types["integer_col"]
  expect_true(integer_type %in% c("INTEGER", "INT"),
    label = paste("integer_col type is", integer_type, "not in expected values")
  )

  decimal_type <- opt_types["decimal_col"]
  expect_true(decimal_type %in% c("DOUBLE", "FLOAT", "REAL", "NUMERIC"),
    label = paste("decimal_col type is", decimal_type, "not in expected values")
  )

  date_type <- opt_types["date_col"]
  expect_true(date_type %in% c("DATE"),
    label = paste("date_col type is", date_type, "not in expected values")
  )

  # Note: Some DuckDB versions might parse timestamps as dates in specific formats
  timestamp_type <- opt_types["timestamp_col"]
  expect_true(timestamp_type %in% c("TIMESTAMP", "DATE", "TIMESTAMPTZ"),
    label = paste("timestamp_col type is", timestamp_type, "not in expected values")
  )

  # Check that mixed content columns remain as text
  mixed_int_type <- opt_types["mixed_integers"]
  expect_true(mixed_int_type %in% c("VARCHAR", "TEXT", "STRING"),
    label = paste("mixed_integers type is", mixed_int_type, "not in expected values")
  )

  mixed_dec_type <- opt_types["mixed_decimals"]
  expect_true(mixed_dec_type %in% c("VARCHAR", "TEXT", "STRING"),
    label = paste("mixed_decimals type is", mixed_dec_type, "not in expected values")
  )

  mixed_date_type <- opt_types["mixed_dates"]
  expect_true(mixed_date_type %in% c("VARCHAR", "TEXT", "STRING"),
    label = paste("mixed_dates type is", mixed_date_type, "not in expected values")
  )

  text_type <- opt_types["text_col"]
  expect_true(text_type %in% c("VARCHAR", "TEXT", "STRING"),
    label = paste("text_col type is", text_type, "not in expected values")
  )

  # Verify that the data is correctly preserved after type conversion
  integer_data <- DBI::dbGetQuery(con, "SELECT integer_col FROM data_opt")
  expect_equal(integer_data$integer_col, c(1L, 2L, 3L, 100L))

  decimal_data <- DBI::dbGetQuery(con, "SELECT decimal_col FROM data_opt")
  expect_equal(decimal_data$decimal_col, c(1.5, 2.7, -3.14, 0.0))

  # Check log entries
  logs <- DBI::dbGetQuery(con, "SELECT * FROM log WHERE type = 'INFO'")

  # More flexible log message checking - look for partial matches
  integer_log <- any(grepl("integer_col.*INTEGER", logs$message, ignore.case = TRUE) |
    grepl("integer_col.*INT", logs$message, ignore.case = TRUE))
  expect_true(integer_log, label = "No log entry found for integer_col conversion")

  decimal_log <- any(grepl("decimal_col.*(DOUBLE|FLOAT|REAL|NUMERIC)", logs$message, ignore.case = TRUE))
  expect_true(decimal_log, label = "No log entry found for decimal_col conversion")

  date_log <- any(grepl("date_col.*DATE", logs$message, ignore.case = TRUE))
  expect_true(date_log, label = "No log entry found for date_col conversion")

  # The timestamp column might be reported differently depending on DuckDB version
  timestamp_log <- any(grepl("timestamp_col", logs$message, ignore.case = TRUE))
  expect_true(timestamp_log, label = "No log entry found for timestamp_col conversion")
})

test_that("optimize_data_types gracefully handles SQLite", {
  skip_on_cran()

  # Create a temporary SQLite connection
  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  db_path <- file.path(test_dir, "optimize_types_test.db")

  # Connect to the database
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db_path)
  db <- list(con = con, path = db_path, type = "sqlite")
  on.exit(cleanup_db(db))

  # Create a log table first
  DBI::dbExecute(
    con,
    "CREATE TABLE log (timestamp TIMESTAMP, type VARCHAR(50), message TEXT)"
  )

  # Create sample data
  sample_data <- data.frame(
    integer_col = c("1", "2", "3", "100"),
    decimal_col = c("1.5", "2.7", "-3.14", "0.0"),
    stringsAsFactors = FALSE
  )

  # Create table for the test
  DBI::dbWriteTable(con, "data_opt", sample_data, overwrite = TRUE)

  # Call the optimize_data_types function
  optimize_data_types(con, "data_opt", "log", FALSE)

  # Verify it didn't crash and logged the message
  logs <- DBI::dbGetQuery(con, "SELECT * FROM log WHERE type = 'WARNING'")
  expect_true(any(grepl("Connection is not DuckDB, skipping optimization", logs$message)))
})

test_that("redcap_to_db handles log table correctly", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("log_test.duckdb", db_type = "duckdb")
  on.exit(cleanup_db(db))

  log_schema <- DBI::dbGetQuery(db$con, "PRAGMA table_info(log)")
  log_columns <- log_schema$name

  expect_true(all(c("timestamp", "type", "message") %in% log_columns))

  success_logs <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) as count FROM log WHERE type = 'SUCCESS'")
  info_logs <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) as count FROM log WHERE type = 'INFO'")

  expect_gt(success_logs$count, 0)
  expect_gt(info_logs$count, 0)

  time_ordered <- DBI::dbGetQuery(db$con, "SELECT timestamp FROM log ORDER BY timestamp")
  expect_equal(nrow(time_ordered), DBI::dbGetQuery(db$con, "SELECT COUNT(*) FROM log")[[1]])
})

test_that("all record IDs from REDCap are present in a SQLite database", {
  skip_on_ci()
  skip_on_cran()

  fetch_all_redcap_record_ids <- function(uri, token, record_id_name = "id") {
    req <- httr2::request(uri) |>
      httr2::req_body_form(
        token = token,
        content = "record",
        format = "csv",
        type = "flat",
        fields = record_id_name
      ) |>
      httr2::req_retry(max_tries = 4)

    resp <- httr2::req_perform(req, verbosity = 0)
    raw_text <- httr2::resp_body_string(resp)

    result_data <- readr::read_csv(
      raw_text,
      col_types = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE
    )

    result_data[[1]]
  }

  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, "sqlite_record_ids_test.db")

  record_id_name <- "id"

  cli::cli_alert_info("Fetching all record IDs directly from REDCap")
  all_redcap_ids <- fetch_all_redcap_record_ids(
    uri = creds$uri,
    token = creds$token,
    record_id_name = record_id_name
  )

  expect_gt(length(all_redcap_ids), 0)
  cli::cli_alert_info(paste("Found", length(all_redcap_ids), "record IDs in REDCap"))

  cli::cli_alert_info("Creating test SQLite database with redcap_to_db")
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = file_path)

  # Transfer data
  result <- redcap_to_db(
    conn = con,
    redcap_uri = creds$uri,
    token = creds$token,
    data_table_name = "data",
    log_table_name = "log",
    record_id_name = record_id_name,
    beep = FALSE
  )

  db <- list(con = con, path = file_path, type = "sqlite")
  on.exit(cleanup_db(db))

  expect_true(result$success)

  query <- paste0("SELECT DISTINCT ", DBI::dbQuoteIdentifier(con, record_id_name), " FROM data")
  db_record_ids <- DBI::dbGetQuery(con, query)[[1]]

  expect_gt(length(db_record_ids), 0)
  cli::cli_alert_info(paste("Found", length(db_record_ids), "unique record IDs in database"))

  missing_ids <- setdiff(all_redcap_ids, db_record_ids)
  extra_ids <- setdiff(db_record_ids, all_redcap_ids)

  expect_equal(length(missing_ids), 0,
    label = paste("Missing", length(missing_ids), "record IDs in database")
  )
  expect_equal(length(extra_ids), 0,
    label = paste("Found", length(extra_ids), "extra record IDs in database")
  )

  expect_equal(sort(all_redcap_ids), sort(db_record_ids),
    label = "Record IDs in database should match those in REDCap"
  )
})

test_that("record_id_name parameter works properly with different DB types", {
  skip_on_ci()
  skip_on_cran()

  creds <- get_redcap_credentials()

  # Test with SQLite
  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  sqlite_path <- file.path(test_dir, "sqlite_record_id_name_test.db")

  custom_id <- "client_id"

  con_sqlite <- DBI::dbConnect(RSQLite::SQLite(), dbname = sqlite_path)

  # Transfer data to SQLite
  result_sqlite <- redcap_to_db(
    conn = con_sqlite,
    redcap_uri = creds$uri,
    token = creds$token,
    data_table_name = "data",
    log_table_name = "log",
    record_id_name = custom_id,
    beep = FALSE,
    forms = "client_profile"
  )

  db_sqlite <- list(con = con_sqlite, path = sqlite_path, type = "sqlite")
  on.exit(cleanup_db(db_sqlite))

  expect_true(DBI::dbExistsTable(con_sqlite, "data"))

  id_exists_sqlite <- tryCatch(
    {
      test <- DBI::dbGetQuery(con_sqlite, paste0("SELECT ", custom_id, " FROM data LIMIT 1"))
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )

  expect_true(id_exists_sqlite)
})
