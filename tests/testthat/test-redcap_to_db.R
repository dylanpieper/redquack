test_that("redquack complete workflow integration test", {
  skip_on_ci()
  skip_on_cran()

  library(dplyr)

  # Test: use_duckdb() returns "duckdb_connection" object class
  conn <- use_duckdb()
  expect_true(inherits(conn, "duckdb_connection"))

  # Test: redcap_to_db() returns list with "success" (logi) == TRUE and "error_chunks" (int) == 0
  result <- redcap_to_db(
    conn,
    redcap_uri = "https://bbmc.ouhsc.edu/redcap/api/",
    token = "9A81268476645C4E5F03428B8AC3AA7B"
  )

  expect_type(result, "list")
  expect_type(result$success, "logical")
  expect_true(result$success)
  expect_type(result$error_chunks, "integer")
  expect_equal(length(result$error_chunks), 0)

  # Test: tbl_redcap() |> collect_list() returns "list" with 3 tibbles
  dat <- tbl_redcap(conn) |>
    collect_list()

  expect_type(dat, "list")
  expect_length(dat, 3)
  expect_true(all(c("demographics", "health", "race_and_ethnicity") %in% names(dat)))
  expect_s3_class(dat$demographics, "tbl_df")
  expect_s3_class(dat$health, "tbl_df")
  expect_s3_class(dat$race_and_ethnicity, "tbl_df")
  expect_equal(dim(dat$demographics), c(5, 9))
  expect_equal(dim(dat$health), c(5, 6))
  expect_equal(dim(dat$race_and_ethnicity), c(5, 3))

  # Test: filtered data returns expected dimensions
  dat_filtered <- tbl_redcap(conn) |>
    filter(name_last == "Nutmouse") |>
    collect_list()

  expect_type(dat_filtered, "list")
  expect_length(dat_filtered, 3)
  expect_equal(dim(dat_filtered$demographics), c(2, 9))
  expect_equal(dim(dat_filtered$health), c(2, 6))
  expect_equal(dim(dat_filtered$race_and_ethnicity), c(2, 3))

  # Test: select() returns expected dimensions
  dat_selected <- tbl_redcap(conn) |>
    select(record_id, email, sex, bmi) |>
    collect_list()

  expect_type(dat_selected, "list")
  expect_length(dat_selected, 3)
  expect_equal(dim(dat_selected$demographics), c(5, 3))
  expect_equal(dim(dat_selected$health), c(5, 2))
  expect_equal(dim(dat_selected$race_and_ethnicity), c(5, 1))

  # Test: single table selection returns "tbl" object class
  dat_single <- tbl_redcap(conn) |>
    select(email, sex) |>
    collect_labeled()

  expect_s3_class(dat_single, "tbl_df")
  expect_equal(dim(dat_single), c(5, 2))

  # Test: complex query with group_by, filter, arrange
  dat_complex <- tbl_redcap(conn) |>
    select(record_id, sex, bmi) |>
    group_by(sex) |>
    filter(bmi < mean(bmi)) |>
    arrange(bmi) |>
    collect_list()

  expect_type(dat_complex, "list")
  expect_length(dat_complex, 3)
  expect_equal(dim(dat_complex$demographics), c(3, 2))
  expect_equal(dim(dat_complex$health), c(3, 2))
  expect_equal(dim(dat_complex$race_and_ethnicity), c(3, 1))
  expect_equal(dat_complex$health$bmi, c(19.8, 24.7, 27.9))

  # Test: collect_labeled_list() preserves labels
  dat_labeled <- tbl_redcap(conn) |>
    collect_labeled_list()

  expect_type(dat_labeled, "list")
  expect_equal(attr(dat_labeled$demographics$sex, "label"), "Gender")
  expect_type(dat_labeled$demographics$sex[[1]], "character")
  expect_equal(class(dat_labeled$demographics$sex[[1]]), "character")

  # Test: collect_labeled_list(convert = FALSE) preserves haven_labelled
  dat_haven <- tbl_redcap(conn) |>
    collect_labeled_list(convert = FALSE)

  expect_type(dat_haven, "list")
  expect_equal(as.numeric(dat_haven$demographics$sex[[1]]), 0)
  expect_true(all(c("haven_labelled", "vctrs_vctr", "integer") %in% class(dat_haven$demographics$sex[[1]])))

  # Test: list_to_env() creates environment variables
  dat_env <- tbl_redcap(conn) |>
    collect_list() |>
    list_to_env()

  expect_true(exists("demographics"))
  expect_true(exists("health"))
  expect_true(exists("race_and_ethnicity"))

  # Clean up environment variables
  suppressWarnings({
    if (exists("demographics")) rm(demographics)
    if (exists("health")) rm(health)
    if (exists("race_and_ethnicity")) rm(race_and_ethnicity)
  })

  # Test: metadata() returns expected structure
  metadata_tbl <- metadata(conn)

  expect_s3_class(metadata_tbl, "tbl_df")
  expect_equal(dim(metadata_tbl), c(18, 19))

  # Test: logs() returns expected structure
  logs_tbl <- logs(conn)

  expect_s3_class(logs_tbl, "tbl_df")
  expect_equal(dim(logs_tbl), c(31, 3))

  # Test: collect_labeled() with default settings
  dat_labeled_flat <- tbl_redcap(conn) |>
    collect_labeled()

  expect_s3_class(dat_labeled_flat, "tbl_df")
  expect_equal(dim(dat_labeled_flat), c(5, 25))
  expect_equal(as.character(dat_labeled_flat$sex), c("Female", "Male", "Male", "Female", "Male"))
  expect_equal(attr(dat_labeled_flat$sex, "label"), "Gender")

  # Test: collect_labeled(convert = FALSE)
  dat_labeled_raw <- tbl_redcap(conn) |>
    collect_labeled(convert = FALSE)

  expect_equal(as.numeric(dat_labeled_raw$sex), c(0, 1, 1, 0, 1))

  # Test: collect_labeled(cols = FALSE)
  dat_no_col_labels <- tbl_redcap(conn) |>
    collect_labeled(cols = FALSE)

  expect_null(attr(dat_no_col_labels$sex, "label"))

  # Test: collect_labeled(vals = FALSE)
  dat_no_val_labels <- tbl_redcap(conn) |>
    collect_labeled(vals = FALSE)

  expect_equal(attr(dat_no_val_labels$sex, "label"), "Gender")

  # Test: collect_labeled(convert = FALSE, cols = FALSE)
  dat_labels_only <- tbl_redcap(conn) |>
    collect_labeled(convert = FALSE, cols = FALSE)

  sex_labels <- attr(dat_labels_only$sex, "labels")
  expect_equal(names(sex_labels), c("Female", "Male"))
  expect_equal(as.numeric(sex_labels), c(0, 1))

  # Test: remove_duckdb() removes file
  remove_duckdb(conn)
  expect_false(file.exists("redcap.duckdb"))
})

test_that("redcap_to_db creates database with correct tables in DuckDB", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("basic_test.duckdb", db_type = "duckdb")
  on.exit(cleanup_db(db))

  expect_true(DBI::dbExistsTable(db$conn, "data"))
  expect_true(DBI::dbExistsTable(db$conn, "logs"))

  data_count <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) AS count FROM data")$count
  expect_gt(data_count, 0)

  log_count <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) AS count FROM logs")$count
  expect_gt(log_count, 0)
})

test_that("redcap_to_db creates database with correct tables in SQLite", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("basic_test.db", db_type = "sqlite")
  on.exit(cleanup_db(db))

  expect_true(DBI::dbExistsTable(db$conn, "data"))
  expect_true(DBI::dbExistsTable(db$conn, "logs"))

  data_count <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) AS count FROM data")$count
  expect_gt(data_count, 0)

  log_count <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) AS count FROM logs")$count
  expect_gt(log_count, 0)
})

test_that("redcap_to_db handles different chunk sizes", {
  skip_on_ci()
  skip_on_cran()

  db_small <- create_test_db("small_chunk.duckdb", db_type = "duckdb", chunk_size = 500)
  on.exit(cleanup_db(db_small))

  db_large <- create_test_db("large_chunk.duckdb", db_type = "duckdb", chunk_size = 5000)
  on.exit(cleanup_db(db_large), add = TRUE)

  small_logs <- DBI::dbGetQuery(db_small$conn, "SELECT * FROM logs WHERE message LIKE '%Chunk%successfully%'")
  large_logs <- DBI::dbGetQuery(db_large$conn, "SELECT * FROM logs WHERE message LIKE '%Chunk%successfully%'")

  # For small datasets, both chunk sizes might result in the same number of chunks
  # So we test that both completed successfully (at least 1 chunk each)
  expect_gte(nrow(small_logs), 1)
  expect_gte(nrow(large_logs), 1)
  # If the dataset is large enough, small chunks should create more log entries
  if (nrow(small_logs) > 1 || nrow(large_logs) > 1) {
    expect_gte(nrow(small_logs), nrow(large_logs))
  }
})

test_that("optimize_data_types correctly converts column types in DuckDB", {
  skip_on_cran()

  # Create a temporary DuckDB connection
  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  db_path <- file.path(test_dir, "optimize_types_test.duckdb")

  # Connect to the database
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  db <- list(conn = conn, path = db_path, type = "duckdb")
  on.exit(cleanup_db(db))

  # Create a log table first
  DBI::dbExecute(
    conn,
    "CREATE TABLE logs (timestamp TIMESTAMP, type VARCHAR(50), message TEXT)"
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
  DBI::dbWriteTable(conn, "data_raw", sample_data, overwrite = TRUE)

  # Create another copy for the optimized table
  DBI::dbWriteTable(conn, "data_opt", sample_data, overwrite = TRUE)

  # Call the optimize_data_types function on the optimized table only
  optimize_data_types(conn, "data_opt", "logs", FALSE)

  # Retrieve schemas for both tables
  raw_schema <- DBI::dbGetQuery(conn, "PRAGMA table_info(data_raw)")
  opt_schema <- DBI::dbGetQuery(conn, "PRAGMA table_info(data_opt)")

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
  integer_data <- DBI::dbGetQuery(conn, "SELECT integer_col FROM data_opt")
  expect_equal(integer_data$integer_col, c(1L, 2L, 3L, 100L))

  decimal_data <- DBI::dbGetQuery(conn, "SELECT decimal_col FROM data_opt")
  expect_equal(decimal_data$decimal_col, c(1.5, 2.7, -3.14, 0.0))

  # Check log entries
  logs <- DBI::dbGetQuery(conn, "SELECT * FROM logs WHERE type = 'INFO'")

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
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = db_path)
  db <- list(conn = conn, path = db_path, type = "sqlite")
  on.exit(cleanup_db(db))

  # Create a log table first
  DBI::dbExecute(
    conn,
    "CREATE TABLE logs (timestamp TIMESTAMP, type VARCHAR(50), message TEXT)"
  )

  # Create sample data
  sample_data <- data.frame(
    integer_col = c("1", "2", "3", "100"),
    decimal_col = c("1.5", "2.7", "-3.14", "0.0"),
    stringsAsFactors = FALSE
  )

  # Create table for the test
  DBI::dbWriteTable(conn, "data_opt", sample_data, overwrite = TRUE)

  # Call the optimize_data_types function
  optimize_data_types(conn, "data_opt", "logs", FALSE)

  # Verify it didn't crash and logged the message
  logs <- DBI::dbGetQuery(conn, "SELECT * FROM logs WHERE type = 'WARNING'")
  expect_true(any(grepl("Connection is not DuckDB, skipping optimization", logs$message)))
})

test_that("redcap_to_db handles log table correctly", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("log_test.duckdb", db_type = "duckdb")
  on.exit(cleanup_db(db))

  log_schema <- DBI::dbGetQuery(db$conn, "PRAGMA table_info(logs)")
  log_columns <- log_schema$name

  expect_true(all(c("timestamp", "type", "message") %in% log_columns))

  success_logs <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) as count FROM logs WHERE type = 'SUCCESS'")
  info_logs <- DBI::dbGetQuery(db$conn, "SELECT COUNT(*) as count FROM logs WHERE type = 'INFO'")

  expect_gt(success_logs$count, 0)
  expect_gt(info_logs$count, 0)

  time_ordered <- DBI::dbGetQuery(db$conn, "SELECT timestamp FROM logs ORDER BY timestamp")
  expect_equal(nrow(time_ordered), DBI::dbGetQuery(db$conn, "SELECT COUNT(*) FROM logs")[[1]])
})

test_that("all record IDs from REDCap are present in a SQLite database", {
  skip_on_ci()
  skip_on_cran()

  fetch_record_ids <- function(uri, token, record_id_name = "record_id") {
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

  creds <- get_creds()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, "sqlite_record_ids_test.db")

  record_id_name <- "record_id"

  cli::cli_alert_info("Fetching all record IDs directly from REDCap")
  all_redcap_ids <- fetch_record_ids(
    uri = creds$uri,
    token = creds$token,
    record_id_name = record_id_name
  )

  # # Normalize IDs
  # all_redcap_ids <- all_redcap_ids |>
  #   gsub("\\?", "", x = _) |>
  #   gsub("[[:punct:]]", "", x = _) |>
  #   gsub("\\s+", "", x = _) |>
  #   gsub("\u00a0", "", x = _) |>
  #   trimws() |>
  #   unique()

  expect_gt(length(all_redcap_ids), 0)
  cli::cli_alert_info(paste("Found", length(all_redcap_ids), "record IDs in REDCap"))

  cli::cli_alert_info("Creating test SQLite database with redcap_to_db")
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = file_path)

  # Transfer data
  result <- redcap_to_db(
    conn = conn,
    redcap_uri = creds$uri,
    token = creds$token,
    data_table_name = "data",
    log_table_name = "logs",
    record_id_name = record_id_name,
    beep = FALSE
  )

  db <- list(conn = conn, path = file_path, type = "sqlite")
  on.exit(cleanup_db(db))

  expect_true(result$success)

  query <- paste0("SELECT DISTINCT ", DBI::dbQuoteIdentifier(conn, record_id_name), " FROM data")
  db_record_ids <- DBI::dbGetQuery(conn, query)[[1]]

  # # Normalize IDs
  # db_record_ids <- db_record_ids |>
  #   gsub("\\?", "", x = _) |>
  #   gsub("[[:punct:]]", "", x = _) |>
  #   gsub("\\s+", "", x = _) |>
  #   gsub("\u00a0", "", x = _) |>
  #   trimws() |>
  #   unique()

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
