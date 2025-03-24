test_that("redcap_to_duckdb creates database with correct tables", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("basic_test.duckdb")
  on.exit(cleanup_db(db$con, db$path))

  expect_true(DBI::dbExistsTable(db$con, "data"))
  expect_true(DBI::dbExistsTable(db$con, "log"))

  data_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM data")$count
  expect_gt(data_count, 0)

  log_count <- DBI::dbGetQuery(db$con, "SELECT COUNT(*) AS count FROM log")$count
  expect_gt(log_count, 0)
})

test_that("redcap_to_duckdb handles different chunk sizes", {
  skip_on_ci()
  skip_on_cran()

  db_small <- create_test_db("small_chunk.duckdb", chunk_size = 500)
  on.exit(cleanup_db(db_small$con, db_small$path))

  db_large <- create_test_db("large_chunk.duckdb", chunk_size = 5000)
  on.exit(cleanup_db(db_large$con, db_large$path), add = TRUE)

  small_logs <- DBI::dbGetQuery(db_small$con, "SELECT * FROM log WHERE message LIKE '%Chunk%successfully%'")
  large_logs <- DBI::dbGetQuery(db_large$con, "SELECT * FROM log WHERE message LIKE '%Chunk%successfully%'")

  expect_gt(nrow(small_logs), nrow(large_logs))
})

test_that("return_duckdb parameter works correctly", {
  skip_on_ci()
  skip_on_cran()

  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, "return_test.duckdb")

  result <- redcap_to_duckdb(
    redcap_uri = creds$uri,
    token = creds$token,
    output_file = file_path,
    record_id_name = "id",
    return_duckdb = FALSE,
    beep = FALSE
  )

  expect_null(result)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file_path)
  on.exit(cleanup_db(con, file_path))

  expect_true(DBI::dbExistsTable(con, "data"))
  expect_true(DBI::dbExistsTable(con, "log"))
})

test_that("optimize_types parameter affects column types", {
  skip_on_ci()
  skip_on_cran()

  db_optimized <- create_test_db("optimized.duckdb", optimize = TRUE)
  on.exit(cleanup_db(db_optimized$con, db_optimized$path))

  db_raw <- create_test_db("raw.duckdb", optimize = FALSE)
  on.exit(cleanup_db(db_raw$con, db_raw$path), add = TRUE)

  opt_schema <- dbGetQuery(db_optimized$con, "PRAGMA table_info(data)")
  raw_schema <- dbGetQuery(db_raw$con, "PRAGMA table_info(data)")

  opt_type_counts <- table(opt_schema$type)
  raw_type_counts <- table(raw_schema$type)

  expect_true("VARCHAR" %in% names(raw_type_counts))

  varchar_count_opt <- ifelse("VARCHAR" %in% names(opt_type_counts), opt_type_counts["VARCHAR"], 0)

  expect_lt(varchar_count_opt, length(opt_schema$name))

  non_varchar_types <- setdiff(names(opt_type_counts), "VARCHAR")
  expect_gt(length(non_varchar_types), 0)
})

test_that("redcap_to_duckdb handles log table correctly", {
  skip_on_ci()
  skip_on_cran()

  db <- create_test_db("log_test.duckdb")
  on.exit(cleanup_db(db$con, db$path))

  log_schema <- dbGetQuery(db$con, "PRAGMA table_info(log)")
  log_columns <- log_schema$name

  expect_true(all(c("timestamp", "type", "message") %in% log_columns))

  success_logs <- dbGetQuery(db$con, "SELECT COUNT(*) as count FROM log WHERE type = 'SUCCESS'")
  info_logs <- dbGetQuery(db$con, "SELECT COUNT(*) as count FROM log WHERE type = 'INFO'")

  expect_gt(success_logs$count, 0)
  expect_gt(info_logs$count, 0)

  time_ordered <- dbGetQuery(db$con, "SELECT timestamp FROM log ORDER BY timestamp")
  expect_equal(nrow(time_ordered), dbGetQuery(db$con, "SELECT COUNT(*) FROM log")[[1]])
})

test_that("all record IDs from REDCap are present in the database", {
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
  file_path <- file.path(test_dir, "complete_record_ids_test.duckdb")

  record_id_name <- "id"

  cli::cli_alert_info("Fetching all record IDs directly from REDCap")
  all_redcap_ids <- fetch_all_redcap_record_ids(
    uri = creds$uri,
    token = creds$token,
    record_id_name = record_id_name
  )

  expect_gt(length(all_redcap_ids), 0)
  cli::cli_alert_info(paste("Found", length(all_redcap_ids), "record IDs in REDCap"))

  cli::cli_alert_info("Creating test database with redcap_to_duckdb")
  con <- redcap_to_duckdb(
    redcap_uri = creds$uri,
    token = creds$token,
    output_file = file_path,
    record_id_name = record_id_name,
    beep = FALSE
  )

  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(file_path)
  })

  query <- paste0("SELECT DISTINCT ", DBI::dbQuoteIdentifier(con, record_id_name), " FROM data")
  db_record_ids <- DBI::dbGetQuery(con, query)[[1]]

  expect_gt(length(db_record_ids), 0)
  cli::cli_alert_info(paste("Found", length(db_record_ids), "unique record IDs in database"))

  missing_ids <- setdiff(all_redcap_ids, db_record_ids)
  extra_ids <- setdiff(db_record_ids, all_redcap_ids)

  if (length(missing_ids) > 0) {
    cli::cli_alert_warning(paste("Missing", length(missing_ids), "record IDs in database"))
    cli::cli_alert_info(paste("First few missing IDs:", paste(head(missing_ids, 5), collapse = ", ")))
  }

  if (length(extra_ids) > 0) {
    cli::cli_alert_warning(paste("Found", length(extra_ids), "extra record IDs in database not present in REDCap"))
    cli::cli_alert_info(paste("First few extra IDs:", paste(head(extra_ids, 5), collapse = ", ")))
  }

  expect_equal(length(missing_ids), 0,
    label = paste("Missing", length(missing_ids), "record IDs in database")
  )
  expect_equal(length(extra_ids), 0,
    label = paste("Found", length(extra_ids), "extra record IDs in database")
  )

  expect_equal(sort(all_redcap_ids), sort(db_record_ids),
    label = "Record IDs in database should match those in REDCap"
  )

  expect_equal(length(all_redcap_ids), length(db_record_ids),
    label = "Number of record IDs should match"
  )

  expect_equal(length(unique(all_redcap_ids)), length(all_redcap_ids),
    label = "All REDCap record IDs should be unique"
  )

  expect_equal(length(unique(db_record_ids)), length(db_record_ids),
    label = "All database record IDs should be unique"
  )

  log_complete <- DBI::dbGetQuery(
    con,
    "SELECT COUNT(*) AS count FROM log WHERE type = 'INFO' AND message LIKE 'Transfer completed in%'"
  )$count

  expect_equal(log_complete, 1, label = "Transfer completion log entry should exist")
})

test_that("record_id_name parameter works properly", {
  skip_on_ci()
  skip_on_cran()

  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, "record_id_name_test.duckdb")

  custom_id <- "client_id"

  con <- redcap_to_duckdb(
    redcap_uri = creds$uri,
    token = creds$token,
    output_file = file_path,
    record_id_name = custom_id,
    beep = FALSE,
    forms = "client_profile"
  )

  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(file_path)
  })

  expect_true(DBI::dbExistsTable(con, "data"))

  id_exists <- tryCatch(
    {
      test <- DBI::dbGetQuery(con, paste0("SELECT ", custom_id, " FROM data LIMIT 1"))
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )

  expect_true(id_exists)
})
