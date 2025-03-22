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

  db_small <- create_test_db("small_chunk.duckdb", chunk_size = 1000)
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

test_that("comprehensive integration test with real API", {
  skip_on_ci()
  skip_on_cran()

  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, "comprehensive.duckdb")

  con <- redcap_to_duckdb(
    redcap_uri = creds$uri,
    token = creds$token,
    output_file = file_path,
    record_id_name = "id",
    chunk_size = 5000,
    export_data_access_groups = TRUE,
    raw_or_label = "raw",
    export_checkbox_label = FALSE,
    beep = FALSE
  )

  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(file_path)
  })

  data <- DBI::dbGetQuery(con, "SELECT * FROM data")
  logs <- DBI::dbGetQuery(con, "SELECT * FROM log")

  expect_gt(nrow(data), 0)
  expect_gt(nrow(logs), 0)

  column_count <- ncol(data)
  expect_gt(column_count, 1)

  data_types <- dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'data'")
  expect_equal(nrow(data_types), column_count)

  unique_types <- unique(data_types$data_type)
  expect_gt(length(unique_types), 1)
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
