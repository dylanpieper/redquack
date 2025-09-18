get_creds <- function() {
  uri <- "https://bbmc.ouhsc.edu/redcap/api/"
  token <- "9A81268476645C4E5F03428B8AC3AA7B"
  list(uri = uri, token = token)
}

create_test_db <- function(file_name, db_type = "duckdb", chunk_size = 5000, optimize = TRUE) {
  creds <- get_creds()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, file_name)

  # Create connection based on database type
  if (db_type == "duckdb") {
    if (!requireNamespace("duckdb", quietly = TRUE)) {
      testthat::skip("duckdb package is not available")
    }
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = file_path)
  } else if (db_type == "sqlite") {
    if (!requireNamespace("RSQLite", quietly = TRUE)) {
      testthat::skip("RSQLite package is not available")
    }
    conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = file_path)
  } else {
    stop("Unsupported database type: ", db_type)
  }

  # Test REDCap API connectivity first
  tryCatch(
    {
      test_req <- httr2::request(creds$uri) |>
        httr2::req_body_form(
          token = creds$token,
          content = "metadata",
          format = "csv"
        ) |>
        httr2::req_perform(verbosity = 0)
    },
    error = function(e) {
      testthat::skip(paste("REDCap API not accessible:", e$message))
    }
  )

  success <- redcap_to_db(
    conn = conn, # Pass the connection
    redcap_uri = creds$uri,
    token = creds$token,
    data_table_name = "data",
    log_table_name = "logs",
    record_id_name = "record_id",
    chunk_size = chunk_size,
    export_data_access_groups = TRUE,
    raw_or_label = "raw",
    export_checkbox_label = FALSE,
    beep = FALSE
  )

  list(
    conn = conn,
    path = file_path,
    type = db_type
  )
}

cleanup_db <- function(db) {
  if (!is.null(db$conn)) {
    tryCatch(
      {
        DBI::dbDisconnect(db$conn)
      },
      error = function(e) {}
    )
  }

  if (file.exists(db$path)) {
    unlink(db$path)
  }
}
