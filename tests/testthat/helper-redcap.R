get_redcap_credentials <- function() {
  uri <- Sys.getenv("REDCAP_URI")
  token <- keyring::key_get("redcap_token", "coe_test")
  if (!nzchar(uri) || !nzchar(token)) {
    testthat::skip("REDCAP_URI and REDCAP_TOKEN environment variables must be set")
  }
  list(uri = uri, token = token)
}

create_test_db <- function(file_name, db_type = "duckdb", chunk_size = 5000, optimize = TRUE) {
  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, file_name)

  # Create connection based on database type
  if (db_type == "duckdb") {
    if (!requireNamespace("duckdb", quietly = TRUE)) {
      testthat::skip("duckdb package is not available")
    }
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file_path)
  } else if (db_type == "sqlite") {
    if (!requireNamespace("RSQLite", quietly = TRUE)) {
      testthat::skip("RSQLite package is not available")
    }
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = file_path)
  } else {
    stop("Unsupported database type: ", db_type)
  }

  # Transfer data using redcap_to_db
  # IMPORTANT: Don't reassign the result to con!
  success <- redcap_to_db(
    conn = con, # Pass the connection
    redcap_uri = creds$uri,
    token = creds$token,
    data_table_name = "data",
    log_table_name = "log",
    record_id_name = "id",
    chunk_size = chunk_size,
    export_data_access_groups = TRUE,
    raw_or_label = "raw",
    export_checkbox_label = FALSE,
    beep = FALSE
  )

  # Return the original connection object, not the boolean result
  list(
    con = con,
    path = file_path,
    type = db_type
  )
}

cleanup_db <- function(db) {
  if (!is.null(db$con)) {
    tryCatch(
      {
        DBI::dbDisconnect(db$con)
      },
      error = function(e) {}
    )
  }

  if (file.exists(db$path)) {
    unlink(db$path)
  }
}
