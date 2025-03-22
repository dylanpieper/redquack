get_redcap_credentials <- function() {
  uri <- Sys.getenv("REDCAP_URI")
  token <- Sys.getenv("REDCAP_TOKEN")
  if (!nzchar(uri) || !nzchar(token)) {
    testthat::skip("REDCAP_URI and REDCAP_TOKEN environment variables must be set")
  }
  list(uri = uri, token = token)
}

create_test_db <- function(file_name, chunk_size = 5000, optimize = TRUE) {
  creds <- get_redcap_credentials()

  test_dir <- file.path(tempdir(), "redcap_tests")
  dir.create(test_dir, showWarnings = FALSE, recursive = TRUE)
  file_path <- file.path(test_dir, file_name)

  con <- redcap_to_duckdb(
    redcap_uri = creds$uri,
    token = creds$token,
    output_file = file_path,
    record_id_name = "id",
    chunk_size = chunk_size,
    optimize_types = optimize,
    export_data_access_groups = TRUE,
    raw_or_label = "raw",
    export_checkbox_label = FALSE,
    beep = FALSE
  )

  list(
    con = con,
    path = file_path
  )
}

cleanup_db <- function(con, path) {
  if (!is.null(con)) {
    tryCatch(
      {
        DBI::dbDisconnect(con, shutdown = TRUE)
      },
      error = function(e) {}
    )
  }

  if (file.exists(path)) {
    unlink(path)
  }
}
