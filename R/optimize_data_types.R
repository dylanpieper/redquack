#' Optimize data types in a DuckDB table
#'
#' @param conn A DBI connection object
#' @param data_table_ref The name of the data table in database
#' @param log_table_ref The name of the log table in database
#' @param verbose Logical to show status messages
#' @return NULL invisibly
#' @details Optimizes column data types by analyzing content and converting to appropriate types
#' @keywords internal
#' @noRd
optimize_data_types <- function(conn, data_table_ref, log_table_ref, verbose) {
  if (!is_db_class(conn)) {
    log_message(conn, log_table_ref, "WARNING", "Connection is not DuckDB, skipping optimization")
    return(invisible(NULL))
  }

  data_table_str <- gsub('"', "", data_table_ref)
  column_info <- DBI::dbGetQuery(
    conn,
    paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", data_table_str, "'")
  )

  log_message(conn, log_table_ref, "INFO", paste("Optimizing", nrow(column_info), "columns"))

  status_id <- NULL

  if (verbose) {
    status_id <- cli::cli_status(paste("Optimizing", nrow(column_info), "columns..."))
  }

  for (col in column_info$column_name) {
    tryCatch(
      {
        safe_col <- DBI::dbQuoteIdentifier(conn, col)
        integer_check <- DBI::dbGetQuery(
          conn,
          paste0(
            "SELECT COUNT(*) = 0 AS is_integer FROM ", data_table_ref, " WHERE ",
            safe_col, " IS NOT NULL AND ",
            safe_col, " != '' AND ",
            "REGEXP_MATCHES(", safe_col, ", '^-?[0-9]+$') = FALSE"
          )
        )$is_integer
        if (integer_check) {
          DBI::dbExecute(conn, paste0("ALTER TABLE ", data_table_ref, " ALTER COLUMN ", safe_col, " TYPE INTEGER USING CAST(", safe_col, " AS INTEGER)"))
          log_message(conn, log_table_ref, "INFO", paste("Column", col, "converted to INTEGER"))
          next
        }

        numeric_check <- DBI::dbGetQuery(
          conn,
          paste0(
            "SELECT COUNT(*) = 0 AS is_numeric FROM ", data_table_ref, " WHERE ",
            safe_col, " IS NOT NULL AND ",
            safe_col, " != '' AND ",
            "REGEXP_MATCHES(", safe_col, ", '^-?[0-9]+(\\.[0-9]+)?$') = FALSE"
          )
        )$is_numeric
        if (numeric_check) {
          DBI::dbExecute(conn, paste0("ALTER TABLE ", data_table_ref, " ALTER COLUMN ", safe_col, " TYPE DOUBLE USING CAST(", safe_col, " AS DOUBLE)"))
          log_message(conn, log_table_ref, "INFO", paste("Column", col, "converted to DOUBLE"))
          next
        }

        date_check <- DBI::dbGetQuery(
          conn,
          paste0(
            "SELECT COUNT(*) = 0 AS is_date FROM ", data_table_ref, " WHERE ",
            safe_col, " IS NOT NULL AND ",
            safe_col, " != '' AND ",
            "TRY_CAST(", safe_col, " AS DATE) IS NULL"
          )
        )$is_date
        if (date_check) {
          DBI::dbExecute(conn, paste0("ALTER TABLE ", data_table_ref, " ALTER COLUMN ", safe_col, " TYPE DATE USING CAST(", safe_col, " AS DATE)"))
          log_message(conn, log_table_ref, "INFO", paste("Column", col, "converted to DATE"))
          next
        }

        timestamp_check <- DBI::dbGetQuery(
          conn,
          paste0(
            "SELECT COUNT(*) = 0 AS is_timestamp FROM ", data_table_ref, " WHERE ",
            safe_col, " IS NOT NULL AND ",
            safe_col, " != '' AND ",
            "TRY_CAST(", safe_col, " AS TIMESTAMP) IS NULL"
          )
        )$is_timestamp
        if (timestamp_check) {
          DBI::dbExecute(conn, paste0("ALTER TABLE ", data_table_ref, " ALTER COLUMN ", safe_col, " TYPE TIMESTAMP USING CAST(", safe_col, " AS TIMESTAMP)"))
          log_message(conn, log_table_ref, "INFO", paste("Column", col, "converted to TIMESTAMP"))
        }
      },
      error = function(e) {
        log_message(conn, log_table_ref, "WARNING", paste("Unable to optimize column", col, ":", e$message))
      }
    )
  }

  if (verbose) {
    cli::cli_status_update(status_id, "Enabling compression...")
  }

  tryCatch(
    {
      DBI::dbExecute(conn, "PRAGMA force_compression = 'Auto'")
      log_message(conn, log_table_ref, "INFO", "Enabled compression for DuckDB")
    },
    error = function(e) {
      log_message(conn, log_table_ref, "WARNING", paste("Unable to enable compression:", e$message))
    }
  )

  if (verbose) {
    cli::cli_status_clear(status_id)
    cli::cli_alert_success(paste("Optimized data types for", nrow(column_info), "columns"))
  }

  return(invisible(NULL))
}
