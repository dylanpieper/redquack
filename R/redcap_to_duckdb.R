#' Transfer 'REDCap' Data to 'DuckDB'
#'
#' @description
#' Transfer REDCap data to DuckDB in chunks to minimize memory usage.
#'
#' @param redcap_uri Character string specifying the URI (uniform resource identifier)
#'   of the REDCap server's API.
#' @param token Character string containing the REDCap API token specific to your project.
#'   This token is used for authentication and must have export permissions.
#' @param raw_or_label A string (either `'raw'` or `'label'`) that specifies
#'   whether to export the raw coded values or the labels for the options of
#'   multiple choice fields. Default is `'raw'`.
#' @param raw_or_label_headers A string (either `'raw'` or `'label'`) that
#'   specifies for the CSV headers whether to export the variable/field names
#'   (raw) or the field labels (label). Default is `'raw'`.
#' @param export_checkbox_label Logical that specifies the format of checkbox field values
#'   specifically when exporting the data as labels. If `raw_or_label` is
#'   `'label'` and `export_checkbox_label` is TRUE, the values will be the text
#'   displayed to the users. Otherwise, the values will be 0/1. Default is FALSE.
#' @param export_survey_fields Logical that specifies whether to export the
#'   survey identifier field (e.g., 'redcap_survey_identifier') or survey
#'   timestamp fields. Default is FALSE.
#' @param export_data_access_groups Logical that specifies whether or not to export
#'   the `redcap_data_access_group` field when data access groups are utilized
#'   in the project. Default is FALSE.
#' @param blank_for_gray_form_status Logical that specifies whether
#'   or not to export blank values for instrument complete status fields that have
#'   a gray status icon. Default is FALSE.
#' @param filter_logic String of logic text (e.g., `[gender] = 'male'`) for
#'   filtering the data to be returned, where the API will only return records
#'   where the logic evaluates as TRUE. Default is an empty string.
#' @param datetime_range_begin To return only records that have been created or
#'   modified *after* a given datetime, provide a POSIXct value.
#'   Default is NA (no begin time).
#' @param datetime_range_end To return only records that have been created or
#'   modified *before* a given datetime, provide a POSIXct value.
#'   Default is NA (no end time).
#' @param fields Character vector specifying which fields to export.
#'   Default is NULL (all fields).
#' @param forms Character vector specifying which forms to export.
#'   Default is NULL (all forms).
#' @param events Character vector specifying which events to export.
#'   Default is NULL (all events).
#' @param record_id_name Character string specifying the field name that contains record
#'   identifiers used for chunking requests. Default is "record_id".
#' @param chunk_size Integer specifying the number of record IDs to process per chunk.
#'   Default is 1000. Consider decreasing this for projects with many fields.
#' @param chunk_delay Numeric value specifying the delay in seconds between chunked
#'   requests. Default is 0.5 seconds. Adjust to respect REDCap server limits.
#' @param max_retries Integer specifying the maximum number of retry attempts for failed
#'   API requests. Default is 3. Set to 0 to disable retries.
#' @param output_file Character string specifying the file path where the DuckDB database
#'   will be created or modified. Default is "redcap.duckdb" in current working directory.
#' @param optimize_types Logical indicating whether column types should be optimized
#'   after all data is inserted. Default is TRUE, which analyzes column content and
#'   converts VARCHAR to more appropriate types (INTEGER, DOUBLE, DATE, TIMESTAMP).
#'   If FALSE, all columns will remain as VARCHAR regardless of content.
#' @param return_duckdb Logical indicating whether to return a DBI connection object.
#'   Default is TRUE. If FALSE, return NULL invisibly.
#' @param verbose Logical indicating whether to show progress and completion messages.
#'   Default is TRUE.
#' @param beep Logical indicating whether to play sound notifications when the process
#'   completes or encounters errors. Default is TRUE.
#' @param ... Additional arguments passed to the REDCap API call.
#'
#' @return
#' If `return_duckdb` is TRUE, returns a DBI connection object to the DuckDB database,
#' whether newly created, partially completed and resumed, or already complete.
#' Connection has attributes:
#' \itemize{
#'   \item `had_errors`: Logical indicating if errors occurred during the transfer
#'   \item `error_chunks`: Vector of chunk numbers that failed processing (if any)
#' }
#' If `return_duckdb` is FALSE, returns invisibly.
#'
#' @details
#' This function transfers data from REDCap to DuckDB in chunks, which helps manage memory
#' usage when dealing with large projects. It creates two tables in the DuckDB database:
#' \itemize{
#'   \item `data`: Contains all transferred REDCap records
#'   \item `log`: Contains timestamped logs of the transfer process
#' }
#'
#' The function automatically detects existing databases and handles them in three ways:
#' \itemize{
#'   \item If no database exists, starts a new transfer process
#'   \item If a database exists but is incomplete, resumes from the last processed record ID
#'   \item If a database exists and is complete, returns a connection without reprocessing
#' }
#'
#' The function fetches record IDs first, then processes records in chunks.
#' If any error occurs during processing, the function will stop further processing
#' to prevent incomplete data. Memory is explicitly managed to handle large datasets.
#'
#' All data is initially stored as VARCHAR type for consistent handling across chunks.
#' When `optimize_types = TRUE` (the default), column types are automatically converted
#' after all data is inserted, based on content analysis:
#' \itemize{
#'   \item Columns containing only integers are converted to INTEGER
#'   \item Columns containing numeric values are converted to DOUBLE
#'   \item Columns with valid date strings are converted to DATE
#'   \item Columns with valid timestamp strings are converted to TIMESTAMP
#'   \item All other columns remain as VARCHAR
#' }
#'
#' When `optimize_types = FALSE`, all columns remain as VARCHAR type. This can be useful
#' when:
#' \itemize{
#'   \item You need consistent string-based handling of all data
#'   \item You're working with complex mixed-type data
#'   \item You plan to handle type conversions manually in subsequent SQL queries
#'   \item Import speed is prioritized over storage efficiency or query optimization
#' }
#'
#' @section Database Connection:
#' The function returns an open connection to the DuckDB database when `return_duckdb = TRUE`.
#' You must explicitly close this connection with `DBI::dbDisconnect()` when finished.
#'
#' @examples
#' \dontrun{
#' # Basic usage with API token
#' con <- redcap_to_duckdb(
#'   redcap_uri = "https://redcap.example.org/api/",
#'   token = "YOUR_API_TOKEN",
#'   record_id_name = "record_id",
#'   chunk_size = 1000
#'   # Increase chunk size for memory-efficient systems (faster)
#'   # Decrease chunk size for memory-constrained systems (slower)
#' )
#'
#' # Query the resulting database
#' data <- DBI::dbGetQuery(con, "SELECT * FROM data LIMIT 10")
#'
#' # View transfer logs
#' logs <- DBI::dbGetQuery(con, "SELECT * FROM log")
#'
#' # Remember to close the connection
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
#' @seealso
#' \code{\link[DBI]{dbConnect}} for database connection details
#' \code{\link[duckdb]{duckdb}} for DuckDB database information
#' \code{\link[httr2]{req_retry}} for retry functionality details
#'
#' @importFrom audio load.wave play
#' @importFrom beepr beep
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbExistsTable dbGetQuery dbAppendTable dbQuoteIdentifier
#' @importFrom dplyr coalesce
#' @importFrom duckdb duckdb
#' @importFrom httr2 request req_body_form req_perform req_retry resp_body_string
#' @importFrom readr read_csv cols col_character
#' @importFrom cli cli_alert_info cli_alert_success cli_alert_warning cli_alert_danger cli_progress_update cli_progress_done
#' @importFrom utils menu
#'
#' @export
redcap_to_duckdb <- function(
    ## REDCap Connection Parameters
    redcap_uri,
    token,
    ## Data Export Options
    raw_or_label = "raw",
    raw_or_label_headers = "raw",
    export_checkbox_label = FALSE,
    export_survey_fields = FALSE,
    export_data_access_groups = FALSE,
    blank_for_gray_form_status = FALSE,
    ## Data Filtering Options
    filter_logic = "",
    datetime_range_begin = as.POSIXct(NA),
    datetime_range_end = as.POSIXct(NA),
    fields = NULL,
    forms = NULL,
    events = NULL,
    ## Processing Options
    record_id_name = "record_id",
    chunk_size = 1000,
    chunk_delay = 0.5,
    max_retries = 3,
    ## DuckDB Options
    output_file = "redcap.duckdb",
    optimize_types = TRUE,
    return_duckdb = TRUE,
    ## User Interface Options
    verbose = TRUE,
    beep = TRUE,
    ## Additional Parameters
    ...) {
  old_options <- options()
  duckdb_options <- names(old_options)[grep("^duckdb\\.", names(old_options))]
  saved_options <- old_options[duckdb_options]
  on.exit(options(saved_options), add = TRUE)

  options(
    "duckdb.progress_display" = FALSE,
    "duckdb.verbose_progress_bar" = FALSE,
    "duckdb.startup_message" = FALSE,
    "duckdb.materialize_message" = FALSE,
    "duckdb.disable_print" = TRUE,
    "duckdb.query_profiling" = FALSE,
    "readr.show_progress" = FALSE
  )

  format_elapsed_time <- function(seconds) {
    seconds <- as.numeric(seconds)
    if (seconds < 60) {
      return(paste0(round(seconds), "s"))
    } else {
      minutes <- floor(seconds / 60)
      remaining_seconds <- round(seconds %% 60)
      return(paste0(minutes, "m ", remaining_seconds, "s"))
    }
  }

  setup_environment <- function() {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = output_file)

    if (!DBI::dbExistsTable(con, "log")) {
      DBI::dbExecute(con, "CREATE TABLE log (
    timestamp TIMESTAMP,
    type VARCHAR,
    message VARCHAR
  )")
    }

    if (DBI::dbExistsTable(con, "data")) {
      completion_check <- DBI::dbGetQuery(
        con,
        "SELECT COUNT(*) AS count FROM log WHERE type = 'INFO' AND message LIKE 'Transfer completed in%'"
      )

      if (completion_check$count > 0) {
        if (verbose) {
          cli::cli_alert_success("Database exists and transfer is complete. No further processing needed.")
        }
        return(list(con = con, status = "complete", start_time = Sys.time()))
      } else {
        if (verbose) {
          cli::cli_alert_info("Resuming from incomplete transfer")
        }
        log_message(con, "INFO", "Resuming from incomplete transfer")
      }
    }

    list(con = con, start_time = Sys.time())
  }

  log_message <- function(con, type, msg) {
    timestamp <- Sys.time()
    DBI::dbExecute(
      con,
      "INSERT INTO log (timestamp, type, message) VALUES (?, ?, ?)",
      list(timestamp, type, msg)
    )
  }

  create_redcap_request <- function(token, params = list(), record_fetcher = FALSE) {
    if (!record_fetcher) {
      default_params <- list(
        token = token,
        content = "record",
        format = "csv",
        type = "flat",
        rawOrLabel = raw_or_label,
        rawOrLabelHeaders = raw_or_label_headers,
        exportCheckboxLabel = tolower(as.character(export_checkbox_label)),
        exportSurveyFields = tolower(as.character(export_survey_fields)),
        exportDataAccessGroups = tolower(as.character(export_data_access_groups)),
        filterLogic = filter_logic,
        dateRangeBegin = dplyr::coalesce(strftime(datetime_range_begin, "%Y-%m-%d %H:%M:%S"), ""),
        dateRangeEnd = dplyr::coalesce(strftime(datetime_range_end, "%Y-%m-%d %H:%M:%S"), ""),
        exportBlankForGrayFormStatus = tolower(as.character(blank_for_gray_form_status))
      )
    } else {
      default_params <- list(
        token = token,
        content = "record",
        format = "csv",
        type = "flat"
      )
    }

    all_params <- c(default_params, list(...))

    if (length(params) > 0) {
      for (param_name in names(params)) {
        param_value <- params[[param_name]]
        if (!is.null(param_value) && nchar(param_value) > 0) {
          all_params[[param_name]] <- param_value
        }
      }
    }

    httr2::request(redcap_uri) |>
      httr2::req_body_form(!!!all_params) |>
      httr2::req_retry(max_tries = max_retries + 1)
  }

  perform_redcap_request <- function(req) {
    resp <- httr2::req_perform(req, verbosity = 0)
    httr2::resp_body_string(resp)
  }

  fetch_record_ids <- function(con, start_time) {
    log_message(con, "INFO", "Fetching record IDs from REDCap")

    status_id <- NULL
    if (verbose) {
      status_id <- cli::cli_status("Sending request to REDCap API for record IDs...")
    }

    tryCatch(
      {
        params <- list()

        if (!is.null(record_id_name) && nchar(record_id_name) > 0) {
          params$fields <- record_id_name
        }

        req <- create_redcap_request(
          token = token,
          params = params
        )

        raw_text <- perform_redcap_request(req)

        if (verbose) {
          cli::cli_status_update(status_id, "Processing returned record IDs...")
        }

        result_data <- readr::read_csv(
          raw_text,
          col_types = readr::cols(.default = readr::col_character()),
          show_col_types = FALSE
        )

        if (ncol(result_data) == 0 || nrow(result_data) == 0) {
          if (verbose) cli::cli_status_clear(status_id)
          stop("No records or fields returned from REDCap")
        }

        all_record_ids <- result_data[[1]]
        total_records <- length(all_record_ids)

        if (DBI::dbExistsTable(con, "data")) {
          if (verbose) {
            cli::cli_status_update(status_id, "Identifying new records to process...")
          }

          processed_ids_query <- paste0("SELECT DISTINCT ", DBI::dbQuoteIdentifier(con, record_id_name), " FROM data")
          processed_ids <- DBI::dbGetQuery(con, processed_ids_query)[[1]]

          record_ids <- setdiff(all_record_ids, processed_ids)

          if (length(record_ids) == 0) {
            log_message(con, "INFO", "All record IDs have been processed")
            if (verbose) {
              cli::cli_status_clear(status_id)
              cli::cli_alert_info("All record IDs have been processed")
            }
            return(NULL)
          }

          log_message(con, "INFO", paste("Received", length(record_ids), "record IDs to process out of", total_records, "total records"))
          if (verbose) {
            cli::cli_status_clear(status_id)
            cli::cli_alert_info("Received {length(record_ids)} record IDs to process out of {total_records} total records")
          }
        } else {
          record_ids <- all_record_ids
          log_message(con, "INFO", paste("Received", total_records, "record IDs to process"))
          if (verbose) {
            cli::cli_status_clear(status_id)
            cli::cli_alert_info("Received {total_records} record IDs to process")
          }
        }

        return(record_ids)
      },
      error = function(e) {
        if (verbose) cli::cli_status_clear(status_id)
        if (beep) beepr::beep("wilhelm")

        error_msg <- e$message

        log_message(con, "ERROR", paste("Failed to fetch record IDs:", error_msg))

        stop(paste("Transfer failed: Unable to fetch record IDs:", {
          error_msg
        }), call. = FALSE)
      }
    )
  }

  process_chunks <- function(con, record_ids, start_time) {
    if (is.null(record_ids) || length(record_ids) == 0) {
      return(list(
        had_errors = FALSE,
        error_chunks = integer(0),
        num_chunks = 0,
        processing_time = difftime(Sys.time(), start_time, units = "secs"),
        total_chunk_time = 0
      ))
    }

    chunks <- split(record_ids, ceiling(seq_along(record_ids) / chunk_size))
    num_chunks <- length(chunks)

    log_message(con, "INFO", paste("Processing data in", num_chunks, "chunks of", chunk_size, "record IDs"))

    processing_start_time <- Sys.time()
    data_table_created <- DBI::dbExistsTable(con, "data")
    error_chunks <- integer(0)
    total_chunk_time <- 0

    if (verbose) {
      pb <- cli::cli_progress_bar(
        format = paste0(
          "Processing chunk [{cli::pb_current}/{cli::pb_total}] ",
          "[{cli::pb_bar}] {cli::pb_percent} | {cli::pb_elapsed}"
        ),
        total = num_chunks
      )
    }

    for (i in seq_along(chunks)) {
      chunk_id <- sprintf("%04d", i)
      chunk_record_ids <- chunks[[i]]

      log_message(con, "INFO", paste("Processing chunk", i, "of", num_chunks, "with", length(chunk_record_ids), "record IDs"))

      chunk_start <- Sys.time()

      chunk_result <- tryCatch(
        {
          params <- list()

          if (!is.null(fields) && length(fields) > 0) {
            params$fields <- paste(fields, collapse = ",")
          }

          if (!is.null(forms) && length(forms) > 0) {
            params$forms <- paste(forms, collapse = ",")
          }

          if (!is.null(events) && length(events) > 0) {
            params$events <- paste(events, collapse = ",")
          }

          if (length(chunk_record_ids) > 0) {
            params$records <- paste(chunk_record_ids, collapse = ",")
          }

          req <- create_redcap_request(
            token = token,
            params = params
          )

          raw_text <- perform_redcap_request(req)

          chunk_data <- readr::read_csv(
            raw_text,
            col_types = readr::cols(.default = readr::col_character()),
            show_col_types = FALSE
          )

          chunk_data <- as.data.frame(lapply(chunk_data, as.character), stringsAsFactors = FALSE)

          if (!data_table_created) {
            columns <- paste(names(chunk_data), "VARCHAR", collapse = ", ")
            DBI::dbExecute(con, paste("CREATE TABLE data (", columns, ")"))
            data_table_created <- TRUE
            log_message(con, "INFO", "Created data table in DuckDB with VARCHAR columns")
          }

          DBI::dbAppendTable(con, "data", chunk_data)

          success_msg <- paste0(
            "Chunk ", chunk_id, " successfully transferred (",
            nrow(chunk_data), " rows)"
          )
          log_message(con, "SUCCESS", success_msg)

          chunk_total <- round(difftime(Sys.time(), chunk_start, units = "secs"))
          total_chunk_time <- total_chunk_time + as.numeric(chunk_total)

          chunk_data <- NULL
          gc(FALSE)

          if (verbose) {
            cli::cli_progress_update()
          }

          list(success = TRUE, error_chunks = error_chunks, total_chunk_time = total_chunk_time)
        },
        error = function(e) {
          if (beep) beepr::beep("wilhelm")
          chunk_total <- round(difftime(Sys.time(), chunk_start, units = "secs"))
          total_chunk_time <- total_chunk_time + as.numeric(chunk_total)
          formatted_chunk_sum <- format_elapsed_time(total_chunk_time)

          error_msg <- e$message

          if (verbose) {
            cli::cli_progress_done()
            cli::cli_alert_danger("Chunk {i}/{num_chunks}: Error - {error_msg} [{formatted_chunk_sum}]")
          }

          log_message(con, "ERROR", paste("Error processing chunk", chunk_id, ":", error_msg))

          list(success = FALSE, error_chunks = c(error_chunks, i), total_chunk_time = total_chunk_time)
        }
      )

      error_chunks <- chunk_result$error_chunks
      total_chunk_time <- chunk_result$total_chunk_time

      if (!chunk_result$success) {
        log_message(con, "WARNING", "Stopping further processing due to error in chunk")

        if (verbose) {
          cli::cli_progress_done()
          cli::cli_alert_warning("Stopping further processing due to error")
        }

        break
      }

      if (i < num_chunks) Sys.sleep(chunk_delay)
    }

    if (verbose && chunk_result$success) {
      cli::cli_progress_done()
    }

    result <- list(
      had_errors = length(error_chunks) > 0,
      error_chunks = error_chunks,
      num_chunks = num_chunks,
      processing_time = difftime(Sys.time(), processing_start_time, units = "secs"),
      total_chunk_time = total_chunk_time
    )

    chunks <- NULL
    record_ids <- NULL
    gc(FALSE)

    result
  }

  optimize_column_types <- function(con) {
    log_message(con, "INFO", "Optimizing column data types")

    column_info <- DBI::dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'data'")

    for (col in column_info$column_name) {
      tryCatch(
        {
          safe_col <- DBI::dbQuoteIdentifier(con, col)

          integer_check <- DBI::dbGetQuery(
            con,
            paste0(
              "SELECT COUNT(*) = 0 AS is_integer FROM data WHERE ",
              safe_col, " IS NOT NULL AND ",
              safe_col, " != '' AND ",
              "REGEXP_MATCHES(", safe_col, ", '^-?[0-9]+$') = FALSE"
            )
          )$is_integer

          if (integer_check) {
            DBI::dbExecute(con, paste0("ALTER TABLE data ALTER COLUMN ", safe_col, " TYPE INTEGER USING CAST(", safe_col, " AS INTEGER)"))
            log_message(con, "INFO", paste("Column", col, "converted to INTEGER"))
            next
          }

          numeric_check <- DBI::dbGetQuery(
            con,
            paste0(
              "SELECT COUNT(*) = 0 AS is_numeric FROM data WHERE ",
              safe_col, " IS NOT NULL AND ",
              safe_col, " != '' AND ",
              "REGEXP_MATCHES(", safe_col, ", '^-?[0-9]+(\\.[0-9]+)?$') = FALSE"
            )
          )$is_numeric

          if (numeric_check) {
            DBI::dbExecute(con, paste0("ALTER TABLE data ALTER COLUMN ", safe_col, " TYPE DOUBLE USING CAST(", safe_col, " AS DOUBLE)"))
            log_message(con, "INFO", paste("Column", col, "converted to DOUBLE"))
            next
          }

          date_check <- DBI::dbGetQuery(
            con,
            paste0(
              "SELECT COUNT(*) = 0 AS is_date FROM data WHERE ",
              safe_col, " IS NOT NULL AND ",
              safe_col, " != '' AND ",
              "TRY_CAST(", safe_col, " AS DATE) IS NULL"
            )
          )$is_date

          if (date_check) {
            DBI::dbExecute(con, paste0("ALTER TABLE data ALTER COLUMN ", safe_col, " TYPE DATE USING CAST(", safe_col, " AS DATE)"))
            log_message(con, "INFO", paste("Column", col, "converted to DATE"))
            next
          }

          timestamp_check <- DBI::dbGetQuery(
            con,
            paste0(
              "SELECT COUNT(*) = 0 AS is_timestamp FROM data WHERE ",
              safe_col, " IS NOT NULL AND ",
              safe_col, " != '' AND ",
              "TRY_CAST(", safe_col, " AS TIMESTAMP) IS NULL"
            )
          )$is_timestamp

          if (timestamp_check) {
            DBI::dbExecute(con, paste0("ALTER TABLE data ALTER COLUMN ", safe_col, " TYPE TIMESTAMP USING CAST(", safe_col, " AS TIMESTAMP)"))
            log_message(con, "INFO", paste("Column", col, "converted to TIMESTAMP"))
          }
        },
        error = function(e) {
          log_message(con, "WARNING", paste("Unable to optimize column", col, ":", e$message))
        }
      )
    }
    log_message(con, "INFO", "Optimized column types")
  }

  finalize_and_report <- function(con, chunk_results, start_time) {
    had_errors <- chunk_results$had_errors
    error_chunks <- chunk_results$error_chunks
    num_chunks <- chunk_results$num_chunks
    processing_time <- chunk_results$processing_time
    total_chunk_time <- chunk_results$total_chunk_time

    successful_chunks <- num_chunks - length(error_chunks)
    failed_chunks <- length(error_chunks)

    if (optimize_types && DBI::dbExistsTable(con, "data")) {
      optimize_column_types(con)
    }

    DBI::dbDisconnect(con, shutdown = TRUE)
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = output_file)

    record_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS count FROM data")$count
    formatted_chunk_time <- format_elapsed_time(total_chunk_time)

    if (verbose) {
      cli::cli_alert_success("Inserted {record_count} rows into DuckDB in {formatted_chunk_time}")
    }

    log_message(con, "INFO", paste("Successfully Inserted", record_count, "rows into to DuckDB"))

    end_time <- Sys.time()
    elapsed <- difftime(end_time, start_time, units = "secs")
    formatted_time <- format_elapsed_time(as.numeric(elapsed))
    formatted_chunk_time <- format_elapsed_time(total_chunk_time)

    log_message(con, "INFO", paste(
      "Transfer completed in", formatted_time,
      "with", successful_chunks, "of", num_chunks, "chunks successful,",
      failed_chunks, "failed (active processing time:", formatted_chunk_time, ")"
    ))

    if (had_errors) {
      error_message <- paste(
        "Errors occurred in chunks:",
        paste(error_chunks, collapse = ", ")
      )
      cli::cli_alert_danger("{failed_chunks} of {num_chunks} chunks failed processing! Check log table for details.")
      log_message(con, "ERROR", error_message)

      attr(con, "had_errors") <- TRUE
      attr(con, "error_chunks") <- error_chunks
    }

    chunk_results <- NULL

    if (return_duckdb && verbose) {
      cli::cli_alert_warning("Remember to call DBI::dbDisconnect() when finished")
    }

    return(con)
  }

  main_process <- function() {
    env <- setup_environment()
    if (is.list(env) && !is.null(env$status) && env$status == "complete") {
      if (return_duckdb) {
        if (verbose) {
          cli::cli_alert_warning("Remember to call DBI::dbDisconnect(...) when finished")
        }
        return(env$con)
      } else {
        DBI::dbDisconnect(env$con, shutdown = TRUE)
        return(invisible(NULL))
      }
    }

    con <- env$con
    start_time <- env$start_time

    log_message(con, "INFO", "Transfer started")

    record_ids <- fetch_record_ids(con, start_time)

    if (is.null(record_ids)) {
      log_message(con, "INFO", "No new records to process")

      if (DBI::dbExistsTable(con, "data")) {
        log_message(con, "INFO", paste("Transfer completed in", format_elapsed_time(difftime(Sys.time(), start_time, units = "secs"))))

        if (verbose) {
          cli::cli_alert_success("transfer complete, no new records to process")
        }

        if (return_duckdb) {
          if (verbose) {
            cli::cli_alert_warning("Remember to call DBI::dbDisconnect(...) when finished")
          }
          return(con)
        } else {
          DBI::dbDisconnect(con, shutdown = TRUE)
          return(invisible(NULL))
        }
      } else {
        log_message(con, "ERROR", "No records returned from REDCap")
        DBI::dbDisconnect(con, shutdown = TRUE)
        if (verbose) {
          cli::cli_alert_danger("No records returned from REDCap")
        }
        return(NULL)
      }
    }

    chunk_results <- process_chunks(
      con,
      record_ids,
      start_time
    )

    record_ids <- NULL
    gc(FALSE)

    result_con <- finalize_and_report(con, chunk_results, start_time)

    chunk_results <- NULL
    gc(FALSE)

    if (beep) {
      if (!isTRUE(attr(result_con, "had_errors"))) {
        audio::play(audio::load.wave(system.file("audio/quack.wav", package = "redquack")))
      }
    }

    if (return_duckdb) {
      result_con
    } else {
      invisible(NULL)
    }
  }

  main_process()
}
