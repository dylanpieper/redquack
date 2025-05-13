#' Transfer 'REDCap' Data to a Database
#'
#' @description
#' Transfer REDCap data to a database in chunks to minimize memory usage.
#'
#' @param conn A DBI connection object to a database.
#' @param redcap_uri Character string specifying the URI (uniform resource identifier)
#'   of the REDCap server's API.
#' @param token Character string containing the REDCap API token specific to your project.
#'   This token is used for authentication and must have export permissions.
#' @param data_table_name Character string specifying the name of the table to create or append
#'   data to. Default is "data". Can include schema name (e.g. "schema.table").
#' @param log_table_name Character string specifying the name of the table to store
#'   transfer logs. Default is "log". Can include schema name (e.g. "schema.log").
#'   Set to NULL to disable logging.
#' @param raw_or_label A string (either "raw" or "label") that specifies
#'   whether to export the raw coded values or the labels for the options of
#'   multiple choice fields. Default is "raw".
#' @param raw_or_label_headers A string (either "raw" or "label") that
#'   specifies for the CSV headers whether to export the variable/field names
#'   (raw) or the field labels (label). Default is "raw".
#' @param export_checkbox_label Logical that specifies the format of checkbox field values
#'   specifically when exporting the data as labels. If `raw_or_label` is
#'   "label" and `export_checkbox_label` is TRUE, the values will be the text
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
#'   API connection or HTTP 504 error. Default is 10.
#' @param verbose Logical indicating whether to show progress and completion messages.
#'   Default is TRUE.
#' @param beep Logical indicating whether to play sound notifications when the process
#'   completes or encounters errors. Default is TRUE.
#' @param ... Additional arguments passed to the REDCap API call.
#'
#' @return
#' Returns a list with the following components:
#' \itemize{
#'   \item `success`: Logical if the transfer was completed with no failed processing
#'   \item `error_chunks`: Vector of chunk numbers that failed processing
#'   \item `time_s`: Numeric value for total seconds to transfer and optimize data
#' }
#'
#' @details
#' This function transfers data from REDCap to any database in chunks, which helps manage memory
#' usage when dealing with large projects. It creates two tables in the database:
#' \itemize{
#'   \item `data_table_name`: Contains all transferred REDCap records
#'   \item `log_table_name`: Contains timestamped logs of the transfer process
#' }
#'
#' The function automatically detects existing databases and handles them in three ways:
#' \itemize{
#'   \item If no table exists, starts a new transfer process
#'   \item If a table exists but is incomplete, resumes from the last processed record ID
#'   \item If a table exists and is complete, returns success without reprocessing
#' }
#'
#' The function fetches record IDs, then processes records in chunks.
#' If any error occurs during processing, the function will continue with remaining chunks
#' but mark the transfer as incomplete.
#'
#' Data is first set to **VARCHAR/TEXT** type for consistent handling across chunks.
#' For DuckDB, data types are automatically optimized after all data is inserted:
#' \itemize{
#'   \item **INTEGER**: Columns with only whole numbers
#'   \item **DOUBLE**: Columns with decimal numbers
#'   \item **DATE**: Columns with valid dates
#'   \item **TIMESTAMP**: Columns with valid timestamps
#'   \item **VARCHAR/TEXT**: All other columns remain as strings
#' }
#'
#' @examples
#' \dontrun{
#' # install.packages("pak")
#' # pak::pak(c("duckdb", "keyring", "redquack"))
#'
#' library(redquack)
#'
#' duckdb <- DBI::dbConnect(duckdb::duckdb(), "redcap.duckdb")
#'
#' result <- redcap_to_db(
#'   redcap_uri = "https://redcap.example.org/api/",
#'   token = keyring::key_get("redcap_token"),
#'   conn = duckdb,
#' )
#'
#' data <- DBI::dbGetQuery(duckdb, "SELECT * FROM data LIMIT 1000")
#' log <- DBI::dbGetQuery(duckdb, "SELECT * FROM log")
#'
#' DBI::dbDisconnect(duckdb)
#' }
#' @seealso
#' \code{\link[DBI]{dbConnect}} for database connection details
#'
#' @importFrom audio load.wave play
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbExistsTable dbGetQuery dbAppendTable dbQuoteIdentifier dbWithTransaction
#' @importFrom httr2 request req_body_form req_perform req_retry resp_body_string
#' @importFrom readr read_csv cols col_character
#' @importFrom cli cli_alert_info cli_alert_success cli_alert_warning cli_alert_danger cli_progress_update cli_progress_done
#'
#' @export
redcap_to_db <- function(
    ## Database Connection
    conn,
    ## REDCap Connection Parameters
    redcap_uri,
    token,
    data_table_name = "data",
    log_table_name = "log",
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
    max_retries = 10,
    ## User Interface Options
    verbose = TRUE,
    beep = TRUE,
    ## Additional Parameters
    ...) {
  old_options <- options()
  readr_options <- names(old_options)[grep("^readr\\.", names(old_options))]
  saved_options <- old_options[readr_options]
  on.exit(options(saved_options), add = TRUE)

  options("readr.show_progress" = FALSE)

  if (is_db_class(conn)) {
    options(
      "duckdb.progress_display" = FALSE,
      "duckdb.verbose_progress_bar" = FALSE,
      "duckdb.startup_message" = FALSE,
      "duckdb.materialize_message" = FALSE,
      "duckdb.disable_print" = TRUE
    )
  }

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

  get_table_reference <- function(conn, table_name) {
    if (is.null(table_name)) {
      return(NULL)
    } else {
      return(DBI::dbQuoteIdentifier(conn, table_name))
    }
  }

  setup_environment <- function(conn) {
    data_table_ref <- get_table_reference(conn, data_table_name)
    log_table_ref <- get_table_reference(conn, log_table_name)

    if (!is.null(log_table_name) && !DBI::dbExistsTable(conn, name = log_table_name)) {
      DBI::dbExecute(
        conn,
        paste0(
          "CREATE TABLE ", log_table_ref, " (",
          "timestamp TIMESTAMP, ",
          "type VARCHAR(50), ",
          "message TEXT)"
        )
      )
    }

    if (DBI::dbExistsTable(conn, name = data_table_name)) {
      completion_check <- DBI::dbGetQuery(
        conn,
        paste0(
          "SELECT COUNT(*) AS count FROM ", log_table_ref,
          " WHERE type = 'INFO' AND message LIKE 'Transfer completed in%'"
        )
      )

      error_check <- DBI::dbGetQuery(
        conn,
        paste0(
          "SELECT COUNT(*) AS count FROM ", log_table_ref,
          " WHERE type = 'ERROR'"
        )
      )

      if (completion_check$count > 0 && error_check$count == 0) {
        if (verbose) {
          cli::cli_alert_success("Database table exists and transfer was completed without errors")
        }
        return(list(data_table_ref = data_table_ref, log_table_ref = log_table_ref, status = "complete", start_time = Sys.time()))
      } else if (completion_check$count > 0 && error_check$count > 0) {
        if (verbose) {
          cli::cli_alert_warning("Database table exists but had errors during previous transfer")
        }
        log_message(conn, log_table_ref, "WARNING", "Resuming from incomplete transfer with errors")
        return(list(data_table_ref = data_table_ref, log_table_ref = log_table_ref, start_time = Sys.time()))
      } else {
        if (verbose) {
          cli::cli_alert_info("Resuming from incomplete transfer")
        }
        log_message(conn, log_table_ref, "INFO", "Resuming from incomplete transfer")
      }
    }

    list(data_table_ref = data_table_ref, log_table_ref = log_table_ref, start_time = Sys.time())
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
        dateRangeBegin = ifelse(is.na(datetime_range_begin), "",
          strftime(datetime_range_begin, "%Y-%m-%d %H:%M:%S")
        ),
        dateRangeEnd = ifelse(is.na(datetime_range_end), "",
          strftime(datetime_range_end, "%Y-%m-%d %H:%M:%S")
        ),
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
      httr2::req_retry(
        max_tries = max_retries + 1,
        is_transient = \(resp) resp$status_code == 504
      )
  }

  perform_redcap_request <- function(req) {
    resp <- httr2::req_perform(req, verbosity = 0)
    httr2::resp_body_string(resp)
  }

  fetch_record_ids <- function(log_table_ref, data_table_ref, start_time) {
    log_message(conn, log_table_ref, "INFO", "Fetching record IDs from REDCap")

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

        table_exists <- DBI::dbExistsTable(conn, name = data_table_name)

        if (table_exists) {
          if (verbose) {
            cli::cli_status_update(status_id, "Identifying new records to process...")
          }

          processed_ids_query <- paste0(
            "SELECT DISTINCT ", DBI::dbQuoteIdentifier(conn, record_id_name),
            " FROM ", data_table_ref
          )
          processed_ids <- DBI::dbGetQuery(conn, processed_ids_query)[[1]]

          record_ids <- setdiff(all_record_ids, processed_ids)

          if (length(record_ids) == 0) {
            log_message(conn, log_table_ref, "INFO", "All record IDs have been processed")
            if (verbose) {
              cli::cli_status_clear(status_id)
              cli::cli_alert_info("All record IDs have been processed")
            }
            return(NULL)
          }

          log_message(conn, log_table_ref, "INFO", paste("Received", length(record_ids), "record IDs to process out of", total_records, "total records"))
          if (verbose) {
            cli::cli_status_clear(status_id)
            cli::cli_alert_info("Received {length(record_ids)} record IDs to process out of {total_records} total records")
          }
        } else {
          record_ids <- all_record_ids
          log_message(conn, log_table_ref, "INFO", paste("Received", total_records, "record IDs to process"))
          if (verbose) {
            cli::cli_status_clear(status_id)
            cli::cli_alert_info("Received {total_records} record IDs to process")
          }
        }

        return(record_ids)
      },
      error = function(e) {
        if (verbose) cli::cli_status_clear(status_id)
        if (beep) {
          tryCatch(
            {
              audio::play(audio::load.wave(system.file("audio/wilhelm.wav", package = "redquack")))
            },
            error = function(e) NULL
          )
        }

        error_msg <- e$message

        log_message(conn, log_table_ref, "ERROR", paste("Failed to fetch record IDs:", error_msg))

        stop(paste("Transfer failed: Unable to fetch record IDs:", {
          error_msg
        }), call. = FALSE)
      }
    )
  }

  process_chunks <- function(log_table_ref, data_table_ref, record_ids, start_time) {
    if (is.null(record_ids) || length(record_ids) == 0) {
      return(list(
        error_chunks = integer(0),
        num_chunks = 0,
        total_chunk_time = 0
      ))
    }

    chunks <- split(record_ids, ceiling(seq_along(record_ids) / chunk_size))
    num_chunks <- length(chunks)

    log_message(conn, log_table_ref, "INFO", paste("Processing data in", num_chunks, "chunks of up to", chunk_size, "record IDs each"))

    processing_start_time <- Sys.time()
    data_table_created <- DBI::dbExistsTable(conn, name = data_table_name)
    error_chunks <- integer(0)
    total_chunk_time <- 0

    processed_ids <- character(0)
    if (data_table_created) {
      processed_ids_query <- paste0(
        "SELECT DISTINCT ", DBI::dbQuoteIdentifier(conn, record_id_name),
        " FROM ", data_table_ref
      )
      processed_ids <- tryCatch(
        {
          DBI::dbGetQuery(conn, processed_ids_query)[[1]]
        },
        error = function(e) {
          log_message(conn, log_table_ref, "WARNING", paste("Could not determine already processed IDs:", e$message))
          character(0)
        }
      )
    }

    if (verbose) {
      pb <- cli::cli_progress_bar(
        format = paste0(
          "Processing chunk [{cli::pb_current}/{cli::pb_total}] ",
          "[{cli::pb_bar}] {cli::pb_percent} | ETA: {cli::pb_eta}"
        ),
        total = num_chunks
      )
    }

    for (i in seq_along(chunks)) {
      chunk_id <- sprintf("%04d", i)
      chunk_record_ids <- chunks[[i]]

      if (length(processed_ids) > 0) {
        original_count <- length(chunk_record_ids)
        chunk_record_ids <- setdiff(chunk_record_ids, processed_ids)
        skipped_count <- original_count - length(chunk_record_ids)

        if (skipped_count > 0) {
          log_message(conn, log_table_ref, "INFO", paste("Skipping", skipped_count, "already processed records in chunk", i))

          if (length(chunk_record_ids) == 0) {
            if (verbose) {
              cli::cli_progress_update()
            }
            next
          }
        }
      }

      log_message(conn, log_table_ref, "INFO", paste("Processing chunk", i, "of", num_chunks, "with", length(chunk_record_ids), "record IDs"))

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

          if (ncol(chunk_data) == 0 || nrow(chunk_data) == 0) {
            log_message(conn, log_table_ref, "ERROR", paste("Chunk", i, "returned no data"))
            stop(paste("Chunk", i, "returned no data"))
          }

          chunk_data <- as.data.frame(lapply(chunk_data, as.character), stringsAsFactors = FALSE)

          if (!data_table_created) {
            if (ncol(chunk_data) > 0) {
              columns <- sapply(names(chunk_data), function(col) {
                col_id <- DBI::dbQuoteIdentifier(conn, col)
                paste(col_id, "TEXT")
              })
              column_spec <- paste(columns, collapse = ", ")
              DBI::dbExecute(conn, paste0("CREATE TABLE ", data_table_ref, " (", column_spec, ")"))
              data_table_created <- TRUE
              log_message(conn, log_table_ref, "INFO", paste("Created data table named", data_table_ref))
            } else {
              stop("Cannot create table: first chunk returned no data")
            }
          }

          if (nrow(chunk_data) > 0) {
            DBI::dbAppendTable(conn, name = data_table_name, chunk_data)
          }

          success_msg <- paste0(
            "Chunk ", chunk_id, " successfully transferred (",
            nrow(chunk_data), " rows)"
          )
          log_message(conn, log_table_ref, "SUCCESS", success_msg)

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
          if (beep) {
            tryCatch(
              {
                audio::play(audio::load.wave(system.file("audio/wilhelm.wav", package = "redquack")))
              },
              error = function(e) NULL
            )
          }
          chunk_total <- round(difftime(Sys.time(), chunk_start, units = "secs"))
          total_chunk_time <- total_chunk_time + as.numeric(chunk_total)
          formatted_chunk_sum <- format_elapsed_time(total_chunk_time)

          error_msg <- e$message

          if (verbose) {
            cli::cli_progress_done()
            cli::cli_alert_danger("Chunk {i}/{num_chunks}: Error - {error_msg} [{formatted_chunk_sum}]")
          }

          log_message(conn, log_table_ref, "ERROR", paste("Error processing chunk", chunk_id, ":", error_msg))

          list(success = FALSE, error_chunks = c(error_chunks, i), total_chunk_time = total_chunk_time)
        }
      )

      error_chunks <- chunk_result$error_chunks
      total_chunk_time <- chunk_result$total_chunk_time

      if (!chunk_result$success) {
        log_message(conn, log_table_ref, "WARNING", paste("Error in chunk", i, "- continuing with remaining chunks"))

        if (verbose) {
          cli::cli_progress_done()
          cli::cli_alert_warning("Error in chunk {i} - continuing with remaining chunks")

          pb <- cli::cli_progress_bar(
            format = paste0(
              "Processing chunk [{cli::pb_current}/{cli::pb_total}] ",
              "[{cli::pb_bar}] {cli::pb_percent} | ETA: {cli::pb_eta}"
            ),
            total = num_chunks,
            current = i
          )
        }
      }

      if (i < num_chunks) Sys.sleep(chunk_delay)
    }

    if (verbose) {
      cli::cli_progress_done()
    }

    result <- list(
      error_chunks = error_chunks,
      num_chunks = num_chunks,
      total_chunk_time = round(total_chunk_time)
    )

    chunks <- NULL
    record_ids <- NULL
    gc(FALSE)

    result
  }

  finalize_and_report <- function(data_table_ref, log_table_ref, chunk_results, start_time) {
    error_chunks <- chunk_results$error_chunks
    num_chunks <- chunk_results$num_chunks
    total_chunk_time <- chunk_results$total_chunk_time

    successful_chunks <- num_chunks - length(error_chunks)
    failed_chunks <- length(error_chunks)

    optimize_data_types(conn, data_table_ref, log_table_ref, verbose)

    record_count_query <- paste0("SELECT COUNT(*) AS count FROM ", data_table_ref)
    record_count <- DBI::dbGetQuery(conn, record_count_query)$count
    formatted_chunk_time <- format_elapsed_time(total_chunk_time)

    if (verbose) {
      cli::cli_alert_success("Inserted {record_count} rows into database in {formatted_chunk_time}")
    }

    log_message(conn, log_table_ref, "INFO", paste("Successfully Inserted", record_count, "rows into database"))

    end_time <- Sys.time()
    elapsed <- difftime(end_time, start_time, units = "secs")
    formatted_time <- format_elapsed_time(as.numeric(elapsed))
    formatted_chunk_time <- format_elapsed_time(total_chunk_time)

    if (length(error_chunks) > 0) {
      error_message <- paste(
        "Errors occurred in chunks:",
        paste(error_chunks, collapse = ", ")
      )
      cli::cli_alert_danger("{failed_chunks} of {num_chunks} chunks failed processing")
      log_message(conn, log_table_ref, "ERROR", error_message)

      log_message(conn, log_table_ref, "WARNING", paste(
        "Transfer partially completed in", formatted_time,
        "with", successful_chunks, "of", num_chunks, "chunks successful,",
        failed_chunks, "failed"
      ))
    } else {
      log_message(conn, log_table_ref, "INFO", paste(
        "Transfer completed in", formatted_time,
        "with", successful_chunks, "of", num_chunks, "chunks successful,",
        failed_chunks, "failed"
      ))
    }

    result <- list(
      error_chunks = error_chunks,
      time_s = round(as.numeric(elapsed))
    )

    chunk_results <- NULL
    return(result)
  }

  attempt_transfer <- function(conn, failed_record_ids = NULL) {
    env <- setup_environment(conn)

    if (is.list(env) && !is.null(env$status)) {
      if (env$status == "complete") {
        return(list(
          success = TRUE,
          error_chunks = integer(0),
          time_s = 0
        ))
      }
    }

    data_table_ref <- env$data_table_ref
    log_table_ref <- env$log_table_ref
    start_time <- env$start_time

    log_message(conn, log_table_ref, "INFO", "Transfer started")

    if (is.null(failed_record_ids)) {
      record_ids <- fetch_record_ids(log_table_ref, data_table_ref, start_time)
    } else {
      log_message(conn, log_table_ref, "INFO", paste("Processing", length(failed_record_ids), "record IDs"))
      if (verbose) {
        cli::cli_alert_info("Processing {length(failed_record_ids)} record IDs")
      }
      record_ids <- failed_record_ids
    }

    if (is.null(record_ids)) {
      log_message(conn, log_table_ref, "INFO", "No new records to process")

      if (DBI::dbExistsTable(conn, name = data_table_name)) {
        elapsed_sec <- difftime(Sys.time(), start_time, units = "secs")
        log_message(conn, log_table_ref, "INFO", paste("Transfer completed in", format_elapsed_time(elapsed_sec)))
        return(list(
          success = TRUE,
          error_chunks = integer(0),
          time_s = round(as.numeric(elapsed_sec))
        ))
      } else {
        log_message(conn, log_table_ref, "ERROR", "No records returned from REDCap")
        if (verbose) {
          cli::cli_alert_danger("No records returned from REDCap")
        }
        return(list(
          success = FALSE,
          error_chunks = integer(0),
          time_s = round(as.numeric(difftime(Sys.time(), start_time, units = "secs")))
        ))
      }
    }

    chunk_results <- process_chunks(
      log_table_ref,
      data_table_ref,
      record_ids,
      start_time
    )

    error_chunks <- chunk_results$error_chunks
    chunks <- split(record_ids, ceiling(seq_along(record_ids) / chunk_size))
    failed_ids <- NULL

    if (length(error_chunks) > 0) {
      failed_ids <- unlist(chunks[error_chunks], use.names = FALSE)
      log_message(conn, log_table_ref, "INFO", paste("Identified", length(failed_ids), "record IDs in", length(error_chunks), "failed chunks"))
    }

    record_ids <- NULL
    chunks <- NULL
    gc(FALSE)

    result <- finalize_and_report(data_table_ref, log_table_ref, chunk_results, start_time)

    chunk_results <- NULL
    gc(FALSE)

    if (beep) {
      if (length(result$error_chunks) == 0) {
        tryCatch(
          {
            audio::play(audio::load.wave(system.file("audio/quack.wav", package = "redquack")))
          },
          error = function(e) NULL
        )
      }
    }

    success <- length(result$error_chunks) == 0
    if (!success) {
      if (verbose) {
        cli::cli_alert_danger("Transfer incomplete with {length(failed_ids)} failed records")
      }
      log_message(conn, log_table_ref, "ERROR", paste("Transfer incomplete with", length(failed_ids), "failed records"))
    }

    result$success <- success
    result <- list(
      success = result$success,
      error_chunks = result$error_chunks,
      time_s = result$time_s
    )

    class(result) <- c("redcap_transfer_result", class(result))
    return(result)
  }

  main_process <- function(conn) {
    result <- attempt_transfer(conn)
    return(result)
  }

  registerS3method("as.logical", "redcap_transfer_result", function(x, ...) {
    return(x$success)
  }, envir = .GlobalEnv)

  result <- main_process(conn)
  return(result)
}
