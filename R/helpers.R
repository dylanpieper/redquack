# EXTERNAL ----------------------------------------------------------------

#' Assign List to Global Environment
#'
#' @description
#' Assign a list of instruments (data frames) to the global environment.
#' Each element of the list becomes a separate object in the global environment.
#'
#' @param instruments A named list of data frames, typically output from tbl_to_list().
#'
#' @return
#' Invisibly returns NULL. Side effect: assigns objects to the global environment.
#'
#' @examples
#' \dontrun{
#' tbl(conn, "data") |>
#'   tbl_to_list() |>
#'   list_to_env()
#' }
#'
#' @export
list_to_env <- function(instruments) {
  list2env(instruments, envir = .GlobalEnv)
  invisible(NULL)
}

#' Create DuckDB Connection
#'
#' @description
#' Creates a DuckDB connection to the REDCap database file.
#'
#' @param dbname Character string specifying the database file name.
#'   Default is "redcap.duckdb".
#'
#' @return
#' A DuckDB connection object.
#'
#' @examples
#' \dontrun{
#' conn <- use_duckdb()
#' # Use the connection...
#' remove_duckdb(conn)
#' }
#'
#' @importFrom DBI dbConnect
#' @importFrom duckdb duckdb
#'
#' @export
use_duckdb <- function(dbname = "redcap.duckdb") {
  DBI::dbConnect(duckdb::duckdb(), dbname)
}

#' Close DuckDB Connection
#'
#' @description
#' Closes a DuckDB connection.
#'
#' @param conn A DuckDB connection object.
#'
#' @return
#' Invisible NULL.
#'
#' @examples
#' \dontrun{
#' conn <- use_duckdb()
#' # Use the connection...
#' close_duckdb(conn)
#' }
#'
#' @importFrom DBI dbDisconnect
#'
#' @export
close_duckdb <- function(conn) {
  DBI::dbDisconnect(conn)
}

#' Remove DuckDB Database
#'
#' @description
#' Closes a DuckDB connection and removes the database file.
#'
#' @param conn A DuckDB connection object.
#' @param dbname Character string specifying the database file name.
#'   Default is "redcap.duckdb".
#'
#' @return
#' Invisible NULL.
#'
#' @examples
#' \dontrun{
#' conn <- use_duckdb()
#' # Use the connection...
#' remove_duckdb(conn)
#' }
#'
#' @importFrom DBI dbDisconnect
#'
#' @export
remove_duckdb <- function(conn, dbname = "redcap.duckdb") {
  DBI::dbDisconnect(conn)
  if (file.exists(dbname)) {
    file.remove(dbname)
  }
  if (file.exists(paste0(dbname, ".wal"))) {
    file.remove(paste0(dbname, ".wal"))
  }
}

#' Create REDCap Data Table Reference
#'
#' @description
#' Creates a tbl reference to the main REDCap data table in the database.
#' The original table name is stored as an attribute to preserve context
#' through dplyr operations (limited to filter, select, arrange, and group_by).
#' Uses the data table name stored in the connection attributes if available.
#'
#' @param conn A DuckDB connection object.
#' @param table_name Character string specifying the table name.
#'   If NULL, uses the table name stored in connection attributes.
#'   Default is NULL.
#'
#' @return
#' A tbl_sql object referencing the data table with redcap_table attribute.
#'
#' @examples
#' \dontrun{
#' data <- tbl_redcap(conn)
#' }
#'
#' @importFrom dplyr tbl arrange filter group_by select
#' @importFrom rlang enquos quo_is_symbol quo_get_expr sym
#' @importFrom dbplyr remote_con remote_name
#'
#' @export
tbl_redcap <- function(conn, table_name = NULL) {
  table_name <- table_name %||% attr(conn, "data_table_name") %||% "data"
  tbl_obj <- dplyr::tbl(conn, table_name)
  attr(tbl_obj, "redcap_table") <- table_name
  class(tbl_obj) <- c("tbl_redcap", class(tbl_obj))
  tbl_obj
}

#' Get Metadata Table
#'
#' @description
#' Creates a tbl reference to the metadata table in the database and
#' automatically collects it as a data frame. Uses the metadata table name
#' stored in the connection attributes if available.
#'
#' @param conn A DuckDB connection object.
#' @param metadata_table_name Character string specifying the metadata table name.
#'   If NULL, uses the table name stored in connection attributes.
#'   Default is NULL.
#'
#' @return
#' A data frame containing the metadata.
#'
#' @examples
#' \dontrun{
#' meta <- metadata(conn)
#' }
#'
#' @importFrom dplyr tbl collect
#'
#' @export
metadata <- function(conn, metadata_table_name = NULL) {
  table_name <- metadata_table_name %||% attr(conn, "metadata_table_name") %||% "metadata"
  dplyr::tbl(conn, table_name) |>
    dplyr::collect()
}

#' Get Log Table
#'
#' @description
#' Creates a tbl reference to the log table in the database and
#' automatically collects it as a data frame. Uses the log table name
#' stored in the connection attributes if available.
#'
#' @param conn A DuckDB connection object.
#' @param log_table_name Character string specifying the log table name.
#'   If NULL, uses the table name stored in connection attributes.
#'   Default is NULL.
#'
#' @return
#' A data frame containing the log data.
#'
#' @examples
#' \dontrun{
#' log <- logs(conn)
#' }
#'
#' @importFrom dplyr tbl collect
#'
#' @export
logs <- function(conn, log_table_name = NULL) {
  table_name <- log_table_name %||% attr(conn, "log_table_name") %||% "logs"
  dplyr::tbl(conn, table_name) |>
    dplyr::collect()
}

#' Inspect Data Table Structure
#'
#' @description
#' Inspects the structure of the data table showing column information
#' including name, type, and properties. This is a convenience wrapper
#' around DBI::dbGetQuery() for examining table schema. Uses the data table name
#' stored in the connection attributes if available.
#'
#' @param conn A DuckDB connection object.
#' @param table_name Character string specifying the table name.
#'   If NULL, uses the table name stored in connection attributes.
#'   Default is NULL.
#'
#' @return
#' A data frame containing table information with columns for
#' column ID, name, type, not null status, default value, and primary key.
#'
#' @examples
#' \dontrun{
#' table_info <- inspect(conn)
#' }
#'
#' @importFrom DBI dbGetQuery
#'
#' @export
inspect <- function(conn, table_name = NULL) {
  table_name <- table_name %||% attr(conn, "data_table_name") %||% "data"
  DBI::dbGetQuery(conn, paste0("PRAGMA table_info(", table_name, ")"))
}

#' Save Data to Parquet
#'
#' @description
#' Saves data directly from the database to a Parquet file using
#' DuckDB's native COPY command. This is much faster than reading
#' into R first and creates smaller files for easy sharing. Uses the data table name
#' stored in the connection attributes if available.
#'
#' @param conn A DuckDB connection object.
#' @param file_path Character string specifying the output file path.
#'   Default is "redcap.parquet".
#' @param table_name Character string specifying the source table name.
#'   If NULL, uses the table name stored in connection attributes.
#'   Default is NULL.
#' @param query Character string with custom SQL query to export.
#'   If provided, table_name is ignored.
#'
#' @return
#' Invisible NULL. Side effect: creates Parquet file at specified path.
#'
#' @examples
#' \dontrun{
#' # Save entire data table
#' save_parquet(conn, "redcap.parquet")
#' }
#'
#' @importFrom DBI dbExecute
#'
#' @export
save_parquet <- function(conn, file_path = "redcap.parquet", table_name = NULL, query = NULL) {
  if (!is.null(query)) {
    sql_cmd <- paste0("COPY (", query, ") TO '", file_path, "' (FORMAT PARQUET)")
  } else {
    table_name <- table_name %||% attr(conn, "data_table_name") %||% "data"
    sql_cmd <- paste0("COPY (SELECT * FROM ", table_name, ") TO '", file_path, "' (FORMAT PARQUET)")
  }

  DBI::dbExecute(conn, sql_cmd)
  invisible(NULL)
}

# INTERNAL ----------------------------------------------------------------

#' Null-coalescing operator
#'
#' @description
#' Similar to `%||%` in rlang. Returns the left-hand side if not NULL,
#' otherwise returns the right-hand side.
#'
#' @param x Left-hand side value
#' @param y Right-hand side value (used if x is NULL)
#' @return x if not NULL, otherwise y
#' @keywords internal
#' @noRd
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Check if connection is of specified database class
#'
#' @param conn A DBI connection object
#' @param class The expected database connection class (default: "duckdb_connection")
#' @return Logical indicating if connection matches the specified class
#' @keywords internal
#' @noRd
is_db_class <- function(conn, class = "duckdb_connection") {
  class(conn)[[1]] == class
}

#' Log a message to a database table
#'
#' @param conn A DBI connection object to a database
#' @param log_table_ref The name of the log table in database
#' @param type Type of log message
#' @param msg The message to log
#' @keywords internal
#' @noRd
log_message <- function(conn, log_table_ref, type, msg) {
  if (!is.null(log_table_ref)) {
    timestamp <- Sys.time()
    DBI::dbExecute(
      conn,
      paste0(
        "INSERT INTO ", log_table_ref, " (timestamp, type, message) ",
        "VALUES (?, ?, ?)"
      ),
      list(timestamp, type, msg)
    )
  }
}


#' Parse REDCap Choice Values
#'
#' @description
#' Parse the select_choices_or_calculations field from REDCap metadata
#' into a named list of value-label pairs.
#'
#' @param choices_string Character string from the select_choices_or_calculations field.
#'
#' @return
#' A named list where names are the coded values and values are the labels.
#' Returns NULL if the input is empty or NA.
#'
#' @examples
#' \dontrun{
#' choices <- "1, Yes | 0, No | 99, Unknown"
#' parse_choices(choices)
#' # Returns: list("1" = "Yes", "0" = "No", "99" = "Unknown")
#' }
#'
#' @keywords internal
#' @noRd
parse_choices <- function(choices_string) {
  if (is.na(choices_string) || choices_string == "" || is.null(choices_string)) {
    return(NULL)
  }

  choices <- strsplit(choices_string, "\\s*\\|\\s*")[[1]]

  choice_codes <- character()
  choice_labels <- character()

  for (choice in choices) {
    # REDCap format: "code, label"
    comma_pos <- regexpr(",\\s*", choice)
    if (comma_pos > 0) {
      code <- trimws(substr(choice, 1, comma_pos - 1))
      label <- trimws(substr(choice, comma_pos + attr(comma_pos, "match.length"), nchar(choice)))
      choice_codes <- c(choice_codes, code)
      choice_labels <- c(choice_labels, label)
    }
  }

  if (length(choice_codes) > 0) {
    choice_vector <- setNames(choice_labels, choice_codes)
    return(choice_vector)
  }

  return(NULL)
}

# S3 METHODS FOR PRESERVING REDCAP_TABLE ATTRIBUTE --------------------

#' Preserve redcap_table attribute through dplyr operations
#' @keywords internal
#' @noRd
preserve_redcap_attr <- function(result, original) {
  if (!is.null(attr(original, "redcap_table"))) {
    attr(result, "redcap_table") <- attr(original, "redcap_table")
    if (!"tbl_redcap" %in% class(result)) {
      class(result) <- c("tbl_redcap", class(result))
    }
  }
  result
}

# All dplyr methods use the same pattern: preserve REDCap table attribute
#' @export
filter.tbl_redcap <- function(.data, ...) {
  result <- NextMethod()
  preserve_redcap_attr(result, .data)
}

#' @export
select.tbl_redcap <- function(.data, ...) {
  # Get the selected expressions
  selected_vars <- rlang::enquos(...)

  # Check if we need to preserve record ID for multi-instrument support
  conn <- dbplyr::remote_con(.data)
  base_table_name <- attr(.data, "redcap_table") %||% dbplyr::remote_name(.data)

  # Try to get the record ID field name from metadata
  record_id_field <- tryCatch(
    {
      metadata_data <- metadata(conn, "metadata")
      config_row <- metadata_data[metadata_data$field_name == "__record_id_name__", ]
      if (nrow(config_row) > 0 && !is.na(config_row$field_label[1])) {
        config_row$field_label[1]
      } else {
        "record_id"
      }
    },
    error = function(e) "record_id"
  )

  # Check if record ID is already being selected
  selected_names <- sapply(selected_vars, function(x) {
    if (rlang::quo_is_symbol(x)) {
      as.character(rlang::quo_get_expr(x))
    } else {
      NA
    }
  })

  record_id_already_selected <- record_id_field %in% selected_names

  # If record ID is not selected and we have multiple instruments, add it
  if (!record_id_already_selected) {
    # Check if we have multiple instruments in metadata
    has_multiple_instruments <- tryCatch(
      {
        metadata_data <- metadata(conn, "metadata")
        instruments <- unique(metadata_data$form_name[!is.na(metadata_data$form_name) & metadata_data$form_name != ""])
        length(instruments) > 1
      },
      error = function(e) FALSE
    )

    if (has_multiple_instruments) {
      # Add record ID to the selection
      record_id_quo <- rlang::sym(record_id_field)
      result <- NextMethod(.data, !!record_id_quo, ...)
    } else {
      result <- NextMethod()
    }
  } else {
    result <- NextMethod()
  }

  preserve_redcap_attr(result, .data)
}

#' @export
arrange.tbl_redcap <- function(.data, ...) {
  result <- NextMethod()
  preserve_redcap_attr(result, .data)
}

#' @export
group_by.tbl_redcap <- function(.data, ...) {
  result <- NextMethod()
  preserve_redcap_attr(result, .data)
}
