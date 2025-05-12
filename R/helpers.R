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
