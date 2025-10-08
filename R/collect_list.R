#' Collect a Database Table into to List of REDCap Instruments
#'
#' @description
#' Takes a database table reference (tbl) and collects it into a list of instruments
#' with column and value labels and optional coded value conversion.
#' This function works in the tidy style with dplyr and separates the data by REDCap instruments.
#' Use \code{collect_labeled_list} as an alias for the same functionality.
#'
#' @param data A tbl object referencing a database table (created with `tbl(conn, "data")`).
#' @param cols Logical indicating whether to apply column (variable) labels.
#'   Default is FALSE.
#' @param vals Logical indicating whether to apply value labels to coded variables.
#'   Default is FALSE.
#' @param convert Logical indicating whether to convert labeled values
#'   to their text labels (e.g., 0/1 becomes "No"/"Yes"). Default is FALSE.
#' @param metadata_table_name Character string specifying the metadata table name.
#'   Default is "metadata".
#'
#' @return
#' Returns a named list of data frames, one per instrument.
#' If only one instrument is present, returns the single data frame instead of a list.
#'
#' @examples
#' \dontrun{
#' library(redquack)
#' library(dplyr)
#'
#' conn <- use_duckdb()
#'
#' result <- redcap_to_db(
#'   conn,
#'   url = "https://bbmc.ouhsc.edu/redcap/api/",
#'   token = "9A81268476645C4E5F03428B8AC3AA7B"
#' )
#'
#' # Convert table to a list of instruments
#' instruments <- tbl_redcap(conn) |>
#'   collect_list()
#'
#' # Control labeling behavior
#' instruments_no_val_labels <- tbl_redcap(conn) |>
#'   collect_list(cols = FALSE)
#'
#' # Convert coded values to text labels
#' instruments_with_codes <- tbl_redcap(conn) |>
#'   collect_list(convert = TRUE)
#'
#' # Works with filtered data
#' filtered_instruments <- tbl_redcap(conn) |>
#'   filter(name_last == "Nutmouse") |>
#'   collect_list()
#'
#' remove_duckdb(conn)
#' }
#'
#' @seealso
#' \code{\link{redcap_to_db}} for transferring data to a database
#' \code{\link{collect_labeled}} for collecting labeled data as a single data frame
#'
#' @importFrom dplyr collect
#' @importFrom dbplyr remote_con remote_name
#' @importFrom labelled is.labelled val_labels to_character var_label set_variable_labels
#' @importFrom cli cli_abort cli_warn
#'
#' @export
collect_list <- function(
    data,
    cols = FALSE,
    vals = FALSE,
    convert = FALSE,
    metadata_table_name = "metadata") {
  if (!inherits(data, "tbl_sql")) {
    cli::cli_abort("collect_list() expects a tbl object. Use tbl(conn, 'data_table_name') to create one.")
  }

  conn <- dbplyr::remote_con(data)

  # Use metadata table name parameter

  processed_data <- data

  if (cols || vals || convert) {
    processed_data <- collect_labeled(processed_data, cols = cols, vals = vals, convert = convert, metadata_table_name = metadata_table_name)
  }

  collected_data <- if (inherits(processed_data, "tbl_sql")) {
    dplyr::collect(processed_data)
  } else {
    processed_data
  }

  if (is.null(collected_data) || nrow(collected_data) == 0) {
    return(NULL)
  }

  # Read metadata for instrument organization
  metadata_data <- tryCatch(
    {
      metadata(conn, metadata_table_name)
    },
    error = function(e) {
      cli::cli_warn("Could not read metadata: {e$message}")
      NULL
    }
  )

  # REDCap organizes fields by forms/instruments - split collected data accordingly
  if (!is.null(metadata_data) && is.data.frame(metadata_data) && nrow(metadata_data) > 0) {
    # Get record ID field name from metadata
    record_id_field <- "record_id"
    config_row <- metadata_data[metadata_data$field_name == "__record_id_name__", ]
    if (nrow(config_row) > 0 && !is.na(config_row$field_label[1])) {
      record_id_field <- config_row$field_label[1]
    }

    instruments <- unique(metadata_data$form_name)
    instruments <- instruments[!is.na(instruments) & instruments != ""]

    if (length(instruments) == 0) {
      return(collected_data)
    }

    result_list <- list()

    for (instrument in instruments) {
      # Get base field names from metadata for this instrument
      instrument_fields <- metadata_data$field_name[metadata_data$form_name == instrument]
      instrument_fields <- instrument_fields[!is.na(instrument_fields)]

      # Start with record ID and base fields
      all_fields <- c(record_id_field, instrument_fields)
      
      # For checkbox fields, find all related columns (REDCap convention: field___1, field___2, etc.)
      checkbox_fields <- metadata_data$field_name[
        metadata_data$form_name == instrument & 
        metadata_data$field_type == "checkbox" & 
        !is.na(metadata_data$field_type)
      ]
      
      if (length(checkbox_fields) > 0) {
        for (checkbox_field in checkbox_fields) {
          # Find all columns that match the checkbox pattern: field___*
          checkbox_pattern <- paste0("^", checkbox_field, "___")
          checkbox_columns <- names(collected_data)[grepl(checkbox_pattern, names(collected_data))]
          all_fields <- c(all_fields, checkbox_columns)
        }
      }
      
      # Add completion field last (REDCap convention: instrument_complete)
      completion_field <- paste0(instrument, "_complete")
      if (completion_field %in% names(collected_data)) {
        all_fields <- c(all_fields, completion_field)
      }

      all_fields <- unique(all_fields)

      # Only include fields that exist in the collected data
      available_fields <- intersect(all_fields, names(collected_data))

      if (length(available_fields) > 0) {
        instrument_data <- collected_data[, available_fields, drop = FALSE]

        if (nrow(instrument_data) > 0) {
          result_list[[instrument]] <- instrument_data
        }
      }
    }

    # Return single data frame if only one instrument, otherwise return list
    if (length(result_list) == 1) {
      return(result_list[[1]])
    } else {
      return(result_list)
    }
  }

  return(collected_data)
}

#' @rdname collect_list
#' @export
collect_labeled_list <- function(data, cols = TRUE, vals = TRUE, convert = TRUE, metadata_table_name = "metadata") {
  collect_list(data = data, cols = cols, vals = vals, convert = convert, metadata_table_name = metadata_table_name)
}
