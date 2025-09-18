#' Collect Labeled Data from Database Table
#'
#' @description
#' Collect data from a database table reference (tbl) and apply column and/or
#' value labels from REDCap metadata using the labelled package. This function
#' works in the tidy style with dplyr.
#'
#' @param data A tbl_sql object (database table reference) to apply labels to.
#'   The connection and table name are automatically extracted.
#' @param cols Logical indicating whether to apply column (variable) labels.
#'   Default is TRUE.
#' @param vals Logical indicating whether to apply value labels to coded variables.
#'   Default is TRUE.
#' @param convert Logical indicating whether to convert coded values to their
#'   text labels (e.g., 0/1 becomes "No"/"Yes"). Default is TRUE.
#' @param metadata_table_name Character string specifying the metadata table name.
#'   Default is "metadata".
#'
#' @return
#' A data frame with labels applied according to the cols and vals parameters.
#' If convert = TRUE, coded values are converted to text.
#'
#' @examples
#' \dontrun{
#' library(redquack)
#' duckdb <- DBI::dbConnect(duckdb::duckdb(), "redcap.duckdb")
#'
#' # Apply both column and value labels (default)
#' labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled()
#'
#' # Apply only column labels
#' col_labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(vals = FALSE)
#'
#' # Apply only value labels
#' val_labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(cols = FALSE)
#'
#' # Apply labels and convert values to text
#' labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(convert = TRUE)
#'
#' # Explicit metadata table name (useful after complex filtering)
#' labeled_data <- tbl(duckdb, "data") |>
#'   dplyr::filter(name_last == "Nutmouse") |>
#'   collect_labeled(metadata_table_name = "metadata")
#'
#' DBI::dbDisconnect(duckdb)
#' }
#'
#' @importFrom labelled set_variable_labels set_value_labels var_label is.labelled val_labels to_character
#' @importFrom cli cli_warn cli_abort
#' @importFrom stats setNames
#' @importFrom rlang := !!
#'
#' @export
collect_labeled <- function(data, cols = TRUE, vals = TRUE, convert = TRUE, metadata_table_name = "metadata") {
  if (!cols && !vals) {
    cli::cli_abort("At least one of 'cols' or 'vals' must be TRUE")
  }

  if (!inherits(data, "tbl_sql")) {
    cli::cli_abort("collect_labeled() requires a tbl_sql object. Use tbl(conn, 'data_table_name') to create one.")
  }

  conn <- dbplyr::remote_con(data)

  # Use metadata table name parameter

  metadata_data <- tryCatch(
    {
      metadata(conn, metadata_table_name)
    },
    error = function(e) {
      # Check if table exists at all
      if (!DBI::dbExistsTable(conn, metadata_table_name)) {
        cli::cli_warn("Metadata table '{metadata_table_name}' does not exist. Run redcap_to_db() first to fetch metadata.")
      } else {
        cli::cli_warn("Could not read metadata: {e$message}")
      }
      return(NULL)
    }
  )

  if (is.null(metadata_data) || nrow(metadata_data) == 0) {
    cli::cli_warn("No metadata available. Returning data unchanged.")
    return(dplyr::collect(data))
  }

  collected_data <- dplyr::collect(data)

  if (cols) {
    var_labels <- list()
    for (field in names(collected_data)) {
      field_meta <- metadata_data[metadata_data$field_name == field, ]
      if (nrow(field_meta) > 0) {
        field_label <- field_meta$field_label[1]
        if (!is.na(field_label) && field_label != "") {
          var_labels[[field]] <- field_label
        }
      }
    }

    if (length(var_labels) > 0) {
      collected_data <- labelled::set_variable_labels(collected_data, .labels = var_labels)
    }
  }

  if (vals) {
    # Variable labels get overwritten during value labeling, so preserve them
    all_var_labels <- list()
    for (field in names(collected_data)) {
      all_var_labels[[field]] <- labelled::var_label(collected_data[[field]])
    }

    for (field in names(collected_data)) {
      field_meta <- metadata_data[metadata_data$field_name == field, ]
      if (nrow(field_meta) > 0 && !is.na(field_meta$select_choices_or_calculations[1])) {
        choices <- parse_choices(field_meta$select_choices_or_calculations[1])
        if (!is.null(choices) && length(choices) > 0) {
          # Value labels only make sense if data contains the actual choice codes
          col_data <- collected_data[[field]]
          choice_codes <- names(choices)
          data_values <- unique(as.character(col_data[!is.na(col_data)]))
          # Skip labeling if data doesn't contain any of the defined choice codes
          has_coded_values <- any(data_values %in% choice_codes)

          if (has_coded_values) {
            tryCatch(
              {
                col_data <- collected_data[[field]]

                # Handle numeric vs character data type alignment with choice codes
                if (is.numeric(col_data)) {
                  numeric_values <- suppressWarnings(as.numeric(names(choices)))
                  if (!any(is.na(numeric_values))) {
                    # REDCap stores as "1"="Male" but we need c(Male=1) for numeric columns
                    choice_labels <- as.character(choices)
                    choices <- setNames(numeric_values, choice_labels)
                  } else {
                    # Choice codes like "abnormal"/"normal" can't be numeric, convert column
                    collected_data[[field]] <- as.character(col_data)
                  }
                }

                collected_data <- labelled::set_value_labels(collected_data, !!field := choices)
              },
              error = function(e) {
                cli::cli_warn("Could not apply value labels to field '{field}': {e$message}")
              }
            )
          }
        }
      }
    }

    # Restore variable labels that were lost during value label application
    valid_var_labels <- all_var_labels[!sapply(all_var_labels, function(x) is.null(x) || is.na(x) || x == "")]
    if (length(valid_var_labels) > 0) {
      tryCatch(
        {
          collected_data <- labelled::set_variable_labels(collected_data, .labels = valid_var_labels)
        },
        error = function(e) {
          cli::cli_warn("Could not restore variable labels: {e$message}")
        }
      )
    }
  }

  if (convert) {
    # Conversion to character removes all labelling, so preserve variable labels
    var_labels <- list()
    for (col_name in names(collected_data)) {
      var_labels[[col_name]] <- labelled::var_label(collected_data[[col_name]])
    }

    for (col_name in names(collected_data)) {
      if (labelled::is.labelled(collected_data[[col_name]])) {
        val_labels <- labelled::val_labels(collected_data[[col_name]])
        if (length(val_labels) > 0) {
          collected_data[[col_name]] <- labelled::to_character(collected_data[[col_name]])
        }
      }
    }

    # Reapply variable labels after character conversion
    valid_labels <- var_labels[!sapply(var_labels, function(x) is.null(x) || is.na(x) || x == "")]
    if (length(valid_labels) > 0) {
      tryCatch(
        {
          collected_data <- labelled::set_variable_labels(collected_data, .labels = valid_labels)
        },
        error = function(e) {
          cli::cli_warn("Could not restore some variable labels: {e$message}")
        }
      )
    }
  }

  return(collected_data)
}
