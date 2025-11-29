# Collect Labeled Data from Database Table

Collect data from a database table reference (tbl) and apply column
and/or value labels from REDCap metadata using the labelled package.
This function works in the tidy style with dplyr.

## Usage

``` r
collect_labeled(
  data,
  cols = TRUE,
  vals = TRUE,
  convert = TRUE,
  metadata_table_name = "metadata"
)
```

## Arguments

- data:

  A tbl_sql object (database table reference) to apply labels to. The
  connection and table name are automatically extracted.

- cols:

  Logical indicating whether to apply column (variable) labels. Default
  is TRUE.

- vals:

  Logical indicating whether to apply value labels to coded variables.
  Default is TRUE.

- convert:

  Logical indicating whether to convert coded values to their text
  labels (e.g., 0/1 becomes "No"/"Yes"). Default is TRUE.

- metadata_table_name:

  Character string specifying the metadata table name. Default is
  "metadata".

## Value

A data frame with labels applied according to the cols and vals
parameters. If convert = TRUE, coded values are converted to text.

## Examples

``` r
if (FALSE) { # \dontrun{
library(redquack)
duckdb <- DBI::dbConnect(duckdb::duckdb(), "redcap.duckdb")

# Apply both column and value labels (default)
labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled()

# Apply only column labels
col_labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(vals = FALSE)

# Apply only value labels
val_labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(cols = FALSE)

# Apply labels and convert values to text
labeled_data <- tbl_redcap(duckdb, "data") |> collect_labeled(convert = TRUE)

# Explicit metadata table name (useful after complex filtering)
labeled_data <- tbl(duckdb, "data") |>
  dplyr::filter(name_last == "Nutmouse") |>
  collect_labeled(metadata_table_name = "metadata")

DBI::dbDisconnect(duckdb)
} # }
```
