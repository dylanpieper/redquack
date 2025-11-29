# Collect a Database Table into to List of REDCap Instruments

Takes a database table reference (tbl) and collects it into a list of
instruments with column and value labels and optional coded value
conversion. This function works in the tidy style with dplyr and
separates the data by REDCap instruments. Use `collect_labeled_list` as
an alias for the same functionality.

## Usage

``` r
collect_list(
  data,
  cols = FALSE,
  vals = FALSE,
  convert = FALSE,
  metadata_table_name = "metadata"
)

collect_labeled_list(
  data,
  cols = TRUE,
  vals = TRUE,
  convert = TRUE,
  metadata_table_name = "metadata"
)
```

## Arguments

- data:

  A tbl object referencing a database table (created with
  `tbl(conn, "data")`).

- cols:

  Logical indicating whether to apply column (variable) labels. Default
  is FALSE.

- vals:

  Logical indicating whether to apply value labels to coded variables.
  Default is FALSE.

- convert:

  Logical indicating whether to convert labeled values to their text
  labels (e.g., 0/1 becomes "No"/"Yes"). Default is FALSE.

- metadata_table_name:

  Character string specifying the metadata table name. Default is
  "metadata".

## Value

Returns a named list of data frames, one per instrument. If only one
instrument is present, returns the single data frame instead of a list.

## See also

[`redcap_to_db`](https://dylanpieper.github.io/redquack/reference/redcap_to_db.md)
for transferring data to a database
[`collect_labeled`](https://dylanpieper.github.io/redquack/reference/collect_labeled.md)
for collecting labeled data as a single data frame

## Examples

``` r
if (FALSE) { # \dontrun{
library(redquack)
library(dplyr)

conn <- use_duckdb()

result <- redcap_to_db(
  conn,
  url = "https://bbmc.ouhsc.edu/redcap/api/",
  token = "9A81268476645C4E5F03428B8AC3AA7B"
)

# Convert table to a list of instruments
instruments <- tbl_redcap(conn) |>
  collect_list()

# Control labeling behavior
instruments_no_val_labels <- tbl_redcap(conn) |>
  collect_list(cols = FALSE)

# Convert coded values to text labels
instruments_with_codes <- tbl_redcap(conn) |>
  collect_list(convert = TRUE)

# Works with filtered data
filtered_instruments <- tbl_redcap(conn) |>
  filter(name_last == "Nutmouse") |>
  collect_list()

remove_duckdb(conn)
} # }
```
