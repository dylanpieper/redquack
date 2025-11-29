# Save Data to Parquet

Saves data directly from the database to a Parquet file using DuckDB's
native COPY command. This is much faster than reading into R first and
creates smaller files for easy sharing. Uses the data table name stored
in the connection attributes if available.

## Usage

``` r
save_parquet(
  conn,
  file_path = "redcap.parquet",
  table_name = NULL,
  query = NULL
)
```

## Arguments

- conn:

  A DuckDB connection object.

- file_path:

  Character string specifying the output file path. Default is
  "redcap.parquet".

- table_name:

  Character string specifying the source table name. If NULL, uses the
  table name stored in connection attributes. Default is NULL.

- query:

  Character string with custom SQL query to export. If provided,
  table_name is ignored.

## Value

Invisible NULL. Side effect: creates Parquet file at specified path.

## Examples

``` r
if (FALSE) { # \dontrun{
# Save entire data table
save_parquet(conn, "redcap.parquet")
} # }
```
