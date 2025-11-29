# Get Metadata Table

Creates a tbl reference to the metadata table in the database and
automatically collects it as a data frame. Uses the metadata table name
stored in the connection attributes if available.

## Usage

``` r
metadata(conn, metadata_table_name = NULL)
```

## Arguments

- conn:

  A DuckDB connection object.

- metadata_table_name:

  Character string specifying the metadata table name. If NULL, uses the
  table name stored in connection attributes. Default is NULL.

## Value

A data frame containing the metadata.

## Examples

``` r
if (FALSE) { # \dontrun{
meta <- metadata(conn)
} # }
```
