# Create REDCap Data Table Reference

Creates a tbl reference to the main REDCap data table in the database.
The original table name is stored as an attribute to preserve context
through dplyr operations (limited to filter, select, arrange, and
group_by). Uses the data table name stored in the connection attributes
if available.

## Usage

``` r
tbl_redcap(conn, table_name = NULL)
```

## Arguments

- conn:

  A DuckDB connection object.

- table_name:

  Character string specifying the table name. If NULL, uses the table
  name stored in connection attributes. Default is NULL.

## Value

A tbl_sql object referencing the data table with redcap_table attribute.

## Examples

``` r
if (FALSE) { # \dontrun{
data <- tbl_redcap(conn)
} # }
```
