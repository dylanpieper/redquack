# Inspect Project Data Table Structure

Inspects the structure of the project data table showing column
information including name, type, and properties. This is a convenience
wrapper around DBI::dbGetQuery() for examining table schema. Uses the
data table name stored in the connection attributes if available.

## Usage

``` r
inspect(conn, table_name = NULL)
```

## Arguments

- conn:

  A DuckDB connection object.

- table_name:

  Character string specifying the table name. If NULL, uses the table
  name stored in connection attributes. Default is NULL.

## Value

A data frame containing table information with columns for column ID,
name, type, not null status, default value, and primary key.

## Examples

``` r
if (FALSE) { # \dontrun{
table_info <- inspect(conn)
} # }
```
