# Get REDCap Logs Table

Creates a tbl reference to the REDCap logs table in the database and
automatically collects it as a data frame. Uses the REDCap logs table
name stored in the connection attributes if available.

## Usage

``` r
redcap_log(conn, redcap_log_table_name = NULL)
```

## Arguments

- conn:

  A DuckDB connection object.

- redcap_log_table_name:

  Character string specifying the REDCap logs table name. If NULL, uses
  the table name stored in connection attributes. Default is NULL.

## Value

A data frame containing the REDCap audit log data.

## Examples

``` r
if (FALSE) { # \dontrun{
redcap_log <- redcap_log(conn)
} # }
```
