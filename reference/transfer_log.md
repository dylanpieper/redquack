# Get Transfer Logs Table

Creates a tbl reference to the transfer logs table in the database and
automatically collects it as a data frame. Uses the transfer logs table
name stored in the connection attributes if available.

## Usage

``` r
transfer_log(conn, transfer_log_table_name = NULL)
```

## Arguments

- conn:

  A DuckDB connection object.

- transfer_log_table_name:

  Character string specifying the transfer logs table name. If NULL,
  uses the table name stored in connection attributes. Default is NULL.

## Value

A data frame containing the transfer log data.

## Examples

``` r
if (FALSE) { # \dontrun{
transfer_log <- transfer_log(conn)
} # }
```
