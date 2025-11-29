# Close DuckDB Connection

Closes a DuckDB connection.

## Usage

``` r
close_duckdb(conn)
```

## Arguments

- conn:

  A DuckDB connection object.

## Value

Invisible NULL.

## Examples

``` r
if (FALSE) { # \dontrun{
conn <- use_duckdb()
# Use the connection...
close_duckdb(conn)
} # }
```
