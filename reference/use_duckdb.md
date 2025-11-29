# Create DuckDB Connection

Creates a DuckDB connection to the REDCap database file.

## Usage

``` r
use_duckdb(dbname = "redcap.duckdb")
```

## Arguments

- dbname:

  Character string specifying the database file name. Default is
  "redcap.duckdb".

## Value

A DuckDB connection object.

## Examples

``` r
if (FALSE) { # \dontrun{
conn <- use_duckdb()
# Use the connection...
remove_duckdb(conn)
} # }
```
