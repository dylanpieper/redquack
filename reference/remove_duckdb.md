# Remove DuckDB Database

Closes a DuckDB connection and removes the database file.

## Usage

``` r
remove_duckdb(conn, dbname = "redcap.duckdb")
```

## Arguments

- conn:

  A DuckDB connection object.

- dbname:

  Character string specifying the database file name. Default is
  "redcap.duckdb".

## Value

Invisible NULL.

## Examples

``` r
if (FALSE) { # \dontrun{
conn <- use_duckdb()
# Use the connection...
remove_duckdb(conn)
} # }
```
