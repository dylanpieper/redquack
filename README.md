# redquack <img src="man/figures/redquack-hex.png" align="right" width="140"/>

[![CRAN status](https://www.r-pkg.org/badges/version/redquack)](https://cran.r-pkg.org/package=redquack) [![R-CMD-check](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml)

Transfer [REDCap](https://www.project-redcap.org/) data to [DuckDB](https://duckdb.org/) with minimal memory overhead, designed for large datasets that exceed available RAM.

## Motivation

R objects live entirely in RAM, causing three problems if not using a specialized framework:

1.  You must load full datasets even if you only need a subset
2.  Unused objects still consume memory
3.  Large datasets can easily exceed available RAM

redquack's solution to this problem is to:

1.  Request all of the REDCap record IDs to sequence in chunks
2.  Process each chunk of the REDCap data in one R object at a time
3.  Remove each object from memory after it has been transferred to DuckDB

## Features

-   Chunked transfers for memory efficiency
-   Auto-resume from interruptions
-   Optimal data type conversion
-   Timestamped operation logs
-   Configurable API request retries
-   Real-time progress indicators
-   Completion notifications (🔊 🦆)

## Installation

From CRAN:

``` r
install.packages("redquack")
```

Development version:

``` r
# install.packages("pak")
pak::pak("dylanpieper/redquack")
```

## Basic Usage

Data from REDCap is transferred to DuckDB in configurable chunks of record IDs:

``` r
library(redquack)

con <- redcap_to_duckdb(
  redcap_uri = "https://redcap.example.org/api/",
  token = "YOUR_API_TOKEN",
  record_id_name = "record_id",
  chunk_size = 1000  
  # Increase chunk size for memory-efficient systems (faster)
  # Decrease chunk size for memory-constrained systems (slower)
)
```

By default, the function returns the DuckDB connection from the output file `redcap.duckdb`.

### Data Manipulation

Query and collect the data with `dplyr`:

``` r
library(dplyr)

demographics <- tbl(con, "data") |>
  filter(demographics_complete == 2) |>
  select(record_id, age, race, gender) |>
  collect()
```

Create a Parquet file directly from DuckDB (efficient for sharing data):

``` r
DBI::dbExecute(con, "COPY (SELECT * FROM data) TO 'redcap.parquet' (FORMAT PARQUET)")
```

Remember to close the connection when finished:

``` r
DBI::dbDisconnect(con)
```

### Workflow Mode

For scripted workflows or automated processes where you don't need to return the connection, you can use the function in workflow mode:

```r
success <- redcap_to_duckdb(
  redcap_uri = "https://redcap.example.org/api/",
  token = "YOUR_API_TOKEN",
  record_id_name = "record_id",
  return_duckdb = FALSE
)

if (success) {
  message("Data transfer completed successfully!")
} else {
  stop("Data transfer failed or is incomplete!")
}
```

When `return_duckdb = FALSE`, the function returns a logical value:
- `TRUE` for a complete successful transfer
- `FALSE` for a failed or partially completed transfer

Workflow mode automatically tries to resume incomplete transfers up to `max_retries` times.

## Database Structure

The DuckDB database created by `redcap_to_duckdb()` contains two tables:

1.  `data`: Contains all exported REDCap records with optimized column types

    ``` r
    DBI::dbGetQuery(con, "SELECT * FROM data LIMIT 10")
    ```

2.  `log`: Contains timestamped logs of the transfer process for troubleshooting

    ``` r
    DBI::dbGetQuery(con, "SELECT timestamp, type, message FROM log ORDER BY timestamp")
    ```
