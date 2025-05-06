# redquack <img src="man/figures/redquack-hex.png" align="right" width="140"/>

[![CRAN status](https://www.r-pkg.org/badges/version/redquack)](https://cran.r-pkg.org/package=redquack) [![R-CMD-check](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml)

Transfer [REDCap](https://www.project-redcap.org/) data to [DuckDB](https://duckdb.org/) with minimal memory overhead, designed for large projects with datasets that exceed available RAM.

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

-   Auto-resume from incomplete transfers
-   Auto-convert column types
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

## Setup API Token

Your REDCap API token allows R to interface with REDCap and should be stored as an environmental variable for security. I recommend using the [usethis](https://usethis.r-lib.org) package to set your API token in `.Renviron` as `REDCAP_TOKEN=your-api-token`.

``` r
usethis::edit_r_environ(scope = c("user", "project"))
```

## Basic Usage

Data from REDCap is transferred to DuckDB in configurable chunks of record IDs:

``` r
library(redquack)

con <- redcap_to_duckdb(
  redcap_uri = "https://redcap.example.org/api/",
  token = Sys.getenv("REDCAP_TOKEN"),
  record_id_name = "record_id",
  chunk_size = 1000  
  # Increase chunk size for memory-efficient systems (faster)
  # Decrease chunk size for memory-constrained systems (slower)
)
```

By default `return_duckdb = TRUE`, returning the DuckDB connection from the output file `redcap.duckdb`.

When `return_duckdb = FALSE`, the function returns a logical value:

-   `TRUE` for a complete / successful transfer
-   `FALSE` for an incomplete / failed transfer

### Data Manipulation

Query and collect the data with [dplyr](https://dplyr.tidyverse.org):

``` r
library(dplyr)

demographics <- tbl(con, "data") |>
  filter(is.na(redcap_repeat_instrument)) |>
  select(record_id, age, race, sex, gender) |>
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

## Column Types

By default, column types are automatically converted after transfer (`optimize_types = TRUE`):

-   **INTEGER**: Columns with only whole numbers
-   **DOUBLE**: Columns with decimal numbers
-   **DATE**: Columns with valid date strings
-   **TIMESTAMP**: Columns with valid timestamp strings
-   **VARCHAR**: All other columns remain as strings

If you disable type conversion (`optimize_types = FALSE`), all columns will be VARCHAR. Disable type conversion when you have complex mixed-type data or plan to handle conversions manually.

You can easily query the data to inspect the column types:

``` r
DBI::dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'data'")
```

## Other REDCap Interfaces

-   [redcapAPI](https://github.com/vubiostat/redcapAPI) (R package; also provides a package comparison table)

-   [PyCap](https://redcap-tools.github.io/PyCap/) (python module)
