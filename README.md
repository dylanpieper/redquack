# redquack <img src="man/figures/redquack-hex.png" align="right" width="140"/>

[![CRAN status](https://www.r-pkg.org/badges/version/redquack)](https://cran.r-project.org/package=redquack) [![R-CMD-check](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/dylanpieper/redquack/actions/workflows/R-CMD-check.yaml)

Transfer [REDCap](https://www.project-redcap.org/) data to a database and use in R without exceeding available memory. Compatible with all databases but specifically optimized for [DuckDB](https://duckdb.org/)—a fast and portable SQL engine with first-class integration in Posit products.

## Use Case and Solution

Is your REDCap project outgrowing your computer? Have you seen this error when you request data via API?

**Error: vector memory limit of 16.0 GB reached, see mem.maxVSize()**

I certainly have. But what does it mean? Well, R objects a stored in your random access memory (RAM). When your data gets too big, you hit your memory limit. **redquack's solution to this error is to store the data out of memory in a local database for easy retrieval in R.**

The process:

1.  Request all record IDs in REDCap, and split them into chunks
2.  Request one chunk of the project data at a time
3.  Transfer the chunk of data to a database
4.  Remove the chunk from memory, and repeat from step 2

Once complete, you can retrieve your data from the database and use it in R (see [Data Manipulation](#data-manipulation)).

## Features

redquack has additional features to make it robust and improve user experience:

-   Retry on API request failures
-   Resume from incomplete transfers
-   Convert data types for optimized queries
-   Store timestamped operation logs
-   Show progress bar and status messages
-   Play sound notifications (quacks on success 🦆)

## Installation

From CRAN:

``` r
# install.packages("pak")
pak::pak("redquack")
```

Development version:

``` r
pak::pak("dylanpieper/redquack")
```

These packages are used in the examples but are not imported by redquack:

``` r
pak::pak(c("dplyr", "duckdb", "keyring"))
```

## Setup API Token

An API token allows R to interface with REDCap, and it should be stored securely. I recommend using the [keyring](https://keyring.r-lib.org) package to store your API token. For example:

``` r
keyring::key_set("redcap_token")
```

## Basic Usage

Data from REDCap is transferred to a database via a [DBI](https://dbi.r-dbi.org) connection in chunks of record IDs:

``` r
library(redquack)

duckdb <- DBI::dbConnect(duckdb::duckdb(), "redcap.duckdb")

result <- redcap_to_db(
  conn = duckdb,
  redcap_uri = "https://redcap.example.org/api/",
  token = keyring::key_get("redcap_token"),
  record_id_name = "record_id",
  chunk_size = 1000  
)
```

The function returns a list of metadata with class `redcap_transfer_result`:

-   `success`: Logical if the transfer was completed with no failed processing
-   `error_chunks`: Vector of chunk numbers that failed processing
-   `time_s`: Numeric value for total seconds to transfer and optimize data

These metadata are useful for programming export pipelines and ETL workflows. The actual data is stored in database and accessed via the connection.

## Database Structure

The database created by `redcap_to_db()` contains two tables:

1.  `data`: Contains all exported REDCap records with optimized column types

    ``` r
    data <- DBI::dbGetQuery(duckdb, "SELECT * FROM data LIMIT 1000")
    ```

2.  `log`: Contains timestamped logs of the transfer process for troubleshooting

    ``` r
    log <- DBI::dbGetQuery(duckdb, "SELECT * FROM log")
    ```

## Data Types

Data is imported as **VARCHAR/TEXT** for consistent handling across chunks.

For DuckDB, data types are automatically optimized after transfer to improve query performance:

-   **BOOLEAN**: Columns with boolean values
-   **INTEGER**: Columns with only whole numbers
-   **DOUBLE**: Columns with decimal numbers
-   **DATE**: Columns with valid dates
-   **TIMESTAMP**: Columns with valid timestamps
-   **VARCHAR/TEXT**: All other columns remain as strings

In DuckDB, you can query the data to inspect the data types:

``` r
DBI::dbGetQuery(duckdb, "PRAGMA table_info(data)")
```

You can also automatically convert data types in R using [readr](https://readr.tidyverse.org):

``` r
readr::type_convert(data)
```

To optimize query performance with other databases, alter the database table manually.

## Data Manipulation

Manipulate your data with [familiar dplyr syntax](https://dbplyr.tidyverse.org/reference/tbl.src_dbi.html). The only difference is you reference the database table first and collect the data into memory last. Everything in between stays the same. Specifically, dplyr builds a lazy query plan through its verb functions, then the database engine executes the plan.

``` r
library(dplyr)

demographics <- tbl(duckdb, "data") |>
  filter(is.na(redcap_repeat_instrument)) |>
  select(record_id, age, race, sex, gender) |>
  collect()
```

By collecting your data into memory last, manipulating big data becomes SO MUCH FASTER. The following example data is 2,825,092 rows x 397 columns:

``` r
system.time(
    duckdb |>
    tbl("data") |>
    collect() |>
    group_by(redcap_repeat_instrument) |>
    summarize(count = n()) |>
    arrange(desc(count))
)
#>   user  system elapsed
#>  5.048   5.006   6.077

system.time(
    duckdb |>
    tbl("data") |>
    group_by(redcap_repeat_instrument) |>
    summarize(count = n()) |>
    arrange(desc(count)) |>
    collect()
)
#>    user  system elapsed
#>   0.040   0.015   0.040
```

You can also write a Parquet file directly from DuckDB and use [arrow](https://arrow.apache.org/docs/r/). A Parquet file will be about 5 times smaller than a DuckDB file and easy to share:

``` r
DBI::dbExecute(duckdb, "COPY (SELECT * FROM data) TO 'redcap.parquet' (FORMAT PARQUET)")
```

Remember to close the connection when finished:

``` r
DBI::dbDisconnect(duckdb)
```

## Collaboration Opportunities

While this package is only optimized for DuckDB, I invite collaborators to help optimize it for other databases. Target your edits in `R/optimize_data_types.R`. Feel free to submit a PR and share any other ideas you may have.

## Other REDCap Interfaces

-   [REDCapR](https://ouhscbbmc.github.io/REDCapR/) (R package)
-   [REDCapTidieR](https://chop-cgtinformatics.github.io/REDCapTidieR/) (R package)
-   [tidyREDCap](https://raymondbalise.github.io/tidyREDCap/) (R package)
-   [redcapAPI](https://github.com/vubiostat/redcapAPI) (R package)
-   [REDCapSync](https://thecodingdocs.github.io/REDCapSync/) (R package)
-   [PyCap](https://redcap-tools.github.io/PyCap/) (python module)
