# redquack <img src="man/figures/redquack-hex.png" align="right" width="140"/>

Transfer [REDCap](https://www.project-redcap.org/) data to [DuckDB](https://duckdb.org/) with minimal memory overhead, designed for large datasets that exceed available RAM.

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

### Working with the data

Query the data with `dplyr`:

``` r
library(dplyr)

demographics <- tbl(con, "data") |>
  filter(demographics_complete == 2) |>
  select(record_id, age, race, gender) |>
  collect()

age_summary <- tbl(con, "data") |>
  group_by(gender) |>
  summarize(
    n = n(),
    mean_age = mean(age, na.rm = TRUE),
    median_age = median(age, na.rm = TRUE)
  ) |>
  collect()
```

Create a Parquet file directly from DuckDB (efficient for sharing data):

``` r
DBI::dbExecute(con, "COPY (SELECT * FROM data) TO 'redcap.parquet' (FORMAT PARQUET)")
```

Remember to close the connection when finished:

``` r
DBI::dbDisconnect(con, shutdown = TRUE)
```

## Database structure

The DuckDB database created by `redcap_to_duckdb()` contains two tables:

1.  `data`: Contains all exported REDCap records with optimized column types

    ``` r
    DBI::dbGetQuery(con, "SELECT * FROM data LIMIT 10")
    ```

2.  `log`: Contains timestamped logs of the transfer process for troubleshooting

    ``` r
    DBI::dbGetQuery(con, "SELECT timestamp, type, message FROM log ORDER BY timestamp")
    ```
