# Assign List to Global Environment

Assign a list of instruments (data frames) to the global environment.
Each element of the list becomes a separate object in the global
environment.

## Usage

``` r
list_to_env(instruments)
```

## Arguments

- instruments:

  A named list of data frames, typically output from tbl_to_list().

## Value

Invisibly returns NULL. Side effect: assigns objects to the global
environment.

## Examples

``` r
if (FALSE) { # \dontrun{
tbl(conn, "data") |>
  tbl_to_list() |>
  list_to_env()
} # }
```
