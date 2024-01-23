
<!-- README.md is generated from README.Rmd. Please edit that file -->

# SyntheaCdmFactory

<!-- badges: start -->
<!-- badges: end -->

The goal of SyntheaCdmFactory is to create synthetic OMOP CDMs

## Installation

You can install the development version of SyntheaCdmFactory from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("OdyOSG/SyntheaCdmFactory")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(SyntheaCdmFactory)

# install the synthea java library. Only do this once.
install_synthea()

# manually download vcoab bundle from https://drive.google.com/file/d/1by7G4pLvUeepOpRqzl3ItO1WDZv_xYoK/view?usp=sharing

# point R to the location

# this step might be automated in the future.
vocab_path <- "~/Downloads/vocabulary_bundle_v5_0-22-JUN-22.zip"

# may take a few minuetes to run
synthesize_cdm(path = here::here("hiv_cdm.duckdb"), 
               vocab_path = "~/Downloads/vocabulary_bundle_v5_0-22-JUN-22.zip",
               n_persons = 100,
               modules =  "hiv",
               age = c(0, 100),
               format = "duckdb",
               overwrite = TRUE,
               seed = 1)
```

Now we have a new cdm with 100 HIV patients

``` r
con <- DBI::dbConnect(duckdb::duckdb(), here::here("hiv_cdm.duckdb"))

DBI::dbListTables(con)

DBI::dbGetQuery(con, "select count(*) as n_persons from main.person;")
```

# Attribution

This package relies heavily on code copied from
[ETLSyntheaBuilder](https://github.com/OHDSI/ETL-Synthea) by Anthony
Molinaro, Clair Blacketer, and Frank DeFalco and would not be possible
without their work.
