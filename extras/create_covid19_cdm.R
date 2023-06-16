
path <- synthesize_cdm(path = "~/Desktop/covid_cdm",
                       vocab_path = here::here("vocab/vocabulary_bundle_v5_0-22-JUN-22.zip"),
                       n_persons = 100000,
                       modules = "covid19",
                       format = "duckdb",
                       overwrite = T)

list.files(path)
con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/covid_cdm/cdm.duckdb")

cdm <- cdm_from_con(con)

cdm$person


cdm$drug_exposure

cdm$drug_era %>%
  filter(drug_concept_id == 1777087)


DBI::dbDisconnect(con, shutdown = T)
.
