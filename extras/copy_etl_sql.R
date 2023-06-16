
# remotes::install_github("OHDSI/ETL-Synthea")

sql_files <- c(
  "insert_person.sql",
  "insert_observation_period.sql",
  "insert_provider.sql",
  "insert_visit_occurrence.sql",
  "insert_visit_detail.sql",
  "insert_condition_occurrence.sql",
  "insert_observation.sql",
  "insert_measurement.sql",
  "insert_procedure_occurrence.sql",
  "insert_drug_exposure.sql",
  "insert_condition_era.sql",
  "insert_drug_era.sql",
  "insert_cdm_source.sql",
  "insert_device_exposure.sql",
  "insert_death.sql",
  "insert_payer_plan_period.sql",
  "insert_cost_v300.sql")


for (i in seq_along(sql_files)) {
  path <- file.path("sql", "sql_server", "cdm_version", "v531", sql_files[i])
  sql <- readr::read_file(system.file(path, package = "ETLSyntheaBuilder", mustWork = T)) %>%
    SqlRender::render(cdm_schema = "main", synthea_schema = "synthea", warnOnMissingParameters = F) %>%
    SqlRender::translate("duckdb") %>%
    readr::write_file(here::here("inst", "sql", "duckdb", "v531", sql_files[i]))
}
