
synthesize_cdm <- function(
  path,
  vocab_path,
  n_persons = 1e5,
  modules =  c("breast_cancer", "colorectal_cancer", "lung_cancer"),
  age = c(0, 100),
  format = "duckdb",
  vocab = c("all", "trimmed", "none"),
  overwrite = FALSE,
  seed = 1,
  base_dir = here::here("synthea_base")) {

  checkmate::check_choice(format, choices = c("duckdb"))
  # checkmate::check_choice(format, choices = c("parquet", "tsv", "duckdb"))
  checkmate::check_path_for_output(path, overwrite = overwrite)
  checkmate::check_integerish(n_persons, lower = 1, upper = 1e9, len = 1)
  checkmate::check_character(modules)
  checkmate::assert_file_exists(vocab_path)

  available_modules <- c("hiv", "breast_cancer", "metabolic_syndrome", "encounter",
    "covid19", "dermatitis", "contraceptives", "allergies", "heart",
    "lung_cancer", "total_joint_replacement", "snf", "weight_loss",
    "anemia", "medications", "surgery", "veterans", "opioid_addiction",
    "cerebral_palsy", "dialysis", "allergic_rhinitis", "pregnancy",
    "atopy", "self_harm", "asthma", "ear_infections", "sinusitis",
    "dementia", "veteran_hyperlipidemia", "mTBI", "veteran_prostate_cancer",
    "anemia___unknown_etiology", "urinary_tract_infections", "hypothyroidism",
    "osteoarthritis", "appendicitis", "copd", "contraceptive_maintenance",
    "fibromyalgia", "veteran_substance_abuse_treatment", "veteran_lung_cancer",
    "prescribing_opioids_for_chronic_pain_and_treatment_of_oud",
    "hospice_treatment", "rheumatoid_arthritis", "sore_throat", "sleep_apnea",
    "gallstones", "bronchitis", "spina_bifida", "sexual_activity",
    "homelessness", "epilepsy", "wellness_encounters", "injuries",
    "colorectal_cancer", "med_rec", "congestive_heart_failure", "veteran_self_harm",
    "veteran_mdd", "osteoporosis", "female_reproduction", "veteran",
    "gout", "home_hospice_snf", "metabolic_syndrome_disease", "sepsis",
    "metabolic_syndrome_care", "chronic_kidney_disease", "home_health_treatment",
    "lupus", "cystic_fibrosis", "attention_deficit_disorder", "food_allergies",
    "mend_program", "hiv_care")


  for (m in modules) {
    if (isFALSE(m %in% available_modules)) {
      rlang::abort(glue::glue("{m} is not one of the available modules.
                              Available modules are {paste(available_modules, collapse = ', ')}"))
    }
  }

  # get the vocab loaded into a new cdm
  dbdir <- new_cdm_dir(vocab_path = vocab_path)

  # generate synthea data ----
  # cli::cli_process_start("Generate Synthea data")
  path_to_jar <- get_data_filepath("synthea-with-dependencies.jar") %>%
    stringr::str_replace_all(" ", "\\\\ ")  # escape spaces for mac. need to quote on windows probably


  # base_dir <- file.path(tempdir(), "synthea_base_dir")

  if (dir.exists(base_dir)) {
    unlink(base_dir, recursive = TRUE)
  }

  module_string <- paste(modules, collapse = ":")
  min_age <- age[1]
  max_age <- age[2]
  cmd <- glue::glue("java -jar {path_to_jar} \\
    -p {format(n_persons, scientific = F)} -s {format(seed, scientific = F)} -m {module_string} -a {min_age}-{max_age} \\
    --exporter.csv.export true --exporter.baseDirectory '{base_dir}' --exporter.fhir.export false")

  spinner <- cli::make_spinner(template = "Generating Synthetic Data {spin}")
  invisible(system(cmd, intern = TRUE))

  if (length(list.files(file.path(base_dir, "csv"))) < 10) {
    rlang::abort("Synthea generation failed! Please report this issue at https://github.com/OdyOSG/SyntheaCdmFactory/issues")
  }
  cli::cli_process_done()

  # load synthea data ----
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  DBI::dbExecute(con, "drop schema if exists synthea")
  DBI::dbExecute(con, "create schema synthea")

  synthea_paths <- list.files(file.path(base_dir, "csv"), full.names = TRUE)
  synthea_tables <- tools::file_path_sans_ext(list.files(here::here("data", "csv")))

  for (i in seq_along(synthea_tables)) {
    cli::cli_process_start(glue::glue("Loading Synthea table {synthea_tables[i]}"))

    df <- readr::read_csv(synthea_paths[i], show_col_types = F, guess_max = 1e9) %>%
      dplyr::rename_all(tolower) %>%
      dplyr::mutate(dplyr::across(dplyr::matches("code"), as.character))

    # print(paste(names(df), collapse = ", "))
    DBI::dbWriteTable(con, "tmp", df, overwrite = TRUE)
    DBI::dbExecute(con, glue::glue("drop table if exists synthea.{synthea_tables[i]};"))
    DBI::dbExecute(con, glue::glue("CREATE TABLE synthea.{synthea_tables[i]} AS SELECT * FROM tmp;"))
    rm("df")
    # DBI::dbExecute(con, glue::glue("CREATE TABLE synthea.{synthea_tables[i]} AS SELECT * FROM read_csv('{synthea_paths[i]}', auto_detect = True, sample_size = 10000000, header = True);"))
    cli::cli_process_done()
  }

  DBI::dbExecute(con, "drop table tmp;")

  # run ETL ----
  # TODO fix the commented sql scripts that are failing
  sql_files <- c(
    "ddl.sql",
    "vocab_mapping.sql",
    "visit_rollup.sql",
    "insert_person.sql",
    "insert_observation_period.sql",
    "insert_provider.sql",
    "insert_visit_occurrence.sql",
    "insert_visit_detail.sql",
    "insert_condition_occurrence.sql",
    # "insert_observation.sql",
    # "insert_measurement.sql",
    "insert_procedure_occurrence.sql",
    "insert_drug_exposure.sql",
    "insert_condition_era.sql",
    "insert_drug_era.sql",
    "insert_cdm_source.sql",
    "insert_device_exposure.sql",
    "insert_death.sql")
    # "insert_payer_plan_period.sql",
    # "insert_cost_v300.sql")
    # "insert_cost_v300.sql")

  for (i in seq_along(sql_files)) {

    sql_path <- file.path("sql", "duckdb", "v531", sql_files[i])

    stopifnot(file.exists(system.file(sql_path, package = "SyntheaCdmFactory", mustWork = T)))

    cli::cli_process_start(glue::glue("Running ETL script {sql_files[i]}"))

    tryCatch({
      sql <- readr::read_file(system.file(sql_path, package = "SyntheaCdmFactory", mustWork = T)) %>%
        stringr::str_remove(";\\s+$") %>%
        stringr::str_split(";") %>%
        {.[[1]]}

      purrr::walk(sql, ~if (nchar(.) > 0) DBI::dbExecute(con, .))
    }, error = function(e) warning(glue::glue("script {sql_files[i]} had an error!")))

    cli::cli_process_done()
  }

  # clean up source data and intermediate tables
  DBI::dbExecute(con, "drop schema synthea cascade;")
  DBI::dbExecute(con, "drop table all_visits;")
  DBI::dbExecute(con, "drop table assign_all_visit_ids;")
  DBI::dbExecute(con, "drop table final_visit_ids;")
  DBI::dbExecute(con, "drop table source_to_source_vocab_map;")
  DBI::dbExecute(con, "drop table source_to_standard_vocab_map;")

  if (format == "duckdb") {
    DBI::dbDisconnect(con, shutdown = TRUE)
    file.copy(dbdir, path, overwrite = overwrite)
    file.rename(file.path(path, basename(dbdir)), file.path(path, paste0(modules[1], "_cdm.duckdb")))
    return(invisible(path))
  }
}




