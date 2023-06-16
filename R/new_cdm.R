

# Create a new duckdb CDM with the vocab pre-loaded
#
# @param dbdir Enclosing directory where the CDM should be created.
# If NULL (default) then a temp folder is created.
# @return The full path to the new Eunomia CDM that can be passed to `dbConnect()`
# @importFrom utils untar download.file menu
new_cdm_dir <- function(dbdir = NULL, vocab_path) {

  rlang::check_installed("duckdb")
  checkmate::assertFileExists(vocab_path)
  # if (stringr::str_detect(list_data(), "vocabulary_bundle")) {
  #   rlang::abort("Synthea has not been correctly installed. Run `install_synthea()`")
  # }

  if (is.null(dbdir)) {
    dbdir <- file.path(tempdir(TRUE), paste(sample(letters, 8, replace = TRUE), collapse = ""))
  }

  # vocab_path <- stringr::str_subset(list_data(), "vocabulary_bundle")[[1]] %>%
  #   get_data_filepath()

  # extract vocab ----
  cli::cli_process_start("Extracting vocabulary")
  # vocab_zip_path <- here::here("vocab/vocabulary_bundle_v5_0-22-JUN-22.zip")
  vocab_zip_path <- vocab_path
  unzip(vocab_zip_path, exdir = tempdir())
  cli::cli_process_done("Extracting vocabulary")

  # load vocab ----
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # execute vocab ddl
  sql <- c("
        DROP TABLE IF EXISTS main.concept;
        CREATE TABLE main.concept (
          concept_id integer NOT NULL,
          concept_name varchar(255) NULL, -- Should be NOT NULL but getting an error
          domain_id varchar(20) NOT NULL,
          vocabulary_id varchar(20) NOT NULL,
          concept_class_id varchar(20) NOT NULL,
          standard_concept varchar(1) NULL,
          concept_code varchar(50) NOT NULL,
          valid_start_date date NOT NULL,
          valid_end_date date NOT NULL,
          invalid_reason varchar(1) NULL
        );

        DROP TABLE IF EXISTS main.vocabulary;
        CREATE TABLE main.vocabulary (
          vocabulary_id varchar(20) NOT NULL,
          vocabulary_name varchar(255) NOT NULL,
          vocabulary_reference varchar(255) NOT NULL,
          vocabulary_version varchar(255) NULL,
          vocabulary_concept_id integer NOT NULL
        );

        DROP TABLE IF EXISTS main.domain;
        CREATE TABLE main.domain (
          domain_id varchar(20) NOT NULL,
          domain_name varchar(255) NOT NULL,
          domain_concept_id integer NOT NULL
        );

        DROP TABLE IF EXISTS main.concept_class;
        CREATE TABLE main.concept_class (
          concept_class_id varchar(20) NOT NULL,
          concept_class_name varchar(255) NOT NULL,
          concept_class_concept_id integer NOT NULL
        );

        DROP TABLE IF EXISTS main.concept_relationship;
        CREATE TABLE main.concept_relationship (
          concept_id_1 integer NOT NULL,
          concept_id_2 integer NOT NULL,
          relationship_id varchar(20) NOT NULL,
          valid_start_date date NOT NULL,
          valid_end_date date NOT NULL,
          invalid_reason varchar(1) NULL
        );

        DROP TABLE IF EXISTS main.relationship;
        CREATE TABLE main.relationship (
          relationship_id varchar(20) NOT NULL,
          relationship_name varchar(255) NOT NULL,
          is_hierarchical varchar(1) NOT NULL,
          defines_ancestry varchar(1) NOT NULL,
          reverse_relationship_id varchar(20) NOT NULL,
          relationship_concept_id integer NOT NULL
        );

        DROP TABLE IF EXISTS main.concept_synonym;
        CREATE TABLE main.concept_synonym (
          concept_id integer NOT NULL,
          concept_synonym_name varchar(1000) NOT NULL,
          language_concept_id integer NOT NULL
        );

        DROP TABLE IF EXISTS main.concept_ancestor;
        CREATE TABLE main.concept_ancestor (
          ancestor_concept_id integer NOT NULL,
          descendant_concept_id integer NOT NULL,
          min_levels_of_separation integer NOT NULL,
          max_levels_of_separation integer NOT NULL
        );

        DROP TABLE IF EXISTS main.source_to_concept_map;
        CREATE TABLE main.source_to_concept_map (
          source_code varchar(50) NOT NULL,
          source_concept_id integer NOT NULL,
          source_vocabulary_id varchar(20) NOT NULL,
          source_code_description varchar(255) NULL,
          target_concept_id integer NOT NULL,
          target_vocabulary_id varchar(20) NOT NULL,
          valid_start_date date NOT NULL,
          valid_end_date date NOT NULL,
          invalid_reason varchar(1) NULL
        );

        DROP TABLE IF EXISTS main.drug_strength;
        CREATE TABLE main.drug_strength (
          drug_concept_id integer NOT NULL,
          ingredient_concept_id integer NOT NULL,
          amount_value NUMERIC NULL,
          amount_unit_concept_id integer NULL,
          numerator_value DECIMAL(28,3) NULL, -- edited data type to accomodate large floats
          numerator_unit_concept_id integer NULL,
          denominator_value NUMERIC NULL,
          denominator_unit_concept_id integer NULL,
          box_size integer NULL,
          valid_start_date date NOT NULL,
          valid_end_date date NOT NULL,
          invalid_reason varchar(1) NULL
        )
      ") %>%
    stringr::str_split(";") %>%
    {.[[1]]}

  purrr::walk(as.list(sql), ~DBI::dbExecute(con, .))

  file_paths <- list.files(file.path(tempdir(), "vocabulary_bundle_v5_0-22-JUN-22"), full.names = TRUE) %>%
    stringr::str_subset("\\.parquet") %>%
    stringr::str_subset("cpt4", negate = TRUE)

  table_names <- tools::file_path_sans_ext(basename(file_paths))

  # df <- arrow::read_parquet(stringr::str_subset(file_paths, "concept_relationship"))

  n <- length(file_paths)
  for (i in seq_along(file_paths)) {
    cli::cli_process_start(glue::glue("Loading {table_names[i]} ({i}/{n})"))
    DBI::dbExecute(con, glue::glue("INSERT INTO main.{table_names[i]} SELECT * FROM read_parquet('{file_paths[i]}');"))
    cli::cli_process_done(glue::glue("Loading {table_names[i]} ({i}/{n})"))
  }

  return(dbdir)
}



