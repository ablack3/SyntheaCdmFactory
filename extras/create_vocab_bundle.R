# create vocab bundle

library(dplyr, warn.conflicts = FALSE)
library(glue)

path_to_vocab <- "/Users/adamblack/projects/synthea-etl-run/vocabulary_download_v5"

file_paths <- list.files(path_to_vocab, full.names = TRUE) %>%
  stringr::str_subset("\\.csv$") %>%
  stringr::str_subset("CPT4", negate = TRUE)

table_names <- tools::file_path_sans_ext(basename(file_paths))

vocab_version <- readr::read_tsv(stringr::str_subset(file_paths, "VOCABULARY"), show_col_types = F) %>%
  filter(vocabulary_id == "None") %>%
  pull(vocabulary_version) %>%
  stringr::str_replace_all("\\.", "_") %>%
  stringr::str_replace_all(" ", "-")

new_path <- file.path(path_to_vocab, glue("vocabulary_bundle_{vocab_version}"))
# new_path <- file.path(path_to_vocab, glue("vocabulary_bundle"))
unlink(new_path, recursive = T)
dir.create(new_path)

for (i in seq_along(file_paths)) {
  # if (table_names[i] == "CONCEPT_ANCESTOR") { next }
  # if (tolower(table_names[i]) %in% tools::file_path_sans_ext(list.files(new_path))) {
  #   message(glue::glue("skipping {table_names[i]}"))
  #   next
  # }
  message(glue::glue("processing {table_names[i]}"))
  df <- readr::read_tsv(file_paths[i], guess_max = 1e9, show_col_types = F)

  # A hack to deal with dates
  # TODO explicitly defined  datatypes
  if (any(stringr::str_detect(names(df), "_date"))) {
    df <- dplyr::mutate(df, dplyr::across(dplyr::matches("_date"), lubridate::ymd)) # format is YYYYMMDD
  }

  arrow::write_parquet(df, file.path(new_path, glue("{tolower(table_names[i])}.parquet")))
  rm(df)
  message("done")
}

# manually zip



# compress Takes too long. mabye compression is too high.
# withr::with_dir(path_to_vocab, {
#   tar(glue("vocabulary_bundle_{tolower(vocab_version)}.tar.xz"),
#       glue("vocabulary_bundle_{vocab_version}"),
#       compression = "xz",
#       compression_level = 4L)
# })


# upload


# code to extract
# if (is.null(exdir)) exdir <- file.path(tempdir(TRUE), paste(sample(letters, 8, replace = TRUE), collapse = ""))
# file <- xzfile(eunomia_cache(), open = "rb")
# untar(file, exdir = exdir)
# close(file)
# path <- file.path(exdir, "cdm.duckdb")




