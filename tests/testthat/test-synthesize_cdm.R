test_that("basic example works", {
  skip("todo")
  path <- tempfile(fileext = "duckdb")
  # debugonce(synthesize_cdm)
  synthesize_cdm(path, n_persons = 10, modules = "hiv", format = "duckdb")
})
