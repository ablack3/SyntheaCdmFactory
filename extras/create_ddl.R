ddl <- CommonDataModel::createDdl("5.3") %>%
  SqlRender::render(targetDialect = "duckdb",
                    cdmDatabaseSchema = "main",
                    warnOnMissingParameters = TRUE) %>%
  SqlRender::translate("duckdb")
