



path_to_jar <- "~/Downloads/synthea-with-dependencies.jar"

modules <- c("breast_cancer", "colorectal_cancer", "lung_cancer")

base_dir <- here::here("data")
module_string <- "breast_cancer:colorectal_cancer:lung_cancer"
seed <- 12345
n_persons <- 100
min_age <- 10
max_age <- 90
cmd <- glue::glue("
  java -jar {path_to_jar} \\
  -p {n_persons} -s {seed} -m {module_string} -a {min_age}-{max_age} \\
  --exporter.csv.export true --exporter.baseDirectory '{base_dir}' --exporter.fhir.export false
")

system(cmd)

path_to_vocab <- "/Users/adamblack/projects/synthea-etl-run/vocabulary_download_v5"


# create csv tables
vocab_tables <- c(
  "concept",
  "vocabulary",
  "concept_ancestor",
  "concept_relationship",
  "relationship",
  "concept_synonym",
  "domain",
  "concept_class",
  "drug_strength"
)

if (all(paste0(vocab_tables, ".csv") %in% list.files(path_to_vocab))) {
  filenames <- file.path(path_to_vocab, paste0(vocab_tables, ".csv"))
} else if (all(paste0(toupper(vocab_tables), ".csv") %in% list.files(path_to_vocab))) {
  case <- "upper"
  filenames <- file.path(path_to_vocab, paste0(toupper(vocab_tables), ".csv"))
} else {
  stop("all required vocab csv files are not available in the vocab directory")
}


DBI::dbDisconnect(con)
con <- DBI::dbConnect(duckdb::duckdb())

# unique(stringr::str_extract_all(CommonDataModel::createDdl("5.3"), "@\\w+")[[1]])


# Note the default format for floats is not large enough to hold some data in in the nuemrator field of drug_strength table
# We could be more eplicit or change the datatype.
# I opted to drop and recreate the table using autodetection.
ddl <- CommonDataModel::createDdl("5.3") %>%
  SqlRender::render(targetDialect = "duckdb",
                    cdmDatabaseSchema = "main",
                    warnOnMissingParameters = TRUE) %>%
  SqlRender::translate("duckdb") %>%
  SqlRender::splitSql()


cat(ddl)
purrr::walk(ddl, ~DBI::dbExecute(con, .))

for (i in seq_along(vocab_tables)) {
  message(paste("loading vocab table", vocab_tables[i]))

  if (vocab_tables[i] == "drug_strength") {
    DBI::dbExecute(con, "drop table drug_strength")
    DBI::dbExecute(con,
      glue::glue("CREATE TABLE drug_strength AS SELECT * FROM read_csv('{filenames[i]}', dateformat = '%Y%m%d', auto_detect = True, sample_size = -1);"))

  } else {
    DBI::dbExecute(con, glue::glue("COPY {vocab_tables[i]} FROM '{filenames[i]}'  (DATEFORMAT '%Y%m%d', QUOTE '', AUTO_DETECT TRUE, SAMPLE_SIZE -1);"))
  }
}

# Load synthea source data

# DBI::dbExecute(con, "drop schema synthea cascade")
DBI::dbExecute(con, "create schema synthea")


synthea_paths <- list.files(here::here("data", "csv"), full.names = TRUE)
synthea_tables <- tools::file_path_sans_ext(list.files(here::here("data", "csv")))

for (i in seq_along(synthea_tables)) {
  message(paste("loading", synthea_tables[i]))
  DBI::dbExecute(con, glue::glue("CREATE TABLE synthea.{synthea_tables[i]} AS SELECT * FROM read_csv('{synthea_paths[i]}', auto_detect = True, sample_size = -1);"))
}




# LoadEventTables <- function(connectionDetails,
# cdmSchema = "main"
# syntheaSchema = "synthea"
# cdmVersion = "5.3"
# syntheaVersion = "2.7.0"
# cdmSourceName = "Synthea synthetic health database"
# cdmSourceAbbreviation = "Synthea"
# cdmHolder = "OHDSI"
# cdmSourceDescription = "SyntheaTM is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats."
# sqlOnly = FALSE

  # Determine which sql scripts to run based on the given version.
  # The path is relative to inst/sql/sql_server.
  # if (cdmVersion == "5.3") {
  #   sqlFilePath <- "cdm_version/v531"
  # } else if (cdmVersion == "5.4") {
  #   sqlFilePath <- "cdm_version/v540"
  # } else {
  #   stop("Unsupported CDM specified. Supported CDM versions are \"5.3\" and \"5.4\".")
  # }
  #
  # supportedSyntheaVersions <- c("2.7.0", "3.0.0")
  #
  # if (!(syntheaVersion %in% supportedSyntheaVersions))
  #   stop("Invalid Synthea version specified. Currently \"2.7.0\" and \"3.0.0\" are supported.")

  # Create Vocabulary mapping tables ----


  # -- Create mapping table as per logic in 3.1.2 Source to Standard Terminology
  # -- found in Truven_CCAE_and_MDCR_ETL_CDM_V5.2.0.doc


  # create source_to_standard_vocab_map
  {c("if object_id('@cdm_schema.source_to_standard_vocab_map', 'U')  is not null drop table @cdm_schema.source_to_standard_vocab_map;

    WITH CTE_VOCAB_MAP AS (
    SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON,
    c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
    FROM @cdm_schema.CONCEPT C
    JOIN @cdm_schema.CONCEPT_RELATIONSHIP CR
    ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    AND CR.invalid_reason IS NULL
    AND lower(cr.relationship_id) = 'maps to'
    JOIN @cdm_schema.CONCEPT C1
    ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    AND C1.INVALID_REASON IS NULL
    UNION
    SELECT source_code, SOURCE_CONCEPT_ID, SOURCE_CODE_DESCRIPTION, source_vocabulary_id, c1.domain_id AS SOURCE_DOMAIN_ID, c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE, c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,target_concept_id, c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME, target_vocabulary_id, c2.domain_id AS TARGET_DOMAIN_ID, c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON, c2.standard_concept AS TARGET_STANDARD_CONCEPT
    FROM @cdm_schema.source_to_concept_map stcm
    LEFT OUTER JOIN @cdm_schema.CONCEPT c1
    ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN @cdm_schema.CONCEPT c2
    ON c2.CONCEPT_ID = stcm.target_concept_id
    WHERE stcm.INVALID_REASON IS NULL
  )
  select * into @cdm_schema.source_to_standard_vocab_map from CTE_VOCAB_MAP;

  create index idx_vocab_map_source_code on @cdm_schema.source_to_standard_vocab_map (source_code);
  create index idx_vocab_map_source_vocab_id on @cdm_schema.source_to_standard_vocab_map (source_vocabulary_id);
  ") %>%
  SqlRender::render(cdm_schema = "main") %>%
  SqlRender::translate("duckdb") %>% cat()
  # SqlRender::splitSql() %>%
  # purrr::walk(DBI::dbExecute, conn = con)
  }

# create source to source vocab map

{
   c("--Use this code to map source codes to source concept ids;

  if object_id('@cdm_schema.source_to_source_vocab_map', 'U')  is not null drop table @cdm_schema.source_to_source_vocab_map;

  WITH CTE_VOCAB_MAP AS (
    SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID, c.domain_id AS SOURCE_DOMAIN_ID, c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.invalid_reason AS SOURCE_INVALID_REASON,
    c.concept_ID as TARGET_CONCEPT_ID, c.concept_name AS TARGET_CONCEPT_NAME, c.vocabulary_id AS TARGET_VOCABULARY_ID, c.domain_id AS TARGET_DOMAIN_ID,
    c.concept_class_id AS TARGET_CONCEPT_CLASS_ID, c.INVALID_REASON AS TARGET_INVALID_REASON,
    c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
    FROM @cdm_schema.CONCEPT c
    UNION
    SELECT source_code, SOURCE_CONCEPT_ID, SOURCE_CODE_DESCRIPTION, source_vocabulary_id, c1.domain_id AS SOURCE_DOMAIN_ID, c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE, c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id, c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME, target_vocabulary_id, c2.domain_id AS TARGET_DOMAIN_ID, c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON, c2.standard_concept AS TARGET_STANDARD_CONCEPT
    FROM @cdm_schema.source_to_concept_map stcm
    LEFT OUTER JOIN @cdm_schema.CONCEPT c1
    ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN @cdm_schema.CONCEPT c2
    ON c2.CONCEPT_ID = stcm.target_concept_id
    WHERE stcm.INVALID_REASON IS NULL
  )

  SELECT * INTO @cdm_schema.source_to_source_vocab_map FROM CTE_VOCAB_MAP;

  create index idx_source_vocab_map_source_code on @cdm_schema.source_to_source_vocab_map (source_code);
  create index idx_source_vocab_map_source_vocab_id on @cdm_schema.source_to_source_vocab_map (source_vocabulary_id);
  ") %>%
   SqlRender::render(cdm_schema = "main") %>%
   SqlRender::translate("duckdb") %>% cat
   # SqlRender::splitSql() %>%
   # purrr::walk(DBI::dbExecute, conn = con)
}

## Create visit rollup ----

{c("
if object_id('@cdm_schema.IP_VISITS', 'U')  is not null drop table @cdm_schema.IP_VISITS;
if object_id('@cdm_schema.ER_VISITS', 'U')  is not null drop table @cdm_schema.ER_VISITS;
if object_id('@cdm_schema.OP_VISITS', 'U')  is not null drop table @cdm_schema.OP_VISITS;
if object_id('@cdm_schema.ALL_VISITS', 'U') is not null drop table @cdm_schema.ALL_VISITS;

/* Inpatient visits */
/* Collapse IP claim lines with <=1 day between them into one visit */

WITH CTE_END_DATES AS (
	SELECT patient, encounterclass, dateadd(day,-1,EVENT_DATE) AS END_DATE
	FROM (
		SELECT patient, encounterclass, EVENT_DATE, EVENT_TYPE,
			MAX(START_ORDINAL) OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ROWS UNBOUNDED PRECEDING) AS START_ORDINAL,
			ROW_NUMBER() OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD
		FROM (
			SELECT patient, encounterclass, start AS EVENT_DATE, -1 AS EVENT_TYPE,
			       ROW_NUMBER () OVER (PARTITION BY patient, encounterclass ORDER BY start, stop) AS START_ORDINAL
			FROM @synthea_schema.encounters
			WHERE encounterclass = 'inpatient'
			UNION ALL
			SELECT patient, encounterclass, dateadd(day,1,stop), 1 AS EVENT_TYPE, NULL
			FROM @synthea_schema.encounters
			WHERE encounterclass = 'inpatient'
		) RAWDATA
	) E
	WHERE (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
),
CTE_VISIT_ENDS AS (
	SELECT MIN(V.id) encounter_id,
	    V.patient,
		V.encounterclass,
		V.start VISIT_START_DATE,
		MIN(E.END_DATE) AS VISIT_END_DATE
	FROM @synthea_schema.encounters V
		JOIN CTE_END_DATES E
			ON V.patient = E.patient
			AND V.encounterclass = E.encounterclass
			AND E.END_DATE >= V.start
	GROUP BY V.patient,V.encounterclass,V.start
)
SELECT T2.encounter_id,
    T2.patient,
	T2.encounterclass,
	T2.VISIT_START_DATE,
	T2.VISIT_END_DATE
INTO @cdm_schema.IP_VISITS
FROM (
	SELECT
	    encounter_id,
	    patient,
		encounterclass,
		MIN(VISIT_START_DATE) AS VISIT_START_DATE,
		VISIT_END_DATE
	FROM CTE_VISIT_ENDS
	GROUP BY encounter_id, patient, encounterclass, VISIT_END_DATE
) T2;


/* Emergency visits */
/* collapse ER claim lines with no days between them into one visit */

SELECT T2.encounter_id,
    T2.patient,
	T2.encounterclass,
	T2.VISIT_START_DATE,
	T2.VISIT_END_DATE
INTO @cdm_schema.ER_VISITS
FROM (
	SELECT MIN(encounter_id) encounter_id,
	    patient,
		encounterclass,
		VISIT_START_DATE,
		MAX(VISIT_END_DATE) AS VISIT_END_DATE
	FROM (
		SELECT CL1.id encounter_id,
			CL1.patient,
			CL1.encounterclass,
			CL1.start VISIT_START_DATE,
			CL2.stop VISIT_END_DATE
		FROM @synthea_schema.encounters CL1
		JOIN @synthea_schema.encounters CL2
			ON CL1.patient = CL2.patient
			AND CL1.start = CL2.start
			AND CL1.encounterclass = CL2.encounterclass
		WHERE CL1.encounterclass in ('emergency','urgent')
	) T1
	GROUP BY patient, encounterclass, VISIT_START_DATE
) T2;


/* Outpatient visits */

WITH CTE_VISITS_DISTINCT AS (
	SELECT MIN(id) encounter_id,
	               patient,
				   encounterclass,
					start VISIT_START_DATE,
					stop VISIT_END_DATE
	FROM @synthea_schema.encounters
	WHERE encounterclass in ('ambulatory', 'wellness', 'outpatient')
	GROUP BY patient,encounterclass,start,stop
)
SELECT MIN(encounter_id) encounter_id,
       patient,
		encounterclass,
		VISIT_START_DATE,
		MAX(VISIT_END_DATE) AS VISIT_END_DATE
INTO @cdm_schema.OP_VISITS
FROM CTE_VISITS_DISTINCT
GROUP BY patient, encounterclass, VISIT_START_DATE;


/* All visits */


SELECT *, row_number()over(order by patient) as visit_occurrence_id
INTO @cdm_schema.all_visits
FROM
(
	SELECT * FROM @cdm_schema.IP_VISITS
	UNION ALL
	SELECT * FROM @cdm_schema.ER_VISITS
	UNION ALL
	SELECT * FROM @cdm_schema.OP_VISITS
) T1;

if object_id('@cdm_schema.IP_VISITS', 'U')  is not null drop table @cdm_schema.IP_VISITS;
if object_id('@cdm_schema.ER_VISITS', 'U')  is not null drop table @cdm_schema.ER_VISITS;
if object_id('@cdm_schema.OP_VISITS', 'U')  is not null drop table @cdm_schema.OP_VISITS;
") %>%
  tolower() %>%
  SqlRender::render(cdm_schema = "main", synthea_schema = "synthea") %>%
  SqlRender::translate("duckdb") %>% cat(file = here::here("visitrollup.txt"))
  SqlRender::splitSql() %>%
  purrr::walk(DBI::dbExecute, conn = con)
}

#aavi table ----
{c("/*Assign VISIT_OCCURRENCE_ID to all encounters*/

if object_id('@cdm_schema.ASSIGN_ALL_VISIT_IDS', 'U')  is not null drop table @cdm_schema.ASSIGN_ALL_VISIT_IDS;

SELECT  E.id AS encounter_id,
		E.patient as person_source_value,
		E.start AS date_service,
		E.stop AS date_service_end,
		E.encounterclass,
		AV.encounterclass AS VISIT_TYPE,
		AV.VISIT_START_DATE,
		AV.VISIT_END_DATE,
		AV.VISIT_OCCURRENCE_ID,
		CASE
			WHEN E.encounterclass = 'inpatient' and AV.encounterclass = 'inpatient'
				THEN VISIT_OCCURRENCE_ID
			WHEN E.encounterclass in ('emergency','urgent')
				THEN (
					CASE
						WHEN AV.encounterclass = 'inpatient' AND E.start > AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						WHEN AV.encounterclass in ('emergency','urgent') AND E.start = AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						ELSE NULL
					END
				)
			WHEN E.encounterclass in ('ambulatory', 'wellness', 'outpatient')
				THEN (
					CASE
						WHEN AV.encounterclass = 'inpatient' AND E.start >= AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						WHEN AV.encounterclass in ('ambulatory', 'wellness', 'outpatient')
							THEN VISIT_OCCURRENCE_ID
						ELSE NULL
					END
				)
			ELSE NULL
		END AS VISIT_OCCURRENCE_ID_NEW
INTO @cdm_schema.ASSIGN_ALL_VISIT_IDS
FROM @synthea_schema.ENCOUNTERS E
JOIN @cdm_schema.ALL_VISITS AV
	ON E.patient = AV.patient
	AND E.start >= AV.VISIT_START_DATE
	AND E.start <= AV.VISIT_END_DATE;
") %>%
    tolower() %>%
    SqlRender::render(cdm_schema = "main", synthea_schema = "synthea") %>%
    SqlRender::translate("duckdb") %>% cat(file = here::here("avitable.txt"))
    SqlRender::splitSql() %>%
    purrr::walk(DBI::dbExecute, conn = con)
}

{

  c("
if object_id('@cdm_schema.FINAL_VISIT_IDS', 'U') is not null drop table @cdm_schema.FINAL_VISIT_IDS;

CREATE TABLE @cdm_schema.FINAL_VISIT_IDS AS
SELECT encounter_id, VISIT_OCCURRENCE_ID_NEW
FROM(
	SELECT *, ROW_NUMBER () OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
	FROM (
		SELECT *,
			CASE
				WHEN encounterclass in ('emergency','urgent')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 1
							WHEN VISIT_TYPE in ('emergency','urgent') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass in ('ambulatory', 'wellness', 'outpatient')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN  1
							WHEN VISIT_TYPE in ('ambulatory', 'wellness', 'outpatient') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass = 'inpatient' AND VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
					THEN 1
				ELSE 99
			END AS PRIORITY
	FROM @cdm_schema.ASSIGN_ALL_VISIT_IDS
	) T1
) T2
WHERE RN=1
") %>%
    tolower() %>%
    SqlRender::render(cdm_schema = "main") %>%
    SqlRender::translate("duckdb") %>% cat()
    SqlRender::splitSql() %>%
    purrr::walk(DBI::dbExecute, conn = con)
}



  # person
  fileQuery <- "insert_person.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # observation period
  fileQuery <- "insert_observation_period.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # provider
  fileQuery <- "insert_provider.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # visit occurrence
  fileQuery <- "insert_visit_occurrence.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # visit detail
  fileQuery <- "insert_visit_detail.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # condition occurrence
  fileQuery <- "insert_condition_occurrence.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # observation
  fileQuery <- "insert_observation.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # measurement
  fileQuery <- "insert_measurement.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema,
    synthea_version = syntheaVersion
  )
  runStep(sql, fileQuery)

  # procedure occurrence
  fileQuery <- "insert_procedure_occurrence.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema,
    synthea_version = syntheaVersion
  )
  runStep(sql, fileQuery)

  # drug exposure
  fileQuery <- "insert_drug_exposure.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # condition era
  fileQuery <- "insert_condition_era.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema
  )
  runStep(sql, fileQuery)

  # drug era
  fileQuery <- "insert_drug_era.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema
  )
  runStep(sql, fileQuery)

  # cdm source
  fileQuery <- "insert_cdm_source.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    cdm_version = cdmVersion,
    cdm_source_name = cdmSourceName,
    cdm_source_abbreviation = cdmSourceAbbreviation,
    cdm_holder = cdmHolder,
    source_description = cdmSourceDescription
  )
  runStep(sql, fileQuery)

  # device exposure
  fileQuery <- "insert_device_exposure.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # death
  fileQuery <- "insert_death.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  # payer_plan_period
  fileQuery <- "insert_payer_plan_period.sql"
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema,
    synthea_version = syntheaVersion
  )
  runStep(sql, fileQuery)

  # cost
  if (syntheaVersion == "2.7.0")
    fileQuery <- "insert_cost_v270.sql"
  else if (syntheaVersion == "3.0.0")
    fileQuery <- "insert_cost_v300.sql"

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path(sqlFilePath, fileQuery),
    packageName = "ETLSyntheaBuilder",
    dbms = connectionDetails$dbms,
    cdm_schema = cdmSchema,
    synthea_schema = syntheaSchema
  )
  runStep(sql, fileQuery)

  if (!sqlOnly) {
    DatabaseConnector::disconnect(conn)
  }
}





synthea_ddl <- c('
--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.allergies (
  start        date,
  stop         date,
  patient      varchar(1000),
  encounter    varchar(1000),
  code         varchar(100),
  system       varchar(255),
  description  varchar(255),
  "type"       varchar(255),
  category     varchar(255),
  reaction1    varchar(255),
  description1 varchar(255),
  severity1    varchar(255),
  reaction2    varchar(255),
  description2 varchar(255),
  severity2    varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.careplans (
  id            varchar(1000),
  start         date,
  stop          date,
  patient       varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(255),
  reasoncode   varchar(255),
  reasondescription   varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.conditions (
  start         date,
  stop          date,
  patient       varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.encounters (
  id            		varchar(1000),
  start         		date,
  stop							date,
  patient       		varchar(1000),
  organization   		varchar(1000),
  provider			varchar(1000),
  payer			varchar(1000),
  encounterclass		varchar(1000),
  code          		varchar(100),
  description   		varchar(255),
  base_encounter_cost numeric,
  total_claim_cost		numeric,
  payer_coverage		numeric,
  reasoncode   			varchar(100),
  reasondescription varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.immunizations (
  "date"        date,
  patient       varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(255),
  base_cost	numeric
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.imaging_studies (
  id			  varchar(1000),
  "date"        date,
  patient					varchar(1000),
  encounter				varchar(1000),
  series_uid			varchar(1000),
  bodysite_code			varchar(100),
  bodysite_description		varchar(255),
  modality_code			varchar(100),
  modality_description		varchar(255),
  instance_uid			varchar(1000),
  SOP_code					varchar(100),
  SOP_description			varchar(255),
  procedure_code			varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.medications (
  start         date,
  stop          date,
  patient       varchar(1000),
  payer		varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(1000),
  base_cost	  numeric,
  payer_coverage		numeric,
  dispenses			int,
  totalcost			numeric,
  reasoncode   	varchar(100),
  reasondescription   varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.observations (
  "date"         date,
  patient       varchar(1000),
  encounter     varchar(1000),
  category      varchar(1000),
  code          varchar(100),
  description   varchar(255),
  value     		varchar(1000),
  units         varchar(100),
  "type"		  	varchar(100)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.organizations (
  id			  varchar(1000),
  "name"	      varchar(1000),
  address       varchar(1000),
  city		  varchar(100),
  state     	  varchar(100),
  zip           varchar(100),
  lat		numeric,
  lon 		numeric,
  phone		  varchar(100),
  revenue		numeric,
  utilization	  varchar(100)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.patients (
  id            varchar(1000),
  birthdate     date,
  deathdate     date,
  ssn           varchar(100),
  drivers       varchar(100),
  passport      varchar(100),
  prefix        varchar(100),
  first         varchar(100),
  last          varchar(100),
  suffix        varchar(100),
  maiden        varchar(100),
  marital       varchar(100),
  race          varchar(100),
  ethnicity     varchar(100),
  gender        varchar(100),
  birthplace    varchar(100),
  address       varchar(100),
  city					varchar(100),
  state					varchar(100),
  county		varchar(100),
  zip						varchar(100),
  lat		numeric,
  lon		numeric,
  healthcare_expenses	numeric,
  healthcare_coverage	numeric
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.procedures (
  start         date,
  stop          date,
  patient       varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(255),
  base_cost		numeric,
  reasoncode	varchar(1000),
  reasondescription	varchar(1000)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.providers (
  id varchar(1000),
  organization varchar(1000),
  name varchar(100),
  gender varchar(100),
  speciality varchar(100),
  address varchar(255),
  city varchar(100),
  state varchar(100),
  zip varchar(100),
  lat numeric,
  lon numeric,
  utilization numeric
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.devices (
  start         date,
  stop          date,
  patient       varchar(1000),
  encounter     varchar(1000),
  code          varchar(100),
  description   varchar(255),
  udi           varchar(255)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.claims (
  id                           varchar(1000),
  patientid                    varchar(1000),
  providerid                   varchar(1000),
  primarypatientinsuranceid    varchar(1000),
  secondarypatientinsuranceid  varchar(1000),
  departmentid                 varchar(1000),
  patientdepartmentid          varchar(1000),
  diagnosis1                   varchar(1000),
  diagnosis2                   varchar(1000),
  diagnosis3                   varchar(1000),
  diagnosis4                   varchar(1000),
  diagnosis5                   varchar(1000),
  diagnosis6                   varchar(1000),
  diagnosis7                   varchar(1000),
  diagnosis8                   varchar(1000),
  referringproviderid          varchar(1000),
  appointmentid                varchar(1000),
  currentillnessdate           date,
  servicedate                  date,
  supervisingproviderid        varchar(1000),
  status1                      varchar(1000),
  status2                      varchar(1000),
  statusp                      varchar(1000),
  outstanding1                 numeric,
  outstanding2                 numeric,
  outstandingp                 numeric,
  lastbilleddate1              date,
  lastbilleddate2              date,
  lastbilleddatep              date,
  healthcareclaimtypeid1       numeric,
  healthcareclaimtypeid2       numeric
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.claims_transactions (
  id                     varchar(1000),
  claimid                varchar(1000),
  chargeid               numeric,
  patientid              varchar(1000),
  "type"                 varchar(1000),
  amount                 numeric,
  method                 varchar(1000),
  fromdate               date,
  todate                 date,
  placeofservice         varchar(1000),
  procedurecode          varchar(1000),
  modifier1              varchar(1000),
  modifier2              varchar(1000),
  diagnosisref1          numeric,
  diagnosisref2          numeric,
  diagnosisref3          numeric,
  diagnosisref4          numeric,
  units                  numeric,
  departmentid           numeric,
  notes                  varchar(1000),
  unitamount             numeric,
  transferoutid          numeric,
  transfertype           varchar(1000),
  payments               numeric,
  adjustments            numeric,
  transfers              numeric,
  outstanding            numeric,
  appointmentid          varchar(1000),
  linenote               varchar(1000),
  patientinsuranceid     varchar(1000),
  feescheduleid          numeric,
  providerid             varchar(1000),
  supervisingproviderid  varchar(1000)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.payer_transitions (
  patient           varchar(1000),
  memberid         varchar(1000),
  start_year       date,
  end_year         date,
  payer            varchar(1000),
  secondary_payer  varchar(1000),
  ownership        varchar(1000),
  ownername       varchar(1000)
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.payers (
  id                       varchar(1000),
  name                     varchar(1000),
  address                  varchar(1000),
  city                     varchar(1000),
  state_headquartered      varchar(1000),
  zip                      varchar(1000),
  phone                    varchar(1000),
  amount_covered           numeric,
  amount_uncovered         numeric,
  revenue                  numeric,
  covered_encounters       numeric,
  uncovered_encounters     numeric,
  covered_medications      numeric,
  uncovered_medications    numeric,
  covered_procedures       numeric,
  uncovered_procedures     numeric,
  covered_immunizations    numeric,
  uncovered_immunizations  numeric,
  unique_customers         numeric,
  qols_avg                 numeric,
  member_months            numeric
);

--HINT DISTRIBUTE_ON_RANDOM
create table @synthea_schema.supplies (
  "date"       date,
  patient      varchar(1000),
  encounter    varchar(1000),
  code         varchar(1000),
  description  varchar(1000),
  quantity     numeric
);
') %>%
  SqlRender::render(synthea_schema = "synthea") %>%
  SqlRender::translate("duckdb") %>%
  SqlRender::splitSql()

purrr::walk(synthea_ddl, ~DBI::dbExecute(con, .))





cdmSchema      <- "cdm_synthea10"
cdmVersion     <- "5.4"
syntheaVersion <- "2.7.0"
syntheaSchema  <- "native"
syntheaFileLoc <- "/tmp/synthea/output/csv"
vocabFileLoc   <- "/tmp/Vocabulary_20181119"

ETLSyntheaBuilder::CreateCDMTables(connectionDetails = cd, cdmSchema = cdmSchema, cdmVersion = cdmVersion)

ETLSyntheaBuilder::CreateSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

ETLSyntheaBuilder::LoadSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaFileLoc = syntheaFileLoc)

ETLSyntheaBuilder::LoadVocabFromCsv(connectionDetails = cd, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc)

ETLSyntheaBuilder::LoadEventTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)


# load vocab
CommonDataModel::l


# java -jar synthea-with-dependencies.jar -p 100 -s 12345 -m *Cancer -a 40-75 --exporter.csv.export true --exporter.baseDirectory "./synthea_cancer/" --exporter.fhir.export false
#
# -- wildcard not working
#
# java -jar synthea-with-dependencies.jar `
# -p 100 -s 12345 -m breast_cancer|colorectal_cancer|lung_cancer -a 40-90 `
# --exporter.csv.export true --exporter.baseDirectory "./synthea_cancer1k/" --exporter.fhir.export false
#
# # on ubuntu
#
# java -jar synthea-with-dependencies.jar \
# -p 1000 -s 12345 -m breast_cancer:colorectal_cancer:lung_cancer -a 40-90 \
# --exporter.csv.export true --exporter.baseDirectory "./synthea_cancer1k/" --exporter.fhir.export false
#
#
# java -jar synthea-with-dependencies.jar \
# -p 100000 -s 12345 -m breast_cancer:colorectal_cancer:lung_cancer -a 40-90 \
# --exporter.csv.export true --exporter.baseDirectory "./synthea_cancer100k/" --exporter.fhir.export false
#
#

