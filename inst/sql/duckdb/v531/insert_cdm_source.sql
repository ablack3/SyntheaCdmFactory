insert into main.cdm_source (
cdm_source_name,
cdm_source_abbreviation,
cdm_holder,
source_description,
source_documentation_reference,
cdm_etl_reference,
source_release_date,
cdm_release_date,
cdm_version,
vocabulary_version
)
select
'Synthea',
'Synthea',
'',
'Synthea Synthetic Data',
'https://synthetichealth.github.io/synthea/',
'https://github.com/OHDSI/ETL-Synthea',
CURRENT_DATE, -- NB: Set this value to the day the source data was pulled
CURRENT_DATE,
'5.3.1',
vocabulary_version
from main.vocabulary
where vocabulary_id = 'None';
