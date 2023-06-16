DROP TABLE IF EXISTS main.source_to_standard_vocab_map;

CREATE TABLE main.source_to_standard_vocab_map AS WITH CTE_VOCAB_MAP AS (
    SELECT
        c.concept_code as source_code,
        c.concept_id as source_concept_id,
        c.concept_name as source_code_description,
        c.vocabulary_id as source_vocabulary_id,
        c.domain_id as source_domain_id,
        c.concept_class_id as source_concept_class_id,
        c.valid_start_date as source_valid_start_date,
        c.valid_end_date as source_valid_end_date,
        c.invalid_reason as source_invalid_reason,
        c1.concept_id as target_concept_id,
        c1.concept_name as target_concept_name,
        c1.vocabulary_id as target_vocabulary_id,
        c1.domain_id as target_domain_id,
        c1.concept_class_id as target_concept_class_id,
        c1.invalid_reason as target_invalid_reason,
        c1.standard_concept as target_standard_concept
    FROM
        main.concept C
        JOIN main.concept_relationship cr ON c.concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL
        AND lower(cr.relationship_id) = CAST('maps to' as TEXT)
        JOIN main.concept c1 ON cr.concept_id_2 = c1.concept_id
        AND c1.invalid_reason IS NULL
    UNION
    SELECT
        source_code,
        source_concept_id,
        source_code_description,
        source_vocabulary_id,
        c1.domain_id as source_domain_id,
        c2.concept_class_id as source_concept_class_id,
        c1.valid_start_date as source_valid_start_date,
        c1.valid_end_date as source_valid_end_date,
        stcm.invalid_reason as source_invalid_reason,
        target_concept_id,
        c2.concept_name as target_concept_name,
        target_vocabulary_id,
        c2.domain_id as target_domain_id,
        c2.concept_class_id as target_concept_class_id,
        c2.invalid_reason as target_invalid_reason,
        c2.standard_concept as target_standard_concept
    FROM
        main.source_to_concept_map stcm
        LEFT OUTER JOIN main.concept c1 ON c1.concept_id = stcm.source_concept_id
        LEFT OUTER JOIN main.CONCEPT c2 ON c2.concept_id = stcm.target_concept_id
    WHERE
        stcm.invalid_reason IS NULL
)
SELECT
    *
FROM
    CTE_VOCAB_MAP;

create index idx_vocab_map_source_code on main.source_to_standard_vocab_map (source_code);

create index idx_vocab_map_source_vocab_id on main.source_to_standard_vocab_map (source_vocabulary_id);

DROP TABLE IF EXISTS main.source_to_source_vocab_map;

CREATE TABLE main.source_to_source_vocab_map AS WITH CTE_VOCAB_MAP AS (
    SELECT
        c.concept_code AS SOURCE_CODE,
        c.concept_id AS SOURCE_CONCEPT_ID,
        c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
        c.vocabulary_id AS SOURCE_VOCABULARY_ID,
        c.domain_id AS SOURCE_DOMAIN_ID,
        c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
        c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
        c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
        c.invalid_reason AS SOURCE_INVALID_REASON,
        c.concept_ID as TARGET_CONCEPT_ID,
        c.concept_name AS TARGET_CONCEPT_NAME,
        c.vocabulary_id AS TARGET_VOCABULARY_ID,
        c.domain_id AS TARGET_DOMAIN_ID,
        c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
        c.INVALID_REASON AS TARGET_INVALID_REASON,
        c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
    FROM
        main.CONCEPT c
    UNION
    SELECT
        source_code,
        SOURCE_CONCEPT_ID,
        SOURCE_CODE_DESCRIPTION,
        source_vocabulary_id,
        c1.domain_id AS SOURCE_DOMAIN_ID,
        c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
        c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
        c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
        stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
        target_concept_id,
        c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
        target_vocabulary_id,
        c2.domain_id AS TARGET_DOMAIN_ID,
        c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
        c2.INVALID_REASON AS TARGET_INVALID_REASON,
        c2.standard_concept AS TARGET_STANDARD_CONCEPT
    FROM
        main.source_to_concept_map stcm
        LEFT OUTER JOIN main.CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
        LEFT OUTER JOIN main.CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
    WHERE
        stcm.INVALID_REASON IS NULL
)
SELECT
    *
FROM
    CTE_VOCAB_MAP;

create index idx_source_vocab_map_source_code on main.source_to_source_vocab_map (source_code);

create index idx_source_vocab_map_source_vocab_id on main.source_to_source_vocab_map (source_vocabulary_id)
