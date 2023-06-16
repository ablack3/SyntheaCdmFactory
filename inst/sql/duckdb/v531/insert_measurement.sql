insert into main.measurement (
measurement_id,
person_id,
measurement_concept_id,
measurement_date,
measurement_datetime,
measurement_time,
measurement_type_concept_id,
operator_concept_id,
value_as_number,
value_as_concept_id,
unit_concept_id,
range_low,
range_high,
provider_id,
visit_occurrence_id,
visit_detail_id,
measurement_source_value,
measurement_source_concept_id,
unit_source_value,
value_source_value
)
select row_number()over(order by person_id) measurement_id,
person_id,
measurement_concept_id,
measurement_date,
measurement_datetime,
measurement_time,
measurement_type_concept_id,
operator_concept_id,
value_as_number,
value_as_concept_id,
unit_concept_id,
range_low,
range_high,
provider_id,
visit_occurrence_id,
visit_detail_id,
measurement_source_value,
measurement_source_concept_id,
unit_source_value,
value_source_value
from (
select
  p.person_id                              person_id,
  srctostdvm.target_concept_id             measurement_concept_id,
  38000267                                 measurement_type_concept_id,
  0                                        operator_concept_id,
  cast(null as NUMERIC)                      value_as_number,
  0                                        value_as_concept_id,
  0                                        unit_concept_id,
  cast(null as NUMERIC)                      range_low,
  cast(null as NUMERIC)                      range_high,
  prv.provider_id                          provider_id,
  fv.visit_occurrence_id_new               visit_occurrence_id,
  fv.visit_occurrence_id_new + 1000000     visit_detail_id,
  pr.code                                  measurement_source_value,
  srctosrcvm.source_concept_id             measurement_source_concept_id,
  cast(null as varchar)                    unit_source_value,
  cast(null as varchar)                    value_source_value
from synthea.procedures pr
join main.source_to_standard_vocab_map  srctostdvm
  on srctostdvm.source_code             = pr.code
 and srctostdvm.target_domain_id        = 'Measurement'
 and srctostdvm.source_vocabulary_id    = 'SNOMED'
 and srctostdvm.target_standard_concept = 'S'
 and srctostdvm.target_invalid_reason is null
join main.source_to_source_vocab_map srctosrcvm
  on srctosrcvm.source_code             = pr.code
 and srctosrcvm.source_vocabulary_id    = 'SNOMED'
left join main.final_visit_ids fv
  on fv.encounter_id                    = pr.encounter
left join synthea.encounters e
  on pr.encounter                       = e.id
 and pr.patient                         = e.patient
left join main.provider prv
  on e.provider                         = prv.provider_source_value
join main.person p
  on p.person_source_value              = pr.patient
union all
select
  p.person_id                               person_id,
  srctostdvm.target_concept_id              measurement_concept_id,
  o.date                                    measurement_date,
  o.date                                    measurement_datetime,
  o.date                                    measurement_time,
  38000267                                  measurement_type_concept_id,
  0                                         operator_concept_id,
  case
  when CASE WHEN (CAST(o.value AS VARCHAR) ~ '^([0-9]+\.?[0-9]*|\.[0-9]+)$') THEN 1 ELSE 0 END = 1
  then cast(o.value as NUMERIC)
  else cast(null as NUMERIC)
  end                                       value_as_number,
  coalesce(srcmap2.target_concept_id,0)     value_as_concept_id,
  coalesce(srcmap1.target_concept_id,0)     unit_concept_id,
  cast(null as NUMERIC)                       range_low,
  cast(null as NUMERIC)                       range_high,
  pr.provider_id                            provider_id,
  fv.visit_occurrence_id_new                visit_occurrence_id,
  fv.visit_occurrence_id_new + 1000000      visit_detail_id,
  o.code                                    measurement_source_value,
  coalesce(srctosrcvm.source_concept_id,0)  measurement_source_concept_id,
  o.units                                   unit_source_value,
  o.value                                   value_source_value
from synthea.observations o
join main.source_to_standard_vocab_map  srctostdvm
  on srctostdvm.source_code             = o.code
 and srctostdvm.target_domain_id        = 'Measurement'
 and srctostdvm.source_vocabulary_id    = 'LOINC'
 and srctostdvm.target_standard_concept = 'S'
 and srctostdvm.target_invalid_reason   is null
left join main.source_to_standard_vocab_map  srcmap1
  on srcmap1.source_code                = o.units
 and srcmap1.target_vocabulary_id       = 'UCUM'
 and srcmap1.source_vocabulary_id       = 'UCUM'
 and srcmap1.target_standard_concept    = 'S'
 and srcmap1.target_invalid_reason      is null
left join main.source_to_standard_vocab_map  srcmap2
  on srcmap2.source_code                = o.value
 and srcmap2.target_domain_id           = 'Meas value'
 and srcmap2.target_standard_concept    = 'S'
 and srcmap2.target_invalid_reason     is null
left join main.source_to_source_vocab_map srctosrcvm
  on srctosrcvm.source_code             = o.code
 and srctosrcvm.source_vocabulary_id    = 'LOINC'
left join main.final_visit_ids fv
  on fv.encounter_id                    = o.encounter
left join synthea.encounters e
  on o.encounter                        = e.id
 and o.patient                          = e.patient
left join main.provider pr
  on e.provider                         = pr.provider_source_value
join main.person p
  on p.person_source_value              = o.patient
  ) tmp
;
