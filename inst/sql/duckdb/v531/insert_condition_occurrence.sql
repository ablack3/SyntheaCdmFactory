delete from main.condition_occurrence;

insert into main.condition_occurrence (
  condition_occurrence_id,
  person_id,
  condition_concept_id,
  condition_start_date,
  condition_start_datetime,
  condition_end_date,
  condition_end_datetime,
  condition_type_concept_id,
  stop_reason,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  condition_source_value,
  condition_source_concept_id,
  condition_status_source_value,
  condition_status_concept_id
)
with cte as(
select c.*, p.person_id, fv.visit_occurrence_id_new
from synthea.conditions c
join main.person p on c.patient = p.person_source_value
left join main.final_visit_ids fv on fv.encounter_id = c.encounter
)
select
  row_number() over (order by cte.person_id) condition_occurrence_id,
  cte.person_id                              person_id,
  map.target_concept_id                      condition_concept_id,
  cte.start                                  condition_start_date,
  cte.start                                  condition_start_datetime,
  coalesce(cte.stop, cte.start)              condition_end_date,
  coalesce(cte.stop, cte.start)              condition_end_datetime,
  38000175                                   condition_type_concept_id,
  cast(null as varchar)                      stop_reason,
  cast(null as integer)                      provider_id,
  cte.visit_occurrence_id_new                visit_occurrence_id,
  cte.visit_occurrence_id_new + 1000000      visit_detail_id,
  cte.code                                   condition_source_value,
  map.source_concept_id                      condition_source_concept_id,
  null                                       condition_status_source_value,
  0                                          condition_status_concept_id
from cte
join main.source_to_standard_vocab_map map
    on map.source_code             = cte.code
   and map.target_domain_id        = 'Condition'
   and map.target_vocabulary_id    = 'SNOMED'
   and map.source_vocabulary_id    = 'SNOMED'
   and map.target_standard_concept = 'S'
   and map.target_invalid_reason is null
;
