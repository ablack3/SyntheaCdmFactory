delete from main.procedure_occurrence;

insert into main.procedure_occurrence (
procedure_occurrence_id,
person_id,
procedure_concept_id,
procedure_date,
procedure_datetime,
procedure_type_concept_id,
modifier_concept_id,
quantity,
provider_id,
visit_occurrence_id,
visit_detail_id,
procedure_source_value,
procedure_source_concept_id,
modifier_source_value
)
select
row_number()over(order by p.person_id)   procedure_occurrence_id,
p.person_id                              person_id,
srctostdvm.target_concept_id             procedure_concept_id,
cast(pr.start as date)                   procedure_date,
cast(pr.start as timestamp)              procedure_datetime,
38000267                                 procedure_type_concept_id,
0                                        modifier_concept_id,
cast(null as integer)                    quantity,
prv.provider_id                          provider_id,
fv.visit_occurrence_id_new               visit_occurrence_id,
fv.visit_occurrence_id_new + 1000000     visit_detail_id,
pr.code                                  procedure_source_value,
srctosrcvm.source_concept_id             procedure_source_concept_id,
null                                     modifier_source_value
from synthea.procedures pr
join main.source_to_standard_vocab_map srctostdvm
  on srctostdvm.source_code             = pr.code
 and srctostdvm.target_domain_id        = 'Procedure'
 and srctostdvm.target_vocabulary_id    = 'SNOMED'
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
;
