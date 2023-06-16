insert into main.payer_plan_period (
  payer_plan_period_id,
  person_id,
  payer_plan_period_start_date,
  payer_plan_period_end_date,
  payer_concept_id,
  payer_source_value,
  payer_source_concept_id,
  plan_concept_id,
  plan_source_value,
  plan_source_concept_id,
  sponsor_concept_id,
  sponsor_source_value,
  sponsor_source_concept_id,
  family_source_value,
  stop_reason_concept_id,
  stop_reason_source_value,
  stop_reason_source_concept_id
)
select ROW_NUMBER()OVER(ORDER BY pat.id, pt.start_year) payer_plan_period_id,
       per.person_id                                    person_id,
	   0                                                payer_concept_id,
	   pt.payer                                         payer_source_value,
	   0                                                payer_source_concept_id,
	   0                                                plan_concept_id,
	   pay.name                                         plan_source_value,
	   0                                                plan_source_concept_id,
	   0                                                sponsor_concept_id,
	   CAST(NULL AS VARCHAR)                            sponsor_source_value,
	   0                                                sponsor_source_concept_id,
	   CAST(NULL AS VARCHAR)                            family_source_value,
	   0                                                stop_reason_concept_id,
	   CAST(NULL AS VARCHAR)                            stop_reason_source_value,
	   0                                                stop_reason_source_concept_id
  from synthea.payers pay 
  join synthea.payer_transitions pt
    on pay.id = pt.payer
  join synthea.patients pat
    on pt.patient = pat.id  
  join main.person per
    on pat.id = per.person_source_value
;	