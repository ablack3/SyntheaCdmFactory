
available_modules <- c("hiv", "breast_cancer", "metabolic_syndrome", "encounter",
                       "covid19", "dermatitis", "contraceptives", "allergies", "heart",
                       "lung_cancer", "total_joint_replacement", "snf", "weight_loss",
                       "anemia", "medications", "surgery", "veterans", "opioid_addiction",
                       "cerebral_palsy", "dialysis", "allergic_rhinitis", "pregnancy",
                       "atopy", "self_harm", "asthma", "ear_infections", "sinusitis",
                       "dementia", "veteran_hyperlipidemia", "mTBI", "veteran_prostate_cancer",
                       "urinary_tract_infections", "hypothyroidism",
                       "osteoarthritis", "appendicitis", "copd", "contraceptive_maintenance",
                       "fibromyalgia", "veteran_substance_abuse_treatment", "veteran_lung_cancer",
                       "prescribing_opioids_for_chronic_pain_and_treatment_of_oud",
                       "hospice_treatment", "rheumatoid_arthritis", "sore_throat", "sleep_apnea",
                       "gallstones", "bronchitis", "spina_bifida", "sexual_activity",
                       "homelessness", "epilepsy", "wellness_encounters", "injuries",
                       "colorectal_cancer", "med_rec", "congestive_heart_failure", "veteran_self_harm",
                       "veteran_mdd", "osteoporosis", "female_reproduction", "veteran",
                       "gout", "home_hospice_snf", "metabolic_syndrome_disease", "sepsis",
                       "metabolic_syndrome_care", "chronic_kidney_disease", "home_health_treatment",
                       "lupus", "cystic_fibrosis", "attention_deficit_disorder", "food_allergies",
                       "mend_program", "hiv_care")

length(available_modules)

for (i in 1:74) {
  tryCatch(
    synthesize_cdm(path = "~/Desktop/covid_cdm",
                       vocab_path = here::here("vocab/vocabulary_bundle_v5_0-22-JUN-22.zip"),
                       n_persons = 10000,
                       modules = available_modules[i],
                       format = "duckdb",
                       overwrite = T)
  ,
  error = function(e) warning(glue::glue("module failed {i} = {available_modules[i]}")))
}

synthesize_cdm(path = "~/Desktop/synthea_cdms",
               vocab_path = here::here("vocab/vocabulary_bundle_v5_0-22-JUN-22.zip"),
               n_persons = 10000,
               modules = "veteran_prostate_cancer",
               format = "duckdb",
               overwrite = T)
