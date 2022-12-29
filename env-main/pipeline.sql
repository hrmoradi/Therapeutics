

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5ef5aa1b-806d-4bb6-bcdf-af52e162ded2"),
    covid_with_earliest_index_and_ab_flag=Input(rid="ri.foundry.main.dataset.fe8bfed9-0d32-4295-94ef-5d9dfd10cbf1")
)
SELECT *
FROM covid_with_earliest_index_and_ab_flag
where post_vaccination_antibody_only = 0 and   covid_index_date > '2020-01-01' and   covid_index_date < '2021-06-30' 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.856f0c0e-c701-4d06-868b-5374a4e5af46"),
    flattened_drugs=Input(rid="ri.foundry.main.dataset.bc736f5c-2c4e-4cfe-b117-57d49c818d7e"),
    levels_by_day=Input(rid="ri.foundry.main.dataset.178867ea-a908-4a22-88fc-d3363a4a6dbf")
)
SELECT *
FROM flattened_drugs
where person_id in
     (select ld.person_id
     from levels_by_day ld)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9ea678d6-619f-46e6-ad61-086979caf39e"),
    data_partner_grades=Input(rid="ri.foundry.main.dataset.6f9bb037-5b05-435f-b267-240f5af62cbb"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
select pers.person_id
from person as pers
inner join 
(select data_partner_id from data_partner_grades where drug_exposure_grade = 1) as part
on part.data_partner_id = pers.data_partner_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d875ccf4-8b17-498c-b3c7-18c3c1de97e1"),
    FMK_covidpos_new_OS_May=Input(rid="ri.foundry.main.dataset.eaed807a-4ebd-4c84-a302-b6fc22d373c6"),
    data_partner_grades=Input(rid="ri.foundry.main.dataset.6f9bb037-5b05-435f-b267-240f5af62cbb"),
    levels_by_day=Input(rid="ri.foundry.main.dataset.178867ea-a908-4a22-88fc-d3363a4a6dbf")
)
SELECT p.person_id, p.date_of_earliest_covid_diagnosis,
       age, gender, race, ethnicity, bmi, -- <<<<<<<<<<<<<<<<<<<<<<
       CCI_INDEX, hypertension, upper_gi_bleed, MI, CHF, PVD,
       stroke, dementia, pulmonary, rheumatic, PUD, liver_mild,
       liversevere, diabetes, dmcx, paralysis, renal, cancer,
       mets, hiv
FROM  FMK_covidpos_new_OS_May p
inner join
-- grade_person grade on p.person_id = grade.person_id
data_partner_grades grade on p.data_partner_id = grade.data_partner_id
inner join
levels_by_day ld   on p.person_id = ld.person_id
where p.date_of_earliest_covid_diagnosis < '2021-06-30' and grade.drug_exposure_grade = 1

-- SELECT p.person_id, diag.date_of_earliest_covid_diagnosis,
--        age_at_visit_start_in_years_int as age, gender_concept_name as gender, Race as race, Ethnicity as ethnicity, BMI as bmi, -- <<<<<<<<<<<<<<<<<<<<<<
--        CCI_INDEX, hypertension, upper_gi_bleed, MI, CHF, PVD,
--        stroke, dementia, pulmonary, rheumatic, PUD, liver_mild,
--        liversevere, diabetes, dmcx, paralysis, renal, cancer,
--        mets, hiv
-- FROM complete_patient_table_with_derived_scores p 
-- inner join 
-- covid_positive_with_cci_categories cci on p.person_id = cci.person_id
-- inner join
-- Covid_positive_persons_LDS diag on p.person_id = diag.person_id
-- inner join
-- levels_by_day ld   on p.person_id = ld.person_id
-- inner join
-- grade_person grade on p.person_id = grade.person_id
-- inner join
-- ab_flag ab   on p.person_id = ab.person_id 
-- where ab.post_vaccination_antibody_only = 0 and   ab.covid_index_date > '2020-01-01' and   ab.covid_index_date < '2021-06-30'  and diag.date_of_earliest_covid_diagnosis > '2020-01-01' and   diag.date_of_earliest_covid_diagnosis < '2021-06-30'  

@transform_pandas(
    Output(rid="ri.vector.main.execute.c9a1a081-0e51-465c-b1fc-3a09e5e72d53"),
    FMK_daily_meds_May=Input(rid="ri.foundry.main.dataset.20d1d65b-0404-46ab-979d-566cf90eb53f")
)
SELECT count(distinct person_id)
FROM FMK_daily_meds_May

@transform_pandas(
    Output(rid="ri.vector.main.execute.db4f5261-4644-49c2-8897-353de84cfb5c"),
    FMK_covidpos_new_OS_May=Input(rid="ri.foundry.main.dataset.eaed807a-4ebd-4c84-a302-b6fc22d373c6")
)
SELECT count(distinct person_id)
FROM FMK_covidpos_new_OS_May

@transform_pandas(
    Output(rid="ri.vector.main.execute.6eba21a7-8439-4eb8-b519-b9c6d0e4bb12"),
    pts_info=Input(rid="ri.foundry.main.dataset.d875ccf4-8b17-498c-b3c7-18c3c1de97e1")
)
SELECT count(distinct person_id)
FROM pts_info

@transform_pandas(
    Output(rid="ri.vector.main.execute.09b9846e-6bef-487a-bd75-954dff16e80e"),
    levels_by_day=Input(rid="ri.foundry.main.dataset.178867ea-a908-4a22-88fc-d3363a4a6dbf")
)
SELECT count(distinct person_id)
FROM levels_by_day

