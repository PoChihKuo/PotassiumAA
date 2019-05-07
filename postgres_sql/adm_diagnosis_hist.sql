--- get patients' admission diagnose  and past history
DROP MATERIALIZED VIEW IF EXISTS AAadmi_hist CASCADE;
CREATE MATERIALIZED VIEW AAadmi_hist as (
with  mi as(
  SELECT distinct patientunitstayid as icustay_id 
    --   , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%myocardial infarction%' 
)
, chf as (
  SELECT distinct patientunitstayid as icustay_id 
     --   , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%congestive heart failure%'
)

, renal_failure as (
  SELECT distinct patientunitstayid as icustay_id 
    --    , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%renal failure%|%renal insufficiency%'
)


-- Admission diagnosis for MI
,admin_mi as (
  SELECT distinct patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%(infarction, acute myocardial|acute mi|mi admitted)%'
) 
,admin_chf as (
SELECT distinct patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%congestive heart failure|chf%'
)
, admi_renalFai as (
SELECT distinct   patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%renal failure%|%renal%insufficiency%'
)
, admi_sepsis as (
SELECT distinct patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%sepsis%'
)

select distinct  icustay_id
			, case when icustay_id in ( select icustay_id from mi) then 1 else 0 end as history_MI
			, case when icustay_id in ( select icustay_id from chf) then 1 else 0 end as history_chf
			, case when icustay_id in ( select icustay_id from renal_failure) then 1 else 0 end as history_renal_failure
			, case when icustay_id in ( select icustay_id from admin_mi) then 1 else 0 end as admission_MI
			, case when icustay_id in ( select icustay_id from admin_chf) then 1 else 0 end as admission_chf
			, case when icustay_id in ( select icustay_id from admi_renalFai) then 1 else 0 end as admission_renal_failure
			, case when icustay_id in ( select icustay_id from admi_sepsis) then 1 else 0 end as admission_sepsis
from AAbasicinfo_allICU
)