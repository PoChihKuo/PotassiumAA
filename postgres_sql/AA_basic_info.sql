
-- contains  all patients' first ICUstay basic information

DROP MATERIALIZED VIEW IF EXISTS AAbasicinfo_allICU CASCADE;
CREATE MATERIALIZED VIEW AAbasicinfo_allICU as (


-- 选出满足条件的患者： 年龄 和 icusaty时长
with firstIcu as (
     SELECT  uniquepid, patienthealthsystemstayid as hadmid, patientunitstayid as ICUSTAY_ID-- , apacheadmissiondx  
           , case when age like '%>%' then '91.4'
					        when age is null then null 
							else age 
							end as age
					 , gender, apache_iv, unittype, icu_los_hours
           , row_number() OVER (PARTITION BY uniquepid, patienthealthsystemstayid ORDER BY unitvisitnumber) AS first_icu
     FROM eicu1_3.icustay_detail
)
, getFirstICU as(
     SELECT  uniquepid
           , hadmid
           , ICUSTAY_ID
          --  , apacheadmissiondx  
           , age, gender, apache_iv, unittype, icu_los_hours
           , ceil(icu_los_hours::numeric/2::numeric) as WindowOfICU -- (floor(icu_los_hours::numeric/2::numeric)+1) as  WindowOfICU
					 --  
      FROM  firstIcu
    WHERE first_icu = 1
		and age != ''
)

-- , basic as (
SELECT icdl.uniquepid
    , icdl.hadmid
	, icdl.ICUSTAY_ID
	, icdl.age
	, icdl.gender
	, icdl.apache_iv
  , icdl.unittype
 -- , icdl.apacheadmissiondx  
  , icdl.icu_los_hours
  , icdl.WindowOfICU 
FROM
	getFirstICU icdl

WHERE
 cast(icdl.age as NUMERIC ) > 16.0 
-- AND 
-- 	 icu_los_hours >=7 
-- AND
--    lower(unittype)  similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%'

)


-- SELECT DISTINCT icustay_id from AAbasicinfo_allICU  -- 166002
-- where lower(unittype)  similar to  '%micu%|sicu%|%med-surg%|%ccu-cticu%|cardiac icu|%csicu%|%cticu%' 
-- and icu_los_hours <= 0 -- 2572
-- '%ccu-cticu%|cardiac icu|%csicu%|%cticu%' -- when > 6 34690 
-- 原来插值时是考虑了大于 等于7小时的患者，现在需要所有患者都应该考虑进去所以现在需要再把这部分患者的potassium值进行插值处理
with basic as (
SELECT a.icustay_id
     , age
		 , gender
		 , apache_iv
		 , unittype
		 , icu_los_hours
		 , (2* windowoficu)-1 as icuwindow

		 , m.postawindow
		 , m.labresult
-- 		 , n.postawindow
-- 		 , n.close_potassium
from AAbasicinfo_allICU a
inner join maxPotassium_ver3 m
     on a.icustay_id = m.icustay_id
-- where icu_los_hours < 7 
-- and icu_los_hours > 0  -- 14828
where icu_los_hours <= 0
)
SELECT * , n.close_potassium
from basic b
inner join nearest_Potassium3 n
    on b.icustay_id = n.icustay_id
		and b.postawindow = n.postawindow
