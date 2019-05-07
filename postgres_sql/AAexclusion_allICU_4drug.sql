
-- 排除不符合用药选入标准的患者， 只保留合适的

DROP MATERIALIZED VIEW IF EXISTS AAexclusion_allICU_4drug CASCADE;
CREATE MATERIALIZED VIEW AAexclusion_allICU_4drug as (
with amiodarone_all as (
  SELECT  patientunitstayid as ICUSTAY_ID
			 , drugstartoffset 
			 , drugorderoffset
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       , ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   -- (floor(drugorderoffset::numeric/120::numeric)+1)  -- window for drug order in  
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%amiodarone%|%cordarone%'  THEN 'amiodarone'
						 else null 
         END as drug_label
	from medication
	WHERE "lower"(drugname)  similar to '%amiodarone%|%cordarone%'                                              -- Anti-arrythmic drugs
		AND lower(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
		and LOWER(drugname) not SIMILAR to '%each package custom ndc%|%amiodarone hcl 200 mg po tabs|amiodarone \(pacerone\)%tablet%|amiodarone 200 mg%'	
	ORDER BY 
		ICUSTAY_ID
)
, amiod_test as (
  SELECT  patientunitstayid as ICUSTAY_ID
			 , drugstartoffset 
			 , drugorderoffset
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       ,  ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   
       , drugname, dosage
--        , routeadmin
--        , medicationid
       , CASE
             WHEN "lower"(drugname) similar to '%amiodarone%|%cordarone%' THEN 'amiodarone'
					 --  and lower(dosage) similar to '(1|2) ml%' 
						 else null 
         END as drug_label
			 , case when "lower"(routeadmin) similar to'%iv%|inf|%intraven%' then 'IV'
			        else null
					end as routeadmin
	from medication
	WHERE "lower"(drugname)  similar to '%amiodarone%|%cordarone%'                                              -- Anti-arrythmic drugs
		AND lower(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
		and LOWER(drugname) not SIMILAR to '%each package custom ndc%|%amiodarone hcl 200 mg po tabs|amiodarone \(pacerone\)%tablet%|amiodarone 200 mg%'
	ORDER BY 
		ICUSTAY_ID
)
, order_amio as (
-- 对用药进行排序
SELECT ICUSTAY_ID, drugstartoffset, drugorderoffset, drugorderoffsetH,orderWindow,drug_label,routeadmin ,drugname, dosage,
       "row_number"() over(PARTITION by ICUSTAY_ID order by drugstartoffset) as first_amio
			 from amiod_test
)
, amiodarone_1 as (
SELECT * from order_amio
where first_amio =1
and routeadmin ='IV'
and lower(dosage) similar to '(150|300)%'
)
, ex_amiodarone as(
  SELECT distinct icustay_id from amiodarone_all
	where icustay_id not in (
	select distinct icustay_id from amiodarone_1
	)
)
, med_drug_AA as (
-- 选出AA_drug(中间结果)
	SELECT  patientunitstayid as ICUSTAY_ID
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       ,  ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   -- (floor(drugorderoffset::numeric/120::numeric)+1)  -- window for drug order in  
       , drugname

       , CASE             
						 WHEN "lower"(drugname) similar to '%digoxin%|%digitek|%digox%|%lanoxin%'  THEN 'digoxin'
						 WHEN "lower"(drugname) similar to '%procainamide%|%procan%'  THEN 'procainamide'
						 else null 
         END as drug_label
	from medication
	WHERE "lower"(drugname)  similar to '%digoxin%|%digitek|%digox%|%lanoxin%|%procainamide%|%procan%'-- Anti-arrythmic drugs
		AND lower(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
	ORDER BY 
		ICUSTAY_ID
)
, infu_drug_AA as (
-- 选出AA_drug(中间结果)
	SELECT  patientunitstayid as ICUSTAY_ID
       , round(infusionoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       , ceil(infusionoffset::numeric/120::numeric)  as  orderWindow  -- window for drug order in  (floor(infusionoffset::numeric/120::numeric)+1)
--        , drugstartoffset
--        , round(drugStopOffset::numeric/60::numeric ,2) as drugStopOffsetH
       , drugname
       , CASE

						 WHEN "lower"(drugname) similar to '%digoxin%|%digitek|%digox%|%lanoxin%'  THEN 'digoxin'
						 WHEN "lower"(drugname) similar to '%procainamide%|%procan%'  THEN 'procainamide' 
						 else null 
         END as drug_label
	from infusiondrug
	WHERE "lower"(drugname)  similar to '%digoxin%|%digitek|%digox%|%lanoxin%|%procainamide%|%procan%'
-- '%lidocaine%|%xylocaine%|%amiodarone%|%cordarone%|%adenosine%|%adenocard%|%adenoscan%|%digoxin%|%digitek|%digox%|%lanoxin%|%procainamide%|%procan%|%ibutilide%|%corvert%|%sotalol%|%betapace%|%sorine%|%sotylize%|%esmolol%|%brevibloc%|%lopressor%|%toprol%|%metoprolol%|%diltiazem%|%cardizem%|%dilacor%|%matzim%|%taztia%|%tiazac%'       -- Anti-arrythmic drugs
	AND drugrate ~ '^[0-9.]{1,8}$' 
				AND drugrate <> '.' 
				AND drugrate <> ''
	ORDER BY ICUSTAY_ID
)
, exclude_med_AA as (
  SELECT icustay_id 
	      , drugorderoffsetH
				, (2*orderWindow)-1 as orderWindow  -- (2 * drugorderwindow) - 1 as drugorderwindow
				, drugname
				, case 
			    when "lower"(drug_label) like 'digoxin%' and LOWER(drugname) SIMILAR to '%each package%|%digoxin 0.05 mg/ml po%|digoxin  50 mcg/ml%|digoxin \(lanoxin\)%tablet%|digoxin (0.0625|0.125|.025) mg%tab%' then null				
					else drug_label
				end as drug_label
	FROM med_drug_AA
)
, com_drugAA as (
  SELECT  ICUSTAY_ID
	      , drugorderoffsetH
				, orderWindow
				, drugname
				, drug_label
		from exclude_med_AA
	where drug_label is not null
	UNION (
	SELECT  ICUSTAY_ID
	      , drugorderoffsetH
				, (2*orderWindow)-1 as orderWindow
				, drugname
				, drug_label
		from infu_drug_AA
	  where drug_label is not null
	
	)
order by ICUSTAY_ID, drug_label, drugname
)
, ex as (
  (SELECT DISTINCT ICUSTAY_ID
    from com_drugAA  fd
		where lower(drug_label) similar to 'digoxin|procainamide'	
		)
		union (
		select icustay_id from ex_amiodarone
		)
) 
-- SELECT DISTINCT  ICUSTAY_ID from ex -- 15936
SELECT icdl.uniquepid
    , icdl.hadmid
	, icdl.ICUSTAY_ID
	, icdl.age
	, icdl.gender
	, icdl.apache_iv
  , icdl.unittype
 --  , icdl.apacheadmissiondx  
  , icdl.icu_los_hours
  , icdl.WindowOfICU 
FROM eicu1_3.AAbasicinfo_allICU icdl
where icdl.ICUSTAY_ID not in (
 SELECT DISTINCT  ICUSTAY_ID from ex
  )
)
-- SELECT distinct icustay_id from AAbasicinfo_allICU 166002
-- SELECT distinct icustay_id from  AAexclusion_allICU_4drug   (150734)
select distinct icdl.icustay_id  
from AAexclusion_allICU_4drug icdl
-- INNER JOIN maxPotassium_ver3 mp
--      ON icdl.ICUSTAY_ID = mp.ICUSTAY_ID
where lower(icdl.unittype)   similar to  '%micu%|sicu%|%med-surg%|%ccu-cticu%|cardiac icu|%csicu%|%cticu%'  -- 139016
and icu_los_hours <= 0

