-- 
DROP MATERIALIZED VIEW IF EXISTS AAFirst5Drugs  CASCADE;
CREATE MATERIALIZED VIEW AAFirst5Drugs  as

-- amiodarone adenosine lidocaine 'ibutilide'  isoprotere
with amiod_test as (
  SELECT  patientunitstayid as ICUSTAY_ID
			 , drugstartoffset 
			 , drugorderoffset
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       ,  ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   -- (floor(drugorderoffset::numeric/120::numeric)+1)  -- window for drug order in  
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
, med_drug_AA as (
-- 选出AA_drug(中间结果)
	SELECT  patientunitstayid as ICUSTAY_ID
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       ,  ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   -- (floor(drugorderoffset::numeric/120::numeric)+1)  -- window for drug order in  
       , drugname
--        , routeadmin
--        , medicationid
       , CASE
             WHEN "lower"(drugname) similar to '%lidocaine%|%xylocaine%' and "lower"(routeadmin) similar to'%iv%|inf|%intraven%'   THEN 'lidocaine'     -- %subq%
--              WHEN "lower"(drugname) similar to '%amiodarone%|%cordarone%' and "lower"(routeadmin) similar to'%iv%|inf|%intraven%' 
-- 					   and lower(dosage) similar to '(1|2) ml%' THEN 'amiodarone'
						 WHEN "lower"(drugname) similar to '%adenosine%|%adenocard%|%adenoscan%'  THEN 'adenosine'
						--  WHEN "lower"(drugname) similar to '%digoxin%|%digitek|%digox%|%lanoxin%'  THEN 'digoxin'
						--  WHEN "lower"(drugname) similar to '%procainamide%|%procan%'  THEN 'procainamide'
						 WHEN "lower"(drugname) similar to '%ibutilide%|%corvert%'  THEN 'ibutilide'
-- 						 WHEN "lower"(drugname) similar to '%sotalol%|%betapace%|%sorine%|%sotylize%'  THEN 'sotalol'
-- 						 WHEN "lower"(drugname) similar to '%esmolol%|%brevibloc%'  THEN 'esmolol'
-- 						 WHEN "lower"(drugname) similar to '%lopressor%|%toprol%|%metoprolol%'  THEN 'metoprolol'
-- 						 WHEN "lower"(drugname) similar to '%diltiazem%|%cardizem%|%dilacor%|%matzim%|%taztia%|%tiazac%'  THEN 'diltiazem'	 
						 else null 
         END as drug_label
	from medication
	WHERE "lower"(drugname)  similar to '%lidocaine%|%xylocaine%|%adenosine%|%adenocard%|%adenoscan%|%ibutilide%|%corvert%'                                              -- Anti-arrythmic drugs
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
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%lidocaine%|%xylocaine%'    THEN 'lidocaine'     -- %subq%
           --   WHEN "lower"(drugname) similar to '%amiodarone%|%cordarone%'  THEN 'amiodarone'
						 WHEN "lower"(drugname) similar to '%adenosine%|%adenocard%|%adenoscan%'  THEN 'adenosine'
-- 						 WHEN "lower"(drugname) similar to '%digoxin%|%digitek|%digox%|%lanoxin%'  THEN 'digoxin'
-- 						 WHEN "lower"(drugname) similar to '%procainamide%|%procan%'  THEN 'procainamide'
						 WHEN "lower"(drugname) similar to '%ibutilide%|%corvert%'  THEN 'ibutilide'
-- 						 WHEN "lower"(drugname) similar to '%sotalol%|%betapace%|%sorine%|%sotylize%'  THEN 'sotalol'
-- 						 WHEN "lower"(drugname) similar to '%esmolol%|%brevibloc%'  THEN 'esmolol'
-- 						 WHEN "lower"(drugname) similar to '%lopressor%|%toprol%|%metoprolol%'  THEN 'metoprolol'
-- 						 WHEN "lower"(drugname) similar to '%diltiazem%|%cardizem%|%dilacor%|%matzim%|%taztia%|%tiazac%'  THEN 'diltiazem'	 
						 else null 
         END as drug_label
	from infusiondrug
	WHERE "lower"(drugname)  similar to '%lidocaine%|%xylocaine%|%adenosine%|%adenocard%|%adenoscan%|%ibutilide%|%corvert%'
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
				when "lower"(drug_label) = 'amiodarone' and LOWER(drugname) SIMILAR to '%each package custom ndc%|%amiodarone hcl 200 mg po tabs|amiodarone \(pacerone\)%tablet%|amiodarone 200 mg%'  then null
				 
-- 				      when "lower"(drug_label) like 'digoxin%' and LOWER(drugname) SIMILAR to '%each package%|%digoxin 0.05 mg/ml po%|digoxin  50 mcg/ml%|digoxin \(lanoxin\)%tablet%|digoxin (0.0625|0.125|.025) mg%tab%' then null
-- 					    WHEN "lower"(drug_label) = 'diltiazem' and LOWER(drugname) SIMILAR to '%oral suspension%|%bottle%ora-plus po liqd%|cardizem%(cd|la|sr)%|diltiazem \(cardizem%\)%(capsule|tablet)%|diltiazem \(tiazac\)%|diltiazem (60|90|120|180|240|30|300) mg%|diltiazem 24 hr%|%(once|twice)-a-day%|diltiazem (cd|er)%(capsule|cper)%|diltiazem gel%|diltiazem hcl (15|30|60|90|180|240|cd|er|sr|coated|tab)%|diltiazem hcl  (30|60)%|diltiazem sr|diltiazem\(%' then NULL
					   WHEN "lower"(drug_label) = 'lidocaine' and LOWER(drugname) SIMILAR to '%lidocaine hcl%(1|2|4)( \%|\%)%|%lidocaine( 10 mg/ml \(| )(1|2)( \%|\%|gm)%(mg|ml|syr|inj|soln|hcl)%|lidocaine \(pf\)%(1|2)( \%|\%)%(iv|inj)%|(30|5)%lidocaine \(pf\)%(1|2)( \%|\%)%(iv|inj)%|%xylocaine (m|2)%'   then null
-- 					   when "lower"(drug_label) = 'metoprolol' and LOWER(drugname) SIMILAR to '%each package custom_ndc%|%oral suspension%|%ora-plus po liqd%|%metoprolol \((lopressor|toprol-xl|toprol)\)%tab%|metoprolol (succ|extend|tar |tart )%|metoprolol(\(compounded\) | )(tartrate|xl|1.25)%(tab|tb|po)%|toprol(-| )xl' then null
-- 					   when "lower"(drug_label) = 'sotalol' and LOWER(drugname) SIMILAR to 'sotalol%tab%'  then null
					else drug_label
				end as drug_label
	FROM med_drug_AA
)

, com_drugAA_first4 as (
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
	union (
	SELECT  ICUSTAY_ID
	      , drugorderoffsetH
				, (2*orderWindow)-1 as orderWindow
				, drugname
				, drug_label
		from amiodarone_1
	where drug_label is not null
	)
order by ICUSTAY_ID, drug_label, drugname
)
, iso_med as(
SELECT patientunitstayid as ICUSTAY_ID
       , round(drugorderoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       ,  ceil(drugorderoffset::numeric/120::numeric)  as  orderWindow   
			 , drugname
			 , dosage
			 , CASE WHEN "lower"(drugname) similar to '%(isopre|isoprotere|isupre)%' 
						 and dosage not SIMILAR to '(Manual Charge|PYXIS|0 MG)' 
						 and dosage <> ''
						 THEN 'isoproterenol'
						 else null 
         END as drug_label
from medication
	WHERE "lower"(drugname)  similar to '%(isopre|isoprotere|isupre)%'
) 
, iso_infu as (
	SELECT patientunitstayid as ICUSTAY_ID
			 , round(infusionoffset::numeric/60::numeric ,2) as drugorderoffsetH -- 转为小时
       , ceil(infusionoffset::numeric/120::numeric)  as  orderWindow
       , drugname
			 , 'isoproterenol'  as drug_label
from infusiondrug
	WHERE "lower"(drugname)  similar to '%(isopre|isoprotere|isupre)%' 
	and lower(drugrate) <> ''
	and lower(drugrate) not in('0')
)
, com_iso as (
select distinct ICUSTAY_ID
	      , drugorderoffsetH
				, orderWindow
				, drugname
				, drug_label
from (
  SELECT ICUSTAY_ID
	      , drugorderoffsetH
				, (2*orderWindow)-1 as orderWindow
				, drugname
				, drug_label
		from iso_med
		where drug_label is not null
		union (
	SELECT ICUSTAY_ID
	      , drugorderoffsetH
				, (2*orderWindow)-1 as orderWindow
				, drugname
				, drug_label
		from iso_infu
		where drug_label is not null
		) 
		)iso
)
, com_drug as (
 select  distinct ICUSTAY_ID
	      , drugorderoffsetH
				, orderWindow
				, drugname
				, drug_label
from (
 SELECT * from com_drugAA_first4
 union(
 SELECT * from com_iso
 )
) cc
)
, drug_AA_1st as (
  SELECT  ICUSTAY_ID
       , drugorderoffsetH
       , orderWindow
--        , drugname
			 , drug_label
        -- 找出第一次用药 ordertime
       , row_number() OVER (PARTITION BY ICUSTAY_ID ORDER BY drugorderoffsetH) AS first_drugOrder     
	from com_drug
	where drug_label in (-- 'adenosine','lidocaine','ibutilide'
	     'adenosine', 'lidocaine',  'ibutilide', 'amiodarone', 'isoproterenol'
	)
  ORDER BY  ICUSTAY_ID
)
 
SELECT  ftr.ICUSTAY_ID
    	, daa.drugorderoffsetH
      , daa.orderWindow
			, daa.drug_label
			, first_drugOrder
	from AAbasicinfo_allICU ftr
 inner join drug_AA_1st  daa 
       on ftr.ICUSTAY_ID = daa.icustay_id 
where  first_drugOrder = 1
GROUP BY ftr.ICUSTAY_ID
    	, daa.drugorderoffsetH
      , daa.orderWindow
			, daa.drug_label
			, first_drugOrder
  order by ftr.ICUSTAY_ID
    	, daa.drugorderoffsetH
      , daa.orderWindow
			, first_drugOrder


