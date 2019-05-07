-- 本脚本用于选择患者入ICU后前两个小时是否有使用Furosemide(利尿磺胺) 药物(use or not )
-- 来源：medication  & infusiondrug
-- Furosemide(利尿磺胺): Lasix, Diaqua, Lo-aqua
DROP MATERIALIZED VIEW IF EXISTS AAcov_furosemide CASCADE;
CREATE MATERIALIZED VIEW AAcov_furosemide as
with med_tmpfur as (
		SELECT   patientunitstayid as ICUSTAY_ID, drugorderoffset
       , round((drugorderoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
       , ceil(drugorderoffset::numeric/120::numeric) as drugorderwindow --  (floor(drugorderoffset::numeric/120::numeric)+1)
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%furosemide%|%lasix%|%diaqua%|%lo-aqua%' then 'furosemide'  -- %subq% 
          else null  
         end as cov_druglabel
		FROM medication 
		WHERE "lower"(drugname) SIMILAR to '%furosemide%|%lasix%|%diaqua%|%lo-aqua%'
			AND "lower"(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
-- 		and drugorderoffset > = 0
		order by ICUSTAY_ID
)
, infu_tmpfur as (
  SELECT   patientunitstayid as ICUSTAY_ID, infusionoffset as drugorderoffset
       , round((infusionoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
       , ceil(infusionoffset::numeric/120::numeric) as drugorderwindow--
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%furosemide%|%lasix%|%diaqua%|%lo-aqua%' then 'furosemide'
          else null  
         end as cov_druglabel
		FROM infusiondrug 
		WHERE "lower"(drugname) SIMILAR to '%furosemide%|%lasix%|%diaqua%|%lo-aqua%'
		AND drugrate ~ '^[0-9.]{1,8}$' 
				AND drugrate <> '.' 
				AND drugrate <> '' 
-- 				and drugorderoffset > = 0
		order by ICUSTAY_ID

)
, exclude_some_furose as (
  SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, (2 * drugorderwindow) - 1 as drugorderwindow
				, drugname
				, CASE
             WHEN "lower"(drugname) similar to '%(po|oral)%|%tablet%|furosemide (40|20|80) mg tab%'  then null
				     else cov_druglabel
				     end as cov_druglabel
		from med_tmpfur
)

,  com_Fur_drug as (
  SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, drugorderwindow
				, drugname
				, cov_druglabel
		from exclude_some_furose
	where cov_druglabel is not null
	UNION (
	SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, (2 * drugorderwindow) - 1 as drugorderwindow
				, drugname
				, cov_druglabel
		from infu_tmpfur
	  where cov_druglabel is not null
	
	)
order by ICUSTAY_ID, cov_druglabel, drugname
)
, furosemide_1st as (
-- 因为这里只要获得前两个小时内(相对24小时为研究开始时间来说)是否用药，因此对用药时间排序(asc),只选一条记录即可; 即[22,24]
SELECT  vs.ICUSTAY_ID
		  , cov_druglabel
      , drugorderoffsetH,drugorderwindow
		  , case when  drugorderoffsetH > 0  then 1
						 ELSE 0
			 end as furosemide
FROM 
   (
    SELECT  ICUSTAY_ID
		      , cov_druglabel
          , drugorderoffsetH   
         , drugorderwindow
         , row_number() OVER (PARTITION BY ICUSTAY_ID ,drugorderwindow ORDER BY drugorderoffsetH ) AS Fistfur   -- , drugorderwindow
		FROM com_Fur_drug 
		order by ICUSTAY_ID
   ) vs 
WHERE Fistfur = 1
and drugorderoffsetH > 0 
)

SELECT ICUSTAY_ID, drugorderwindow , drugorderoffsetH, furosemide
from  furosemide_1st
GROUP BY ICUSTAY_ID, drugorderwindow , drugorderoffsetH, furosemide
-- 57025 rows


-- medication
-- SELECT  med.drugname as med_furosemidename
-- 			, med.cov_druglabel as med_druglabel
--  from AAbasicinfo ftr
--  left join med_tmpfur med 
--        on ftr.ICUSTAY_ID = med.icustay_id
--  where med.cov_druglabel is not null 
--  group by med.drugname , med.cov_druglabel 
--  order by med.cov_druglabel
 
 -- infusiondrug
-- SELECT  inf.drugname as inf_furosemidename
-- 			, inf.cov_druglabel as inf_druglabel
--  from AAbasicinfo ftr
--  left join infu_tmpfur inf 
--        on ftr.ICUSTAY_ID = inf.icustay_id
--  where inf.cov_druglabel is not null 
--  group by inf.drugname , inf.cov_druglabel 
--  order by inf.cov_druglabel