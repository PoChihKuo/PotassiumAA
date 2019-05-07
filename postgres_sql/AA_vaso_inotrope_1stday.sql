-- vasopressors within 1 day

-- Inotropes within 1 day (may not be able to include)

-- Intubated within 1 day

-- History of heart disease

-- 备注：本脚本用于选择 患者入ICU后前24个小时是否有使用 inotropes(正性肌力药) or vasopressors(血管升压类药物) (use or not )
--  如果有使用这两种药物任意一种，即可(use)
-- 来源：medication  & infusiondrug



-- Here's a list of the vasopressors(血管升压类药物): 
-- Norepinephrine: Levophed
-- epinephrine: Adrenalin

DROP MATERIALIZED VIEW IF EXISTS AAvaso_firstday CASCADE;
CREATE MATERIALIZED VIEW AAvaso_firstday as
with med_tmpvaso as (
		SELECT   patientunitstayid as ICUSTAY_ID
		   , drugorderoffset
--        , round((drugorderoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
     --  , ceil(drugorderoffset::numeric/120::numeric)  as drugorderwindow  
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%norepinephrine%|%levophed%' then 'norepinephrine'  -- %subq%
             WHEN "lower"(drugname) similar to '%epinephrine%|%adrenalin%'  THEN 'epinephrine'
						
						     		 
          else null  
         end as cov_druglabel
		FROM medication 
		WHERE "lower"(drugname) SIMILAR to '%norepinephrine%|%epinephrine%|%levophed%|%adrenalin%' 
			AND "lower"(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
		and drugorderoffset BETWEEN 0 and 1440
		order by ICUSTAY_ID
)
, infu_tmpvaso as (
  SELECT   patientunitstayid as ICUSTAY_ID , infusionoffset as drugorderoffset
--        , round((infusionoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
    --   , ceil(infusionoffset::numeric/120::numeric) as drugorderwindow-- (floor(infusionoffset::numeric/120::numeric)+1) as drugorderwindow--
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%norepinephrine%|%levophed%' then 'norepinephrine'  -- %subq%
             WHEN "lower"(drugname) similar to '%epinephrine%|%adrenalin%'  THEN 'epinephrine' 
          else null  
         end as cov_druglabel
		FROM infusiondrug 
		WHERE "lower"(drugname) SIMILAR to '%norepinephrine%|%epinephrine%|%levophed%|%adrenalin%' 
		AND drugrate ~ '^[0-9.]{1,8}$' 
				AND drugrate <> '.' 
				AND drugrate <> '' 
				AND infusionoffset BETWEEN 0 and 1440
		order by ICUSTAY_ID

)
, exclude_med_va as (
  -- 把Collin 标出来的去除
  SELECT  ICUSTAY_ID , drugorderoffset
				, drugname
				,  case when "lower"(cov_druglabel) = 'epinephrine' and LOWER(drugname) SIMILAR to '(1|20|200|30) (each|ml)%|10 ml%\%%|[0-9]+a:%\(%|bupivacaine%(/|-)epinephrine%|epinephrine(-| \()%|epinephrine 1 mg/( 1|1)%inj|epinephrine%\(1 ml\)%solution|epinephrine 1 mg/ml|(race|epinephrine)%2.25(\%| \%)%|epinephrine (race|injec)%|(lidocaine|race|xylocaine)%'
				then null
				   else cov_druglabel
				   end as cov_druglabel
		from med_tmpvaso
)
, com_drug as (
  SELECT  ICUSTAY_ID
				, drugorderoffset
				, drugname
				, cov_druglabel
		from exclude_med_va
	where cov_druglabel is not null
	UNION (
	SELECT  ICUSTAY_ID, drugorderoffset
				, drugname
				, cov_druglabel
		from infu_tmpvaso
	  where cov_druglabel is not null
	
	)
order by ICUSTAY_ID, cov_druglabel, drugname
)

, vaso_use as (
-- 因为这里只要获得前两个小时内(相对24小时为研究开始时间来说)是否用药，因此对用药时间排序(asc),只选一条记录即可
SELECT  vs.ICUSTAY_ID
		  , cov_druglabel
      , drugorderoffset
		  , case when (drugorderoffset BETWEEN 0 and 1440) then 1
						 ELSE 0
			 end as vasopressure_use
FROM 
   (
    SELECT  ICUSTAY_ID,drugorderoffset
		      , cov_druglabel
         , row_number() OVER (PARTITION BY ICUSTAY_ID ORDER BY drugorderoffset ) AS FistVaso  -- , drugorderwindow
		FROM com_drug 
		order by ICUSTAY_ID
   ) vs 
WHERE FistVaso = 1
-- GROUP BY vs.ICUSTAY_ID ,drugorderoffsetH
)

SELECT distinct ICUSTAY_ID, drugorderoffset,  vasopressure_use
from vaso_use
GROUP BY ICUSTAY_ID, drugorderoffset,  vasopressure_use
ORDER BY ICUSTAY_ID, drugorderoffset,  vasopressure_use

-------------------------------------
-- Here's a list of the inotropes(正性肌力药):
-- Dopamine: Intropin, myocard-dx
-- Dobutamine: Dobutrex
-- Milrinone: Primacor
-- Levosimenden: Simdax 
DROP MATERIALIZED VIEW IF EXISTS AAintro_firstday CASCADE;
CREATE MATERIALIZED VIEW AAintro_firstday as
with med_tmpintr as (
		SELECT   patientunitstayid as ICUSTAY_ID
		   , drugorderoffset
--        , round((drugorderoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
     --  , ceil(drugorderoffset::numeric/120::numeric)  as drugorderwindow  
       , drugname
       , CASE
            WHEN "lower"(drugname) similar to '%dopamine%|%intropin%|%myocard-dx%'  THEN 'Dopamine' 
						 WHEN "lower"(drugname) similar to '%dobutamine%|%dobutrex%'  THEN 'Dobutamine' 
						 WHEN "lower"(drugname) similar to '%milrinone%|%primacor%'  THEN 'Milrinone' 
						 WHEN "lower"(drugname) similar to '%levosimenden%|%simdax%'  THEN 'Levosimenden'
						     		 
          else null  
         end as cov_druglabel
		FROM medication 
		WHERE "lower"(drugname) SIMILAR to '%dopamine%|%intropin%|%myocard-dx%|%dobutamine%|%dobutrex%|%milrinone%|%primacor%|%levosimenden%|%simdax%' 
			AND "lower"(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
		and drugorderoffset BETWEEN 0 and 1440
		order by ICUSTAY_ID
)
, infu_tmpintr as (
  SELECT   patientunitstayid as ICUSTAY_ID , infusionoffset as drugorderoffset
--        , round((infusionoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
    --   , ceil(infusionoffset::numeric/120::numeric) as drugorderwindow-- (floor(infusionoffset::numeric/120::numeric)+1) as drugorderwindow--
       , drugname
       , CASE
              WHEN "lower"(drugname) similar to '%dopamine%|%intropin%|%myocard-dx%'  THEN 'Dopamine' 
						 WHEN "lower"(drugname) similar to '%dobutamine%|%dobutrex%'  THEN 'Dobutamine' 
						 WHEN "lower"(drugname) similar to '%milrinone%|%primacor%'  THEN 'Milrinone' 
						 WHEN "lower"(drugname) similar to '%levosimenden%|%simdax%'  THEN 'Levosimenden'
          else null  
         end as cov_druglabel
		FROM infusiondrug 
		WHERE "lower"(drugname) SIMILAR to '%dopamine%|%intropin%|%myocard-dx%|%dobutamine%|%dobutrex%|%milrinone%|%primacor%|%levosimenden%|%simdax%' 
		AND drugrate ~ '^[0-9.]{1,8}$' 
				AND drugrate <> '.' 
				AND drugrate <> '' 
				AND infusionoffset BETWEEN 0 and 1440
		order by ICUSTAY_ID

)

, com_drug as (
  SELECT  ICUSTAY_ID
				, drugorderoffset
				, drugname
				, cov_druglabel
		from med_tmpintr
	where cov_druglabel is not null
	UNION (
	SELECT  ICUSTAY_ID, drugorderoffset
				, drugname
				, cov_druglabel
		from infu_tmpintr
	  where cov_druglabel is not null
	
	)
order by ICUSTAY_ID, cov_druglabel, drugname
)

, intro_use as (
-- 因为这里只要获得前两个小时内(相对24小时为研究开始时间来说)是否用药，因此对用药时间排序(asc),只选一条记录即可
SELECT  vs.ICUSTAY_ID
		  , cov_druglabel
      , drugorderoffset
		  , case when (drugorderoffset BETWEEN 0 and 1440) then 1
						 ELSE 0
			 end as inotrope_use
FROM 
   (
    SELECT  ICUSTAY_ID,drugorderoffset
		      , cov_druglabel
         , row_number() OVER (PARTITION BY ICUSTAY_ID ORDER BY drugorderoffset ) AS Fistintro -- , drugorderwindow
		FROM com_drug 
		order by ICUSTAY_ID
   ) vs 
WHERE Fistintro = 1
-- GROUP BY vs.ICUSTAY_ID ,drugorderoffsetH
)

SELECT distinct ICUSTAY_ID, drugorderoffset,  inotrope_use
from intro_use
GROUP BY ICUSTAY_ID, drugorderoffset,  inotrope_use
ORDER BY ICUSTAY_ID, drugorderoffset,  inotrope_use
