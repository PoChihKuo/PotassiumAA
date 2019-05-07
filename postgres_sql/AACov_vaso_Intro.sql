-- 备注：本脚本用于选择 患者入ICU后每两个小时是否有使用 inotropes(正性肌力药) or vasopressors(血管升压类药物) (use or not )
--  如果有使用这两种药物任意一种，即可(use)
-- 来源：medication  & infusiondrug

-- Here's a list of the inotropes(正性肌力药):
-- Dopamine: Intropin, myocard-dx
-- Dobutamine: Dobutrex
-- Milrinone: Primacor
-- Levosimenden: Simdax 

-- Here's a list of the vasopressors(血管升压类药物): 
-- Norepinephrine: Levophed
-- epinephrine: Adrenalin

DROP MATERIALIZED VIEW IF EXISTS AACov_vaso_Intro CASCADE;
CREATE MATERIALIZED VIEW AACov_vaso_Intro as
with med_tmpvaso_intro as (
		SELECT   patientunitstayid as ICUSTAY_ID
		   , drugorderoffset
       , round((drugorderoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
       , ceil(drugorderoffset::numeric/120::numeric) as drugorderwindow 
			 -- (floor(drugorderoffset::numeric/120::numeric)+1) as drugorderwindow --
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%norepinephrine%|%levophed%' then 'norepinephrine'  -- %subq%
             WHEN "lower"(drugname) similar to '%epinephrine%|%adrenalin%'  THEN 'epinephrine'
						 WHEN "lower"(drugname) similar to '%dopamine%|%intropin%|%myocard-dx%'  THEN 'Dopamine' 
						 WHEN "lower"(drugname) similar to '%dobutamine%|%dobutrex%'  THEN 'Dobutamine' 
						 WHEN "lower"(drugname) similar to '%milrinone%|%primacor%'  THEN 'Milrinone' 
						 WHEN "lower"(drugname) similar to '%levosimenden%|%simdax%'  THEN 'Levosimenden'    		 
          else null  
         end as cov_druglabel
		FROM medication 
		WHERE "lower"(drugname) SIMILAR to '%norepinephrine%|%epinephrine%|%levophed%|%adrenalin%|%dopamine%|%intropin%|%myocard-dx%|%dobutamine%|%dobutrex%|%milrinone%|%primacor%|%levosimenden%|%simdax%' 
			AND "lower"(drugOrderCancelled) like 'no'
		AND lower(dosage) !~ '^0[^.-](\s)*[^0-9][a-zA-Z]*'	
		and lower(dosage) != '0'
-- 		and drugorderoffset > 0
		order by ICUSTAY_ID
)
, infu_tmpvaso_intro as (
  SELECT   patientunitstayid as ICUSTAY_ID ,infusionoffset as drugorderoffset
       , round((infusionoffset::numeric/60::numeric),2) as drugorderoffsetH -- 转为小时  
       , ceil(infusionoffset::numeric/120::numeric) as drugorderwindow--
       , drugname
       , CASE
             WHEN "lower"(drugname) similar to '%norepinephrine%|%levophed%' then 'norepinephrine'  -- %subq%
             WHEN "lower"(drugname) similar to '%epinephrine%|%adrenalin%'  THEN 'epinephrine' 
						 WHEN "lower"(drugname) similar to '%dopamine%|%intropin%|%myocard-dx%'  THEN 'Dopamine' 
						 WHEN "lower"(drugname) similar to '%dobutamine%|%dobutrex%'  THEN 'Dobutamine' 
						 WHEN "lower"(drugname) similar to '%milrinone%|%primacor%'  THEN 'Milrinone' 
						 WHEN "lower"(drugname) similar to '%levosimenden%|%simdax%'  THEN 'Levosimenden'
          else null  
         end as cov_druglabel
		FROM infusiondrug 
		WHERE "lower"(drugname) SIMILAR to '%norepinephrine%|%epinephrine%|%levophed%|%adrenalin%|%dopamine%|%intropin%|%myocard-dx%|%dobutamine%|%dobutrex%|%milrinone%|%primacor%|%levosimenden%|%simdax%' 
		AND drugrate ~ '^[0-9.]{1,8}$' 
				AND drugrate <> '.' 
				AND drugrate <> '' 
-- 				AND infusionoffset > 0
		order by ICUSTAY_ID

)
, exclude_med_vai as (
  -- 把Collin 标出来的去除
  SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, (2 * drugorderwindow) - 1 as drugorderwindow
				, drugname
				,  case when "lower"(cov_druglabel) = 'epinephrine' and LOWER(drugname) SIMILAR to '(1|20|200|30) (each|ml)%|10 ml%\%%|[0-9]+a:%\(%|bupivacaine%(/|-)epinephrine%|epinephrine(-| \()%|epinephrine 1 mg/( 1|1)%inj|epinephrine%\(1 ml\)%solution|epinephrine 1 mg/ml|(race|epinephrine)%2.25(\%| \%)%|epinephrine (race|injec)%|(lidocaine|race|xylocaine)%'
				then null
				   else cov_druglabel
				   end as cov_druglabel
		from med_tmpvaso_intro
)
, com_drug as (
  SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, drugorderwindow
				, drugname
				, cov_druglabel
		from exclude_med_vai
	where cov_druglabel is not null
	UNION (
	SELECT  ICUSTAY_ID, drugorderoffset
	      , drugorderoffsetH
				, (2 * drugorderwindow) - 1 as drugorderwindow
				, drugname
				, cov_druglabel
		from infu_tmpvaso_intro
	  where cov_druglabel is not null
	
	)
order by ICUSTAY_ID, cov_druglabel, drugname
)

, vaso_inotro_t as (
-- 因为这里只要获得前两个小时内(相对24小时为研究开始时间来说)是否用药，因此对用药时间排序(asc),只选一条记录即可
SELECT  vs.ICUSTAY_ID
		  , cov_druglabel
      , drugorderoffsetH
			, drugorderwindow
		  , case when drugorderoffsetH > 0 then 1
						 ELSE 0
			 end as vaso_inotro
FROM 
   (
    SELECT  ICUSTAY_ID
		      , cov_druglabel
          , drugorderoffsetH   
         , drugorderwindow
         , row_number() OVER (PARTITION BY ICUSTAY_ID, drugorderwindow ORDER BY drugorderoffsetH ) AS FistVaso  -- , drugorderwindow
		FROM com_drug 
		order by ICUSTAY_ID
   ) vs 
WHERE FistVaso = 1
and drugorderoffsetH > 0
-- GROUP BY vs.ICUSTAY_ID ,drugorderoffsetH
)

SELECT ICUSTAY_ID, drugorderoffsetH, drugorderwindow,  vaso_inotro
from vaso_inotro_t
GROUP BY ICUSTAY_ID, drugorderoffsetH, drugorderwindow,  vaso_inotro

-- SELECT DISTINCT icustay_id  FROM aavasopressor
-- medication
-- SELECT  med.drugname as med_vasoprename
-- 			, med.cov_druglabel as med_druglabel
--  from AAbasicinfo ftr
--  left join med_tmpvaso_intro med 
--        on ftr.ICUSTAY_ID = med.icustay_id
--  where med.cov_druglabel is not null 
--  group by med.drugname , med.cov_druglabel 
--  order by med.cov_druglabel
 
 -- infusiondrug
-- SELECT  inf.drugname as inf_vasoprename
-- 			, inf.cov_druglabel as inf_druglabel
--  from AAbasicinfo ftr
--  left join infu_tmpvaso_intro inf 
--        on ftr.ICUSTAY_ID = inf.icustay_id
--  where inf.cov_druglabel is not null 
--  group by inf.drugname , inf.cov_druglabel 
--  order by inf.cov_druglabel
 