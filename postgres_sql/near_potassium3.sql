 
 --  这里主要是想用后面最近的potassium值插值
 
DROP MATERIALIZED VIEW IF EXISTS nearest_Potassium3  CASCADE;
CREATE MATERIALIZED VIEW nearest_Potassium3 as(

with Potassium as(

	SELECT  patientunitstayid as ICUSTAY_ID
      , labid
      , round(labresultoffset::numeric/60::numeric,2) as labresultoffsetH
      , lab.labname
      , lab.labresult
      , lab.labresulttext
      , ceil((labresultoffset::numeric/120::numeric)) as postaWindow-- (floor(labresultoffset::numeric/120::numeric)+1) 
	from lab
	WHERE lower(lab.labname) like 'potassium'
	      and lab.labresult is not null
-- 				AND labresultoffset >= 0
)

SELECT  mp.*
 FROM (
	SELECT  ICUSTAY_ID
--       , labid
      , labresultoffsetH
      , labname
      , labresult as close_potassium
      , labresulttext
      , (2* postawindow)-1 as postawindow -- postaWindow
			-- 升序排列才能得到最先测量的
      , row_number() OVER (PARTITION BY pt.ICUSTAY_ID, pt.postaWindow ORDER BY pt.labresultoffsetH ASC) AS near_Pota 
from Potassium pt
		) mp
WHERE near_Pota = 1
GROUP BY ICUSTAY_ID
--       , labid
      , labresultoffsetH
      , labname
      , close_potassium
      , labresulttext
      , postawindow -- postaWindow
      , near_Pota -- 是标志位，标志着是当前窗口内最先测量的值
ORDER BY ICUSTAY_ID, postaWindow
);

-- 180473 ICUSTAY_IDs


-- SELECT DISTINCT mp.ICUSTAY_ID from nearest_Potassium mp
-- inner join AAexclusion_ver2 af 
--  on af.ICUSTAY_ID = mp.icustay_id  -- 105290 -- 33171 icustay_ids
   -- lower(af.unittype)   similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%' 20791

--  where mp.ICUSTAY_ID not in (SELECT distinct ICUSTAY_ID FROM AAexclusion_ver2) -- 28859

-- SELECT distinct labresult from maxPotassium_ver2 



with basic as (
SELECT a.icustay_id
     , age
		 , gender
		 , apache_iv
		 , unittype
		 , icu_los_hours
		 , (2* windowoficu)-1 as icuwindow
		 , fd.orderWindow as AA_order_window
	   , fd.drug_label
	   , fd.drugorderoffseth
from AAbasicinfo_allICU_6hr a
left join AAFirst3Drugs fd
     on a.icustay_id = fd.icustay_id
)
, b2 as (
SELECT icustay_id
     , age
		 , gender
		 , apache_iv
		 , unittype
		 , icu_los_hours
		 , icuwindow
		 , AA_order_window
	   ,  drug_label
	   ,  drugorderoffseth
		 ,  case when AA_order_window is not null then AA_order_window
				       else icuwindow 
				end as observation_Window   
from basic
)
SELECT b.icustay_id
--      , age
-- 		 , gender
-- 		 , apache_iv
-- 		 , unittype
-- 		 , icu_los_hours
		 , icuwindow
		 , AA_order_window
	   ,  drug_label
	   ,  drugorderoffseth
     , observation_Window
		 , n.postawindow
		 , n.close_potassium
 from b2 b
inner join nearest_Potassium3 n
    on b.icustay_id = n.icustay_id
-- 		 where drug_label is not null

