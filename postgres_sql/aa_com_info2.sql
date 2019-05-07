-- 获得患者总体信息
-- 扩展为5类药物

-- 该视图仅包含 MICU和SICU，Med-SurgICU 和%ccu-cticu%|cardiac icu|%csicu%|%cticu%（CICU）
-- 观察时间小于168hrs(1 week),将观察时间大于168hr 且用药的患者label定为0


-- 
DROP MATERIALIZED VIEW IF EXISTS AA_com_info2 CASCADE;
CREATE MATERIALIZED VIEW AA_com_info2 as (

with tmp as(
SELECT  distinct  icdl.ICUSTAY_ID
	, icdl.age
	, case when icdl.gender = 0 then 'f'
	       else 'm'
		 end as gender 
	, icdl.apache_iv
  , icdl.unittype
 -- , icdl.apacheadmissiondx
	, cln.final_charlson_score as Charlson_score
  , icdl.icu_los_hours
	, case when icdl.icustay_id in (SELECT  distinct  ICUSTAY_ID from AAvaso_firstday) then 1 else 0 end as vaso_use_firstday
	, case when icdl.icustay_id in (SELECT  distinct  ICUSTAY_ID from AAintro_firstday) then 1 else 0 end as inotrope_use_firstday
	, ap.oobintubday1 as intubated_firstday
  , ((2 * icdl.WindowOfICU)-1) as windowOfICu
	, dg.vaso_inotro_use
	, fc.furosemide_use
	
	, fd.orderWindow as AA_order_window
	, history_MI
	,	history_chf 
	, history_renal_failure 
	, admission_MI 
	, admission_chf 
	, admission_renal_failure 
	, admission_sepsis
-- 	, fd.drugname 
	, fd.drug_label   
	, fd.drugorderoffseth
from  AAbasicinfo_allICU  icdl  -- AAexclusion_allICU_3drug icdl
-- INNER JOIN maxPotassium_ver3 mp
--      ON icdl.ICUSTAY_ID = mp.ICUSTAY_ID
left join Charlson cln
	on icdl.ICUSTAY_ID = cln.patientunitstayid
	-- 连接用药物表，获得label
left join AAFirst5Drugs fd   --AAFirst4Drugs fd  -- AAFirstDrug_ver2 fd
     ON icdl.ICUSTAY_ID = fd.ICUSTAY_ID
 LEFT JOIN  AAvaso_Intro_use_6hr1  dg   
     ON icdl.ICUSTAY_ID = dg.ICUSTAY_ID
  left JOIN AAfurosemide_use_6hr  fc -- AAcov_furosemide
	   ON icdl.ICUSTAY_ID = fc.ICUSTAY_ID
 left join apachepredvar ap
 on icdl.ICUSTAY_ID = ap.patientunitstayid
 left join AAadmi_hist adh
 on icdl.ICUSTAY_ID = adh.ICUSTAY_ID
where lower(icdl.unittype)   similar to  '%micu%|sicu%|%med-surg%|%ccu-cticu%|cardiac icu|%csicu%|%cticu%'
GROUP BY icdl.ICUSTAY_ID, icdl.age
	,   icdl.gender 
	, icdl.apache_iv
  , icdl.unittype
  --, icdl.apacheadmissiondx
	, cln.final_charlson_score
  , icdl.icu_los_hours
	, vaso_use_firstday ,inotrope_use_firstday, intubated_firstday
	, history_MI
	,	history_chf 
	, history_renal_failure 
	, admission_MI 
	, admission_chf 
	, admission_renal_failure 
	, admission_sepsis
  ,  icdl.WindowOfICU 
	, dg.vaso_inotro_use
	, fc.furosemide_use
	, fd.orderWindow     -- , fd.drugname
	, fd.drug_label
	, fd.drugorderoffseth
)
-- SELECT distinct icustay_id from tmp  -- 153724
-- 需要把之前插值的与现状插值的进行合并
, trim_tmp as (
SELECT  ICUSTAY_ID
        , age
				, gender
				, apache_iv
				, Charlson_score
				, windowOfICu
				
				, vaso_use_firstday
				, inotrope_use_firstday
				, intubated_firstday
				
				, history_MI
				,	history_chf 
				, history_renal_failure 
				, admission_MI 
				, admission_chf 
				, admission_renal_failure 
				, admission_sepsis
				, case when vaso_inotro_use is null then 0
				       else vaso_inotro_use
					 end as vaso_inotro_use
				, case when furosemide_use is null then 0
				       else furosemide_use 
					 end as furosemide_use
				, case when drug_label is not null then 1
				       else 0
					 end as AAdrug_use
			  , case when AA_order_window is not null then AA_order_window
				       else windowOfICu end as observation_Window    -- 这里因为如果有用药即我们观察到了终点时间，研究时间即发生用药时间，反之没有用药即 整个icu时间
				, unittype
				-- , vaso_use_firstday ,inotrope_use_firstday, intubated_firstday
        , icu_los_hours
				, drugorderoffseth
--        , window
				, AA_order_window
        , drug_label
from  tmp
where icu_los_hours >0
)
--  SELECT distinct icustay_id from trim -- 151152
, mspota1 as (
SELECT icustay_id,  
       time_window, 
	   case when labresult is null then close_potassium
	        else labresult
	    end as potassium
from "AAcomplete_kvalue"
union (
	SELECT icustay_id,  
       time_window, 
	   case when labresult is null then close_potassium
	        else labresult
	    end as potassium
	from pota_value0_7
)
)
-- 去除potassium 值在ICU前8小时都没有值的患者
, mspota as (
 select * from mspota1
 where potassium is not null
)
, join_re as (
SELECT p.icustay_id
       ,p.time_window
			 , (p.time_window::NUMERIC	- 1::NUMERIC) as tstart1
			, (p.time_window::NUMERIC + 1::NUMERIC) as tstop1
			 , case when (aadrug_use =1 and observation_Window =  p.time_window) then 1
						else 0
				end as status
			 ,p.potassium
			 , case when p.potassium <3.0 then 1
					   when (p.potassium >=3.0 and p.potassium <3.50) then 2
						 when (p.potassium >=3.50 and p.potassium <4.0) then 3
						 when (p.potassium >=4.0 and p.potassium <4.50) then 4
						 when (p.potassium >=4.50 and p.potassium <=5.0) then 5
					 else p.potassium
				  end as five_bin
			, case when potassium <3.0 then 4
					   when (potassium >=3.0 and potassium <3.50) then 3
						 when (potassium >=3.5 and potassium <4.0) then 2
						 when (potassium >=4 and potassium <=5.0 ) then 1
					 else potassium
				  end as four_bin
        , tr.age::NUMERIC
				, tr.gender
				, tr.apache_iv
				, tr.Charlson_score
				, tr.windowOfICu
				, tr.vaso_use_firstday
				, tr.inotrope_use_firstday
				, tr.intubated_firstday
				, tr.history_MI
				,	tr.history_chf 
				, tr.history_renal_failure 
				, tr.admission_MI 
				, tr.admission_chf 
				, tr.admission_renal_failure 
				, tr.admission_sepsis
				, tr.vaso_inotro_use::INT2 as Initial_vaso_intro
				, tr.furosemide_use::INT2 as Initial_furosemide
				, case
			     	when vi.vaso_inotro is not null then 1
				    when vi.vaso_inotro is  null then 0
					end as vaso_inotro
				, case
				   when fi.furosemide is not null then 1
					 when fi.furosemide is null then 0
				 end as furosemide
				, tr.AAdrug_use 
			  , tr.observation_Window  
				, tr.unittype
        , tr.icu_los_hours
				, tr.drugorderoffseth
        , tr.drug_label
from mspota p 
inner join trim_tmp  tr
   on p.icustay_id = tr.icustay_id
left join AAcov_furosemide fi
  on p.icustay_id = fi.icustay_id
	and p.time_window = fi.drugorderwindow
left join AACov_vaso_Intro vi
	on p.icustay_id = vi.icustay_id
	and p.time_window = vi.drugorderwindow
where p.time_window >0
)
-- select distinct icustay_id from join_re  -- 146398
, remove_above5 as(
  select distinct icustay_id from join_re
  where potassium >5 
)
--  SELECT distinct icustay_id from remove_above5  -- 27593
, include_pota as(
  SELECT * from join_re
	where icustay_id not in (
	SELECT distinct icustay_id from remove_above5
	)
)
-- select distinct icustay_id from include_pota -- 118805

-- 接着 排除不符合要求的用药患者（begin  with loading dose）
, include_4aa as (
	SELECT ip.* from include_pota ip
  inner join AAexclusion_allICU_4drug ex
	on ip.icustay_id = ex.icustay_id
)
-- select distinct icustay_id from include_4aa  -- 108112(减少了 10693)
--  只考虑用药时间在icu内
, trim1 as (
select * from include_4aa 
where observation_Window >0  -- 107672
-- 不做限制，超过观察时间的为control
-- and observation_Window <= windowOfICu  -- 107268
)
-- select distinct icustay_id from trim1 -- 107672
-- 设置观察窗口为168hours， 即如果观察>168, label 1->0, 其他label 0; observation  time = 168
, trim_observationtime as (
  SELECT *,
	      case when observation_Window > 168 and AAdrug_use =1 then 0
				else AAdrug_use
				end as AAdrug_use_ind
				, case when observation_Window > 168  then 167
				else observation_Window
				end as observation_Window_ind
	from trim1
	where tstop1 <= 168
)
-- select distinct icustay_id from trim_observationtime -- 107268
, fina_d as(
SELECT icustay_id
       , time_window-- (time_window::NUMERIC	- 6::NUMERIC) as time_window
			 ,  tstart1 as tstart-- (tstart1::NUMERIC	- 6::NUMERIC) as tstart
			,  tstop1 as tstop-- (tstop1::NUMERIC - 6::NUMERIC) as tstop
			, potassium
			, status
			,  five_bin
			,   four_bin
        ,  age
				,  gender
				,  apache_iv
				,  Charlson_score
				, vaso_use_firstday ,inotrope_use_firstday, intubated_firstday
				, history_MI
				,	history_chf 
				, history_renal_failure 
				, admission_MI 
				, admission_chf 
				, admission_renal_failure 
				, admission_sepsis
-- 				,  windowOfICu
				,   Initial_vaso_intro
				,   Initial_furosemide
				, case when vaso_inotro is null then 0
				       else vaso_inotro
					 end as vaso_inotro
				, case when furosemide is null then 0
				       else furosemide
					 end as furosemide
				,  AAdrug_use_ind as aadrug_use
				, observation_Window_ind -- as 
				, observation_Window
-- 			  , (observation_Window::NUMERIC - 6::NUMERIC) as observation_Window
				,  unittype
        ,  icu_los_hours
				,  drugorderoffseth
        ,  drug_label 
--         , out_icu_index
from  trim_observationtime 
)
-- SELECT count(distinct icustay_id) from fina_d -- 
-- where apache_iv is null --  17293
-- where apache_iv is not null  --


-- (<168
-- SELECT distinct icustay_id from fina_d -- 102064
-- where apache_iv is null -- 16682
-- where apache_iv is not null  --85382)


SELECT icustay_id
       ,time_window
			 , tstart
			, tstop
			 , potassium
			 , status
			 ,  five_bin
			,   four_bin
        ,  age
				,  gender
				,  apache_iv
				,  Charlson_score
				, vaso_use_firstday ,inotrope_use_firstday, intubated_firstday
				, history_MI
				,	history_chf 
				, history_renal_failure 
				, admission_MI 
				, admission_chf 
				, admission_renal_failure 
				, admission_sepsis
-- 				,  windowOfICu
				,   Initial_vaso_intro
				,   Initial_furosemide
				,  vaso_inotro
				,  furosemide
				,  aadrug_use
			  , observation_Window
				, observation_Window_ind
				,  unittype
        ,  icu_los_hours
				,  drugorderoffseth
        ,  drug_label 
--         , out_icu_index
from fina_d
where tstop <= observation_Window_ind + 1
and tstop<=168
and  apache_iv is not  null 
)
select * from AA_com_info2; 



-- get time series data to analysis 
SELECT icustay_id
    --   ,time_window
			 , tstart
			, tstop
			-- , potassium
			 , status
			-- ,  five_bin
			,   four_bin
        ,  age
				,  gender
				,  apache_iv
				,  Charlson_score
				, vaso_use_firstday ,inotrope_use_firstday, intubated_firstday
				, history_MI
				,	history_chf 
				, history_renal_failure 
				, admission_MI 
				, admission_chf 
				, admission_renal_failure 
				, admission_sepsis
				,   Initial_vaso_intro
				,   Initial_furosemide
				,  vaso_inotro
				,  furosemide
				,  aadrug_use
			  , observation_Window
				,  unittype
        ,  icu_los_hours
			--	,  drugorderoffseth
      --  ,  drug_label 
from AA_com_info2;


-- get basic to build tableone
select distinct icustay_id
        ,  age
				,  gender
				,  apache_iv
				,  Charlson_score
				, vaso_use_firstday 
				, inotrope_use_firstday
				, intubated_firstday
				, history_MI
				,	history_chf 
				, history_renal_failure 
				, admission_MI 
				, admission_chf 
				, admission_renal_failure 
				, admission_sepsis
				,  aadrug_use
				,  unittype
--         ,  icu_los_hours

from AA_com_info2;--(90379 = 3471 + 86908 -- 168)  

-- statistics
-- select count(distinct icustay_id)		
-- from AA_com_info2
-- where  status=1;

-- select count(distinct icustay_id)		
-- from AA_com_info2
-- where  status=1
-- and lower(unittype)   similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%'; -- 19294+1371  

-- select count(distinct icustay_id)		
-- from AA_com_info2
-- where  status=1
-- and lower(unittype)   similar to  '%micu%|sicu%|%med-surg%'; --67614 +2100(new )