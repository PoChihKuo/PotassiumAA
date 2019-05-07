
-- 用于统计1到7天 不同bin在当天所占比例
with t1 as (
SELECT  icustay_id
			 , tstart
			, tstop
			 , status
			 , four_bin
			 , unittype
			,  case when time_window <=24 then 1
			        when time_window >24 and time_window <=48 then 2
							when time_window >48 and time_window <=72 then 3
							when time_window >72 and time_window <=96 then 4
							when time_window >96 and time_window <=120 then 5
							when time_window >120 and time_window <=144 then 6
							when time_window >144 and time_window <=168 then 7
			end as daytime
	from AA_com_info2
	)
	SELECT distinct icustay_id, daytime, max(four_bin) as potassium_bin
	from t1
	where lower(unittype)  similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%' --%micu%|sicu%|%med-surg%
	group by icustay_id, daytime;


SELECT distinct unittype  from aa_com_info2










select count(distinct icustay_id)		
from AA_com_info2;--90379

select count(distinct icustay_id)		
from AA_com_info2
where  aadrug_use=1; --3471
select count(distinct icustay_id)		
from AA_com_info2
where  aadrug_use=1
and lower(unittype)   similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%'; -- 1371+19294
select count(distinct icustay_id)		
from AA_com_info2
where  aadrug_use=0
and lower(unittype)   similar to  '%ccu-cticu%|cardiac icu|%csicu%|%cticu%'; 
-- and lower(unittype)   similar to  '%micu%|sicu%|%med-surg%'; -- 66697+1885= 68582
select count(distinct icustay_id)		
from AA_com_info2
where  aadrug_use=1
and lower(unittype)   similar to  '%micu%|sicu%|%med-surg%';  2100 + 67614
select count(distinct icustay_id)		
from AA_com_info2
where  aadrug_use=0
and lower(unittype)   similar to  '%micu%|sicu%|%med-surg%'; 