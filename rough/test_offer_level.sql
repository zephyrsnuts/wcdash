select 
count(*)
from usdm_case_fact as ucf
inner join vw_ani_6208_filters_wc as ani
on ani.case_wid = ucf.case_wid
left join vw_ani_6208_offer_wc as aof
	on aof.incdnt_id = ucf.case_id
left join vw_ani_6208_cal_wc as cal
	on cal.calendar_date = cast(ucf.case_crt_dts as date)
where aof.offer_level in ('OWR')
and cal.quarter_lag in (-1, -2)
--where ucf.case_nbr = '126166297'
;


select distinct offer_level
from vw_ani_6208_offer_wc as aof
;


select 
count(*)
from usdm_case_fact as ucf
inner join vw_ani_6208_filters_wc as ani
on ani.case_wid = ucf.case_wid
left join vw_ani_6208_offer_wc as aof
	on aof.incdnt_id = ucf.case_id
left join vw_ani_6208_cal_wc as cal
	on cal.calendar_date = cast(ucf.case_crt_dts as date)
where aof.offer_level in ('OWR')
and cal.quarter_lag in (-1, -2)
--where ucf.case_nbr = '126166297'
;