select 
person_hist_id,
count(*)
from(
with filter_agents as (
select distinct
	x.person_hist_id 
--	x.person_join_type
from
	(
	select 
		per.src_prsn_hist_gen_id as person_hist_id 
--		'wo_creator':: varchar as person_join_type
	from
		ws_svc_gsa_bi.assoc_dim per
	left join ws_svc_gsa_bi.usdm_wo_fact uwf on
		per.src_prsn_hist_gen_id = uwf.wo_crt_by_bdge_hist_id
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = uwf.asst_prod_hier_key
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = uwf.wo_crt_utc_dts :: date
	where
		1 = 1
		and cast(uwf.wo_crt_utc_dts as date) > '2019-01-01'
		and uwf.asst_id is not null
		and uwf.wo_nbr is not null
--		and (lower(ucf.usdm_case_type) not in ('care'))
--		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
--		and ucf.case_rec_type not in ('Internal Case')
--		and lower(ucf.int_case_rec_type_cd) in ('external case'))
--		and ucf.quick_case_flg <> 1
--		and scc.fisc_qtr_rltv between -5 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')))
union
	select 
		per.src_prsn_hist_gen_id as person_hist_id 
--		'wo_approver':: varchar as person_join_type
	from
		ws_svc_gsa_bi.assoc_dim per
	left join ws_svc_gsa_bi.usdm_wo_fact uwf on
		per.src_prsn_hist_gen_id = uwf.wo_apprvd_bdge_hist_id 
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = uwf.asst_prod_hier_key
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = uwf.wo_crt_utc_dts :: date
	where
		1 = 1
		and cast(uwf.wo_crt_utc_dts as date) > '2019-01-01'
		and uwf.asst_id is not null
		and uwf.wo_nbr is not null
--		and (lower(ucf.usdm_case_type) not in ('care'))
--		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
--		and ucf.case_rec_type not in ('Internal Case')
--		and lower(ucf.int_case_rec_type_cd) in ('external case'))
--		and ucf.quick_case_flg <> 1
--		and scc.fisc_qtr_rltv between -5 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')))
union
	select
		per.src_prsn_hist_gen_id as person_hist_id 
--		'case_owner':: varchar as person_join_type
	from
		ws_svc_gsa_bi.assoc_dim per
	left join ws_svc_gsa_bi.usdm_case_fact ucf on
		per.src_prsn_hist_gen_id = coalesce (ucf.owner_bdge_hist_id_at_crt ,
		ucf.owner_bdge_hist_id_at_cmplt ,
		ucf.owner_latest_bdge_hist_id )
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = ucf.asst_prod_hier_key
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = ucf.case_crt_dts :: date
	where
		1 = 1
		and cast(ucf.case_crt_dts as date) > '2019-01-01'
		and (lower(ucf.usdm_case_type) not in ('care')
		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
		and ucf.case_rec_type not in ('Internal Case')
		and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
--		and scc.fisc_qtr_rltv between -5 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')))
union
	select
		per.src_prsn_hist_gen_id as person_hist_id 
--		'case_creator':: varchar as person_join_type
	from
		ws_svc_gsa_bi.assoc_dim per
	left join ws_svc_gsa_bi.usdm_case_fact ucf on
		per.src_prsn_hist_gen_id = coalesce (ucf.crt_by_bdge_hist_id ,
		ucf.owner_crt_bdge_hist_id )
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = ucf.asst_prod_hier_key
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = ucf.case_crt_dts :: date
	where
		1 = 1
		and cast(ucf.case_crt_dts as date) > '2019-01-01'
		and (lower(ucf.usdm_case_type) not in ('care')
		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
		and ucf.case_rec_type not in ('Internal Case')
		and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
--		and scc.fisc_qtr_rltv between -5 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group',
		'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management'))) )x
group by
	1
--	2 
--	)x
),
gdpr_man_data as (
select distinct
	pers.src_prsn_hist_gen_id as person_hist_id ,
	pers.assoc_bdge_nbr ,
	case
		when pers.bus_frst_mgr_bdge_nbr in ('111111', '76684', '-1', '0') then pers.frst_mgr_last_nm || ', ' || pers.frst_mgr_frst_nm
		when PERS.frst_mgr_bdge_nbr is null then 'TBD - NO NAME AVAILABLE'
		else pers.bus_frst_mgr_last_nm || ', ' || pers.bus_frst_mgr_frst_nm
	end manager_first ,
	case
		when pers.bus_secnd_mgr_bdge_nbr in ('111111', '76684', '-1', '0') then pers.secnd_mgr_last_nm || ', ' || pers.secnd_mgr_frst_nm
		when PERS.secnd_mgr_bdge_nbr is null then 'TBD - NO NAME AVAILABLE'
		else pers.bus_secnd_mgr_last_nm || ', ' || pers.secnd_mgr_frst_nm
	end manager_second ,
	case
		when pers.CTRY_NM in ('Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Republic of Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'United Kingdom') then 1
		else 0
	end GDPR_Protection_flag ,
	coalesce (aehh.epicenter_lvl_3_assoc_nm,
	aehh.hr_lvl_3_assoc_nm ) manager_six ,
	coalesce (aehh.epicenter_lvl_4_assoc_nm ,
	aehh.hr_lvl_4_assoc_nm ) manager_five ,
	coalesce (aehh.epicenter_lvl_5_assoc_nm ,
	aehh.hr_lvl_5_assoc_nm ) manager_four ,
	coalesce (aehh.epicenter_lvl_6_assoc_nm ,
	aehh.hr_lvl_6_assoc_nm ) manager_three ,
	row_number() over(partition by pers.assoc_bdge_nbr
order by
	pers.gp_ins_upd_dts desc) as order_list
from
	ws_svc_gsa_bi.assoc_dim pers
left join (select a1.*, row_number() over(partition by a1.src_prsn_hist_gnrtn_id order by a1.gpetl_update_dt desc) as row_rank from ws_svc_gsa_bi.assoc_epicenter_hr_hier a1) aehh on
	aehh.src_prsn_hist_gnrtn_id = pers.src_prsn_hist_gen_id 
	and aehh.row_rank = 1
left join filter_agents fag on
	fag.person_hist_id = pers.src_prsn_hist_gen_id ),
gdpr_masked as (
select distinct
	gmd1.assoc_bdge_nbr ,
	'Mask_' || md5(gmd1.assoc_bdge_nbr || now()::VARCHAR(64)) as masked_name
from
	gdpr_man_data gmd1
left join filter_agents fag on
	fag.person_hist_id = gmd1.person_hist_id
where 1=1
--	order_list = 1
	and GDPR_Protection_flag = 1 ),
robotics as (/*checking for robotic user*/
select
	per.src_prsn_hist_gen_id ,
	per.updated_order ,
	per.ROBOTIC_USER
from
	(
	select
		distinct ad.src_prsn_hist_gen_id ,
		case
			when ad.bus_rptg_dept_nm in ('Robotics')
			and ad.assoc_frst_nm in ('RUI') then 1
			else 0
		end robotic_user1 ,
		case
			when sud.BOT_FLG = 'Y'
			or sud.FIN_SUB_QUEUE_NM in ('Robotics')
			or sud.VNDR_DESC in ('ROBOTIC USER ID')
			or ad.bus_rptg_team_cd in ('Robotic Process Auto')
			or ad.bus_rptg_team_nm in ('Robotic Process Automation Queue') then 'Y'
			else 'N'
		end ROBOTIC_USER ,
		row_number() over(partition by ad.assoc_bdge_nbr
	order by
		ad.src_updt_dts desc) as updated_order
	from
		ws_svc_gsa_bi.assoc_dim ad
	left join ws_svc_gsa_bi.sfdc_user_dtl sud on
		sud.assoc_bdge_nbr = ad.assoc_bdge_nbr
	where
		1 = 1
		--and (ad.bus_secnd_mgr_bdge_nbr = '346554' or ad.secnd_mgr_bdge_nbr ='346554')
		--and ad.bus_rptg_dept_nm in ('Robotics')
		--and ad.assoc_ntwk_login_nm not like '%RUI_GABFO%'
		--and updated_order = 1 
		--fetch first 500 rows only
) per
where
	per.ROBOTIC_USER = 'Y'
	and per.updated_order = 1 /*end of robotic user*/
	)
select distinct
	ad.assoc_bdge_nbr ,
	ad.assoc_email_addr ,
	ad.assoc_full_nm ,
	ad.assoc_loc_city_nm ,
	ad.assoc_loc_ctry_cd ,
	ad.assoc_loc_nm ,
	ad.assoc_loc_tmzn_nm ,
	ad.assoc_ptnr_nm ,
	ad.bus_frst_mgr_bdge_nbr ,
	ad.bus_rptg_catg_nm ,
	ad.bus_rptg_dept_nm ,
	ad.bus_rptg_func_cls_nm ,
	ad.bus_rptg_func_nm ,
	ad.bus_rptg_grp_nm ,
	ad.bus_rptg_queue_desc ,
	ad.bus_rptg_queue_nm ,
	ad.bus_rptg_rgn_nm ,
	ad.bus_rptg_subrgn_nm ,
	ad.bus_rptg_subgrp_nm ,
	ad.bus_rptg_team_nm ,
	ad.bus_secnd_mgr_bdge_nbr ,
	ad.ctry_nm ,
	ad.cco_queue_cd ,
	ad.emc_flg ,
	ad.src_eff_end_dt ,
	ad.src_eff_strt_dt ,
	ad.src_prsn_hist_gen_id ,
	gmd.person_hist_id ,
--	fag.person_join_type ,
	gmd.GDPR_Protection_flag ,
	gmd.manager_first as manager_1 ,
	gmd.manager_second as manager_2 ,
	gmd.manager_three as manager_3 ,
	gmd.manager_four as manager_4 ,
	gmd.manager_five as manager_5 ,
	gmd.manager_six as manager_6 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else ad.assoc_full_nm
	end gdpr_assoc_name ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else ad.assoc_email_addr
	end gdpr_email_addr ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_first
	end gdpr_manager_1 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_second
	end gdpr_manager_2 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_three
	end gdpr_manager_3 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_four
	end gdpr_manager_4 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_five
	end gdpr_manager_5 ,
	case
		when gmd.GDPR_Protection_flag = 1 then gmdm.masked_name
		else gmd.manager_six
	end gdpr_manager_6 ,
	coalesce(rob.ROBOTIC_USER, 'N') as robotic_user_Flag
from
	ws_svc_gsa_bi.assoc_dim ad
left join gdpr_man_data gmd on
	gmd.person_hist_id = ad.src_prsn_hist_gen_id
left join gdpr_masked gmdm on
	gmdm.assoc_bdge_nbr = ad.assoc_bdge_nbr
left join robotics rob on
	rob.src_prsn_hist_gen_id = ad.src_prsn_hist_gen_id
inner join filter_agents fag
	on fag.person_hist_id = ad.src_prsn_hist_gen_id 
where
	1 = 1
	--and order_list = 1
	--	left join ws_svc_gsa_bi.assoc_epicenter_hr_hier aehh 
	--		on aehh.src_prsn_hist_gnrtn_id = pers.src_prsn_hist_gen_id 
	--	inner join filter_agent_wo_creator faw
	--		on faw.wo_creator_hist_id = pers.src_prsn_hist_gen_id 
	--	and rob.ROBOTIC_USER = 'Y'
) ain
group by person_hist_id
having count(*) = 1