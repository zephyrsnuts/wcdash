--=========================
-- for hes case flags
--
--
select 
ucf.cust_bu_id
,ucf.hes_case_flg
,case when (ucf.cust_bu_id = 99901 or ucf.hes_case_flg = 1) then 1 else 0 end HES_case

from usdm_case_fact ucf
where ucf.hes_case_flg = 1
--==============================

/*
 * test if the person data is pulling in all the required columns and none of them are null. 
 */
select
	pers.src_prsn_hist_gen_id ,
	pers.assoc_bdge_nbr ,
	case
		when pers.bus_frst_mgr_bdge_nbr in ('111111', '76684', '-1', '0') then pers.frst_mgr_last_nm || ', ' || pers.frst_mgr_frst_nm
		when coalesce(pers.bus_frst_mgr_bdge_nbr, PERS.frst_mgr_bdge_nbr) is null then 'TBD'
		else pers.bus_frst_mgr_last_nm || ', ' || pers.bus_frst_mgr_frst_nm
	end manager_first ,
	case
		when pers.bus_secnd_mgr_bdge_nbr in ('111111', '76684', '-1', '0') then pers.secnd_mgr_last_nm || ', ' || pers.secnd_mgr_frst_nm
		when coalesce(pers.bus_secnd_mgr_bdge_nbr, PERS.secnd_mgr_bdge_nbr) is null then 'TBD'
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
where pers.src_prsn_hist_gen_id = '22444196'--'22429058'

--======================================================
select distinct 
uwf.hist
from ws_svc_gsa_bi.usdm_wo_fact uwf 
where uwf.asst_id = '1BHZYQ2'

select 
ucf.queue_desc 
,ucf.case_owner_queue_nm 
,ucf.rgn
from ws_svc_gsa_bi.usdm_case_fact ucf 

--===================================================

select distinct
	uph.prod_bu_type 
	,uph.prod_grp_desc 
	from ws_svc_gsa_bi.usdm_prod_hier uph 
	left join ws_svc_gsa_bi.usdm_case_fact ucf 
		on ucf.itm_cls_cd = uph.prod_key 
	limit 100
;
/*checking for robotic user*/
select
per.assoc_bdge_nbr
,per.updated_order
,per.ROBOTIC_USER
from (
select distinct 
ad.*
,case 
	when ad.bus_rptg_dept_nm in ('Robotics') and ad.assoc_frst_nm in ('RUI') then 1
	else 0
end robotic_user1

,case when sud.BOT_FLG = 'Y' or sud.FIN_SUB_QUEUE_NM in ('Robotics') or sud.VNDR_DESC in ('ROBOTIC USER ID') or ad.bus_rptg_team_cd in ('Robotic Process Auto') or ad.bus_rptg_team_nm in ('Robotic Process Automation Queue') then 'Y' else 'N' end ROBOTIC_USER
,row_number() over(partition by ad.assoc_bdge_nbr order by ad.src_updt_dts desc) as updated_order
from ws_svc_gsa_bi.assoc_dim ad
left join ws_svc_gsa_bi.sfdc_user_dtl sud 
	on sud.assoc_bdge_nbr = ad.assoc_bdge_nbr
where 1=1
--and (ad.bus_secnd_mgr_bdge_nbr = '346554' or ad.secnd_mgr_bdge_nbr ='346554')
--and ad.bus_rptg_dept_nm in ('Robotics')
--and ad.assoc_ntwk_login_nm not like '%RUI_GABFO%'
--and updated_order = 1 
--fetch first 500 rows only
) per
where per.ROBOTIC_USER = 'Y'
and per.updated_order = 1
/*end of robotic user*/


select DISTINCT 
--ucf.CASE_REC_TYPE
--,ucf.USDM_CASE_TYPE 
--,ucf.INT_CASE_REC_TYPE_CD 
--,ucf.CASE_SRC_DESC 
--,ucf.rptg_case_chnl 
count(per.assoc_bdge_nbr) badge_count
from WS_SVC_GSA_BI.USDM_CASE_FACT UCF
inner join ws_svc_gsa_bi.usdm_prod_hier uph 
	on uph.prod_key = ucf.asst_prod_hier_key 
inner join ws_svc_gsa_bi.assoc_dim per
	on per.src_prsn_hist_gen_id = ucf.owner_latest_bdge_hist_id 
inner join ws_svc_gsa_bi.svc_corp_cldr scc 
	on scc.cldr_date = ucf.case_crt_dts :: date
where 1=1
and (lower(ucf.usdm_case_type) not in ('care') 
	and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
	and ucf.case_rec_type not in ('Internal Case') 
	and lower(ucf.int_case_rec_type_cd) in ('external case'))
and ucf.quick_case_flg <> 1
and scc.fisc_qtr_rltv between -5 and 0
and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') 
	or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage','Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')))
;
create or replace view vw_ani_6208_filters_wc as (
select DISTINCT 
uwf.case_wid 
,uwf.wo_wid 
,uwf.asst_id 
,uwf.case_id 
,uwf.rgn_id 
,uwf.mdr_rptg_hash_id 
,uwf.dsp_prvdr_id 
,uwf.sfdc_case_id 
,uwf.sfdc_wo_id 
,uwf.src_cust_prod_id 
,uwf.wo_bu_id 
,uwf.wo_ord_bu_id 
,uwf.sys_itm_cls_cd 
,uwf.asst_orig_ord_bu_id 
,uwf.asst_unified_iso_ctry_cd 
,uwf.wo_apprvd_bdge_hist_id 
,uwf.wo_crt_by_bdge_hist_id 
,uwf.wo_crt_stewarded_bdge_hist_id 
from ws_svc_gsa_bi.usdm_wo_fact uwf 
inner join ws_svc_gsa_bi.usdm_case_fact ucf
	on ucf.case_wid = uwf.case_wid 
inner join ws_svc_gsa_bi.usdm_prod_hier uph 
	on uph.prod_key = uwf.asst_prod_hier_key 
inner join ws_svc_gsa_bi.assoc_dim per
	on per.src_prsn_hist_gen_id = uwf.wo_crt_by_bdge_hist_id 
inner join ws_svc_gsa_bi.svc_corp_cldr scc 
	on scc.cldr_date = uwf.wo_crt_utc_dts :: date
where 1=1
and (lower(ucf.usdm_case_type) not in ('care') 
	and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
	and ucf.case_rec_type not in ('Internal Case') 
	and lower(ucf.int_case_rec_type_cd) in ('external case'))
and ucf.quick_case_flg <> 1
and scc.fisc_qtr_rltv between -5 and 0
and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') 
	or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage','Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')))
);

select 
count(*)
from ws_svc_gsa_bi.usdm_case_fact ucf 
inner join ws_svc_gsa_bi.vw_ani_6208_filters_wc vafw  
	on ucf.case_wid = vafw.case_wid 
where 1=1



select DISTINCT 
ucf.delta_case_nbr 
from ws_svc_gsa_bi.usdm_case_fact ucf 
where ucf.int_case_rec_type_cd = 'Escalation Case'
fetch first 500 rows only;