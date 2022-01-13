drop view if exists vw_ani_6208_case_wc;
create or replace view 
vw_ani_6208_case_wc as
/*case table direct build without person or any other details*/
with case_filter as (
	select DISTINCT 
		ucf.asst_id 
		,ucf.case_id 
		,ucf.cust_bu_id 
		,ucf.delta_case_id 
		,ucf.dw_dell_acct_id 
		,ucf.frst_case_owner_queue_id 
		,ucf.ord_bu_id 
		,ucf.parnt_case_id 
		,ucf.s360_sfdc_case_id 
		,ucf.sfdc_case_id 
		,ucf.src_crm_asst_id 
		,ucf.src_cust_prod_id 
		,ucf.svc_tag_id 
		,ucf.case_wid as case_wid_1
		,ucf.parnt_case_wid 
		,ucf.clos_by_bdge_hist_id 
		,ucf.cmplt_by_bdge_hist_id 
		,ucf.crt_by_bdge_hist_id  
		,ucf.frst_case_owner_bdge_hist_id 
		,ucf.last_upd_by_bdge_hist_id 
		,ucf.owner_bdge_hist_id_at_cmplt 
		,ucf.owner_bdge_hist_id_at_crt 
		,ucf.owner_crt_bdge_hist_id 
		,ucf.owner_latest_bdge_hist_id 
		,case 
			when per.bus_rptg_catg_id = 5
				and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
				'High End Storage','Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')
		then 1 else 0 end ISG_Flag
		,case when (ucf.cust_bu_id = 99901 or ucf.hes_case_flg = 1) then 1 else 0 end HES_case_flag_custom
		,count(distinct ucf.case_wid) as case_count
		from WS_SVC_GSA_BI.USDM_CASE_FACT UCF
		left join ws_svc_gsa_bi.usdm_prod_hier uph 
			on uph.prod_key = ucf.asst_prod_hier_key 
		left join ws_svc_gsa_bi.assoc_dim per
			on per.src_prsn_hist_gen_id = ucf.owner_bdge_hist_id_at_crt 
		left join ws_svc_gsa_bi.svc_corp_cldr scc 
			on scc.cldr_date = ucf.case_crt_dts :: date
		where 1=1
		and (lower(ucf.usdm_case_type) not in ('care') 
			and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
			and ucf.case_rec_type not in ('Internal Case') 
			and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
		and scc.fisc_qtr_rltv between -3 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') 
			or (per.bus_rptg_catg_id = 5
				and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage',
				'Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')))
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,ucf.hes_case_flg
),
ttc_ttr as (
	select
	UCF.case_wid as case_wid2
	,age(coalesce(ucf.rptg_rslvd_dts, ucf.case_closd_dts, ucf.case_cmplt_dts), ucf.case_crt_dts) ttc_age
	,sum(extract (day from (ucf.closed_date_comb - ucf.case_crt_dts))*24*60*60
		+ extract (hour from (ucf.closed_date_comb - ucf.case_crt_dts))*60*60
		+ extract (minute from (ucf.closed_date_comb - ucf.case_crt_dts))*60
		+ extract (second from (ucf.closed_date_comb - ucf.case_crt_dts))) as TTC_NUMERATOR_SECNDS
	,sum(case when ucf.case_id is not null then 1 else 0 end) TTC_Denom
	from (
			select
				coalesce(ucf1.rptg_rslvd_dts, ucf1.case_closd_dts, ucf1.case_cmplt_dts) closed_date_comb
				,ucf1.*
			from WS_SVC_GSA_BI.USDM_CASE_FACT ucf1
		) UCF
		left join ws_svc_gsa_bi.usdm_prod_hier uph 
			on uph.prod_key = ucf.asst_prod_hier_key 
		left join ws_svc_gsa_bi.assoc_dim per
			on per.src_prsn_hist_gen_id = ucf.owner_bdge_hist_id_at_crt 
		left join ws_svc_gsa_bi.svc_corp_cldr scc 
			on scc.cldr_date = ucf.case_crt_dts :: date
		where 1=1
--		and (lower(ucf.usdm_case_type) not in ('care') 
--			and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
--			and ucf.case_rec_type not in ('Internal Case') 
--			and lower(ucf.int_case_rec_type_cd) in ('external case'))
--		and ucf.quick_case_flg <> 1
--		and scc.fisc_qtr_rltv between -5 and 0
--		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') 
--			or (per.bus_rptg_catg_id = 5
--				and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
--				'High End Storage','Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')))
	group by 1,2
)
select distinct
	cft.*
	,ttc.ttc_age
	,ttc.TTC_NUMERATOR_SECNDS
	,ttc.TTC_Denom
	,ucf.case_wid
	,ucf.case_crt_dts 
	,coalesce(ucf.case_cmplt_dts, ucf.case_closd_dts, ucf.rptg_rslvd_dts) as case_closed_dts
	,ucf.case_crt_dts :: date as calendar_join_create
	,coalesce(ucf.case_cmplt_dts, ucf.case_closd_dts, ucf.rptg_rslvd_dts):: date as calendar_join_closed
	,ucf.clos_flg 
	,ucf.hes_case_flg 
	,ucf.quick_case_flg 
	,ucf.sms_case_clos_flg 
	,ucf.dse_enbld_flg
	,ucf.last_upd_dts
	,ucf.case_closd_dts
	,ucf.rptg_rslvd_dts
	,ucf.reopen_dts
	,ucf.case_acpt_dts
	,ucf.case_cmplt_dts
	,ucf.asst_ship_dts
	,ucf.asst_instl_dts
	,ucf.ord_inv_dts
	,ucf.usdm_case_type
	,ucf.dspsn_rsn_desc
	,ucf.cust_lcl_chnl_cd
	,ucf.proj_nbr
	,ucf.case_stat
	,ucf.case_sevrty_priorty
	,ucf.origin_nm
	,ucf.rptg_case_chnl
	,ucf.cust_cls_cd
	,ucf.case_nbr
	,ucf.case_rec_type
	,ucf.sfdc_case_nbr
	,ucf.delta_case_nbr
	,ucf.s360_sfdc_case_nbr
	,ucf.ord_nbr
	,ucf.itm_nbr
	,ucf.queue_desc
	,ucf.prod_srl_nbr
	,ucf.case_owner_queue_nm
	,ucf.owner_bdge_nbr
	,ucf.int_case_rec_type_cd
	,rank() over(partition by ucf.case_id order by ucf.case_crt_dts asc) row_rank_case_asc
	,rank() over(partition by ucf.case_id order by ucf.case_crt_dts desc) row_rank_case_desc
from 
	ws_svc_gsa_bi.usdm_case_fact ucf 
	inner join case_filter cft
		on cft.case_wid_1 = ucf.case_wid
	left join ttc_ttr ttc
		on ttc.case_wid2 = ucf.case_wid
--where ucf.del_flg = 0