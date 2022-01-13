drop view if exists vw_ani_6208_wo_wc;
 create or replace
view vw_ani_6208_wo_wc as (
WITH leg_rdr AS (
	select 
	uwlrf.wo_wid as leg_wo_wid
	,uwlrf.wo_nbr as leg_wo_nbr
	,uwlrf.wo_crt_utc_dts as leg_wo_crt_utc_dts
	,uwlrf.mdr_dspch_flg as leg_mdr_dspch_flg 
	,uwlrf.repeat_qualify_flg as leg_repeat_qualify_flg 
	,uwlrf.repeat_7_defect_flg as leg_repeat_7_defect_flg 
	,uwlrf.repeat_28_defect_flg as leg_repeat_28_defect_flg 
	,uwlrf.repeat_wo_nbr as leg_repeat_wo_nbr 
	,uwlrf.repeat_tm_gap_secnd as leg_repeat_tm_gap_secnd 
	,uwlrf.dummy_asst_flg as leg_dummy_asst_flg 
	,uwlrf.gbl_wo_seq_nbr as leg_gbl_wo_seq_nbr 
	,uwlrf.gbl_mdr_seq_nbr as leg_gbl_mdr_seq_nbr 
	,uwlrf.rix_grp_id as leg_rix_grp_id 
	,uwlrf.rix_chain as leg_rix_chain 
	,uwlrf.rix_seq_nbr as leg_rix_seq_nbr 
	,uwlrf.rix_frst_flg as leg_rix_frst_flg 
	,uwlrf.rix_last_flg as leg_rix_last_flg 
	,uwlrf.rix_strt_dts as leg_rix_strt_dts 
	,uwlrf.rix_end_dts as leg_rix_end_dts 
	,uwlrf.rix_tm_gap_secnd as leg_rix_tm_gap_secnd 
	,uwf.wo_clos_utc_dts as leg_wo_clos_utc_dts
	,uwf.curr_stat as leg_curr_stat
	,age(uwf.wo_clos_utc_dts, uwf.wo_crt_utc_dts) as leg_wo_age
--	,case 
--		when (prev_repeat_7_defect_flg is null and repeat_7_defect_flg is null)
--		then 1
--		else 0
--	end leg_single_dispatch_7Day
	,case when uwlrf.rix_seq_nbr < 1 then 0 else 1 end leg_rix_flag
	,uwf.case_id as leg_case_id
	,uwlrf.asst_id as leg_asst_id
	,row_number() over(partition by uwf.case_id order by uwlrf.wo_crt_utc_dts) leg_rdr_row_rank
	,SUM(CASE WHEN scc.fisc_week_rltv NOT IN (0,-1)
						THEN uwlrf.repeat_7_defect_flg END) AS leg_RD_N7_lag
	,SUM(CASE WHEN scc.fisc_week_rltv NOT IN (0,-1)
						THEN uwlrf.repeat_qualify_flg END) AS leg_RD_D7_lag
	,SUM(CASE WHEN scc.fisc_week_rltv NOT IN (0,-1,-2,-3)
						THEN uwlrf.repeat_28_defect_flg END) AS leg_RD_N28_lag
	,SUM(CASE WHEN scc.fisc_week_rltv NOT IN (0,-1,-2,-3)
						THEN uwlrf.repeat_qualify_flg END) AS leg_RD_D28_lag
	FROM ws_svc_gsa_bi.usdm_wo_leg_rdr_fact uwlrf
		LEFT JOIN ws_svc_gsa_bi.usdm_wo_fact uwf 
			on uwf.wo_wid = uwlrf.wo_wid 
		LEFT JOIN ws_svc_gsa_bi.svc_corp_cldr scc 
			on scc.cldr_date = cast(uwlrf.wo_crt_utc_dts as date) 
			WHERE 1=1
			and cast(uwlrf.wo_crt_utc_dts as date) > current_date - 1000
--			and uwlrf.mdr_dspch_flg = 1
--			and uwf.asst_id = '1BHZYQ2'--'1-4HHLV3'
			and uwlrf.null_asst_flg = 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,uwlrf.wo_crt_utc_dts	
),
cust_rdr as (
	select
		uwcrf.wo_wid as cust_wo_wid 
		,uwcrf.wo_crt_utc_dts as cust_wo_crt_utc_dts 
		,uwcrf.repeat_7_defect_flg as cust_repeat_7_defect_flg 
		,uwcrf.repeat_28_defect_flg as cust_repeat_28_defect_flg 
		,uwcrf.repeat_wo_nbr as cust_repeat_wo_nbr 
		,uwcrf.repeat_tm_gap_secnd as cust_repeat_tm_gap_secnd 
		,uwcrf.dummy_asst_flg as cust_dummy_asst_flg 
		,uwcrf.gcc_crt_wo_seq_nbr as cust_gcc_crt_wo_seq_nbr 
		,uwcrf.rix_grp_id as cust_rix_grp_id 
		,uwcrf.rix_chain as cust_rix_chain 
		,uwcrf.rix_seq_nbr as cust_rix_seq_nbr 
		,uwcrf.rix_frst_flg as cust_rix_frst_flg 
		,uwcrf.rix_last_flg as cust_rix_last_flg 
		,uwcrf.rix_strt_dts as cust_rix_strt_dts 
		,uwcrf.rix_end_dts as cust_rix_end_dts 
		,uwcrf.rix_tm_gap_secnd as cust_rix_tm_gap_secnd 
		,uwcrf.rd_qualify_flg as cust_rd_denom
		,uwcrf.rd_pre_qualify_flg as cust_prev_rd_denom
		,uwcrf.case_rd_rptg_flg as cust_case_rd_rptg_flg
		,uwcrf.gcc_crt_wo_flg as cust_gcc_crt_wo_flg
		,uwcrf.gcc_reissued_flg as cust_gcc_reissued_flg
		,cuwf.wo_clos_utc_dts as cust_wo_clos_utc_dts
		,age(cuwf.wo_clos_utc_dts, cuwf.wo_crt_utc_dts) cust_wo_age
--		,case 
--			when prev_repeat_7_defect_flg is null 
--				or repeat_7_defect_flg is null 
--			then 1
--			else 0
--		end cust_single_dispatch_7Day
		,case when uwcrf.rix_seq_nbr < 1 then 0 else 1 end cust_rix_flag
		,cuwf.case_id as cust_case_id
		,uwcrf.asst_id as cust_asst_id
		,cuwf.curr_stat as cust_curr_stat
		,row_number() over(partition by cuwf.case_id order by uwcrf.wo_crt_utc_dts) cust_rdr_row_rank
		,SUM(CASE WHEN cscc.fisc_week_rltv NOT IN (0,-1)
							THEN uwcrf.repeat_7_defect_flg END) AS cust_RD_N7_lag
		,SUM(CASE WHEN cscc.fisc_week_rltv NOT IN (0,-1)
							THEN uwcrf.rd_qualify_flg END) AS cust_RD_D7_lag
		,SUM(CASE WHEN cscc.fisc_week_rltv NOT IN (0,-1,-2,-3)
							THEN uwcrf.repeat_28_defect_flg END) AS cust_RD_N28_lag
		,SUM(CASE WHEN cscc.fisc_week_rltv NOT IN (0,-1,-2,-3)
							THEN uwcrf.rd_qualify_flg END) AS cust_RD_D28_lag
	from ws_svc_gsa_bi.usdm_wo_cust_rdr_fact uwcrf 
		LEFT JOIN ws_svc_gsa_bi.usdm_wo_fact cuwf 
			on cuwf.wo_wid = uwcrf.wo_wid 
		LEFT JOIN ws_svc_gsa_bi.svc_corp_cldr cscc 
			on cscc.cldr_date = cast(uwcrf.wo_crt_utc_dts as date) 
WHERE 1=1
	and cast(uwcrf.wo_crt_utc_dts as date) > current_date - 1000 
--			and cuwf.curr_stat = 'Closed'
--			and uwlrf.mdr_dspch_flg = 1
--			and cuwf.asst_id = '1BHZYQ2'--'1-4HHLV3'
--			and cuwf.case_id = '1-URG3DN'
--			and cuwf.hes_wo_flg = 1
--			and cuwf.asst_id is null
			and uwcrf.null_asst_flg = 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,uwcrf.wo_crt_utc_dts
--fetch first 500 rows only
),
single_dps as (
/*single dispatch is a dispatch which has no dispatch created created within 14 days previously or next*/
select
	uwf4.wo_wid
	,uwf4.asst_id
	,uwf4.case_wid
	,uwf4.prev_dps
	,uwf4.next_dps
	,uwf4.dps_order_case
	,uwf4.dps_order_asset
	,case when (uwf4.tt_nextdps > 14 and uwf4.TT_PREVDPS is null)
	    or (uwf4.tt_prevdps < -14 and uwf4.tt_nextdps is null)
	    or (uwf4.tt_nextdps > 14 and uwf4.tt_prevdps < -14)
	    or (uwf4.tt_prevdps is null and uwf4.tt_nextdps is null)
    then 1 else 0 end single_dps
from(
	select
		uwf3.wo_wid
		,uwf3.asst_id
		,uwf3.case_wid
		,uwf3.prev_dps
		,uwf3.next_dps
		,uwf3.dps_order_case
		,uwf3.dps_order_asset
	    ,sum(extract(day from (uwf3.next_dps_date-uwf3.wo_crt_utc_dts))) tt_nextdps
	    ,sum(extract(day from (uwf3.prev_dps_date-uwf3.wo_crt_utc_dts))) tt_prevdps
	from(
		select
		uwf2.wo_wid 
		,uwf2.wo_nbr
		,uwf2.asst_id 
		,uwf2.case_wid 
		,uwf2.wo_crt_utc_dts
		,row_number() over(partition by uwf2.case_wid order by uwf2.wo_crt_utc_dts asc) dps_order_case
		,row_number() over(partition by uwf2.asst_id order by uwf2.wo_crt_utc_dts asc) dps_order_asset
		,min(uwf2.wo_nbr) over(partition by uwf2.asst_id order by uwf2.wo_crt_utc_dts rows between 1 preceding and 1 preceding) prev_dps
		,max(uwf2.wo_nbr) over(partition by uwf2.asst_id order by uwf2.wo_crt_utc_dts rows between 1 following and 1 following) next_dps
		,min(uwf2.wo_crt_utc_dts) over(partition by uwf2.asst_id order by uwf2.wo_crt_utc_dts rows between 1 preceding and 1 preceding) prev_dps_date
		,max(uwf2.wo_crt_utc_dts) over(partition by uwf2.asst_id order by uwf2.wo_crt_utc_dts rows between 1 following and 1 following) next_dps_date
		from ws_svc_gsa_bi.usdm_wo_fact uwf2 
--		where uwf2.asst_id = 'GKV5MP2'--'FJZJQC2'--uwf2.wo_nbr = '388631936' --'741287839'
		) uwf3
--	where uwf3.asst_id='GKV5MP2'--uwf3.wo_nbr = '388631936' --uwf3.asst_id = 'FJZJQC2'
	group by 1,2,3,4,5,6,7
	) uwf4
	order by dps_order_asset
)
select
uwf.wo_nbr 
,uwf.wo_bu_id 
,uwf.wo_wid 
,uwf.dps_type 
,uwf.call_type 
,uwf.rsn_cd 
,uwf.curr_stat 
,uwf.wo_type 
,uwf.svc_type 
,uwf.svc_opt 
,uwf.svc_opt_hrs 
,uwf.case_id 
,uwf.case_wid 
,uwf.delta_sr_row_id 
,uwf.delta_actvy_id 
,uwf.sfdc_wo_id 
,uwf.dsp_prvdr_id 
,uwf.lgst_prvdr_id 
,uwf.src_cust_prod_id 
,uwf.cust_bu_id 
,uwf.rgn_id 
,uwf.asst_id 
,uwf.asst_instl_dts 
,uwf.asst_ship_dts 
,uwf.asst_instl_iso_ctry_cd 
,uwf.asst_unified_iso_ctry_cd 
,uwf.asst_prod_hier_key 
,uwf.wo_apprvd_utc_dts 
,uwf.wo_apprvd_bdge_nbr 
,uwf.wo_apprvd_bdge_hist_id 
,uwf.wo_crt_utc_dts 
,uwf.wo_crt_by_bdge_nbr 
,uwf.wo_crt_by_bdge_hist_id 
,uwf.wo_crt_stewarded_bdge_nbr 
,uwf.wo_crt_stewarded_bdge_hist_id 
,uwf.wo_submt_utc_dts 
,uwf.wo_clos_utc_dts 
,uwf.tmzn_nm 
,uwf.tmzn_offset 
,uwf.ctry_cd 
,uwf.cust_lcl_chnl_cd 
,uwf.cust_cls_cd 
,uwf.hes_wo_flg 
,uwf.dw_src_crm 
,coalesce(uwf.crt_prcs, 'NORPA') as rpa_dps
,uwf.cust_nm
,vafw.isg_esg_hes_filter 
,case when uwf.wo_type = 'Break Fix'
	then 1
	else 0
end as break_fix_flag
,sdp.prev_dps
,sdp.next_dps
,sdp.dps_order_case
,sdp.dps_order_asset
,sdp.single_dps as single_dps_flag
,lrd.*
,crd.*
,case when uwf.curr_stat in  ('Cancel','Cancellation','Cancellation Request','Cancelled') then 1 else 0 end cancelled_flag
,'1'::int as wo_count
from WS_SVC_GSA_BI.USDM_WO_FACT uwf 
	left join leg_rdr lrd 
		on lrd.leg_wo_wid = uwf.wo_wid 
	left join cust_rdr crd 
		on crd.cust_wo_wid = uwf.wo_wid 
	left join single_dps sdp
		on sdp.wo_wid = uwf.wo_wid 
	inner join ws_svc_gsa_bi.vw_ani_6208_filters_wc vafw 
		on vafw.wo_wid = uwf.wo_wid 
where 1=1
and uwf.wo_crt_utc_dts::date > '2021-08-01'
--and uwf.asst_id = '1BHZYQ2'--'BSJGBT2'--'B6C95B3'--'1BHZYQ2'
)