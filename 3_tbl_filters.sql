alter view ws_svc_gsa_bi.vw_ani_6208_filters_wc rename to vw_ani_6208_filters_wc_old;
drop view if exists ws_svc_gsa_bi.vw_ani_6208_filters_wc_old cascade;
create or replace
view ws_svc_gsa_bi.vw_ani_6208_filters_wc as (
select distinct
	allin.*
from
	(
	select
		uwf.wo_wid ,
		ucf.case_wid ,
		uph.prod_key ,
		uph.src_sys_id ,
--		case
--			when ((lower(ucf.usdm_case_type) not in ('care') 
--				or lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care'))
--				and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
--				'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')
--				then 'ISG WO') and 
--				(uwf.hes_wo_flg in (1) or uwf.cust_bu_id = 99901) then 'HES WO'
--			when ((lower(ucf.usdm_case_type) not in ('care') 
--				or lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care'))
--				and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
--				'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')
--				then 'ISG WO') 
--					and (uwf.hes_wo_flg not in (1) or uwf.cust_bu_id <> 99901) then 'ESG WO'
--		end ISG_ESG_HES_Filter ,
		case
			when (uwf.hes_wo_flg in (1) or uwf.cust_bu_id = 99901) then 'HES WO'
			when (uwf.hes_wo_flg not in (1) or uwf.cust_bu_id <> 99901) then 'ESG WO'
		end ISG_ESG_HES_Filter 
--		'ISG_wo_creator'::varchar as person_join_type
	from
		ws_svc_gsa_bi.usdm_wo_fact uwf
	left join ws_svc_gsa_bi.usdm_case_fact ucf on
		ucf.case_wid = uwf.case_wid
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = uwf.asst_prod_hier_key
	left join ws_svc_gsa_bi.assoc_dim per on
		per.src_prsn_hist_gen_id = uwf.wo_crt_by_bdge_hist_id
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = uwf.wo_crt_utc_dts :: date
	where
		1 = 1
		and (lower(ucf.usdm_case_type) not in ('care')
		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
		and ucf.case_rec_type not in ('Internal Case')
		and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
		and scc.fisc_qtr_rltv between -3 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')))
union
	select
		'No_Work_Order' as wo_wid ,
		ucf.case_wid ,
		uph.prod_key ,
		uph.src_sys_id ,
--		case
--			when (lower(ucf.usdm_case_type) not in ('care') or lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')) 
--			and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
--				'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')
--			then 'ISG CASE'
--			when (ucf.hes_case_flg in (1) or ucf.cust_bu_id = 99901) then 'ISG HES case'
--			when (ucf.hes_case_flg not in (1) or ucf.cust_bu_id <> 99901) then 'ISG ESG case'
--		end ISG_ESG_HES_Filter ,
		case
			when (ucf.hes_case_flg in (1) or ucf.cust_bu_id = 99901) then 'HES case'
			when (ucf.hes_case_flg not in (1) or ucf.cust_bu_id <> 99901) then 'ESG case'
		end ISG_ESG_HES_Filter 
--		'ISG_case_owner'::varchar as person_join_type
	from
		ws_svc_gsa_bi.usdm_case_fact ucf
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = ucf.asst_prod_hier_key
	left join ws_svc_gsa_bi.assoc_dim per on
		per.src_prsn_hist_gen_id = coalesce (ucf.owner_bdge_hist_id_at_crt, ucf.owner_latest_bdge_hist_id, ucf.owner_bdge_hist_id_at_cmplt)
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = ucf.case_crt_dts :: date
	where
		1 = 1
		and (lower(ucf.usdm_case_type) not in ('care')
		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
		and ucf.case_rec_type not in ('Internal Case')
		and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
		and scc.fisc_qtr_rltv between -3 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')))
union
	select
		'No_Work_Order' as wo_wid ,
		ucf.case_wid ,
		uph.prod_key ,
		uph.src_sys_id ,
--		case
--			when (lower(ucf.usdm_case_type) not in ('care') or lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')) 
--			and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 
--				'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management')
--			then 'ISG CASE'
--			when (ucf.hes_case_flg in (1) or ucf.cust_bu_id = 99901) then 'ISG HES case'
--			when (ucf.hes_case_flg not in (1) or ucf.cust_bu_id <> 99901) then 'ISG ESG case'
--		end ISG_ESG_HES_Filter ,
		case
			when (ucf.hes_case_flg in (1) or ucf.cust_bu_id = 99901) then 'HES case'
			when (ucf.hes_case_flg not in (1) or ucf.cust_bu_id <> 99901) then 'ESG case'
		end ISG_ESG_HES_Filter 
--		'ISG_case_creator'::varchar as person_join_type
	from
		ws_svc_gsa_bi.usdm_case_fact ucf
	left join ws_svc_gsa_bi.usdm_prod_hier uph on
		uph.prod_key = ucf.asst_prod_hier_key
	left join ws_svc_gsa_bi.assoc_dim per on
		per.src_prsn_hist_gen_id = coalesce (ucf.crt_by_bdge_hist_id , ucf.owner_crt_bdge_hist_id )
	left join ws_svc_gsa_bi.svc_corp_cldr scc on
		scc.cldr_date = ucf.case_crt_dts :: date
	where
		1 = 1
		and (lower(ucf.usdm_case_type) not in ('care')
		and lower(ucf.CASE_REC_TYPE) not in ('care_case_read_only', 'care')
		and ucf.case_rec_type not in ('Internal Case')
		and lower(ucf.int_case_rec_type_cd) in ('external case'))
		and ucf.quick_case_flg <> 1
		and scc.fisc_qtr_rltv between -3 and 0
		and (uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		or (per.bus_rptg_catg_id = 5
		and per.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services', 'Large Business', 'Large Enterprise', 'Technical Account Management'))) 
	) as allin 
--where allin.case_wid = 'B85AE2F1581A8F23ADF1A577AB5B276B'
group by 1,2,3,4,5
)