drop view if exists ws_svc_gsa_bi.vw_ani_6208_offer_all_wc;
create or replace view ws_svc_gsa_bi.vw_ani_6208_offer_all_wc as (
SELECT distinct
ain.*
from (
	select distinct
		uier1.incdnt_id
		,uier1.offer_level
		,uier1.incdnt_cntct_type
		,uier1.incdnt_type_cd
		,uier1.asst_id
		,uier1.incdnt_wid
		,uier1.cntrct_lbr_type_ind
		,uier1.svc_lvl_desc
		,uier1.tech_supp_rank
		,uier1.cntrct_strt_gmt_dt as contract_start_date
		,uier1.incdnt_crt_dt as incident_create_date
		,uiop.dtc_flg as dtc_PON_flag
		,uiop.oos_flg as oos_software_support_flag
		,uiop.owr_flg as owr_flag
		,uiop.pps_flg as premium_phone_support_flag
		,uiop.prosupport_flg as pro_support_flag
		,uiop.ps_flg  as premium_support_flag
		,uiop.psp_flg as premium_support_plus_flag
		,uiop.dw_src_crm as source_crm_offer
		,uiop.mfrr_nm 
		,uiop.itm_cls_cd 
		,uiop.new_wrnty_type_cd 
		,uiop.ord_nbr 
		,uiop.wrnty_stat_cd 
		,uiop.wrnty_type_cd 
	from
		(select distinct
			uier.svc_lvl_desc
			,case 
				when uier.entlt_prod_offrg_cd in ('OWR') then 'OWR'
				when to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('(prosupport | pro) & !Plus') then 'ProSupport'
				when to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('prosupport & plus') then 'ProSupport Plus'
				when (to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('NBD & !PRO & !ProSupport')
						or to_tsvector(uier.monarch_prod_offrg_cd) @@ to_tsquery('basic')
							or lower(uier.monarch_prod_offrg_cd) like ('%basic%'))
						then 'Basic'
				else 'NA'
			end offer_level
			,uier.incdnt_cntct_type
			,uier.cntrct_lbr_type_ind
			,uier.incdnt_type_cd
			,uier.incdnt_wid 
			,uier.incdnt_id 
			,uier.asst_id 
			,uier.cntrct_strt_gmt_dt 
			,uier.incdnt_crt_dt 
			,row_number() over(partition by uier.incdnt_id order by uier.entlt_techl_supp_rnk desc) as tech_supp_rank
		from ws_svc_gsa_bi.usdm_incdnt_entlt_rnk uier 
		where 1=1 
			and uier.incdnt_type_cd = 'CASE'
			and uier.entlt_techl_supp_rnk in (0,1)
			) uier1
	left join ws_svc_gsa_bi.usdm_incdnt_offrg_pon uiop 
		on uiop.incdnt_wid = uier1.incdnt_wid
	left join ws_svc_gsa_bi.usdm_case_fact ucf 
	--	on ucf.case_id = uier1.incdnt_id
		on ucf.case_wid = uier1.incdnt_wid
	inner join ws_svc_gsa_bi.vw_ani_6208_filters_wc vafw 
		on vafw.case_wid = uier1.incdnt_wid
		--and vafw.person_join_type in ('ISG_case_creator', 'ISG_case_owner')
	where 1 = 1
		--and ucf.case_nbr = '124408096'--'124350175' --'117073130'
		and tech_supp_rank = 1
		and ucf.case_crt_dts::date > '2020-01-01'
		and ucf.quick_case_flg <> 1
		and lower(ucf.usdm_case_type) not in ('care')
union all 
	SELECT distinct
		uier1.incdnt_id as wo_incdnt_id --primary join key
		,uier1.offer_level as wo_offer_level 
		,uier1.incdnt_cntct_type as wo_incdnt_cntct_type
		,uier1.incdnt_type_cd as wo_incdnt_type_cd
		,uier1.asst_id as wo_asst_id
		,uier1.incdnt_wid as wo_incdnt_wid --primary join key
		,uier1.cntrct_lbr_type_ind as wo_cntrct_lbr_type_ind
		,uier1.svc_lvl_desc as wo_svc_lvl_desc
		,uier1.tech_supp_rank as wo_tech_supp_rank
		,uier1.cntrct_strt_gmt_dt as wo_contract_start_date
		,uier1.incdnt_crt_dt as wo_incident_create_date
		,uiop.dtc_flg as wo_dtc_PON_flag
		,uiop.oos_flg as wo_oos_software_support_flag
		,uiop.owr_flg as wo_owr_flag
		,uiop.pps_flg as wo_premium_phone_support_flag
		,uiop.prosupport_flg as wo_pro_support_flag
		,uiop.ps_flg as wo_premium_support_flag
		,uiop.psp_flg as wo_premium_support_plus_flag
		,uiop.dw_src_crm as wo_source_crm_offer
		,uiop.mfrr_nm as wo_mfrr_nm 
		,uiop.itm_cls_cd as wo_itm_cls_cd 
		,uiop.new_wrnty_type_cd as wo_new_wrnty_type_cd 
		,uiop.ord_nbr as wo_ord_nbr 
		,uiop.wrnty_stat_cd as wo_wrnty_stat_cd 
		,uiop.wrnty_type_cd as wo_wrnty_type_cd 
	from
		(select distinct
			uier.svc_lvl_desc
			,case 
				when uier.entlt_prod_offrg_cd in ('OWR') then 'OWR'
				when to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('(prosupport | pro) & !Plus') then 'ProSupport'
				when to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('prosupport & plus') then 'ProSupport Plus'
				when (to_tsvector(uier.svc_lvl_desc) @@ to_tsquery('NBD & !PRO & !ProSupport')
						or to_tsvector(uier.monarch_prod_offrg_cd) @@ to_tsquery('basic')
							or lower(uier.monarch_prod_offrg_cd) like ('%basic%'))
						then 'Basic'
				else 'NA'
			end offer_level
			,uier.incdnt_cntct_type
			,uier.cntrct_lbr_type_ind
			,uier.incdnt_type_cd
			,uier.incdnt_wid 
			,uier.incdnt_id 
			,uier.asst_id 
			,uier.cntrct_strt_gmt_dt 
			,uier.incdnt_crt_dt 
			,row_number() over(partition by uier.incdnt_id order by uier.entlt_techl_supp_rnk desc) as tech_supp_rank
		from ws_svc_gsa_bi.usdm_incdnt_entlt_rnk uier 
		where 1=1 
			and lower(uier.incdnt_type_cd) = 'wo'
			and uier.entlt_techl_supp_rnk in (0,1)
		) uier1
		left join ws_svc_gsa_bi.usdm_incdnt_offrg_pon uiop 
			on uiop.incdnt_wid = uier1.incdnt_wid
		left join ws_svc_gsa_bi.usdm_wo_fact uwf 
			on uwf.wo_wid = uier1.incdnt_wid
		inner join ws_svc_gsa_bi.vw_ani_6208_filters_wc vafw 
			on vafw.wo_wid = uier1.incdnt_wid
			--and vafw.person_join_type in ('ISG_wo_creator')
	where 1=1 
		--and ucf.case_nbr = '124408096'--'124350175' --'117073130'
		and tech_supp_rank = 1
		and uwf.wo_crt_utc_dts::date > '2020-01-01'
		--and ucf.quick_case_flg <> 1
		--and lower(ucf.usdm_case_type) not in ('care') 
) ain
group by 
1,2,3,4,5,6,7,8,9,
10,11,2,13,14,15,16,17,18,19,
20,21,22,23,24,25, ain.dtc_pon_flag
)