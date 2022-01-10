drop view if exists ws_svc_gsa_bi.vw_ani_6208_cust_wc;
create or replace
view vw_ani_6208_cust_wc as (
--select
--count(*)
--from (
with gmv as (
select
	distinct 
	gm.ctry_desc ,
	gm.sub_rgn_desc ,
	gm.rgn_desc ,
	gm.area_desc ,
	gm.iso2_ctry_cd
	--	row_number() over(partition by gm.iso2_ctry_cd order by gm. 
from
	ws_svc_gsa_bi.geo_mstr_vw gm
)
select
	distinct
	uaf.asst_id ,
	uaf.asst_instl_at_cust_nbr ,
	--joins to party_base.emc_cust_dim.ucid (universal customer id for EMC)
	uaf.asst_bilto_cust_nbr ,
	--joins party_base.sls_svc_cus for LDELL customers and to ucid for LEMC
	uaf.asst_shpto_cust_nbr ,
	--joins party_base.sls_svc_cus for LDELL customers and to ucid for LEMC
	uaf.asst_end_user_cust_nbr ,
	--joins party_base.sls_svc_cus for LDELL customers and to ucid for LEMC
	uaf.asst_crt_utc_dts ,
	uaf.asst_shipd_dtsz ,
	uaf.asst_instl_dtsz ,
	uaf.asst_instl_utc_dt ,
	uaf.asst_instl_iso_ctry_cd ,
	uaf.asst_dinstl_dtsz ,
	uaf.asst_dinstl_utc_dt ,
	uaf.asst_id_type_cd ,
	uaf.asst_qty ,
	uaf.asst_drct_iso_ctry_cd ,
	uaf.asst_bill_iso_ctry_cd ,
	uaf.asst_unified_iso_ctry_cd ,
	uaf.emc_asst_cust_desc ,
	uaf.mfrr_srl_nbr ,
	uaf.src_cust_prod_id ,
	uaf.mfrr_id ,
	uaf.svc_tag_id ,
	uaf.asst_ord_bu_id,
	uaf.asst_cust_bu_id ,
	uaf.asst_src_ord_bu_id ,
	uaf.emc_hrdwr_instld_by ,
	uaf.asst_shp_iso_ctry_cd ,
	uaf.asst_sld_iso_ctry_cd ,
	uaf.asst_prim_dspch_iso_ctry_cd ,
	uaf.ord_tie_nbr ,
	uaf.lcl_chnl_cd ,
	uaf.cust_lcl_chnl_cd ,
	uaf.cust_cls_cd ,
	uaf.asst_status ,
	uaf.eosl_dt ,
	uaf.part_ord_tie_nbr ,
	--LEMC PSTN part number
	uaf.dnr_flg ,
	uaf.trmntd_flg ,
	uaf.free_radcl_flg ,
	uaf.del_flg ,
	gmv.ctry_desc as bill_to_ctry_desc ,
	gmv.sub_rgn_desc as bill_to_sub_rgn_desc ,
	gmv.rgn_desc as bill_to_rgn_desc ,
	gmv.area_desc as bill_to_area_desc ,
	gmv.iso2_ctry_cd as bill_to_iso2_ctry_cd ,
	gmv_ship.ctry_desc as ship_to_ctry_desc ,
	gmv_ship.sub_rgn_desc as ship_to_sub_rgn_desc ,
	gmv_ship.rgn_desc as ship_to_rgn_desc ,
	gmv_ship.area_desc as ship_to_area_desc ,
	gmv_ship.iso2_ctry_cd as ship_to_iso2_ctry_cd ,
	gmv_sold.ctry_desc as sold_to_ctry_desc ,
	gmv_sold.sub_rgn_desc as sold_to_sub_rgn_desc ,
	gmv_sold.rgn_desc as sold_to_rgn_desc ,
	gmv_sold.area_desc as sold_to_area_desc ,
	gmv_sold.iso2_ctry_cd as sold_to_iso2_ctry_cd ,
	gmv_install.ctry_desc as install_ctry_desc , 
	gmv_install.sub_rgn_desc as install_sub_rgn_desc , 
	gmv_install.rgn_desc as install_rgn_desc , 
	gmv_install.area_desc as install_area_desc , 
	gmv_install.iso2_ctry_cd as install_iso2_ctry_cd , 
	gmv_direct.ctry_desc as direct_ctry_desc ,
	gmv_direct.sub_rgn_desc as direct_sub_rgn_desc ,
	gmv_direct.rgn_desc as direct_rgn_desc ,
	gmv_direct.area_desc as direct_area_desc ,
	gmv_direct.iso2_ctry_cd as direct_iso2_ctry_cd
	--	count(distinct uaf.asst_id) as asset_count
from
	(
	select
		uaf1.* ,
		row_number() over(partition by uaf1.src_cust_prod_id
	order by
		uaf1.asst_crt_utc_dts asc) as row_rank_order
	from
		ws_svc_gsa_bi.usdm_asst_fact uaf1) uaf
left join gmv 
		on
	gmv.iso2_ctry_cd = uaf.asst_bill_iso_ctry_cd
	--		and uaf.row_rank_order = 1
left join gmv as gmv_ship 
		on
	gmv_ship.iso2_ctry_cd = uaf.asst_shp_iso_ctry_cd
	--		and uaf.row_rank_order = 1
left join gmv as gmv_sold
		on
	gmv_sold.iso2_ctry_cd = uaf.asst_sld_iso_ctry_cd
left join gmv as gmv_install
		on
	gmv_install.iso2_ctry_cd = uaf.asst_instl_iso_ctry_cd
left join gmv as gmv_direct
		on
	gmv_direct.iso2_ctry_cd = uaf.asst_drct_iso_ctry_cd
left join usdm_case_fact ucf 
		on
	ucf.asst_id = uaf.asst_id
inner join vw_ani_6208_filters_wc fil
		on
	fil.case_wid = ucf.case_wid
	--	left join ws_svc_gsa_bi.country cab
	--		on cab.country_code_iso2 = uaf.
WHERE 1=1
	--uaf.asst_crt_utc_dts::date > '2019-01-01'
	and uaf.row_rank_order = 1
	and uaf.del_flg <> 'Y'
	and uaf.asst_id is not null
	--and uaf.asst_id = '24194958'--'524GJW2'
	--fetch first 500 rows only
--group by 
--1,2,3,4,5,6,7,8,9,
--10,11,2,13,14,15,16,17,18,19,
--20,21,22,23,24,25,26,27,28,29,
--30,31,32,33,34,35,36,37,38,39,
--40,41,42,43,44,45,46,47,48,49,
--50,51,52,53,54,55,56,57,58,59, 
--60,61,62,63,64,66,66, asst_dinstl_utc_dt, gmv_direct.area_desc
--) foo
)