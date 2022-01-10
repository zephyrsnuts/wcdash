--drop view if exists ws_svc_gsa_bi.vw_ani_6208_products_wc;

create or replace
view ws_svc_gsa_bi.vw_ani_6208_products_wc as (
select
	uph.prod_key ,
	uph.prod_bu_type as prod_business_unit ,
	uph.prod_grp_desc as prod_group_desc ,
	uph.prod_desc as prod_description ,
	uph.src_sys_id as prod_source ,
	case
		uph.src_sys_id when 'LEMC' then uph.fmly
		when 'LDELL' then uph.prod_lob_rptg_grp
		else coalesce(uph.fmly, uph.prod_lob_rptg_grp, 'UNKN')
	end as product_lob_group ,
	case
		uph.src_sys_id when 'LEMC' then uph.prod_ln
		when 'LDELL' then uph.prod_ln_nm
		else coalesce(uph.prod_ln, uph.prod_ln_nm, 'UNKN')
	end as prod_line_name ,
	case
		uph.src_sys_id when 'LEMC' then uph.prod_lvl
		when 'LDELL' then uph.prod_ln_cd
		else coalesce(uph.prod_lvl, uph.prod_ln_cd, 'UNKN')
	end as prod_code
from
	ws_svc_gsa_bi.usdm_prod_hier uph
where
	1 = 1
	and uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') )