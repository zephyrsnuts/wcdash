drop view if exists ws_svc_gsa_bi.vw_ani_6208_products_wc;

create or replace
view ws_svc_gsa_bi.vw_ani_6208_products_wc as (
select
	uph.prod_key ,
	uph.prod_bu_type as prod_business_unit ,
	uph.prod_grp_desc as prod_group_desc ,
	uph.prod_desc as prod_description ,
	uph.src_sys_id as prod_source ,
	case uph.prod_ln_cd when 'UNKN' then uph.prod_ln 
		else coalesce (uph.prod_ln_cd , uph.itm_nbr)
	end as prod_line_code ,
	case
		uph.src_sys_id when 'LEMC' then coalesce(uph.fmly, 'UNKN' )
		when 'LDELL' then coalesce(uph.prod_lob_rptg_grp, 'UNKN')
		else coalesce(uph.fmly, uph.prod_lob_rptg_grp, 'UNKN')
	end as product_lob_group_family ,
	case
		uph.src_sys_id when 'LEMC' then coalesce(uph.prod_ln, 'UNKN')
		when 'LDELL' then coalesce(uph.prod_ln_nm, 'UNKN')
		else coalesce(uph.prod_ln, uph.prod_ln_nm, 'UNKN')
	end as prod_line_name ,
	case
		uph.src_sys_id when 'LEMC' then coalesce(uph.prod_lvl, 'UNKN')
		when 'LDELL' then coalesce(uph.prod_lob_cd, 'UNKN')
		else coalesce(uph.prod_lvl, uph.prod_lob_cd, 'UNKN')
	end as prod_lob_code
from
	ws_svc_gsa_bi.usdm_prod_hier uph
where
	1 = 1
	and uph.prod_bu_type in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU') )