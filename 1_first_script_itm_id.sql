drop table ws_svc_gsa_bi.avn_6208_wcdash_parts;
create table ws_svc_gsa_bi.avn_6208_wcdash_parts (
	wo_bu_id varchar(15)
	,itm_src_id varchar(20)
	,part_ord_id varchar(20)
	,cre_ord_bu_id varchar(5)
	,rgn_id int2
	,delta_actvy_id varchar(20)
	,sfdc_wo_id varchar(20)
	,wo_wid varchar(32)
);
insert into ws_svc_gsa_bi.avn_6208_wcdash_parts (
	wo_bu_id
	,itm_src_id
	,part_ord_id
	,cre_ord_bu_id
	,rgn_id
	,delta_actvy_id
	,sfdc_wo_id
	,wo_wid
)
select distinct
	uwiof.wo_bu_id
	,uwiof.itm_src_id
	,uwiof.part_ord_id
	,uwiof.cre_ord_bu_id
	,uwiof.rgn_id
	,uwiof.delta_actvy_id
	,uwiof.sfdc_wo_id
	,uwiof.wo_wid
from ws_svc_gsa_bi.usdm_wo_itm_ord_fact uwiof 
left join ws_svc_gsa_bi.usdm_wo_fact uwf 
	on uwf.wo_wid = uwiof.wo_wid 
left join ws_svc_gsa_bi.assoc_dim ad 
	on uwf.wo_crt_by_bdge_hist_id = ad.src_prsn_hist_gen_id 
where 1=1
and cast (uwf.wo_crt_utc_dts as date) > '2019-01-01'
and ad.bus_rptg_catg_nm in ('Tech Support')
and ad.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage',
'Commercial Shared Services','Large Business','Large Enterprise','Technical Account Management')