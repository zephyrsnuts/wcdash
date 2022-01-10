select * from ws_svc_gsa_bi.vw_ani_6208_products_wc prod 
where 
prod.prod_lob_code = '4MD'
--lower(prod.prod_line_name) like '%powerstore%' 
fetch first 500 rows only;

select distinct
ad.assoc_ptnr_nm 
,ad.assoc_bdge_nbr 
,ad.assoc_email_addr 
from ws_svc_gsa_bi.usdm_case_fact ucf 
left join ws_svc_gsa_bi.assoc_dim ad 
	on ad.src_prsn_hist_gen_id =ucf.owner_bdge_hist_id_at_crt 
where ucf.case_nbr = '1063194228'