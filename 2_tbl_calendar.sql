--drop view if exists ws_svc_gsa_bi.vw_ani_6208_cal_wc;
create or replace view ws_svc_gsa_bi.vw_ani_6208_cal_wc
as (
	select
	scc.cldr_date::date AS calendar_date
	,scc.cldr_day_val AS calendar_day
	,scc.fisc_day_rltv AS day_lag
	,scc.weekday_flag AS weekday_flag
	,scc.fisc_week_rltv AS week_lag
	,scc.fisc_mth_rltv AS month_lag
	,scc.fisc_qtr_rltv AS quarter_lag
	,scc.fisc_yr_rltv AS year_lag
	,scc.fisc_week_val AS fiscal_week
	,scc.fisc_mth_val AS fiscal_month
	,scc.fisc_qtr_val AS fiscal_quarter
	,scc.fisc_yr_val AS fiscal_year
	,current_timestamp as current_refresh_time
	from ws_svc_gsa_bi.svc_corp_cldr scc 
	where 1=1
	and scc.fisc_yr_rltv between -2 and 0
);
GRANT SELECT ON TABLE ws_svc_gsa_bi."vw_ani_6208_cal_wc" TO ws_grp_svc_gsa_bi_readers;
GRANT ALL ON TABLE ws_svc_gsa_bi."vw_ani_6208_cal_wc" TO ws_grp_svc_gsa_bi_writers;
GRANT SELECT ON TABLE ws_svc_gsa_bi."vw_ani_6208_cal_wc" TO ws_grp_svc_gsa_bi_writers;