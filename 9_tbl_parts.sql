drop view if exists vw_ani_6208_part_wc;

create or replace
view vw_ani_6208_part_wc as (
/* part table part table as the main table, everything else will be joined to that*/
with part_ord as (
select
		uwi.wo_nbr as ord_wo_nbr ,
		uwi.wo_bu_id as ord_wo_bu_id ,
		uwi.wo_wid as ord_wo_wid ,
		uwi.itm_nbr as ord_itm_nbr ,
		uwi.itm_src_id as ord_itm_src_id ,
		uwi.itm_qty as ord_itm_qty ,
		uwi.itm_comdty_desc as ord_itm_comdty_desc ,
		uwi.part_ord_seq_nbr as ord_part_ord_seq_nbr ,
		uwi.part_ord_ln_nbr as ord_part_ord_ln_nbr ,
		uwi.part_ord_crt_dts as ord_part_ord_crt_dts ,
		uwi.maj_part_flg as ord_maj_part_flg ,
		uwi.non_std_part_flg as ord_non_std_part_flg ,
		uwi.batt_comdty_flg as ord_batt_comdty_flg ,
		uwi.non_batt_comdty_flg as ord_non_batt_comdty_flg ,
		uwi.sys_flg as ord_sys_flg ,
		uwi.cru_flg ,
		uwi.fru_flg ,
		uwi.parts_row_rank_wo ,
		case
			when
		--uwi.parts_row_rank_wo = 1
		uwi.actual_part_flag = 1
		--			and uwi.part_ord_seq_nbr <= 1
		and uwi.parts_per_dispatch_ex_docu <= 1
			then 1
		else 0
	end as single_commodity ,
		case 
			when uwi.parts_row_rank_wo = 1
			then uwi.parts_per_dispatch
		else 0
	end parts_per_dispatch ,
		case 
			when uwi.parts_row_rank_wo = 1
			then uwi.parts_per_dispatch_ex_docu
		else 0
	end parts_per_dispatch_ex_docu ,
		case 
			when uwi.parts_per_dispatch_ex_docu >= 1
			then 1
		else 0
	end ppd_denom_ex_docu ,
		case 
			when uwi.maj_part_flg = 1 then uwi.itm_qty
		else 0
	end mpd_num,
		case 
			when uwi.maj_part_flg = 1 then 1
		else 0
	end mpd_denom
from
	(
	select
		*
		,
		case
			when (uw.maj_part_flg = 1
			or uw.itm_comdty_desc not in ('ATTACH KIT', 'Documentation'))
			then row_number() over(partition by uw.wo_wid
		order by
			uw.part_ord_ln_nbr asc)
			else 0
		end as parts_row_rank_wo
		,
		case
			when (uw.maj_part_flg = 1
			or uw.itm_comdty_desc not in ('ATTACH KIT', 'Applications', 'BACKPLANE', 'Backplanes', 'CABINET', 'Documentation', 'Drivers', 'SOFTWARE', 'SOFTWARE LICENSE', 'Drivers'))
				then 1
			else 0
		end as actual_part_flag
		--		,row_number() over(partition by uw.wo_wid order by uw.part_ord_ln_nbr asc) as parts_row_rank_wo
		,
		sum(uw.itm_qty) over(partition by uw.wo_wid) as parts_per_dispatch
		,
		sum(case when uw.itm_comdty_desc not in ('ATTACH KIT', 'Documentation') then uw.itm_qty else 0 end) over(partition by uw.wo_wid) as parts_per_dispatch_ex_docu
	from
		usdm_wo_itm_ord_fact uw
		--where (uw.maj_part_flg = 1 or uw.itm_comdty_desc not in ('ATTACH KIT','Applications','BACKPLANE','Backplanes','CABINET','Documentation','Drivers','SOFTWARE','SOFTWARE LICENSE','Drivers'))
		--,'CABLE','Cables','POWER CORD','POWERCORD'
		) uwi
where
	uwi.part_ord_crt_dts :: date > '2020-01-01'
	--	and uwi.wo_nbr = '737691321'--'43736468548'
)
--,
--part_ship as (
--	select
--		uwisf.wo_nbr as ship_wo_nbr ,
--		uwisf.wo_bu_id as ship_wo_bu_id ,
--		uwisf.wo_wid as ship_wo_wid ,
--		uwisf.itm_nbr as ship_itm_nbr ,
--		uwisf.itm_src_id as ship_itm_src_id ,
--		uwisf.itm_qty as ship_itm_qty ,
--		uwisf.itm_comdty_desc as ship_itm_comdty_desc ,
--		uwisf.part_ord_seq_nbr as ship_part_ord_seq_nbr ,
--		uwisf.part_ord_ln_nbr as ship_part_ord_ln_nbr ,
--		uwisf.bklg_eta_lcl_dts as ship_bklg_eta_lcl_dts ,
--		uwisf.part_ord_shipd_lcl_dts as ship_part_ord_shipd_lcl_dts ,
--		uwisf.part_ord_cmpltn_lcl_dts as ship_part_ord_cmpltn_lcl_dts ,
--		uwisf.new_part_flg as ship_new_part_flg ,
--		uwisf.part_type_cd as ship_part_type_cd 
--	from ws_svc_gsa_bi.usdm_wo_itm_shipd_fact uwisf 
--	where uwisf.part_ord_shipd_lcl_dts :: date > '2019-01-01'
--),
--part_used as (
--	select
--		uwiuf.wo_nbr as used_wo_nbr ,
--		uwiuf.wo_bu_id as used_wo_bu_id ,
--		uwiuf.wo_wid as used_wo_wid ,
--		uwiuf.itm_nbr as used_itm_nbr ,
--		uwiuf.itm_src_id as used_itm_src_id ,
--		uwiuf.itm_qty as used_itm_qty ,
--		uwiuf.itm_used_dts as used_itm_used_dts ,
--		uwiuf.itm_comdty_desc as used_itm_comdty_desc ,
--		uwiuf.itm_cost_amt as used_itm_cost_amt 
--	from ws_svc_gsa_bi.usdm_wo_itm_used_fact uwiuf 
--	where uwiuf.itm_used_dts :: date > '2019-01-01'
--)
select
		ord.ord_wo_nbr,
		ord.ord_wo_bu_id,
		ord.ord_wo_wid,
		ord.ord_itm_nbr,
		ord.ord_itm_src_id,
		ord.ord_itm_qty,
		ord.ord_itm_comdty_desc,
		ord.ord_part_ord_seq_nbr,
		ord.ord_part_ord_ln_nbr,
		ord.ord_part_ord_crt_dts,
		ord.ord_maj_part_flg,
		ord.ord_non_std_part_flg,
		ord.ord_batt_comdty_flg,
		ord.ord_non_batt_comdty_flg ,
		ord.ord_sys_flg ,
		ord.cru_flg ,
		ord.fru_flg ,
		ord.parts_row_rank_wo ,
		ord.single_commodity ,
		ord.parts_per_dispatch ,
		ord.parts_per_dispatch_ex_docu ,
		ord.ppd_denom_ex_docu ,
		ord.mpd_num ,
		ord.mpd_denom ,
		uwf.wo_type 
	--		ship.ship_itm_nbr,
	--		ship.ship_itm_src_id,
	--		ship.ship_itm_qty,
	--		ship.ship_itm_comdty_desc,
	--		ship.ship_part_ord_seq_nbr,
	--		ship.ship_part_ord_ln_nbr,
	--		ship.ship_bklg_eta_lcl_dts,
	--		ship.ship_part_ord_shipd_lcl_dts,
	--		ship.ship_part_ord_cmpltn_lcl_dts,
	--		ship.ship_new_part_flg,
	--		ship.ship_part_type_cd,
	--		used.used_itm_nbr,
	--		used.used_itm_src_id,
	--		used.used_itm_qty,
	--		used.used_itm_used_dts,
	--		used.used_itm_comdty_desc,
	--		used.used_itm_cost_amt
from
	part_ord ord
	--	left join part_ship ship
	--		on ship.ship_wo_wid = ord.ord_wo_wid
	--	left join part_used used
	--		on used.used_wo_wid = ord.ord_wo_wid
inner join vw_ani_6208_filters_wc fil
		on
	fil.wo_wid = ord.ord_wo_wid
left join ws_svc_gsa_bi.usdm_wo_fact uwf 
		on
	uwf.wo_wid = ord.ord_wo_wid
where
	1 = 1
	--and ord.ord_wo_nbr = '739573509'
	and ord.ord_sys_flg <> 1
	and uwf.wo_type = 'Break Fix'
)