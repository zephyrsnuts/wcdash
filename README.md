# wcdash
I will be the guy to give them the WO data.

Labor only % in the total dispatches.
HES only

exclude VXRAIL
have FED as well
RUI dispatches that can be tracked separately. RUI is PFN

partner filter.

to be able to see the

something that is easy and couple of clicks away.

no raw data required right?




points to talk about
1. numbers are not accurate need to be fixed, in the process of correcting it
2. instead of Delta SR i can include S360 case numbers
3. in the process of trying to clean up the data a little more for the agent names, some of the data shows UNKN which how it is updated in the person tables, i am trying to figure out a way to 
actually use another column to populate the Region for example, that is a work in progress, on the entire project itself.

4. percentage of labor against total dispatches
	filtering will be 
		a. PFN
		b. FED included
5. need to put in a filter for PFN, working on it.
6. partner filter will be added once i update the base tables that are feeding this report.

i will share this with all of you, i will add a few more charts first, and correct the data. please let me know if the data is valid. 
this is based off the same tables that are feeding the cancelled WO order report, which already has validated data.


separate PFN and FED
RDR info
helper task volume.
WO special instructions

refreshed weekly

HES work order data the cust bu_id is 99901 this indicates, HES data,, ./


https://gsa-project.visualstudio.com/GSA/_wiki/wikis/GSA%20Private/500/PowerBI-Dashboard-Template

ddlgpmprd11.us.dell.com:6420
gp_ns_ddl_prod


agent primary table
====================
filtered with not just person, but also product.
joined epicenter table, to get manager level 3 and above.
epicenter table causes duplicates, need to qualify.
qualify in postgres not available, hence using row_rank desc, and choosing row_rank = 1 in the join.
works well.

there were nulls in the agent primary table, it was found that it was because of the filters in the agent filters CTE
1. removed case filtering for the work order section in the agent's filter table, 
2. commented --		and scc.fisc_qtr_rltv between -5 and 0,
3. changed the person table to teh main table. and then joined everything else to that, that is why some data was missing or showing as null.
/* make the person table as the primary table. and then join everything else to that
 * otherwise the filters will act funny, eg, the following hist ID will not pull in data because the case or wo may be missing for that specific hist ID cos there probably was no case created
 * at all during that time of the agent history. HIST.
 * 
 * */
 
 currently the view for agents is set to have distinct lines of agent hist ID, this is working however, i have a column which says person join type.
 this basically has 3 values, wo creator, case owner and case creator, because of this the agent view will give 3 rows of data as unique for the same agent case wo etc...
 so if i want a single line then i should also set the person join type in the join. 
 eg: left join ws_svc_gsa_bi.vw_ani_6208_person_wc vapw 
		on vapw.src_prsn_hist_gen_id = ucf.owner_bdge_hist_id_at_crt 
		and vapw.person_join_type = 'wo_creator'

offer table
==========

incdnt_id is the case_id or the wo_nbr, (it is not the case_nbr, it is only the case_id)
but its better to join to wid anyway, so thats where the join is.
incidnt_wid to either case_wid or wo_wid

offer_wo was giving 0 rows, because i had joined the case table to the WO id
i had to change that to the WO table.

offer table was pulling in 40 million rows, had to reduce it, so i joined the filter table to it, if there are any issues, then i will have to remove it and then add its own filters by joining
the actual person calendar and case tables (case table is already joined).
i also added the second level filter person_join_type in ('case_owner', 'case_creator') this reduced the number of rows even further to 5 lacks from 7 lacks
this however did not make any difference to the wo offer table when i added person_join_type('wo_creator') the rows remained the same, whether it was added or not.

filter table
============

1. updated the ISG_ESG_HES_Filter column as it was illogical
ESG and HES combined is ISG, i cannot possibly put that in a case statement.
because of this i also found that there are cases in the report which are not ISG purely, because the filters i use uses
either person table or product table and not both together
so case or wo can either be in (ESG PBU and ISG PBU or person ISG) because of this some additional cases are there, almost 30% i will wait till business responds to advise on numbers and counts.
if they say it is wrong then i can change my filters to filter only cases where it is person ISG and product ISG only. (and not either or)

eg: in the following case i will replace the OR with an AND

and (vapw2.prod_business_unit in ('Enterprise Solution Group PBU', 'Infrastructure Solutions PBU')
		OR (vapw.bus_rptg_catg_nm = 'Tech Support'
		and vapw.bus_rptg_grp_nm in ('Commercial Enterprise Services', 'Infrastructure Solutions Group', 'High End Storage', 'Commercial Shared Services',
		 'Large Business', 'Large Enterprise', 'Technical Account Management')))
		 
2. i also updated the wo_creator, case_creator, case_owner to have ISG prefix, so it will be ISG_wo_creator and so on, will have to update my other views to have fix.

case table
===========

1. added calendar join dates rather than join dts by removing time and keeping only date.


updated the filters table vw_ani_6208_offer_wc
changes were 
select distinct
--	and vafw.person_join_type in ('ISG_case_creator', 'ISG_case_owner')
removed this join because this column will be removed from teh person table.

removed teh columns from the filter table too, i had not removed it from there.


