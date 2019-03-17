/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with HPD Project Data
START DATE: 2/10/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge HPD Deduped file with HPD RFP projects
2. Calculate incremental units
************************************************************************************************************************************************************************************/

with hpd_rfp_1 as
(
	select
		a.the_geom,
		a.project_id as RFP_ID,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units,
		b.unique_project_id 	as HPD_Project_ID,
		b.total_units 			as HPD_Project_Total_Units,
		b.hpd_incremental_units as HPD_Project_Incremental_Units
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.hpd_deduped b
	on
	 		trim(split_part(b.hpd_rfp_ids,',',1)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',1)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',2)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',2)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',3)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',3)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',4)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',4)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',5)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',5)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',6)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',6)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',7)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',7)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',8)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',8)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',9)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',9)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',10)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',10)) 	<> '' 

	where
		a.source = 'HPD RFPs'
),

	hpd_rfp_deduped as
(
	select
		the_geom,
		RFP_ID,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		array_to_string(array_agg(HPD_Project_ID),', ')  							as HPD_Project_IDs,
		sum(HPD_Project_Incremental_Units) 											as HPD_Project_Incremental_Units,
		greatest(0,total_units - sum(coalesce(HPD_Project_Incremental_Units,0)))	as Incremental_HPD_RFP_Units
	from
		hpd_rfp_1 a
	group by
		a.the_geom,
		a.RFP_ID,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units
	order by
		cast(RFP_ID as integer)
)


select * from hpd_rfp_deduped


