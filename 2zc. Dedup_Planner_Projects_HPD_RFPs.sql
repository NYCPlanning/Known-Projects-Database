/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with HPD RFPs
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with HPD RFP jobs.
2. If an RFP  maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
3. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_hpd_rfps
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)					then 'Proximity'
			end																				as match_type,
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.hpd_rfp_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	
	order by
		a.map_id asc 													 
) planner_projects_hpd_rfps

/*****************************************************************DIAGNOSTICS******************************************************/

/*
Two accurate spatial matches:
MAP ID 55321, Chestnut Commons to HPD RFP 12, Dinsmore-Chestnut
MAP ID 85420, LIC Waterfront to HPD RFP 9, LIC Waterfront
*/

/*
There is one proximity-based match, and it is inaccurate. See: https://newyorkyimby.com/2015/03/boerum-hill-shell-gas-station-wants-to-become-apartments-98-third-avenue.html
MAP ID 85335, 98 3 Avenue to HPD RFP 32: NYCHA Wyckoff
*/

select
	*
from
	planner_projects_hpd_rfps
where
	match_type is not null


/**************************************************************END OF DIAGNOSTICS*******************************************/

/*Limiting to spatial matches*/

select
	*
into
	planner_projects_hpd_rfps_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)					then 'Proximity'
			end																				as match_type,
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.hpd_rfp_deduped b
	on 
		st_intersects(a.the_geom,b.the_geom)
	order by
		a.map_id asc 													 
) planner_projects_hpd_rfps_1


/*Creating aggregate matches*/

select
	*
into
	planner_projects_hpd_rfps_final
from
(
	select
		map_id,
		project_name,
		boro as borough,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input,
		array_to_string(array_agg(nullif(concat_ws(', ',hpd_rfp_id,nullif(hpd_rfp_name,'')),'')),' | ') 			as HPD_RFP_IDs,
		sum(HPD_RFP_Total_Units) 																					as HPD_RFP_Total_Units,
		sum(HPD_RFP_Incremental_Units) 																				as HPD_RFP_Incremental_Units
	from
		planner_projects_hpd_rfps_1
	where
		map_id <> 85321 /*Eliminating inaccurate matches to Chestnut Commons future site -- the matched project has been zeroed out by another DOB job
								matching to DOB job 321384177, which does not match to Chestnut Commons*/ 
	group by
		map_id,
		project_name,
		boro,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input
	order by 
		map_id asc
)	planner_projects_hpd_rfps_final	
