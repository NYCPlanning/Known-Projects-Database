/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with Neighborhood Study Commitments
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with Neighborhood Study Commitments.
2. If Neighborhood Study Commitments maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
3. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_nstudy
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
		b.project_id 																		as nstudy_project_id,
		b.project_name																		as nstudy_project_name,
		b.total_units																		as nstudy_units,
		b.nstudy_incremental_units,
		b.planner_input																		as nstudy_planner_input,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.nstudy_deduped b
	on 
	case
		when b.status = 'Rezoning Commitment' then 	st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20)
		else										st_intersects(a.the_geom,b.the_geom) end
		 	
	order by
		a.map_id asc 													 
)   planner_projects_nstudy



/************************************************************DIAGNOSTICS*******************************************************/
/*There are 2 matches.
MAP ID 85321, Chestnut Commons, 	to East New York Rezoning Commitment 1, Dinsmore-Chestnut. 	This match is spatial and accurate.
MAP ID 85326, Grace Baptist Church, to East New York Rezoning Commitment 3, 247 Vermont St. 	This match is proximity-based and inaccurate based on unit count, project information, and distance.

The next step will omit proximity-based matches, given that there is only 1 and it is inaccurate

*/


select
	*
into
	planner_projects_nstudy_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			end																				as match_type,
		b.project_id 																		as nstudy_project_id,
		b.project_name																		as nstudy_project_name,
		b.total_units																		as nstudy_units,
		b.nstudy_incremental_units,
		b.planner_input																		as nstudy_planner_input,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.nstudy_deduped b
	on 
		st_intersects(a.the_geom,b.the_geom)
	order by
		a.map_id asc 													 
)   planner_projects_nstudy


/*Aggregate projects*/

select
	*
into
	planner_projects_nstudy_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(nstudy_project_id,''),nullif(nstudy_project_name,'')),'')),' | ') 	as nstudy_project_ids,
		sum(nstudy_units)																											as nstudy_total_units,
		sum(nstudy_incremental_units)																								as nstudy_incremental_units
	from
		planner_projects_nstudy_1
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
) planner_projects_nstudy_final

