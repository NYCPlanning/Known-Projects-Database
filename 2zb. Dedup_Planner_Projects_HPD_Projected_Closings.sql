/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with HPD Projected Closings
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with HPD jobs.
2. If a DOB job maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
3. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_hpd_projected_closings
from
(
	select
		a.*,
		case
			when st_intersects(a.the_geom,b.the_geom)						then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 'Proximity'
																			end 			as Match_Type,
		b.project_id 																		as HPD_Project_ID,
		b.address																			as HPD_Address,
		b.bbl 																				as HPD_BBL,
		b.total_units 																		as HPD_Project_Total_Units,
		b.hpd_incremental_units 															as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.hpd_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	
	order by
		a.map_id asc 													 
) planner_projects_hpd_projected_closings


/*There are only two matches. Both are spatial and accurate based on a review of unit count, project name, and manual research.
  MAP ID: 65321 -- Chestnut Commons, HPD Projected Closing: 58780/986454, 76 Dinsmore Place
  MAP ID: 85408 -- T-Buildling, 	 HPD Projected Closing: 62299/985940, 72-41 Parsons Boulevard
*/

select
	*
into
	planner_projects_hpd_projected_closings_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_project_id,''),nullif(hpd_address,'')),'')),' | ') 	as HPD_Project_IDs,
		sum(HPD_Project_Total_Units) 																					as HPD_Project_Total_Units,
		sum(HPD_Project_Incremental_Units) 																				as HPD_Project_Incremental_Units
	from
		planner_projects_hpd_projected_closings
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
)	planner_projects_hpd_projected_closings_final	

	
