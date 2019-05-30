/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with City Hall Public Sites
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with City Hall Public Sites.
2. If City Hall Public Sites maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
3. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_public_sites
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
		b.project_id																		as public_sites_project_id,
		b.project_name																		as public_sites_project_name,
		b.total_units																		as public_sites_total_units,
		b.public_sites_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.public_sites_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	
	order by
		a.map_id asc 													 
)   planner_projects_public_sites


/************************************************************DIAGNOSTICS**********************************************************************************/

/*There is only 1 match, and it is proximity-based:
MAP ID 85424, Hunters Point Subdistrict B to Public Sites Pipeline 29, Hunters Point D+E.*/

select
	*
from
	planner_projects_public_sites
where
	match_type is not null

/*******************************************************************************************************************************************************/

/*Omitting proximity-based matches, because only one project is proximity-based and it is inaccurate*/


select
	*
into
	planner_projects_public_sites_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			end																				as match_type,
		b.project_id																		as public_sites_project_id,
		b.project_name																		as public_sites_project_name,
		b.total_units																		as public_sites_total_units,
		b.public_sites_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.public_sites_deduped b
	on 
		st_intersects(a.the_geom,b.the_geom)
	order by
		a.map_id asc 													 
)   planner_projects_public_sites_1


/*Aggregating matches*/


select
	*
into
	planner_projects_public_sites_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(public_sites_project_id,''),nullif(public_sites_project_name,'')),'')),' | ') 	as public_sites_project_ids,
		sum(public_sites_total_units)																											as public_sites_total_units,
		sum(public_sites_incremental_units)																										as public_sites_incremental_units
	from
		planner_projects_public_sites_1
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
) planner_projects_public_sites_final

