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
	planner_projects_edc
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
		b.edc_project_id,
		b.project_name 											as edc_project_name,
		b.project_description 									as edc_project_description,
		b.comments_on_phasing									as edc_comments_on_phasing,
		b.build_year											as edc_build_year,
		b.total_units 											as edc_total_units,
		b.edc_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.edc_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	
	order by
		a.map_id asc 													 
) planner_projects_edc


/*	There is only one match, and it is proximity-based between MAP ID 85306, 456 Eastern Parkway & EDC 1, Bedford-Union Armory. 
	This match is inaccurate -- 456 Eastern Parkway is not an EDC project.
*/

select
	*
from
	planner_projects_edc
where
	match_type is not null


/*Omitting proximity-based matches*/

select
	*
into
	planner_projects_edc_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			end																				as match_type,
		b.edc_project_id,
		b.project_name 											as edc_project_name,
		b.project_description 									as edc_project_description,
		b.comments_on_phasing									as edc_comments_on_phasing,
		b.build_year											as edc_build_year,
		b.total_units 											as edc_total_units,
		b.edc_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.edc_deduped b
	on 
		st_intersects(a.the_geom,b.the_geom) 	
	order by
		a.map_id asc 													 
) planner_projects_edc


/*Aggregating*/

select
	*
into
	planner_projects_edc_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',edc_project_id,nullif(edc_project_name,'')),'')),' | ') 				as edc_project_ids,
		sum(edc_total_units) 																									as edc_total_units,
		sum(edc_incremental_units) 																								as edc_incremental_units
	from
		planner_projects_edc_1
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
)	planner_projects_edc_final	