/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with ZAP
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with ZAP.
2. Omitting matches to Complete ZAP projects
2. If ZAP project maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
3. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_zap
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
		b.project_id 																		as ZAP_Project_ID,
		b.project_name 																		as ZAP_Project_Name,
		b.project_status 																	as ZAP_Project_Status,
		b.project_description																as ZAP_Project_Description,
		b.project_brief 																	as ZAP_Project_Brief,
		b.applicant_type 																	as ZAP_Applicant_Type,
		b.total_units 																		as ZAP_Total_Units,
		b.zap_incremental_units 															as zap_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.zap_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	
	order by
		a.map_id asc 													 
)   planner_projects_zap

/*****************************************************************DIAGNOSTICS******************************************************************************************/

/*There are 14 matches to Complete ZAP projects. All of these matches are inaccurate, including spatial matches. 
  Ex. Planner-Added Project West Cove has a slight overlap with ZAP Astoria Cove, but these are primarily distinct geographies and completely distinct projects.*/

  select
  	*
  from
  	planner_projects_zap
  where
  	zap_project_status = 'Complete'

/*There are 10 matches to incomplete ZAP projects. These matches are accurate when reviewing project names and geographies. Continuing to keep these matches*/

select
	*
from
	planner_projects_zap
where
	zap_project_status <> 'Complete'

/******************************************************************END OF DIAGNOSTICS***********************************************************************************/

/*Omitting matches to Complete ZAP projects*/

select
	*
into
	planner_projects_zap_1
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
		b.project_id 																		as ZAP_Project_ID,
		b.project_name 																		as ZAP_Project_Name,
		b.project_status 																	as ZAP_Project_Status,
		b.project_description																as ZAP_Project_Description,
		b.project_brief 																	as ZAP_Project_Brief,
		b.applicant_type 																	as ZAP_Applicant_Type,
		b.total_units 																		as ZAP_Total_Units,
		b.zap_incremental_units 															as zap_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.zap_deduped b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) and
		b.project_status <> 'Complete'
	order by
		a.map_id asc 													 
)   planner_projects_zap_1

/************************************************************DIAGNOSTICS**************************************************************/

/*Assessing whether any ZAP Projects match with multiple planner added projects. Preferencing spatial matches over proximity matches. 
  THERE ARE NO ZAP PROJECTS MATCHING WITH MULTIPLE PLANNER ADDED PROJECTS.*/


select
	*
into
	multi_planner_projects_zap_matches
from
(
	select
		zap_project_id,
		sum(case when match_type = 'Spatial' 	then 1 else 0 end) 												as Spatial_Matches,
		sum(case when match_type = 'Proximity' 	then 1 else 0 end) 												as Proximity_Matches,
		count(*)																								as total_matches,
		min(case when match_type = 'Proximity' 	then 	distance end)											as minimum_proximity_distance,
		min(case when match_type = 'Spatial' 	then	abs(ZAP_Total_Units - coalesce(total_units,0)) end)		as min_unit_difference_spatial,
		min(case when match_type = 'Proximity' 	then 	abs(ZAP_Total_Units - coalesce(total_units,0)) end)		as min_unit_difference_proximity			
	from
		planner_projects_zap_1
	where
		zap_project_id is not null
	group by
		zap_project_id
	having
		count(*)>1
) multi_planner_projects_zap_matches



/*Checking proximity matches. There are 4 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup planner_projects_zap_proximate_matches_190530_v2 with manual
  checks on the accuracy of each proximity match. */
select
	*
from
	planner_projects_zap_1
where
	match_type = 'Proximity'


/*Removing inaccurate proximity-based matches*/


select
	*
into
	planner_projects_zap_2_pre
from
(
	select
		a.map_id as planner_project_id_temp,
		a.match_type,
		a.ZAP_Project_ID,
		a.ZAP_Project_Name,
		a.ZAP_Project_Status,
		a.ZAP_Project_Description,
		a.ZAP_Project_Brief,
		a.ZAP_Applicant_Type,
		a.ZAP_Total_Units,
		a.zap_incremental_units,
		a.distance
	from
		planner_projects_zap_1 a
	left join
		planner_projects_zap_proximate_matches_190530_v2 b
	on
		concat(a.map_id,a.zap_project_id) = concat(b.planner_project_id,b.zap_project_id) and
		b.accurate_match = 0
	where
		b.zap_project_id is null
) planner_projects_zap_2_pre

select
	*
into
	planner_projects_zap_2
from
(
	select
		a.*,
		b.*
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		planner_projects_zap_2_pre b
	on
		a.map_id = b.planner_project_id_temp
) planner_projects_zap_2


/*Aggregate projects*/
select
	*
into
	planner_projects_zap_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(zap_project_id,''),nullif(zap_project_name,'')),'')),' | ') 	as zap_project_ids,
		sum(zap_total_units)																								as zap_total_units,
		sum(zap_incremental_units)																							as zap_incremental_units
	from
		planner_projects_zap_2
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
)	planner_projects_zap_final	

