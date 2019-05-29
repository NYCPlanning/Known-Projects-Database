/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Neighborhood Study Rezoning Commitments data with DOB
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match neighborhood study rezoning commitments DOB jobs.
2. If a DOB job maps to multiple neighborhood study projects, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	nstudy_dob
from
(
	select
		a.*,
		case
			/*No additional matches were found using BBL*/
			when st_intersects(a.the_geom,b.the_Geom)				then 'Spatial'
			when b.job_number is not null							then 'Proximity' end 	as DOB_Match_Type,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as DOB_Distance,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status
	from
		(select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment') a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	and
		b.job_type <> 'Demolition'													 
) nstudy_dob


/*Assessing whether any DOB jobs match with multiple rezoning commitments. Preferencing spatial matches over proximity matches. 
  THERE ARE NO DOB JOBS MATCHING WITH MULTIPLE REZONING COMMITMENTS.*/

select
	*
into
	multi_nstudy_dob_matches
from
(
	select
		dob_job_number,
		sum(case when dob_match_type = 'Spatial' 		then 1 else 0 end) 											as Spatial_Matches,
		sum(case when dob_match_type = 'Proximity' 		then 1 else 0 end) 											as Proximity_Matches,
		count(*)																									as total_matches,
		min(case when dob_match_type = 'Proximity' then dob_distance end)											as minimum_proximity_distance,
		min(case when dob_match_type = 'Spatial' then abs(dob_units_net - coalesce(units,0)) end) 					as min_unit_difference_spatial,
		min(case when dob_match_type = 'Proximity' then abs(dob_units_net - coalesce(units,0)) end)					as min_unit_difference_proximity			
	from
		nstudy_dob
	where
		dob_job_number is not null
	group by
		dob_job_number
	having
		count(*)>1
) multi_nstudy_dob_matches


/*Checking proximity matches. There are 5 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup nstudy_dob_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

  select
  	*
  from
   	nstudy_dob
  where
   	dob_Match_Type = 'Proximity' and units <> dob_units_net
  order by
  	dob_distance asc


/*Removing the inaccurate proximate matches by selecting the subset of all NStudy projects which are not inaccurately proximity-matched,
 and then placing all matches back onto the original relevant projects list. This would be done by creating the nstudy_dob_1_pre
 and nstudy_dob_1 datasets below.*/

select
	*
into
	nstudy_dob_1_pre
from
(
	select
		a.project_id as nstudy_project_id_temp,
		a.dob_match_type,
		a.dob_job_number,
		a.dob_address,
		a.dob_job_type,
		a.dob_units_net,
		a.dob_status,
		a.dob_distance
	from
		nstudy_dob a
	left join
		nstudy_dob_proximate_matches_190529_v2 b
	on
		concat(a.project_id,a.dob_job_number) = concat(b.neighborhood_study_id,b.dob_job_number) and
		b.accurate_match = 0
	where
		b.neighborhood_study_id is null
) nstudy_dob_1_pre

select
	*
into
	nstudy_dob_1
from
(
	select
		a.*,
		b.*
	from
		(select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment') a
	left join
		nstudy_dob_1_pre b
	on
		a.project_id = b.nstudy_project_id_temp
) nstudy_dob_1


/*Aggregating the matches by neighborhood study project*/

select
	*
into
	nstudy_dob_final
from
(
	select
		the_geom,
		the_geom_webmercator,
		cartodb_id,
		project_id,
		project_name,
		neighborhood,
		status,
		units,
		included_bbls,
		nycha_flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		planner_input,
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') 	as dob_job_numbers,
		sum(dob_units_net) 																					as dob_units_net
	from
		nstudy_dob_1
	group by
		the_geom,
		the_geom_webmercator,
		cartodb_id,
		project_id,
		project_name,
		neighborhood,
		status,
		units,
		included_bbls,
		nycha_flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		planner_input
) nstudy_dob_final
