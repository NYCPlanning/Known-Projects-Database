/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate Neighborhood Study Rezoning Commitments from EDC
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match neighborhood study rezoning commitments to ZAP projects.
2. If a ZAP project maps to multiple neighborhood study projects, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
drop table if exists nstudy_zap;
drop table if exists multi_nstudy_zap_matches;
drop table if exists nstudy_zap_1_pre;
drop table if exists nstudy_zap_1;
drop table if exists nstudy_zap_final;


select
	*
into
	nstudy_zap
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
		b.status 		 																	as ZAP_Project_Status,
		b.project_description																as ZAP_Project_Description,
		b.project_brief 																	as ZAP_Project_Brief,
		b.applicant_type 																	as ZAP_Applicant_Type,
		b.total_units 																		as ZAP_Total_Units,
		b.zap_incremental_units 															as zap_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		capitalplanning.dep_ndf_by_site a
	left join
		capitalplanning.zap_deduped b
	on
		case
			when a.status = 'Rezoning Commitment' then 	st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
			else 										st_intersects(a.the_geom,b.the_geom) end
) nstudy_zap;	

/*Assessing whether any ZAP projects match with multiple rezoning commitments. Preferencing spatial matches over proximity matches. 
  THERE ARE NO ZAP PROJECTS MATCHING WITH MULTIPLE REZONING COMMITMENTS, but there are zap projects matching multiple times with various
  projected and potential sites. This is theoretically possible, given ZAP projects can span multiple lots and areas.*/


select
	*
into
	multi_nstudy_zap_matches
from
(
	select
		zap_project_id,
		sum(case when match_type = 'Spatial' 	then 1 else 0 end) 												as Spatial_Matches,
		sum(case when match_type = 'Proximity' 	then 1 else 0 end) 												as Proximity_Matches,
		count(*)																								as total_matches,
		min(case when match_type = 'Proximity' 	then 	distance end)											as minimum_proximity_distance,
		min(case when match_type = 'Spatial' 	then	abs(zap_total_units - coalesce(units,0)) end)			as min_unit_difference_spatial,
		min(case when match_type = 'Proximity' 	then 	abs(zap_total_units - coalesce(units,0)) end)			as min_unit_difference_proximity			
	from
		nstudy_zap
	where
		zap_project_id is not null
	group by
		zap_project_id
	having
		count(*)>1
) multi_nstudy_zap_matches;


/*Checking proximity matches. There are 2 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup nstudy_ZAP_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

  select
  	*
  from
   	nstudy_ZAP
  where
   	Match_Type = 'Proximity' and units <> ZAP_total_units
  order by
  	distance asc

/*Removing the inaccurate proximate matches by selecting the subset of all NStudy projects which are not inaccurately proximity-matched,
 and then placing all matches back onto the original relevant projects list. This would be done by creating the nstudy_zap_1_pre
 and nstudy_zap_1 datasets below.*/


select
	*
into
	nstudy_zap_1_pre
from
(
	select
		a.project_id as nstudy_project_id_temp,
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
		nstudy_zap a
	left join
		nstudy_zap_proximate_matches_190529_v2 b
	on
		concat(a.project_name,a.zap_project_id) = concat(b.neighborhood_study_project_name,b.zap_project_id) and
		b.accurate_match = 0
	where
		b.neighborhood_study_project_id is null
) nstudy_zap_1_pre;

select
	*
into
	nstudy_zap_1
from
(
	select
		a.*,
		b.*
	from
		capitalplanning.dep_ndf_by_site a
	left join
		nstudy_zap_1_pre b
	on
		a.project_id = b.nstudy_project_id_temp
	order by
		a.project_id asc
) nstudy_zap_1;


/*Aggregating the matches by neighborhood study project*/

select
	*
into
	nstudy_zap_final
from
(
	select
		the_geom,
		the_geom_webmercator,
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
		array_to_string(array_agg(nullif(concat_ws(', ',zap_project_id,nullif(zap_project_name,'')),'')),' | ') 	as zap_project_ids,
		sum(zap_total_units)																						as zap_total_units,
		sum(zap_incremental_units)																					as zap_incremental_units
	from
		nstudy_zap_1
	group by
		the_geom,
		the_geom_webmercator,
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
	order by
		project_id asc
) nstudy_zap_final;


/********************************************************************************DIAGNOSTICS***************************************************************************************/


/*
	Of the 4 projects with matches, all have an exact unit count match. 
*/

	select
		case
			when abs(units-ZAP_total_units) < 0 then '<0'
			when abs(units-ZAP_total_units) <= 1 then '<=1'
			when abs(units-ZAP_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(units-ZAP_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(units-ZAP_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(units-ZAP_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(units-ZAP_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(units-ZAP_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(units-ZAP_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(units-ZAP_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(units-ZAP_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-ZAP_total_units) > 50 then '>50' end
															 	as nstudy_Units_minus_hpd_Units,
		count(*) as Count
	from 
		nstudy_ZAP_FINAL
	where
		ZAP_PROJECT_IDS <>'' and units is not null 
	group by 
		case
			when abs(units-ZAP_total_units) < 0 then '<0'
			when abs(units-ZAP_total_units) <= 1 then '<=1'
			when abs(units-ZAP_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(units-ZAP_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(units-ZAP_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(units-ZAP_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(units-ZAP_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(units-ZAP_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(units-ZAP_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(units-ZAP_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(units-ZAP_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-ZAP_total_units) > 50 then '>50' 
															end


/*Approx. 1/3rd of East Harlem has materialized. 1 Jerome project has materialized in ZAP, and no projects for other rezonings have yet
  materialized.*/

select
	neighborhood,
	count(*) as project_count,
	sum(units) as unit_count,
	count(case when ZAP_project_ids <> '' then 1 end) as match_count,
	sum(ZAP_total_units) as matched_units
from
	nstudy_ZAP_FINAL
group by
	neighborhood