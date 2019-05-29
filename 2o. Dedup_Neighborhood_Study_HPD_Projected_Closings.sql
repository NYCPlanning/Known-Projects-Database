/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Neighborhood Study Rezoning Commitments data with HPD Projected Closings
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match neighborhood study rezoning commitments to HPD Projected Closings.
2. If a Projected Closing job maps to multiple neighborhood study projects, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	nstudy_hpd_projected_closings
from
(
	select
		a.*,
		case
			when position(b.bbl in a.included_bbls) > 0 					then 'BBL'
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
		(select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment') a
	left join
		capitalplanning.hpd_deduped b
	on
		position(b.bbl in a.included_bbls) > 0 						or
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		project_id asc
) nstudy_hpd_projected_closings

/*Assessing whether any HPD Projected Closings match with multiple rezoning commitments. Preferencing BBL matches over spatial matches over proximity matches. 
  THERE ARE NO HPD PROJECTED CLOSINGS MATCHING WITH MULTIPLE REZONING COMMITMENTS.*/

select
	*
into
	multi_nstudy_hpd_projected_closings_matches
from
(
	select
		HPD_PROJECT_ID,
		sum(case when match_type = 'BBL' 		then 1 else 0 end) 												as BBL_Matches,
		sum(case when match_type = 'Spatial' 	then 1 else 0 end) 												as Spatial_Matches,
		sum(case when match_type = 'Proximity' 	then 1 else 0 end) 												as Proximity_Matches,
		count(*)																								as total_matches,
		min(case when match_type = 'Proximity' 	then 	distance end)											as minimum_proximity_distance,
		min(case when match_type = 'BBL' 		then	abs(HPD_Project_Total_Units - coalesce(units,0)) end)	as min_unit_difference_BBL,
		min(case when match_type = 'Spatial' 	then	abs(HPD_Project_Total_Units - coalesce(units,0)) end)	as min_unit_difference_spatial,
		min(case when match_type = 'Proximity' 	then 	abs(HPD_Project_Total_Units - coalesce(units,0)) end)	as min_unit_difference_proximity			
	from
		nstudy_hpd_projected_closings
	where
		hpd_project_id is not null
	group by
		hpd_project_id
	having
		count(*)>1
) multi_nstudy_hpd_projected_closings_matches

/*Checking proximity matches. There are 0 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup nstudy_hpd_projected_closings_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

  select
  	*
  from
   	nstudy_hpd_projected_closings
  where
   	Match_Type = 'Proximity' and units <> hpd_project_total_units
  order by
  	distance asc


/*Aggregating HPD projected closings matches to neighborhood study projects*/

select
	*
into
	nstudy_hpd_projected_closings_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_project_id,''),nullif(hpd_address,'')),'')),' | ') 	as HPD_Project_IDs,
		sum(HPD_Project_Total_Units) 																					as HPD_Project_Total_Units,
		sum(HPD_Project_Incremental_Units) 																				as HPD_Project_Incremental_Units
	from
		nstudy_hpd_projected_closings
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
	order by
		project_id asc
)   nstudy_hpd_projected_closings_final

/******************************************************************DIAGNOSTICS***********************************************************/
/*
	Of the 5 projects with matches, 3 have an exact unit count match. 2 are > 50 units apart. 
*/

	select
		case
			when abs(units-hpd_project_total_units) < 0 then '<0'
			when abs(units-hpd_project_total_units) <= 1 then '<=1'
			when abs(units-hpd_project_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(units-hpd_project_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(units-hpd_project_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(units-hpd_project_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(units-hpd_project_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(units-hpd_project_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(units-hpd_project_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(units-hpd_project_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(units-hpd_project_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-hpd_project_total_units) > 50 then '>50' end
															 	as nstudy_Units_minus_HPD_Units,
		count(*) as Count
	from 
		nstudy_hpd_projected_closings_final
	where
		hpd_project_ids <>'' and units is not null and units is not null 
	group by 
		case
			when abs(units-hpd_project_total_units) < 0 then '<0'
			when abs(units-hpd_project_total_units) <= 1 then '<=1'
			when abs(units-hpd_project_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(units-hpd_project_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(units-hpd_project_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(units-hpd_project_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(units-hpd_project_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(units-hpd_project_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(units-hpd_project_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(units-hpd_project_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(units-hpd_project_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-hpd_project_total_units) > 50 then '>50' 
															end

/*Checking the matches with large unit count differences. The matches are for Phipps House and Sendero Verde, which are both expected to have future additional development.*/

select
	*
from
	nstudy_hpd_projected_closings_final
where
	abs(units - hpd_project_total_units) > 50



/*Approx. 1/3rd of East Harlem has materialized. 1/4 of Inwood projects, all DTFR projects, and 1/9th of East Harlem projects have materialized. No BSC or Jerome projects
  have materialized in HPD Projected Closings.*/

select
	neighborhood,
	count(*) as project_count,
	sum(units) as unit_count,
	count(case when hpd_project_ids <> '' then 1 end) as match_count,
	sum(hpd_project_total_units) as matched_units
from
	nstudy_hpd_projected_closings_final
group by
	neighborhood