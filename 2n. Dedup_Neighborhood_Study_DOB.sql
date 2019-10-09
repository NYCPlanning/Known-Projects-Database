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

NOTE: ALL NOTES PERTAIN TO RSEULTS FROM MATCHING TO NEIGHBORHOOD STUDY REZONING COMMITMENTS, NOT PROJECTED/POTENTIAL SITES
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
drop table if exists nstudy_dob;
drop table if exists multi_nstudy_dob_matches;
drop table if exists nstudy_dob_1_pre;
drop table if exists nstudy_dob_1;
drop table if exists nstudy_dob_final;

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
		capitalplanning.dep_ndf_by_site a
		-- (select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment') a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		(
			st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	and
			b.job_type <> 'Demolition' and
			case 
				when a.status <> 	'Rezoning Commitment' then st_intersects(a.the_geom,b.the_geom)
				else a.status = 	'Rezoning Commitment' end
		)
		or
		(
			a.project_name = '125th St MEC Center' and b.job_number = 121204464
			--This step matches the 125th St MEC Center with a development on the same block as part of the affordable housing commitment: 201 E 125th St. The
			--DOB project is part of the rezoning commitment, but is >50 meters away from the rezoning commitment.
		)

) nstudy_dob;

/*Assessing whether any DOB jobs match with multiple projects. Preferencing spatial matches over proximity matches. 
  THERE ARE NO DOB JOBS MATCHING WITH MULTIPLE NSTUDY PROJECTS.*/

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
		min(case when dob_match_type = 'Proximity' 	then dob_distance end)											as minimum_proximity_distance,
		min(case when dob_match_type = 'Spatial' 	then abs(dob_units_net - coalesce(units,0)) end)				as min_unit_difference_spatial,
		min(case when dob_match_type = 'Proximity' 	then abs(dob_units_net - coalesce(units,0)) end)				as min_unit_difference_proximity			
	from
		nstudy_dob
	where
		dob_job_number is not null
	group by
		dob_job_number
	having
		count(*)>1
) multi_nstudy_dob_matches;


/*Checking proximity matches. There are 5 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup nstudy_dob_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

/*  select
  	*
  from
   	nstudy_dob
  where
   	dob_Match_Type = 'Proximity' and units <> dob_units_net
  order by
  	dob_distance asc
*/

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
		(	
			b.neighborhood_study_id is null and
			not (a.dob_match_type = 'Proximity' and b.accurate_match is null and a.units <> a.dob_units_net) 	
		)																									or
		--Accomodating for additional accurate proximity matches
		(
			a.project_id = 'East Harlem Rezoning Commitment 4' and a.dob_job_number = 121204464
		) 																									or
		(
			a.project_id = 'East Harlem Rezoning Commitment 8' and a.dob_job_number = 121191432		
		)

) nstudy_dob_1_pre;

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
		capitalplanning.dep_ndf_by_site a
	left join
		nstudy_dob_1_pre b
	on
		a.project_id = b.nstudy_project_id_temp
) nstudy_dob_1;


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
	order by
		project_id asc
) nstudy_dob_final;

/******************************************************************DIAGNOSTICS***********************************************************/


/*
	Of the 5 projects with matches, 3 have an exact unit count match. 2 are > 50 units apart. 
*/

	select
		case
			when abs(units-dob_units_net) < 0 then '<0'
			when abs(units-dob_units_net) <= 1 then '<=1'
			when abs(units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-dob_units_net) > 50 then '>50' end
															 	as nstudy_Units_minus_DOB_Units,
		count(*) as Count
	from 
		nstudy_dob_final 
	where
		dob_job_numbers <>'' and units is not null and status = 'Rezoning Commitment' 
	group by 
		case
			when abs(units-dob_units_net) < 0 then '<0'
			when abs(units-dob_units_net) <= 1 then '<=1'
			when abs(units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(units-dob_units_net) > 50 then '>50' 
															end


/*Checking the matches with large unit count differences. The matches are for Phipps House and Sendero Verde, which are both expected to have future additional development.*/

select
	*
from
	nstudy_dob_final 
where
	abs(units - dob_units_net) > 50 and status = 'Rezoning Commitment'



/*Approx. 1/3rd of ENY and East Harlem have materialized. No other neighborhood study commitments have materialized. This makes sense -- ENY and East Harlem are the two
  oldest rezonings included*/

select
	neighborhood,
	count(*) as project_count,
	sum(units) as unit_count,
	count(case when dob_job_numbers <> '' then 1 end) as match_count,
	sum(dob_units_net) as matched_units
from
	nstudy_dob_final
where
	status = 'Rezoning Commitment'
group by
	neighborhood