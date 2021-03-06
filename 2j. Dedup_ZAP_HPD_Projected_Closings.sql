/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from HPD Projected Closings
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to HPD projects using Address, spatial, and proximity-based matching.
2. If an HPD job maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
3. Eliminate inaccurate proximity-based matches
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
drop table if exists zap_hpd_projected_closings;
drop table if exists multi_dcp_hpd_projected_closings_matches;
drop table if exists zap_hpd_projected_closings_1;
drop table if exists zap_hpd_projected_closings_2_pre;
drop table if exists zap_hpd_projected_closings_2;
drop table if exists zap_hpd_projected_closings_final;

select
	*
into
	zap_hpd_projected_closings
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		a.total_unit_source,
		a.ZAP_Unit_Source,
		a.applicant_type,
		a.project_status,
		a.previous_project_status,
		a.process_stage,
		a.previous_process_stage,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.Anticipated_year_built,
		a.remaining_units_likely_to_be_built_2018,
		a.rationale_2018,
		a.rationale_2019,
		a.phasing_notes_2019,
		a.additional_notes_2019,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.si_seat_cert,
		case 
			when
				position(upper(b.address) in upper(a.project_name)) > 0 and
				case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) END 	then 'Address' 
			when
				st_intersects(a.the_geom,b.the_geom)																				then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)															then 'Proximity'
																																	end as Match_Type,
		b.project_id 					as HPD_Project_ID,
		b.address						as HPD_Address,
		b.bbl 							as HPD_BBL,
		b.total_units 					as HPD_Project_Total_Units,
		b.hpd_incremental_units 		as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography)			as distance
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.hpd_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) or 
		(
			position(upper(b.address) in upper(a.project_name)) > 0 and
			case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) end
		)
) zap_hpd_projected_closings;



/*Assessing whether any HPD projected closings match with multiple ZAP projects. Preferencing matches by address,
then spatially, then by proximity. THERE ARE TWO HPD PROJECTED CLOSINGS MATCHING WITH MULTIPLE ZAP PROJECTS.*/

select
	*
into
	multi_dcp_hpd_projected_closings_matches
from
(
		select
			hpd_project_id,
			sum(case when match_type = 'Address' 		then 1 else 0 end) 	as Address_Matches,
			sum(case when match_type = 'Spatial' 		then 1 else 0 end) 	as Spatial_Matches,
			sum(case when match_type = 'Proximity' 		then 1 else 0 end) 	as Proximity_Matches,
			count(*)														as total_matches,
			min(case when match_type = 'Proximity' 	then distance end)		as minimum_proximity_distance,
			min(case when match_type = 'Address' 	then abs(HPD_Project_Total_Units - coalesce(total_units,0)) end) 	as min_unit_difference_address,
			min(case when match_type = 'Spatial' 	then abs(HPD_Project_Total_Units - coalesce(total_units,0)) end) 	as min_unit_difference_spatial,
			min(case when match_type = 'Proximity' 	then abs(HPD_Project_Total_Units - coalesce(total_units,0)) end) 	as min_unit_difference_proximity
		from
			zap_hpd_projected_closings
		where
			hpd_project_id is not null
		group by
			hpd_project_id
		having
			count(*) > 1
) multi_dcp_hpd_projected_closings_matches;


/*2 HPD Projected Closings match with multiple ZAP projects.*/
/*REMOVE THE MATCHES BY THE PREFERENCING SYSTEM ABOVE*/

select
	*
into
	zap_hpd_projected_closings_1
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		a.total_unit_source,
		a.ZAP_Unit_Source,
		a.applicant_type,
		a.project_status,
		a.previous_project_status,
		a.process_stage,
		a.previous_process_stage,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.Anticipated_year_built,
		a.remaining_units_likely_to_be_built_2018,
		a.rationale_2018,
		a.rationale_2019,
		a.phasing_notes_2019,
		a.additional_notes_2019,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.si_seat_cert,
		b.match_type,
		b.HPD_Project_ID,
		b.HPD_Address,
		b.HPD_BBL,
		b.HPD_Project_Total_Units,
		b.HPD_Project_Incremental_Units,
		b.distance
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
	/*Selecting the subset of zap_hpd_projected_closings which preferences specific matches if there are duplicates*/
		(
			select
				b.*
			from
				zap_hpd_projected_closings b
			left join
				multi_dcp_hpd_projected_closings_matches c
			on
				b.hpd_project_id = c.hpd_project_id and
				case 
					when c.address_matches 		>= 1 then b.match_type <> 'Address'
					when c.spatial_matches 		>= 1 then b.match_type <> 'Spatial'
					when c.Proximity_Matches	>= 1 then b.match_type is null /*Not omitting proximity-based matches if they are the only type of match,
																				because they will be manually researched in a later step*/
					end
			where
				c.hpd_project_id is null
		) b
	on
		a.project_id = b.project_id
) zap_hpd_projected_closings_1;

/*Checking proximity matches. There are 0 matches by proximity. 
  If there are >0 proximity-based matches, create 
  lookup zap_hpd_projected_closings_proximate_matches_190603_v3 with manual
  checks on the accuracy of each proximity match. */

  select
  	*
  from
   	zap_hpd_projected_closings_1
  where
   	Match_Type = 'Proximity' and total_units <> HPD_Project_Total_Units
  order by
  	distance asc


/*Removing the inaccurate proximate matches by selecting the subet of all ZAP projects which are not inaccurately proximity-matched,
 and then placing all matches back onto the original relevnat projects list. This would be done by creating the zap_hpd_projected_closings_2_pre
 and zap_hpd_projected_closings_2 datasets below.*/

select
	*
into
	zap_hpd_projected_closings_2_pre
from
(
	select
		a.project_id as zap_project_id_temp,
		a.match_type,
		a.HPD_Project_ID,
		a.HPD_Address,
		a.HPD_BBL,
		a.HPD_Project_Total_Units,
		a.HPD_Project_Incremental_Units,
		a.distance
	from
		zap_hpd_projected_closings_1 a
	left join
		zap_hpd_projected_closings_proximate_matches_190603_v3 b
	on
		concat(a.project_id,a.hpd_project_id) = concat(b.zap_project_id,b.hpd_projected_closing_id) and
		b.accurate_match = 0
	where
		b.zap_project_id is null
) zap_hpd_projected_closings_2_pre;

select
	*
into
	zap_hpd_projected_closings_2
from
(
	select
		a.*,
		b.*
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		zap_hpd_projected_closings_2_pre b
	on
		a.project_id = b.zap_project_id_temp
) zap_hpd_projected_closings_2;

select
	*
into
	zap_hpd_projected_closings_final 
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		total_units,
		total_unit_source,
		ZAP_Unit_Source,
		applicant_type,
		project_status,
		previous_project_status,
		process_stage,
		previous_process_stage,
		dcp_target_certification_date,
		certified_referred,
		project_completed,
		Anticipated_year_built,
		remaining_units_likely_to_be_built_2018,
		rationale_2018,
		rationale_2019,
		phasing_notes_2019,
		additional_notes_2019,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		si_seat_cert,
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_project_id,''),nullif(hpd_address,'')),'')),' | ') 	as HPD_Project_IDs,
		sum(HPD_Project_Total_Units) 																					as HPD_Project_Total_Units,		
		sum(HPD_Project_Incremental_Units) 																				as HPD_Project_Incremental_Units
	from
		zap_hpd_projected_closings_2
	-- where
		-- project_id <> 85321 Eliminating inaccurate matches to Chestnut Commons future site -- the matched project has been zeroed out by another DOB job
		-- 						matching to DOB job 321384177, which does not match to Chestnut Commons. 
	group by
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		total_units,
		total_unit_source,
		ZAP_Unit_Source,
		applicant_type,
		project_status,
		previous_project_status,
		process_stage,
		previous_process_stage,
		dcp_target_certification_date,
		certified_referred,
		project_completed,
		Anticipated_year_built,
		remaining_units_likely_to_be_built_2018,
		rationale_2018,
		rationale_2019,
		phasing_notes_2019,
		additional_notes_2019,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		si_seat_cert
) zap_hpd_projected_closings_final;


/**********************************************************DIAGNOSTICS**************************************************************/

/*
	Of the 38 projects with matches, 16 have an exact unit count match. Another 2 are b/w 1-5 units apart, and 2 are b/w 5-10 units apart.
	10 are > 50 units apart. 
*/

	select
		case
			when abs(total_units-HPD_PROJECT_TOTAL_units) < 0 then '<0'
			when abs(total_units-HPD_PROJECT_TOTAL_units) <= 1 then '<=1'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-HPD_PROJECT_TOTAL_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-HPD_PROJECT_TOTAL_units) > 50 then '>50' end
															 	as ZAP_Units_minus_DOB_Units,
		count(*) as Count
	from 
		zap_hpd_projected_closings_final
	where
		hpd_project_ids <>'' and total_units is not null and HPD_PROJECT_TOTAL_units is not null 
	group by 
		case
			when abs(total_units-HPD_PROJECT_TOTAL_units) < 0 then '<0'
			when abs(total_units-HPD_PROJECT_TOTAL_units) <= 1 then '<=1'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-HPD_PROJECT_TOTAL_units) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-HPD_PROJECT_TOTAL_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-HPD_PROJECT_TOTAL_units) > 50 then '>50' 
															end


/*Checking the matches with large unit count differences. The matches are all for larger rezonings which indicate multi-building projects
  and for which it would make sense that HPD projected closings would not encompass the entire project. The matches are the following ZAP projects:
P2012K0041	RHEINGOLD REZONING
P2012Q0062	HALLETTS POINT
P2012X0215	SOUNDVIEW PARTNERS APARTMENTS
P2015K0526	Ebenezer Plaza Rezoning
P2015M0454	207th Street Rezoning North Cove at Sherman Creek
P2016K0159	Linden Boulevard rezoning
P2016M0199	Ennis Francis Houses LSRD
P2016X0409	Lower Concourse North Rezoning
P2017M0259	Sendero Verde East 111th Street
P2017X0037	Spofford Campus Redevelopment LSGD
*/

select
	*
from
	zap_hpd_projected_closings_final
where
	abs(total_units - HPD_Project_Total_Units) > 50


/*
	Checking HPD Projected Closings matches to ZAP projects which are much larger. There are 31 projects where this is the case. All seem accurate because they are either:
	1. large, multi-building rezonings
	2. confirmed to be the same building
	3. confirmed to be the same lot
*/

	select * from zap_hpd_projected_closings_final where HPD_Project_Total_Units < .5*total_units::float