/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from HPD RFPs
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to HPD RFPs using spatial and proximity matching.
2. If an HPD RFP maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
3. Eliminate inaccurate proximity-based matches.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	zap_hpd_rfp
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
				st_intersects(a.the_geom,b.the_geom)																				then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)															then 'Proximity'
			end																				as match_type,
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_project_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.hpd_rfp_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) and
		a.applicant_type <> 'Private' /*See diagnostics for logic of this restriction*/
) zap_hpd_rfp

/*Checking if there are any HPD RFPs which match to multiple ZAP projects. If so, preferencing spatial matches
  over proximity-based matches. If an RFP has more than one spatial match, manually assess these matches*/

select
	*
into
	multi_dcp_hpd_rfp_matches
from
(
		select
			hpd_rfp_id,
			sum(case when match_type = 'Spatial' 		then 1 else 0 end) 	as Spatial_Matches,
			sum(case when match_type = 'Proximity' 		then 1 else 0 end) 	as Proximity_Matches,
			count(*)														as total_matches,
			min(case when match_type = 'Proximity' then distance end)		as minimum_proximity_distance,
			min(case when match_type = 'Spatial' then abs(hpd_rfp_total_units - coalesce(total_units,0)) end) 		as min_unit_difference_spatial,
			min(case when match_type = 'Proximity' then abs(hpd_rfp_total_units - coalesce(total_units,0)) end)		as min_unit_difference_proximity			
		from
			zap_hpd_rfp
		where
			hpd_rfp_id is not null
		group by
			hpd_rfp_id
		having
			count(*) > 1
) multi_dcp_hpd_rfp_matches


/*4 HPD RFPs match with multiple ZAP projects. The HPD RFPs are:
22	Brownsville Site C - Livonia Sites
35	Slaughterhouse - EDC
36	SustaiNYC (E. 111th Street)
9	LIC Waterfront Mixed-Use Development

If there is a spatial match, all proximity-based matches are removed (confirmed to be inaccurate). If there are multiple spatial matches 
(Brownsville Site C overlaps with an unrelated Resilient Housing project spanning SI and BK, Slaughterhouse - EDC overlaps with both 
ZAP's specific Slaughterhouse project and Hudson Yards), the match with the closest unit count is taken. */

select
	*
into
	zap_hpd_rfps_1
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
		b.HPD_rfp_ID,
		b.hpd_rfp_project_name,
		b.hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
		b.distance
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
	/*Selecting the subset of zap_hpd_rfp which preferences specific matches if there are duplicates*/
		(
			select
				b.*
			from
				zap_hpd_rfp b
			left join
				multi_dcp_hpd_rfp_matches c
			on
				b.hpd_rfp_id = c.hpd_rfp_id and
				case 
					when c.Spatial_Matches 		>  1 then b.match_type <> 'Spatial' or abs(b.hpd_rfp_total_units - b.total_units) <> c.min_unit_difference_spatial /*Preferencing the closest spatial match by unit count*/ 
					when c.spatial_matches 		=  1 then b.match_type <> 'Spatial' /*If there is only one spatial match, eliminating proximity-based matches*/
					when c.Proximity_Matches	>= 1 then b.match_type is null /*Not omitting proximity-based matches if they are the only type of match for a particular HPD RFP,
																				because they will be manually researched in a later step*/
				end
			where
				c.hpd_rfp_id is null
		) b
	on
		a.project_id = b.project_id
) zap_hpd_rfps_1

/*Checking proximity matches. There are 0 matches by proximity. IF THERE ARE ANY PROXIMITY-BASED MATCHES REMAINING, create 
  lookup zap_hpd_rfps_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

select
	*
from
	zap_hpd_rfps_1
where
	match_type = 'Proximity' and
	total_units <> hpd_rfp_total_units
order by
	distance asc


/*IF THERE ARE ANY PROXIMITY-BASED MATCHES, CREATE DATASETS ZAP_HPD_RFPS_2_PRE AND ZAP_HPD_RFPS_2 TO: 
 Remove the inaccurate proximate matches by selecting the subset of all ZAP projects which are not inaccurately proximity-matched,
 and then place all matches back onto the original relevant projects list.*/

select
	*
into
	zap_hpd_rfps_2_pre
from
(
	select
		a.project_id as zap_project_id_temp
		a.match_type
		a.HPD_rfp_ID,
		a.HPD_rfp_Total_Units,
		a.HPD_rfp_Incremental_Units,
		a.distance
	from
		zap_hpd_rfps_1 a
	left join
		zap_hpd_rfps_proximate_matches_190529_v2 b
	on
		concat(a.project_id,a.hpd_rfp_id) = concat(b.project_id,b.hpd_rfp_id) and
		b.accurate_match = 0
	where
		b.project_id is null
) zap_hpd_rfps_2_pre

select
	*
into
	zap_hpd_rfps_2
from
(
	select
		a.*,
		b.*
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		zap_hpd_rfps_2_pre b
	on
		a.project_id = b.zap_project_id_temp
) zap_hpd_rfps_2


/*Aggregating matches by ZAP project*/

select
	*
into
	zap_hpd_rfps_final 
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
		array_to_string(array_agg(nullif(concat_ws(', ',hpd_rfp_id,nullif(hpd_rfp_project_name,'')),'')),' | ') 	as HPD_RFP_IDs,
		sum(HPD_RFP_Total_Units) 																					as HPD_RFP_Total_Units,		
		sum(HPD_RFP_Incremental_Units)		 																		as HPD_RFP_Incremental_Units
	from
		zap_hpd_rfps_1
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
) zap_hpd_rfps_final


/**********************************************************DIAGNOSTICS**************************************************************/

/*
	Of the 16 projects with matches, 1 (LIC Watefront) has no unit count. 7 have an exact unit count match. Another 1 is b/w 1-5 units apart.
	4 are > 50 units apart. 
*/

	select
		case
			when abs(total_units-HPD_rfp_TOTAL_UNITS) < 0 then '<0'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) <= 1 then '<=1'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) > 50 then '>50' end
															 	as ZAP_Units_minus_DOB_Units,
		count(*) as Count
	from 
		zap_hpd_rfps_final
	where
		hpd_rfp_ids <>'' and total_units is not null and HPD_rfp_TOTAL_UNITS is not null 
	group by 
		case
			when abs(total_units-HPD_rfp_TOTAL_UNITS) < 0 then '<0'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) <= 1 then '<=1'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-HPD_rfp_TOTAL_UNITS) > 50 then '>50' 
															end


/*Checking the matches with large unit count differences. There are 5 HPD RFP/ZAP matches with unit count difference > 50. All are accurate matches.*/

select
	*
from
	zap_hpd_rfps_final
where
	abs(total_units - HPD_rfp_Total_Units) > 50


/*Checking matches to projects where the applicant type is not 'Other Public Agency' or 'HPD'. The only match to a private applicant type is the P2013Q0443, 11-55 49th Avenue Rezoning
  matching with HPD RFP 10, 11-24 Jackson Ave (EDC). This is an inaccurate match, so we are omitting matches where the Applicant_Type is private. */

select
	*
from
	zap_hpd_rfps_final
where
	hpd_rfp_ids <> '' and
	applicant_type = 'Private'
