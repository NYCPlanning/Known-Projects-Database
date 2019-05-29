/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from HPD RFPs
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to EDC projects using spatial and proximity matching.
2. If an EDC project maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
3. Eliminate inaccurate proximity-based matches.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	zap_edc
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
			end 												as match_type,
		b.edc_project_id,
		b.project_name 											as edc_project_name,
		b.project_description 									as edc_project_description,
		b.comments_on_phasing									as edc_comments_on_phasing,
		b.build_year											as edc_build_year,
		b.total_units 											as edc_total_units,
		b.edc_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.edc_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) and
		a.applicant_type <> 'Private' /*Only Private applicant type match is proximity-based b/w projects ZAP P2014K0494 and EDC 8. This match is confirmed inaccurate*/
) zap_edc

/*Checking if there are any EDC projects which match to multiple ZAP projects. If so, preferencing spatial matches
  over proximity-based matches. If an RFP has more than one spatial match, manually assess these matches*/

select
	*
into
	multi_dcp_edc_matches
from
(
		select
			edc_project_id,
			sum(case when match_type = 'Spatial' 		then 1 else 0 end) 	as Spatial_Matches,
			sum(case when match_type = 'Proximity' 		then 1 else 0 end) 	as Proximity_Matches,
			count(*)														as total_matches,
			min(case when match_type = 'Spatial' then abs(edc_total_units - coalesce(total_units,0)) end) 		as min_unit_difference_spatial,
			min(case when match_type = 'Proximity' then abs(edc_total_units - coalesce(total_units,0)) end)		as min_unit_difference_proximity			
		from
			zap_edc
		where
			edc_project_id is not null
		group by
			edc_project_id
		having
			count(*) > 1
) multi_dcp_edc_matches

/*0 EDC projects match with multiple ZAP projects.*/
/*REMOVE THE MATCHES BY THE PREFERENCING SYSTEM ABOVE*/

select
	*
into
	zap_edc_1
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
		b.edc_project_id,
		b.edc_project_name,
		b.edc_project_description,
		b.edc_comments_on_phasing,
		b.edc_total_units,
		b.edc_incremental_units,
		b.distance
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
	/*Selecting the subset of zap_edc which preferences specific matches if there are duplicates*/
		(
			select
				b.*
			from
				zap_edc b
			left join
				multi_dcp_edc_matches c
			on
				b.edc_project_id = c.edc_project_id and
				case 
					when c.Spatial_Matches 		>  1 then b.match_type <> 'Spatial' or abs(b.edc_total_units - b.total_units) <> c.min_unit_difference_spatial /*Preferencing the closest spatial match by unit count*/ 
					when c.spatial_matches 		=  1 then b.match_type <> 'Spatial' /*If there is only one spatial match, eliminating proximity-based matches*/
					when c.Proximity_Matches	>= 1 then b.match_type is null /*Not omitting proximity-based matches if they are the only type of match for a particular HPD RFP,
																				because they will be manually researched in a later step*/
				end
			where
				c.edc_project_id is null
		) b
	on
		a.project_id = b.project_id
) zap_edc_1

/*Checking proximity matches. There are 0 matches by proximity. IF THERE ARE ANY PROXIMITY-BASED MATCHES, create 
  lookup zap_edc_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

select
	*
from
	zap_edc_1
where
	match_type = 'Proximity' and
	total_units <> edc_total_units
order by
	distance asc


/*IF THERE ARE PROXIMITY-BASED MATCHES, do the following to create zap_edc_2_pre and zap_edc_2. Otherwise, move on to zap_edc_final.
 Removing the inaccurate proximate matches by selecting the subset of all ZAP projects which are not inaccurately proximity-matched,
 and then placing all matches back onto the original relevnat projects list.*/

select
	*
into
	zap_edc_2_pre
from
(
	select
		a.project_id as zap_project_id_temp
		a.match_type
		a.edc_project_id,
		a.edc_project_name,
		a.edc_project_description,
		a.edc_comments_on_phasing,
		a.edc_total_units,
		a.edc_incremental_units,
		a.distance
	from
		zap_edc_1 a
	left join
		zap_edc_proximate_matches_190529_v2 b
	on
		concat(a.project_id,a.edc_project_id) = concat(b.project_id,b.edc_project_id) and
		b.accurate_match = 0
	where
		b.project_id is null
) zap_edc_2_pre

select
	*
into
	zap_edc_2
from
(
	select
		a.*,
		b.*
	from
		relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		zap_edc_2_pre b
	on
		a.project_id = b.zap_project_id_temp
) zap_edc_2


/*Aggregating matches by ZAP project*/

select
	*
into
	zap_edc_final 
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
		array_to_string(array_agg(nullif(concat_ws(', ',edc_project_id,nullif(edc_project_name,'')),'')),' | ') 			as EDC_Project_IDs,
		sum(EDC_Total_Units) 																								as EDC_Total_Units,		
		sum(EDC_Incremental_Units)		 																					as EDC_Incremental_Units
	from
		zap_edc_1
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
) zap_edc_final


/**********************************************************DIAGNOSTICS**************************************************************/

/*
	Of the 4 projects with matches, 3 have an exact unit count match. Another 1 is b/w 35-40 units apart.  
*/

	select
		case
			when abs(total_units-edc_TOTAL_UNITS) < 0 then '<0'
			when abs(total_units-edc_TOTAL_UNITS) <= 1 then '<=1'
			when abs(total_units-edc_TOTAL_UNITS) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-edc_TOTAL_UNITS) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-edc_TOTAL_UNITS) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-edc_TOTAL_UNITS) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-edc_TOTAL_UNITS) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-edc_TOTAL_UNITS) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-edc_TOTAL_UNITS) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-edc_TOTAL_UNITS) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-edc_TOTAL_UNITS) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-edc_TOTAL_UNITS) > 50 then '>50' end
															 	as ZAP_Units_minus_DOB_Units,
		count(*) as Count
	from 
		zap_edc_final
	where
		edc_project_ids <>'' and total_units is not null and edc_TOTAL_UNITS is not null 
	group by 
		case
			when abs(total_units-edc_TOTAL_UNITS) < 0 then '<0'
			when abs(total_units-edc_TOTAL_UNITS) <= 1 then '<=1'
			when abs(total_units-edc_TOTAL_UNITS) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-edc_TOTAL_UNITS) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-edc_TOTAL_UNITS) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-edc_TOTAL_UNITS) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-edc_TOTAL_UNITS) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-edc_TOTAL_UNITS) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-edc_TOTAL_UNITS) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-edc_TOTAL_UNITS) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-edc_TOTAL_UNITS) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-edc_TOTAL_UNITS) > 50 then '>50' 
															end


/*Checking the matches with large unit count differences. Only match is for Spofford and is accurate. ZAP ID P2017X0037 and EDC ID 3*/

select
	*
from
	zap_edc_final
where
	abs(total_units - edc_Total_Units) > 35

/*Checking the portion of EDC projects which have materialized in ZAP -- are there any with early build years which have not yet materialized?
  STAPLETON PHASE I AND II HAVE NOT MATERIALIZED IN ZAP. THIS MAKES SENSE: PHASE II IS NOT YET IN PROGRESS, AND THE UNIT COUNT/GEOGRAPHY WE HAVE
  FOR PHASE I IS INCREMENTAL -- THE PORTION WHICH HAS NOT YET BEEN COMPLETED.*/

  select
  	*
  from
  	edc_deduped
  where
  	edc_project_id not in(select DISTINCT edc_project_id from zap_edc_1 WHERE EDC_PROJECT_ID IS NOT NULL) and build_year < 2025

 

