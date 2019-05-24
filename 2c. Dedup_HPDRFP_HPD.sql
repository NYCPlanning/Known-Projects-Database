/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with HPD Projected Closings Data
Sources: 
*************************************************************************************************************************************************************************************/

/*********************************************
		RUN THE FOLLOWING QUERY IN
		CARTO BATCH
*********************************************/

select
	*
into
	HPDRFP_HPD_Match
from
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.lead_agency,
		a.borough,
		a.status,
		a.total_units,
		a.bbl,
		a.nycha_flag,
		a.gq_flag,
		a.senior_housing_flag,
		a.assisted_living_flag,
		a.likely_to_be_built_by_2025_flag,
		a.excluded_project_flag,
		a.rationale_for_exclusion,
		b.project_id 					as HPD_Project_ID,
		b.address						as HPD_Address,
		b.bbl 							as HPD_BBL,
		b.total_units 					as HPD_Project_Total_Units,
		b.hpd_incremental_units 		as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography) as Distance,
		case 
			when a.bbl = b.bbl and b.bbl not in('','0')							then 'BBL'
			when st_intersects(a.the_geom,b.the_geom)							then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)		then 'Proximity'
																				end as Match_Type		
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.HPD_Deduped b
	on
		(
			/*BBL-Match*/
			(a.bbl = b.bbl 	and b.bbl not in('','0')) or
			/*Spatial Match*/
			st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
		)
	where a.source = 'HPD RFPs'
) as HPDRFP_HPD_Match
									 
									 
/*****************************RUN THE FOLLOWING QUERY IN REGULAR CARTO************/
							


/*Export the following query as HPD_HPDRFP_Proximate_Matches. Identify
  whether the matches in this dataset are accurate by flagging Reimport as
  a lookup and omit inaccurate matches*/
							
/*THERE ARE NO PROXIMITY-BASED MATCHES*/

select
	*
from
	hpdrfp_hpd_match
where
	match_type = 'Proximity' and
	total_units <> HPD_Project_Total_Units
									 
/***************************RUN THE FOLLOWING QUERY IN CARTO BATCH****************/



select
	*
into
	HPDRFP_HPD_Match_1
FROM
(
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		lead_agency,
		borough,
		status,
		total_units,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		likely_to_be_built_by_2025_flag,
		excluded_project_flag,
		rationale_for_exclusion,
		case when concat(project_id,hpd_project_id)	in(select concat(hpd_rfp_id,hpd_projected_closings_id) from capitalplanning.hpd_rfp_hpd_proximate_matches_190523_v2 where accurate_match = 0) then null else hpd_project_id 				end as hpd_project_id,
		case when concat(project_id,hpd_project_id)	in(select concat(hpd_rfp_id,hpd_projected_closings_id) from capitalplanning.hpd_rfp_hpd_proximate_matches_190523_v2 where accurate_match = 0) then null else hpd_address 					end as hpd_address,
		case when concat(project_id,hpd_project_id)	in(select concat(hpd_rfp_id,hpd_projected_closings_id) from capitalplanning.hpd_rfp_hpd_proximate_matches_190523_v2 where accurate_match = 0) then null else hpd_project_total_units 		end as hpd_project_total_units,
		case when concat(project_id,hpd_project_id)	in(select concat(hpd_rfp_id,hpd_projected_closings_id) from capitalplanning.hpd_rfp_hpd_proximate_matches_190523_v2 where accurate_match = 0) then null else hpd_project_incremental_units 	end as hpd_project_incremental_units
	from
		capitalplanning.HPDRFP_HPD_Match
) as HPDRFP_HPD_Match_1
									 
select
	*
into						
	hpd_rfp_hpd_final
from									 
(
	select							
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		lead_agency,
		borough,
		status,
		total_units,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		likely_to_be_built_by_2025_flag,
		excluded_project_flag,
		rationale_for_exclusion,
		array_to_string(array_agg(nullif(concat_ws(', ',hpd_project_id,nullif(hpd_address,'')),'')),' | ') as hpd_projected_closings_ids,
		sum(hpd_project_total_units) 		as hpd_projected_closings_total_units,
		sum(hpd_project_incremental_units) 	as hpd_projected_closings_incremental_units
	from
		HPDRFP_HPD_Match_1
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		lead_agency,
		borough,
		status,
		total_units,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		likely_to_be_built_by_2025_flag,
		excluded_project_flag,
		rationale_for_exclusion
	order by
		project_id::numeric
) as hpd_rfp_hpd_final



/**************************************************************DIAGNOSTICS******************************************************************/


/*
14/37 projects have materialized in HPD projected closings data.
*/
SELECT
	COUNT(CASE WHEN hpd_projected_closings_ids <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_rfp_hpd_final

/*
Only projects without closed financing have materialized in the HPD Projected Closings data. All but one are already designated. 
*/
SELECT
	status,
	COUNT(CASE WHEN hpd_projected_closings_ids <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_rfp_hpd_final
group by
	status


/*Of the 14 materialized projects, 4 match exactly with their HPD unit count. 4 match between 1-5 units, 0 b/w 5-10, 0 b/w 10-15, 1 b/w 35-40, and 4 > 50.*/

	select
		case
			when abs(total_units-hpd_projected_closings_total_units) < 1 then '<1'
			when abs(total_units-hpd_projected_closings_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-hpd_projected_closings_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-hpd_projected_closings_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-hpd_projected_closings_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-hpd_projected_closings_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-hpd_projected_closings_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-hpd_projected_closings_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-hpd_projected_closings_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-hpd_projected_closings_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-hpd_projected_closings_total_units) > 50 then '>50' end
															 	as HPD_Units_minus_DOB_Units,
		count(*) as Count
	from 
		hpd_rfp_hpd_final
	where
		hpd_projected_closings_ids <> ''
	group by 
		case
			when abs(total_units-hpd_projected_closings_total_units) < 1 then '<1'
			when abs(total_units-hpd_projected_closings_total_units) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-hpd_projected_closings_total_units) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-hpd_projected_closings_total_units) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-hpd_projected_closings_total_units) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-hpd_projected_closings_total_units) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-hpd_projected_closings_total_units) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-hpd_projected_closings_total_units) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-hpd_projected_closings_total_units) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-hpd_projected_closings_total_units) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-hpd_projected_closings_total_units) > 50 then '>50' end

/*
	The >50 unit difference matches below are correct. They include a portion of Spofford, a portion of Lower Concourse, and NYCHA Harborview, which is now an excluded RFP project.
*/
SELECT
	*
from
	hpd_rfp_hpd_final
where
	hpd_projected_closings_ids <> '' and
	abs(total_units - hpd_projected_closings_total_units) > 50


									 
/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_rfp_deduped')
