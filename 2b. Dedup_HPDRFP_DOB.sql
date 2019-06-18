/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with DOB Project Data
Sources: 
*************************************************************************************************************************************************************************************/

/*******************************************************
		RUN IN CARTO BATCH
*******************************************************/
drop table if exists hpd_rfp_dob;
drop table if exists hpd_rfp_dob_1;
drop table if exists hpd_rfp_dob_final;


/*Matching HPD RFPs to DOB jobs by address, BBL, and spatially*/
select
	*
into
	hpd_rfp_dob
from
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id 							as project_id,
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
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status,
		st_distance(a.the_geom::geography,b.the_geom::geography) 	as DOB_Distance,
		case 
			when position(concat(b.bbl) in a.bbl)>0 and b.bbl is not null and b.bbl<>0 then 'BBL'
			when concat(b.bbl) = a.bbl and b.bbl is not null and b.bbl<>0 then				'BBL'
			when st_intersects(a.the_geom,b.the_geom) then 									'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 			'Proximity' end as DOB_Match_Type
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on
		b.job_type = 'New Building' and 
		b.status <> 'Complete' and /*3 Complete matches and all are inaccurate, projects cannot be completed before financing is closed.*/
		(
		(
			b.bbl is not null and b.bbl<>0 and
				(
					concat(b.bbl) = a.bbl or
					position(concat(b.bbl) in a.bbl) > 0
				)
		) 																		or
		(
			st_intersects(a.the_geom,b.the_geom)
		)																		or
		(
			st_dwithin(a.the_geom::geography,b.the_geom::geography,20) and
			case
				when a.total_units > 10 then abs(a.total_units - b.units_net)::float/a.total_units::float 	<= .5
				else 						 abs(a.total_units - b.units_net) 								<=  5 end
		)
		)
	where
		a.source = 'HPD RFPs' 
) as hpd_rfp_dob;





/*******************************************************
		RUN IN REGULAR CARTO
*******************************************************/

/*Diagnostic: No DOB jobs match to multiple RFPs*/

select dob_job_number from hpd_rfp_dob group by dob_job_number having count(*)>1

/*Filter hpd_rfp_dob for only proximity matches that don't match in unit count.
  Manually examine these matches to assess whether they are accurate.
  CREATE A LOOKUP AND REUPLOAD IT as hpd_rfp_dob_proximate_matches_190523_v2 */

select
	*
from
	hpd_rfp_dob
where
	DOB_Match_Type = 'Proximity' and
	dob_units_net <> total_units


/*******************************************************
		RUN IN CARTO BATCH
*******************************************************/
/*Filter out inaccurate matches from the above created lookup*/

select
	*
into
	hpd_rfp_dob_1
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
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_job_number end as dob_job_number,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_units_net  end as dob_units_net,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_address	 end as dob_address,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_job_type   end as dob_job_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_status 	 end as dob_status,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_match_type end as dob_match_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_rfp_dob_proximate_matches_190523_v2 where accurate_match = 0) then null else dob_distance   end as dob_distance
	from
		hpd_rfp_dob
) x;

select
	*
into
	hpd_rfp_dob_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') as dob_job_numbers,
		sum(dob_units_net) as dob_units_net
	from
		hpd_rfp_dob_1
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
) x;


/*
8/37 projects have materialized in DOB data.
*/
SELECT
	COUNT(CASE WHEN dob_job_numbers <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_rfp_dob_final

/*
No non-designated RFPs have materialized. All the RFPs with closed financing have materialized, and 7/30 designated non-closed RFPs have materialized. 
*/
SELECT
	status,
	COUNT(CASE WHEN dob_job_numbers <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_rfp_dob_final
group by
	status


/*Of the 8 materialized jobs, 1 matches exactly with its DOB unit count. 3 match between 1-5 units, 0 b/w 5-10, 0 b/w 10-15, 2 b/w 35-40, and 2 > 50.*/

	select
		case
			when abs(total_units-dob_units_net) < 1 then '<1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' end
															 	as HPD_Units_minus_DOB_Units,
		count(*) as Count
	from 
		hpd_rfp_dob_final
	where
		dob_job_numbers <> ''
	group by 
		case
			when abs(total_units-dob_units_net) < 1 then '<1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' end

/*
	The >50 unit difference matches below are correct. They are for portions of SustaiNYC and the first ground-break of Spofford.
*/
SELECT
	*
from
	hpd_rfp_dob_final
where
	dob_job_numbers <> '' and
	abs(total_units - dob_units_net) > 50


/*
	Checking if any excessively small DOB jobs have matched with HPD RFPs.
	There is 1 project that has materialized with fewer than 50% of its expected units. This is project ID 33--the first phase of Spofford
	and is correct.
*/ 

select * from hpd_rfp_dob_final where dob_units_net < total_units::float*.5 












