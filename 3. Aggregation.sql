/*Aggregating deduplicated data*/

select
	the_geom,
	the_geom_webmercator,
	'DOB' as source,
	concat(job_number) 	as project_id,
	address				as project_name_address,
	job_type			as dob_job_type,
	status				as status,
	borough,
	units_net			as total_units,
	null 				as deduplicated_units,
	null				as counted_units,
	'' 				as dob_matches,
	null				as dob_matched_units,
	''				as hpd_projected_closing_matches,
	null				as hpd_projected_closing_matched_units,
	''				as hpd_rfp_matches,
	null				as hpd_rfp_matched_units,
	''				as edc_matches,
	null				as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	dob_2018_sca_inputs_ms
union all
select
	the_geom,
	the_geom_webmercator,
	'HPD Projected Closings' as source,
	project_id 			as project_id,
	address				as project_name_address,
	''				as dob_job_type,
	'Projected'				as status,
	borough,
	total_units,
	hpd_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	''				as hpd_projected_closing_matches,
	null				as hpd_projected_closing_matched_units,
	''				as hpd_rfp_matches,
	null				as hpd_rfp_matched_units,
	''				as edc_matches,
	null				as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	hpd_deduped

/*
union all
select
	the_geom,
	the_geom_webmercator,
	'HPD RFPs' 			as source,
	concat(project_id) 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	status,
	borough,
	total_units,
	hpd_rfp_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	''				as hpd_rfp_matches,
	null				as hpd_rfp_matched_units,
	''				as edc_matches,
	null				as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	hpd_rfp_deduped
union all
select
	the_geom,
	the_geom_webmercator,
	'EDC' 				as source,
	project_id 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	'Projected'			as status,
	borough,
	total_units,
	edc_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	''				as edc_matches,
	null				as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	edc_deduped
union all
select
	the_geom,
	the_geom_webmercator,
	'DCP Applications'	as source,
	project_id 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	concat(project_status,'; ',process_stage)			as status,
	borough,
	total_units,
	zap_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	edc_project_ids											as edc_matches,
	edc_incremental_units									as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	zap_deduped
where
	project_id not like '%[ESD%'
union all
select
	the_geom,
	the_geom_webmercator,
	'Empire State Development'	as source,
	project_id 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	'Projected'			as status,
	borough,
	total_units,
	zap_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	edc_project_ids											as edc_matches,
	edc_incremental_units									as edc_matched_units,
	'' 				as dcp_application_matches,
	null				as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	zap_deduped
where
	project_id like '%[ESD%'
union all
select
	the_geom,
	the_geom_webmercator,
	'Neighborhood Study Rezoning Commitments'	as source,
	project_id 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	'Projected'			as status,
	borough,
	total_units,
	nstudy_incremental_units	as deduplicated_units,
	null				as counted_units,
	dob_job_numbers		as dob_matches,
	dob_units_net		as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	edc_project_ids											as edc_matches,
	edc_incremental_units									as edc_matched_units,
	zap_project_ids 										as dcp_application_matches,
	zap_incremental_units									as dcp_application_matched_units,
	''				as state_project_matches,
	null				as state_project_matched_units,
	''				as neighborhood_study_matches,
	null				as neighborhood_study_units,
	''				as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	nstudy_deduped
union all
select
	the_geom,
	the_geom_webmercator,
	'City Hall Public Sites'	as source,
	project_id 			as project_id,
	project_name		as project_name_address,
	''				as dob_job_type,
	'Projected'			as status,
	borough,
	total_units,
	public_sites_incremental_units	as deduplicated_units,
	null							as counted_units,
	dob_job_numbers					as dob_matches,
	dob_units_net					as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	edc_project_ids											as edc_matches,
	edc_incremental_units									as edc_matched_units,
	zap_project_ids 										as dcp_application_matches,
	zap_incremental_units									as dcp_application_matched_units,
	''									as state_project_matches,
	null									as state_project_matched_units,
	nstudy_project_ids						as neighborhood_study_matches,
	nstudy_incremental_units				as neighborhood_study_units,
	''									as public_sites_matches,
	null				as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	public_sites_deduped
union all
select
	the_geom,
	the_geom_webmercator,
	'DCP Planner-Added Projects'	as source,
	project_id 						as project_id,
	project_name					as project_name_address,
	''								as dob_job_type,
	'Potential'						as status,
	borough,
	total_units,
	planner_projects_incremental_units	as deduplicated_units,
	null							as counted_units,
	dob_job_numbers					as dob_matches,
	dob_units_net					as dob_matched_units,
	hpd_projected_closings_ids								as hpd_projected_closing_matches,
	hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
	hpd_rfp_ids												as hpd_rfp_matches,
	hpd_rfp_incremental_units								as hpd_rfp_matched_units,
	edc_project_ids											as edc_matches,
	edc_incremental_units									as edc_matched_units,
	zap_project_ids 										as dcp_application_matches,
	zap_incremental_units									as dcp_application_matched_units,
	''									as state_project_matches,
	null									as state_project_matched_units,
	nstudy_project_ids						as neighborhood_study_matches,
	nstudy_incremental_units				as neighborhood_study_units,
	public_sites_project_ids									as public_sites_matches,
	public_sites_incremental_units								as public_sites_units,
	''				as planner_projects_matches,
	null				as planner_projects_units
from
	planner_projects_deduped
*/