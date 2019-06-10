/*Aggregating deduplicated data*/

create table
	known_projects_db_20190610_v4
as
(
	select
		the_geom,
		the_geom_webmercator,
		'DOB' as source,
		concat(job_number) 			as project_id,
		address						as project_name_address,
		job_type					as dob_job_type,
		status						as status,
		borough,
		units_net					as total_units,
		units_net 					as deduplicated_units,
		units_net					as counted_units,
		null::numeric				as portion_built_2025,
		null::numeric				as portion_built_2035,
		null::numeric				as portion_built_2055,
		''							as planner_input,
		'' 							as dob_matches,
		null::numeric				as dob_matched_units,
		''							as hpd_projected_closing_matches,
		null::numeric				as hpd_projected_closing_matched_units,
		''							as hpd_rfp_matches,
		null::numeric				as hpd_rfp_matched_units,
		''							as edc_matches,
		null::numeric				as edc_matched_units,
		'' 							as dcp_application_matches,
		null::numeric				as dcp_application_matched_units,
		''							as state_project_matches,
		null::numeric				as state_project_matched_units,
		''							as neighborhood_study_matches,
		null::numeric				as neighborhood_study_units,
		''							as public_sites_matches,
		null::numeric				as public_sites_units,
		''							as planner_projects_matches,
		null::numeric				as planner_projects_units,
		'' 							as matches_to_nstudy_projected,
		null::numeric				as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		dob_2018_sca_inputs_ms
	where
		status not in('Complete','Complete (demolition)')
	union all
	select
		the_geom,
		the_geom_webmercator,
		'HPD Projected Closings' 	as source,
		project_id 					as project_id,
		address						as project_name_address,
		''							as dob_job_type,
		'Projected'					as status,
		borough,
		total_units,
		hpd_incremental_units 		as deduplicated_units,
		case
			when total_units::float*.2>hpd_incremental_units 	then 0
			when hpd_incremental_units<=2						then 0
			else hpd_incremental_units end				as counted_units,
		null::numeric				as portion_built_2025,
		null::numeric				as portion_built_2035,
		null::numeric				as portion_built_2055,
		''							as planner_input,
		dob_job_numbers				as dob_matches,
		dob_units_net				as dob_matched_units,
		''							as hpd_projected_closing_matches,
		null::numeric				as hpd_projected_closing_matched_units,
		''							as hpd_rfp_matches,
		null::numeric				as hpd_rfp_matched_units,
		''							as edc_matches,
		null::numeric				as edc_matched_units,
		'' 							as dcp_application_matches,
		null::numeric				as dcp_application_matched_units,
		''							as state_project_matches,
		null::numeric				as state_project_matched_units,
		''							as neighborhood_study_matches,
		null::numeric				as neighborhood_study_units,
		''							as public_sites_matches,
		null::numeric				as public_sites_units,
		''							as planner_projects_matches,
		null::numeric				as planner_projects_units,
		'' 							as matches_to_nstudy_projected,
		null::numeric				as units_matched_to_nstudy_projected,
		null as nycha_flag,
		null as gq_flag,
		null as senior_housing_flag,
		null as assisted_living_flag,
		hpd_children_unlikely_flag
	from
		hpd_deduped


	union all
	select
		the_geom,
		the_geom_webmercator,
		'HPD RFPs' 					as source,
		concat(project_id) 			as project_id,
		project_name				as project_name_address,
		''							as dob_job_type,
		status,
		borough,
		total_units,
		hpd_rfp_incremental_units	as deduplicated_units,
		case
			when total_units::float*.2>hpd_rfp_incremental_units 		then 0
			when hpd_rfp_incremental_units<=2							then 0
			else hpd_rfp_incremental_units end							as counted_units,
		null::numeric				as portion_built_2025,
		null::numeric				as portion_built_2035,
		null::numeric				as portion_built_2055,
		''							as planner_input,
		dob_job_numbers				as dob_matches,
		dob_units_net				as dob_matched_units,
		hpd_projected_closings_ids								as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
		''							as hpd_rfp_matches,
		null::numeric						as hpd_rfp_matched_units,
		''							as edc_matches,
		null::numeric						as edc_matched_units,
		'' 							as dcp_application_matches,
		null::numeric						as dcp_application_matched_units,
		''							as state_project_matches,
		null::numeric						as state_project_matched_units,
		''							as neighborhood_study_matches,
		null::numeric						as neighborhood_study_units,
		''							as public_sites_matches,
		null::numeric						as public_sites_units,
		''							as planner_projects_matches,
		null::numeric						as planner_projects_units,
		'' 														as matches_to_nstudy_projected,
		null::numeric											as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		hpd_rfp_deduped
	union all
	select
		the_geom,
		the_geom_webmercator,
		'EDC Projected Projects'								as source,
		concat(project_id) 										as project_id,
		project_name											as project_name_address,
		''														as dob_job_type,
		'Projected'												as status,
		borough,
		total_units,
		edc_incremental_units									as deduplicated_units,
		case
			when total_units::float*.2>edc_incremental_units 			then 0
			when edc_incremental_units<=2								then 0
			else edc_incremental_units end								as counted_units,
		null::numeric											as portion_built_2025,
		null::numeric											as portion_built_2035,
		null::numeric											as portion_built_2055,
		''														as planner_input,
		dob_job_numbers											as dob_matches,
		dob_units_net											as dob_matched_units,
		hpd_projected_closings_ids								as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
		hpd_rfp_ids												as hpd_rfp_matches,
		hpd_rfp_incremental_units								as hpd_rfp_matched_units,
		''														as edc_matches,
		null::numeric											as edc_matched_units,
		'' 														as dcp_application_matches,
		null::numeric											as dcp_application_matched_units,
		''														as state_project_matches,
		null::numeric											as state_project_matched_units,
		''														as neighborhood_study_matches,
		null::numeric											as neighborhood_study_units,
		''														as public_sites_matches,
		null::numeric											as public_sites_units,
		''														as planner_projects_matches,
		null::numeric											as planner_projects_units,
		'' 														as matches_to_nstudy_projected,
		null::numeric											as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		edc_deduped
	union all
	select
		the_geom,
		the_geom_webmercator,
		'DCP Applications'										as source,
		project_id 												as project_id,
		project_name											as project_name_address,
		''														as dob_job_type,
		status													as status,
		borough,
		total_units,
		zap_incremental_units									as deduplicated_units,
		case
			when total_units::float*.2>zap_incremental_units 				then 0
			when zap_incremental_units<=2									then 0
			when remaining_likely_to_be_built_2018 = 'No units remaining'	then 0
			else zap_incremental_units end								as counted_units,
		portion_built_2025::numeric								as portion_built_2025,
		portion_built_2035::numeric								as portion_built_2035,
		portion_built_2055::numeric								as portion_built_2055,
		planner_input											as planner_input,
		dob_job_numbers											as dob_matches,
		dob_units_net											as dob_matched_units,
		hpd_projected_closings_ids								as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
		hpd_rfp_ids												as hpd_rfp_matches,
		hpd_rfp_incremental_units								as hpd_rfp_matched_units,
		edc_project_ids											as edc_matches,
		edc_incremental_units									as edc_matched_units,
		'' 														as dcp_application_matches,
		null::numeric											as dcp_application_matched_units,
		''														as state_project_matches,
		null::numeric											as state_project_matched_units,
		''														as neighborhood_study_matches,
		null::numeric											as neighborhood_study_units,
		''														as public_sites_matches,
		null::numeric											as public_sites_units,
		''														as planner_projects_matches,
		null::numeric											as planner_projects_units,
		'' 														as matches_to_nstudy_projected,
		null::numeric											as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		zap_deduped_build_year
	where
		project_id not like '%[ESD%'
	union all
	select
		the_geom,
		the_geom_webmercator,
		'Empire State Development Projected Projects'			as source,
		project_id 												as project_id,
		project_name											as project_name_address,
		''														as dob_job_type,
		'Projected'												as status,
		borough,
		total_units,
		zap_incremental_units									as deduplicated_units,
		case
			when total_units::float*.2>zap_incremental_units 				then 0
			when zap_incremental_units<=2									then 0
			when remaining_likely_to_be_built_2018 = 'No units remaining'	then 0
			else zap_incremental_units end								as counted_units,
		portion_built_2025::numeric								as portion_built_2025,
		portion_built_2035::numeric								as portion_built_2035,
		portion_built_2055::numeric								as portion_built_2055,
		planner_input											as planner_input,
		dob_job_numbers											as dob_matches,
		dob_units_net											as dob_matched_units,
		hpd_projected_closings_ids								as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
		hpd_rfp_ids												as hpd_rfp_matches,
		hpd_rfp_incremental_units								as hpd_rfp_matched_units,
		edc_project_ids											as edc_matches,
		edc_incremental_units									as edc_matched_units,
		'' 														as dcp_application_matches,
		null::numeric											as dcp_application_matched_units,
		''														as state_project_matches,
		null::numeric											as state_project_matched_units,
		''														as neighborhood_study_matches,
		null::numeric											as neighborhood_study_units,
		''														as public_sites_matches,
		null::numeric											as public_sites_units,
		''														as planner_projects_matches,
		null::numeric											as planner_projects_units,
		'' 														as matches_to_nstudy_projected,
		null::numeric											as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		zap_deduped_build_year
	where
		project_id like '%[ESD%'
	union all
	select
		the_geom,
		the_geom_webmercator,
		'Neighborhood Study Rezoning Commitments'				as source,
		project_id 												as project_id,
		project_name											as project_name_address,
		''														as dob_job_type,
		status													as status,
		borough,
		total_units,
		nstudy_incremental_units								as deduplicated_units,
		case
			when total_units::float*.2>nstudy_incremental_units 			then 0
			when nstudy_incremental_units<=2							then 0
			else nstudy_incremental_units end							as counted_units,
		portion_built_2025::numeric								as portion_built_2025,
		portion_built_2035::numeric								as portion_built_2035,
		portion_built_2055::numeric								as portion_built_2055,
		planner_input											as planner_input,
		dob_job_numbers											as dob_matches,
		dob_units_net											as dob_matched_units,
		hpd_projected_closings_ids								as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units				as hpd_projected_closing_matched_units,
		hpd_rfp_ids												as hpd_rfp_matches,
		hpd_rfp_incremental_units								as hpd_rfp_matched_units,
		edc_project_ids											as edc_matches,
		edc_incremental_units									as edc_matched_units,
		zap_project_ids 										as dcp_application_matches,
		zap_incremental_units									as dcp_application_matched_units,
		''														as state_project_matches,
		null::numeric											as state_project_matched_units,
		''														as neighborhood_study_matches,
		null::numeric											as neighborhood_study_units,
		''														as public_sites_matches,
		null::numeric											as public_sites_units,
		''														as planner_projects_matches,
		null::numeric											as planner_projects_units,
		'' 														as matches_to_nstudy_projected,
		null::numeric											as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		nstudy_deduped
	union all
	select
		the_geom,
		the_geom_webmercator,
		'Future City-Sponsored RFPs/RFEIs'							as source,
		project_id 													as project_id,
		project_name												as project_name_address,
		''															as dob_job_type,
		'Projected'													as status,
		borough,
		total_units,
		public_sites_incremental_units								as deduplicated_units,
		case
			when total_units::float*.2>public_sites_incremental_units			then 0
			when public_sites_incremental_units<=2								then 0
			else public_sites_incremental_units end								as counted_units,
		portion_built_2025::numeric									as portion_built_2025,
		portion_built_2035::numeric									as portion_built_2035,
		portion_built_2055::numeric									as portion_built_2055,
		planner_input												as planner_input,
		dob_job_numbers												as dob_matches,
		dob_units_net												as dob_matched_units,
		hpd_projected_closings_ids									as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units					as hpd_projected_closing_matched_units,
		hpd_rfp_ids													as hpd_rfp_matches,
		hpd_rfp_incremental_units									as hpd_rfp_matched_units,
		edc_project_ids												as edc_matches,
		edc_incremental_units										as edc_matched_units,
		zap_project_ids 											as dcp_application_matches,
		zap_incremental_units										as dcp_application_matched_units,
		''															as state_project_matches,
		null::numeric												as state_project_matched_units,
		nstudy_project_ids											as neighborhood_study_matches,
		nstudy_incremental_units									as neighborhood_study_units,
		''															as public_sites_matches,
		null::numeric												as public_sites_units,
		''															as planner_projects_matches,
		null::numeric												as planner_projects_units,
		'' 															as matches_to_nstudy_projected,
		null::numeric												as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		public_sites_deduped
	union all
	select
		the_geom,
		the_geom_webmercator,
		'DCP Planner-Added Projects'								as source,
		concat(project_id)											as project_id,
		project_name												as project_name_address,
		''															as dob_job_type,
		'Potential'													as status,
		borough,
		total_units,
		planner_projects_incremental_units							as deduplicated_units,
		case
			when total_units::float*.2>planner_projects_incremental_units			then 0
			when planner_projects_incremental_units<=2								then 0
			else planner_projects_incremental_units end								as counted_units,
		portion_built_2025::numeric									as portion_built_2025,
		portion_built_2035::numeric									as portion_built_2035,
		portion_built_2055::numeric									as portion_built_2055,
		planner_input												as planner_input,
		dob_job_numbers												as dob_matches,
		dob_units_net												as dob_matched_units,
		hpd_projected_closings_ids									as hpd_projected_closing_matches,
		hpd_projected_closings_incremental_units					as hpd_projected_closing_matched_units,
		hpd_rfp_ids													as hpd_rfp_matches,
		hpd_rfp_incremental_units									as hpd_rfp_matched_units,
		edc_project_ids												as edc_matches,
		edc_incremental_units										as edc_matched_units,
		zap_project_ids 											as dcp_application_matches,
		zap_incremental_units										as dcp_application_matched_units,
		''															as state_project_matches,
		null::numeric												as state_project_matched_units,
		nstudy_project_ids											as neighborhood_study_matches,
		nstudy_incremental_units									as neighborhood_study_units,
		public_sites_project_ids									as public_sites_matches,
		public_sites_incremental_units								as public_sites_units,
		''															as planner_projects_matches,
		null::numeric												as planner_projects_units,
		'' 															as matches_to_nstudy_projected,
		null::numeric												as units_matched_to_nstudy_projected,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		planner_projects_deduped
	union all
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		'' 															as project_name_address,
		''															as dob_job_type,
		status,
		borough,
		total_units,
		nstudy_projected_potential_incremental_units				as deduplicated_units,
		case
			when total_units::float*.2>nstudy_projected_potential_incremental_units				then 0
			when nstudy_projected_potential_incremental_units<=2								then 0
			else nstudy_projected_potential_incremental_units end								as counted_units,
		null														as portion_built_2025,
		null														as portion_built_2035,
		null														as portion_built_2055,
		''															as planner_input,
		''															as dob_matches,
		null::numeric												as dob_matched_units,
		''															as hpd_projected_closing_matches,
		null::numeric												as hpd_projected_closing_matched_units,
		''															as hpd_rfp_matches,
		null::numeric												as hpd_rfp_matched_units,
		''															as edc_matches,
		null::numeric												as edc_matched_units,
		''				 											as dcp_application_matches,
		null::numeric												as dcp_application_matched_units,
		''															as state_project_matches,
		null::numeric												as state_project_matched_units,
		''															as neighborhood_study_matches,
		null::numeric 												as neighborhood_study_units,
		''															as public_sites_matches,
		null::numeric												as public_sites_units,
		''															as planner_projects_matches,
		null::numeric												as planner_projects_units,
		project_matches 											as matches_to_nstudy_projected,
		matched_incremental_units									as units_matched_to_nstudy_projected,
		null as nycha_flag,
		null as gq_flag,
		null as senior_housing_flag,
		null as assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		nstudy_projected_potential_deduped_final
	where
		status = 'Projected'
	union all
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		'' 															as project_name_address,
		''															as dob_job_type,
		status,
		borough,
		total_units,
		total_units 												as deduplicated_units,
		case
			when total_units::float*.2>total_units					then 0
			when total_units<=2										then 0
			else total_units end									as counted_units,
		null														as portion_built_2025,
		null														as portion_built_2035,
		null														as portion_built_2055,
		''															as planner_input,
		''															as dob_matches,
		null::numeric												as dob_matched_units,
		''															as hpd_projected_closing_matches,
		null::numeric												as hpd_projected_closing_matched_units,
		''															as hpd_rfp_matches,
		null::numeric												as hpd_rfp_matched_units,
		''															as edc_matches,
		null::numeric												as edc_matched_units,
		''				 											as dcp_application_matches,
		null::numeric												as dcp_application_matched_units,
		''															as state_project_matches,
		null::numeric												as state_project_matched_units,
		''															as neighborhood_study_matches,
		null::numeric 												as neighborhood_study_units,
		''															as public_sites_matches,
		null::numeric												as public_sites_units,
		''															as planner_projects_matches,
		null::numeric												as planner_projects_units,
		'' 															as matches_to_nstudy_projected,
		null::numeric												as units_matched_to_nstudy_projected,
		null as nycha_flag,
		null as gq_flag,
		null as senior_housing_flag,
		null as assisted_living_flag,
		null as hpd_children_unlikely_flag
	from
		nstudy_future
)
order by 
	source asc,
	project_id asc,
	project_name_address asc,
	status asc




select cdb_cartodbfytable('capitalplanning','known_projects_db_20190610_v4') 