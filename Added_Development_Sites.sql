/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Appending added planner development sites from KS and RL
*************************************************************************************************************************************************************************************/


/**********************************RUN IN CARTO BATCH*****************/

select
	*
into
	added_development_sites_20190510_MS
from
	(
		select
			the_geom,
			the_geom_webmercator,
			mapid,
			objectid,
			shape_length,
			shape_area,
			area_sqft,
			'KS' as source
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_ks_1
		union
		select
			the_geom,
			the_geom_webmercator,
			mapid,
			objectid,
			shape_length,
			shape_area,
			null,
			'RL' as source
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_rl_1			
	) as added_development_sites
order by
	mapid asc


/*************************************RUN IN REGULAR CARTO****************/

select cdb_cartodbfytable('capitalplanning', 'added_development_sites_20190510_MS')


/**********************************RUN IN CARTO BATCH*****************/


select
	*
into
	all_planner_inputs
from
	(
		select
			source,
			map_id
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units,
			null as ks_assumed_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.table_20190510_queens_planner_inputs_housing_pipeline
		union
		select
			source,
			map_id
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units,
			null as ks_assumed_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.table_20190510_staten_island_planner_inputs_housing_pipeline
		union
		select
			source,
			map_id
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units_from_planner as total_units,
			ks_assumed_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.table_20190510_manhattan_planner_inputs_housing_pipeline
		union
		select
			source,
			map_id
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units_from_planner as total_units,
			ks_assumed_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.table_20190510_bronx_planner_inputs_housing_pipeline
		union
		select
			source,
			map_id
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units_from_planner as total_units,
			ks_assumed_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.table_20190510_brooklyn_planner_inputs_housing_pipeline
	) as all_planner_inputs
	order by
		borough, project_id


/*Joining in planner inputs, starting with Queens planner inputs.*/





select
	*
into
	joined_test
from
	(
		select
			a.the_geom,
			a.the_geom_webmercator,
			a.mapid,
			source,
			project_id,
			project_name,
			boro as borough,
			cd,
			status,
			total_units,
			lead_planner,
			request_for_update,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055,
			outdated_overlapping_project,
			non_residential_project,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project
		from
			capitalplanning.added_development_sites_20190510_MS a
		left join
			capitalplanning.table_20190510_queens_planner_inputs_housing_pipeline b
		on 
			a.mapid = map_id::numeric and
			map_id <> '' and map_id not like '%/%'
		left join
			capitalplanning.table_20190510_brooklyn_planner_inputs_housing_pipeline c
	) as Queens_join
	order by 
		mapid asc
