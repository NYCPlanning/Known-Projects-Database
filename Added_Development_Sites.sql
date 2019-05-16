/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Appending added planner development sites from KS and RL
Methodology: 
1. Append KS and RL map files
2. Append Planner inputs by borough
3. Merge map files and inputs to then use as planner inputs on projects previously identified in the pipeline
4. Omit planner inputs on existing projects to create a dataset for just planner-added projects.
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



/*Compile planner inputs*/

select
	*
into
	planner_inputs_consolidated_ms
from
(
	select
		boro,
		cd,
		map_id, /*Manually convert this field to numeric*/
		source,
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.bronx_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id, /*Manually convert this field to int*/
		source,		
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.brooklyn_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.manhattan_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units as total_units_from_planner,
		notes_on_total_ks_assumed_units,
		null as ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.staten_island_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units as total_units_from_planner,
		notes_on_total_ks_assumed_units,
		units_ks as ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.queens_planner_inputs_housing_pipeline
) as planner_inputs


/**********************RUN IN REGULAR CARTO**************************/


select cdb_cartodbfytable('capitalplanning', 'planner_inputs_consolidated_ms')


/*Join the planner inputs to the mapped developments. Then do the intersect and delete those projects which are based on the flag*/

select
	*
into
	mapped_planner_inputs_consolidated_inputs_ms
from
(
	select
			row_number() over() as cartodb_id,
			a.the_geom,
			a.the_geom_webmercator,
			boro,
			cd,
			map_id, /*Manually convert this field to numeric*/
			b.source,
			project_id,
			project_name,
			status,
			total_units_from_planner,
			notes_on_total_ks_assumed_units,
			ks_assumed_units,
			units_remaining_not_accounted_for_in_other_sources,
			lead_planner
			/*Manually convert all of the following fields to numeric in Carto*/
			,outdated_overlapping_project,
			non_residential_project_incl_group_quarters,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_geometry,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project,
			a.objectid,
			a.shape_length,
			a.shape_area,
			a.area_sqft
	from
		planner_inputs_consolidated_ms b
	left join
		added_development_sites_20190510_ms a
	on
		a.mapid = b.map_id	
) as mapped_planner_inputs_consolidated_inputs_ms


/**********************RUN IN REGULAR CARTO**************************/

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_inputs_consolidated_inputs_ms')


/*********************RUN IN CARTO BATCH*******************************/

/*Limit to solely planner-added projects which should be included in the pipeline*/

select
	*
into
	mapped_planner_added_projects_ms
from
(
	select
		*
	from
		capitalplanning.mapped_planner_inputs_consolidated_inputs_ms
	where
		planner_added_project = 1 							and  
		outdated_overlapping_project is null 				and 
		non_residential_project_incl_group_quarters is null and 
		withdrawn_project is null							and 
		inactive_project is null							and 
		other_reason_to_omit is null						and 
		should_be_in_old_zap_pull is null					and 
		should_be_in_new_zap_pull is null					and
		source in
				(	
					'EDC',
					'MNO',
					'QNO',
					'SIO',
					'SIO map',
					'map',
					'BXO',
					'Map'
				)

) x

/**********************RUN IN REGULAR CARTO**************************/

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_added_projects_ms')
