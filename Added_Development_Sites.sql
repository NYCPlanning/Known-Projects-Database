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
		lead_planner,
		mql_view,
		suggestion,
		must_get_boro_input,
		response_to_mql_view,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
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
		capitalplanning.table_20190516_bronx_planner_inputs_housing_pipeline
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
		lead_planner,
		mql_view,
		suggestion,
		must_get_boro_input,
		response_to_mql_view,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
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
		capitalplanning.table_20190516_brooklyn_planner_inputs_housing_pipeline
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
		lead_planner,
		mql_view,
		suggestion,
		must_get_boro_input,
		response_to_mql_view,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
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
		capitalplanning.table_20190516_manhattan_planner_inputs_housing_pipeline
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
		lead_planner,
		mql_view,
		suggestion,
		must_get_boro_input,
		response_to_mql_view,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
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
		capitalplanning.table_20190516_staten_island_planner_inputs_housing_pipeline
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
		lead_planner,
		mql_view,
		suggestion,
		must_get_boro_input,
		response_to_mql_view,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
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
		capitalplanning.table_20190516_queens_planner_inputs_housing_pipeline
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
			lead_planner,
			mql_view,
			suggestion,
			must_get_boro_input,
			response_to_mql_view,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055
			/*Manually convert all of the following fields to numeric in Carto*/
			,outdated_overlapping_project,
			non_residential_project_incl_group_quarters,
			withdrawn_project,
			inactive_project,
			case when 
				b.source not like '%DCP%' and b.source not like '%HPD%' and upper(b.source) not like '%CITY HALL%'
				and (b.project_name like '%NYCHA%' or upper(b.project_name) like '%BUILD TO PRESERVE%') then 1 end as Exclude_NYCHA_Flag,
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
	/*Two projects (in addition to SI incorrect, quarantined projects) do not exist in planner inputs. 94519 and 94500. These are incorrect geocodes according to KS, and are accurately not included.*/

) as mapped_planner_inputs_consolidated_inputs_ms


/**********************RUN IN REGULAR CARTO**************************/

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_inputs_consolidated_inputs_ms')


/*********************RUN IN CARTO BATCH*******************************/

/*Limit to solely planner-added projects which should be included in the pipeline. Omitting projects which have already explicitly been pulled out of zap*/

select
	*
into
	mapped_planner_added_projects_ms
from
(
	select
		a.*
	from
		capitalplanning.mapped_planner_inputs_consolidated_inputs_ms a
	left join
		capitalplanning.table_20190517_unidentified_zap_projects_planner_additions_ms_1 b
	on
		a.map_id = b.map_id and 
		(
			b.name_match = 1 or
			b.name_match_manual = 1
		)
	where
		a.planner_added_project = 1 							and 
		a.exclude_nycha_flag is null 							and 
		a.outdated_overlapping_project is null 				and 
		a.non_residential_project_incl_group_quarters is null and 
		a.withdrawn_project is null							and 
		a.inactive_project is null							and 
		a.other_reason_to_omit is null						and 
		a.should_be_in_old_zap_pull is null					and 
		a.should_be_in_new_zap_pull is null					and
		a.source in
				(	
					'EDC',
					'MNO',
					'QNO',
					'SIO',
					'SIO map',
					'map',
					'BXO',
					'Map'
				)											and
		b.map_id is null 									and
		a.project_id not in
							(
								select project_id from relevant_dcp_projects_housing_pipeline_ms_v4
							)

) x


/*Formatting table for output*/

select
	*
into
	mapped_planner_inputs_added_projects_ms_1
from
	(
		select
			the_geom,
			the_geom_webmercator,
			cartodb_id,
			source,
			map_id,
			boro,
			cd,
			project_name
			status,
			coalesce(
						total_units_from_planner,
					case
						when ks_assumed_units <> '' and ks_assumed_units not like '%(%' then ks_assumed_units::numeric
						when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
						else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end						
					) as total_units,
			case
				when total_units_from_planner is not null then 'DCP'
				when ks_assumed_units <> '' then 'PLUTO FAR Estimate' end as Total_Units_Source,
			lead_planner,
			mql_view,
			suggestion,
			must_get_boro_input,
			response_to_mql_view,
			portion_built_2025,
			portion_built_2035,
			portion_built_2055
			,outdated_overlapping_project,
			non_residential_project_incl_group_quarters,
			withdrawn_project,
			inactive_project,
			Exclude_NYCHA_Flag
			other_reason_to_omit,
			corrected_existing_geometry,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project,
			objectid,
			shape_length,
			shape_area,
			area_sqft
		from
			mapped_planner_added_projects_ms
		order by
			map_id asc
) x


/**********************RUN IN REGULAR CARTO**************************/

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_added_projects_ms')
