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
		initcap(remaining_units_likely_to_be_built)
										 	as remaining_units_likely_to_be_built_2018,
		rationale							as rationale_2018,
		rationale_for_likely_to_be_built	as rationale_2019,
		phasing_if_known					as phasing_notes_2019,
		'' 									as additional_notes_2019,
		''									as ZAP_Checked_Project_ID_2019,
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
		initcap(remaining_units_likely_to_be_built)
										 	as remaining_units_likely_to_be_built_2018,
		rationale							as rationale_2018,
		rationale_for_likely_to_be_built	as rationale_2019,
		phasing_if_known					as phasing_notes_2019,
		'' 									as additional_notes_2019,
		''									as ZAP_Checked_Project_ID_2019,
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
		initcap(remaining_units_likely_to_be_built)
										 	as remaining_units_likely_to_be_built_2018,
		rationale							as rationale_2018,
		rationale_for_likely_to_be_built_other_comments
											as rationale_2019,
		phasing_if_known					as phasing_notes_2019,
		'' 									as additional_notes_2019,
		''									as ZAP_Checked_Project_ID_2019,
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
		initcap(remaining_units_likely_to_be_built)
										 	as remaining_units_likely_to_be_built_2018,
		rationale							as rationale_2018,
		rationale_for_likely_to_be_built	as rationale_2019,
		phasing_if_known					as phasing_notes_2019,
		'' 									as additional_notes_2019,
		field_20							as ZAP_Checked_Project_ID,
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
		initcap(remaining_units_likely_to_be_built)
										 	as remaining_units_likely_to_be_built_2018,
		rationale							as rationale_2018,
		rationale_for_likely_to_be_built	as rationale_2019,
		phasing_if_known					as phasing_notes_2019,
		note 								as additional_notes_2019,
		''									as ZAP_Checked_Project_ID_2019,
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
			remaining_units_likely_to_be_built_2018,
			rationale_2018,
			rationale_2019,
			phasing_notes_2019,
			additional_notes_2019,
			ZAP_Checked_Project_ID_2019,
			replace
			(	
				replace
				(
					replace
						(
						case 
							when
								trim(rationale_2019) 			<> '' or
								trim(phasing_notes_2019)		<> '' or
								trim(additional_notes_2019) 	<> '' then 
								concat_ws
									(
										' | ',
										nullif(trim(rationale_2019),''),
										nullif(trim(phasing_notes_2019),''),
										nullif(trim(additional_notes_2019),'')
									)	
							when
								trim(rationale_2018) <> '' then concat('2018 INPUT: ', trim(rationale_2018))
							else null end 
						 ,'''',
						 ''
						 ),
					'"',
					''
				),
				'–',
				''	
			)
			as planner_input,
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
				b.source not like '%DCP%' and b.source not like '%HPD%' and upper(b.source) not like '%CITY HALL%' and b.project_id <> 'P2012Q0062' /*Correcting Halletts Point inclusion*/
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
		capitalplanning.table_20190520_unidentified_zap_projects_planner_additions_ms_v b
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
								select project_id from relevant_dcp_projects_housing_pipeline_ms_v5
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
			project_name,
			coalesce(
						/*In one instance, for Flushing Commons MAP ID 85410, the planner has not included future units
						  in the total_units_from_planner field, instead including them in the units_remaining_not_accounted_for_in_other_sources field.
						  The geography similarly only encompasses the future developments, not the complete development.
						  For situations like these, I am preferencing the units_remaining_not_accounted_for_in_other_sources. This is the only MAP ID affected.*/
					case when total_units_from_planner <> units_remaining_not_accounted_for_in_other_sources then units_remaining_not_accounted_for_in_other_sources end,
					total_units_from_planner,
					case
						when ks_assumed_units <> '' and ks_assumed_units not like '%(%' then ks_assumed_units::numeric
						when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
						else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end						
					) as total_units,
			case
				when total_units_from_planner is not null then 'DCP'
				when ks_assumed_units <> '' then 'PLUTO FAR Estimate' end as Total_Units_Source,
			status,
			remaining_units_likely_to_be_built_2018,
			rationale_2018,
			rationale_2019,
			phasing_notes_2019,
			additional_notes_2019,
			concat_ws(' | ',nullif(rtrim(ltrim(status)),''),nullif(rtrim(ltrim(planner_input)),'')) as planner_input,
		CASE 
			WHEN upper(concat(project_name,planner_input))  like '%NYCHA%' THEN 1   		
			WHEN upper(concat(project_name,planner_input))  like '%BTP%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%HOUSING AUTHORITY%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%NEXT GEN%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%NEXT-GEN%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%NEXTGEN%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%BUILD TO PRESERVE%' THEN 1 ELSE 0 END 		AS NYCHA_Flag,

		CASE 
			WHEN upper(concat(project_name,planner_input))  like '%CORRECTIONAL%' THEN 1   		
			WHEN upper(concat(project_name,planner_input))  like '%NURSING%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '% MENTAL%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%DORMITOR%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%MILITARY%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%GROUP HOME%' THEN 1  		
			WHEN upper(concat(project_name,planner_input))  like '%BARRACK%' THEN 1 ELSE 0 END 		AS GQ_fLAG,

		/*Identifying definite senior housing projects*/
		CASE 
			WHEN upper(concat(project_name,planner_input))  	like '%SENIOR%' THEN 1
			WHEN upper(concat(project_name,planner_input))  	like '%ELDERLY%' THEN 1 	
			WHEN concat(project_name,planner_input)  			like '% AIRS%' THEN 1
			WHEN upper(concat(project_name,planner_input))  	like '%A.I.R.S%' THEN 1 
			WHEN upper(concat(project_name,planner_input))  	like '%CONTINUING CARE%' THEN 1
			WHEN upper(concat(project_name,planner_input))  	like '%NURSING%' THEN 1
			WHEN concat(project_name,planner_input)  			like '% SARA%' THEN 1
			WHEN upper(concat(project_name,planner_input))  	like '%S.A.R.A%' THEN 1 else 0 end as Senior_Housing_Flag,
		CASE
			WHEN upper(concat(project_name,planner_input))  like '%ASSISTED LIVING%' THEN 1 else 0 end as Assisted_Living_Flag,

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

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_inputs_added_projects_ms_1')


select
	*
into
	planner_added_projects_share_20190522
from
(
	select
		the_geom,
		the_geom_webmercator,
		map_id as project_id,
		project_name,
		total_units
	from
		mapped_planner_inputs_added_projects_ms_1
)   planner_added_projects_share_20190522
	order by
		project_id asc

select cdb_cartodbfytable('capitalplanning', 'planner_added_projects_share_20190522')

