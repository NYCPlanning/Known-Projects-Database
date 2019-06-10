/**************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Planner Mods for ZAP
***************************************************************************************************************************************************************************************/
/**************************************************************************************************************************************************************************************
METHODOLOGY:
1. Omit projects based on planner inputs.
2. Update projects based on planner inputs.
3. Include previously excluded projects based on planner inputs.
***************************************************************************************************************************************************************************************/
/*************************************************************************************
RUN THIS SCRIPT IN CARTO BATCH
*************************************************************************************/ 

/*********************MODIFYING THESE PROJECTS WITH PLANNER INPUTS****************/


/*Flag projects which planners indicated should be omitted due to overlaps, non-residential, withdrawals, inactivity, or otherwise*/

SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2_1
from
(
	SELECT
		*,
		CASE 
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and outdated_overlapping_project 					= 1) then 'Planner Noted Overlap'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and non_residential_project_incl_group_quarters 	= 1) then 'Planner Noted Non-Residential'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and withdrawn_project 							= 1) then 'Planner Noted Withdrawn'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and inactive_project 								= 1) then 'Planner Noted Inactive'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and Exclude_NYCHA_Flag 							= 1) then 'Planner Noted NYCHA Exclusion'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and other_reason_to_omit 							= 1) then 'Planner Noted Other Reason to Omit'
			else null end as Planner_Noted_Omission
	from
		relevant_dcp_projects_housing_pipeline_ms_v2
) relevant_dcp_projects_housing_pipeline_ms_v2_1


/*Replace ZAP unit count and ZAP geom, where appropriate, with planner input. There are 60 planner inputs on unit count and all are within reason. Hudson Yards also has a planner input of 13,508 (EAS) joined on,
  but the unit count in relevant_dcp_projects_housing_pipeline_ms_v2_1 reflects HY after DIB deductions, so we are omitting this match. There are 11 location corrections and 1 location addition as well.*/


SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2_2
from
(
	select
		a.*,
		case 
			when a.project_id <> 'P2005M0053' /*HY*/ then coalesce
																	(
																		b.updated_unit_count,
																		b.total_units_from_planner,
																		a.total_units_1,
																		case 
																			when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
																			else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end
																	) 
			else a.total_units_1 end 																														as total_units_2
		,case
			when b.updated_unit_count is not null 																							then 'Planner'
			when b.total_units_from_planner is not null and b.total_units_from_planner<>coalesce(a.total_units_1,0) and 
			a.project_id <> 'P2005M0053' 																									then 'Planner'
			when a.total_units_1 is not null and a.total_units_1 <> 0 	and upper(a.project_id) not like '%ESD PROJECT%'					then 'ZAP or Internal Research'
			when upper(a.project_id) like '%ESD PROJECT%'																					then 'Internal Research'
			when b.ks_assumed_units is not null and b.ks_assumed_units<>'' 																	then 'PLUTO FAR Estimate'
			else null end as Total_Unit_Source
		,b.map_id /*Manually convert this field to numeric in Carto interface*/
		,b.the_geom as planner_geom
		,b.the_geom_webmercator as planner_geom_webmercator
		,b.source
		,b.project_id as planner_project_id
		,b.project_name as planner_project_name
		,b.status as planner_status
		,total_units_from_planner
		,notes_on_total_ks_assumed_units
		,case 
			when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
			else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end as ks_assumed_units
		,units_remaining_not_accounted_for_in_other_sources
		,lead_planner
		/*Manually convert all of the following fields to numeric in Carto interface*/
		,b.outdated_overlapping_project
		,non_residential_project_incl_group_quarters
		,withdrawn_project
		,inactive_project
		,Exclude_NYCHA_flag
		,other_reason_to_omit
		,corrected_existing_geometry
		,corrected_existing_unit_count
		,updated_unit_count
		,should_be_in_old_zap_pull
		,should_be_in_new_zap_pull
		,planner_added_project
from
	relevant_dcp_projects_housing_pipeline_ms_v2_1 a
left join
	mapped_planner_inputs_consolidated_inputs_ms b
on
	a.project_id = b.project_id or
	(a.project_id = 'P2018R0276' and b.map_id = 94518) /*Matching Sea View in ZAP data with planner geometry for Sea View City Hall Public Sites project*/ or
	(a.project_id = '2019K0177'	 and b.map_id = 85357) /*Matching Greenpoint Hospital in ZAP data with planner geometry for Greenpoint Hospital */
) x

select
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v3
from
	(
		select
			cartodb_id,
			coalesce(planner_geom,the_geom) as the_geom,
			coalesce(planner_geom_webmercator,the_geom_webmercator) as the_geom_webmercator,
			project_id,
			project_name,
			borough, 
			project_description,
			project_brief,
			case 
				when project_id in('P2017Q0067','P2018Q0046') then null else total_units_2 end as total_units,
			case 
				when project_id in('P2017Q0067','P2018Q0046') then null else total_unit_source end as total_unit_source,
			case
				when project_id in('P2017Q0067','P2018Q0046') then null 
				when total_unit_source = 'ZAP or Internal Research' then total_units_source end as ZAP_Unit_Source,
			applicant_type,
			project_status,
			previous_project_status,
			process_stage,
			previous_process_stage,
			anticipated_year_built,
			dcp_target_certification_date,
			certified_referred,
			project_completed,
			ulurp,
			si_seat_cert,
			initiation_flag,
			pre_pas_flag,
			Diff_Between_Total_and_New_Units,
			Historical_Project_Pre_2012,
			Historical_Project_Pre_2008
		from
			capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v2_2
		where
			Planner_Noted_Omission is null /*Removing planner omitted and flagged projects except for Anable Basin and LICIC, which we are keeping with null unit count*/ or
			project_id in('P2017Q0067','P2018Q0046')
	) x
	order by
		project_id asc

/*********************RUN IN CARTO BATCH**************************/


/*
	Final comparison of planner inputs to ZAP projects. What non-identified ZAP projects are in the planner-inputs? Identifying and including these projects  
	from DCP_PROJECT_FLAGS_V2
*/

SELECT
	*
into
	table_20190520_unidentified_zap_projects_planner_additions_ms
from
(
select 
		A.*, 
		c.map_id, 
		c.project_id as project_id_map, 
		c.project_name as project_name_map, 
		c.status as status_map, 
		c.total_units_from_planner,
		c.ks_assumed_units,
		c.source,
		c.ZAP_Checked_Project_ID_2019, 
		case when a.project_name <> '' and (position(upper(a.project_name) in upper(c.project_name))>0 or position(upper(c.project_name) in upper(a.project_name))>0) then 1 
			 when c.ZAP_Checked_Project_ID_2019 = a.project_id then 1
			 else 0 end as name_match
from 
	DCP_PROJECT_FLAGS_V2 A 
LEFT JOIN 
	relevant_dcp_projects_housing_pipeline_ms_v3 B 
ON 
	A.PROJECT_ID = B.PROJECT_ID 
LEFT JOIN
	mapped_planner_inputs_consolidated_inputs_ms C
ON 
	ST_INTERSECTS(A.THE_GEOM,C.THE_GEOM) or
	(c.ZAP_Checked_Project_ID_2019 = a.project_id and c.ZAP_Checked_Project_ID_2019 <> '') 
WHERE 
	B.PROJECT_ID IS NULL 								AND 
	(
		C.THE_GEOM IS NOT NULL or 
		c.ZAP_Checked_Project_ID_2019 <> ''
	)													AND 
	A.PROJECT_STATUS NOT LIKE '%Closed%' 				and 
	a.project_status not like '%Withdrawn%' 			and
	a.historical_project_pre_2012 = 0 					and 
	outdated_overlapping_project is null 				and 
	non_residential_project_incl_group_quarters is null and 
	withdrawn_project is null							and 
	inactive_project is null							and
	exclude_nycha_flag is null 							and
	other_reason_to_omit is null							
) x


/*Reupload this table back into Carto as table_20190520_unidentified_zap_projects_planner_additions_ms_v after creating a name_match_manual field which
  supports the automatic name match field. Include projects in which name_match = 1 or name_match_manual = 1 from dcp_project_flags_v2
  and append them into relevant_dcp_projects_housing_pipeline_ms_v3
*/

SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v4
from
(
	SELECT
		*, null as map_id
	from
		relevant_dcp_projects_housing_pipeline_ms_v3
	WHERE
		TOTAL_UNITS <> 0 or
		project_id in('P2017Q0067','P2018Q0046')
	union
	SELECT
		row_number() over() + (select max(cartodb_id) from relevant_dcp_projects_housing_pipeline_ms_v3) 	as cartodb_id,
		coalesce(a.the_geom,c.the_geom) 																	as the_geom,
		coalesce(a.the_geom_webmercator,c.the_geom_webmercator)												as the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.borough, 
		a.project_description,
		a.project_brief,
		coalesce(
					b.total_units_from_planner,
					case 
						when a.PROJECT_ID = 'P2017M0394' THEN 588
						when length(b.ks_assumed_units)<2 or position('units' in b.ks_assumed_units)<1 then null
						else substring(b.ks_assumed_units,1,position('units' in b.ks_assumed_units)-1)::numeric end
				) as total_units,
		case when b.total_units_from_planner is not null then 'Planner'
			 when b.ks_assumed_units <> '' then 'PLUTO FAR Estimate' end total_unit_source,
		null as ZAP_Unit_Source,
		a.applicant_type,
		a.project_status,
		a.previous_project_status,
		a.process_stage_name_stage_id_process_stage as process_stage,
		a.previous_process_stage,
		a.anticipated_year_built,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.ulurp_non_ulurp as ulurp,
		a.si_seat_cert,
		a.initiation_flag,
		a.pre_pas_flag,
		a.Diff_Between_Total_and_New_Units,
		a.Historical_Project_Pre_2012,
		a.Historical_Project_Pre_2008,
		c.map_id
	from
		dcp_project_flags_v2 a
	inner join
		table_20190520_unidentified_zap_projects_planner_additions_ms_v b
	on
		a.project_id = b.project_id and
		(
			name_match = 1 or
			name_match_manual = 1
		)
	left join
		mapped_planner_inputs_consolidated_inputs_ms c
	on
		b.map_id = c.map_id
	WHERE 
		coalesce(
					b.total_units_from_planner,
					case 
						when a.PROJECT_ID = 'P2017M0394' THEN 588
						when length(b.ks_assumed_units)<2 or position('units' in b.ks_assumed_units)<1 then null
						else substring(b.ks_assumed_units,1,position('units' in b.ks_assumed_units)-1)::numeric end
				) <> 0 
) x



/*Removing duplicates which exist both in the planners inputs and ZAP*/


SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v5
from
(
	SELECT
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
		case
			when a.project_status = 'Complete' or 		a.project_completed is not null or 		a.process_stage = 'Completed' 	 	then 'Complete'
			when a.certified_referred is not null or 	a.process_stage = 'Public Review' 		then 'Active, Certified'
			when a.project_status = 'Active' then
				case
					when a.process_stage = 'Pre-Cert' and a.initiation_flag <> 1 and a.pre_pas_flag <>1 then 'Active, Pre-Cert'
					when a.initiation_flag = 1															then 'Active, Initiation'
					when a.pre_pas_flag = 1																then 'Active, Pre-PAS'		end
			when a.project_status = 'On-Hold' then													
				CASE
					when a.process_stage = 'Pre-Cert' and a.initiation_flag <> 1 and a.pre_pas_flag <>1 then 'On-Hold, Pre-Cert'
					when a.initiation_flag = 1															then 'On-Hold, Initiation'
					when a.pre_pas_flag = 1																then 'On-Hold, Pre-PAS'		end
			else a.project_status end 																as dcp_edit_project_status,
		a.project_status,
		a.previous_project_status,
		a.process_stage,
		a.previous_process_stage,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.ulurp,
		a.Anticipated_year_built,
		b.remaining_units_likely_to_be_built_2018,
		b.rationale_2018,
		b.rationale_2019,
		b.phasing_notes_2019,
		b.additional_notes_2019,
		b.portion_built_2025,
		b.portion_built_2035,
		b.portion_built_2055,
		a.si_seat_cert,
		case
			when a.pre_pas_flag 	= 1 then 1
			when a.initiation_flag 	= 1 then 1 else 0 end as early_stage_flag,
		/*Identifying NYCHA Projects*/
		CASE 
			when a.project_id = 'P2012Q0062'											  								THEN 0 /*NYCHA only a small part of this Hallets Point*/
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%NYCHA%' 					THEN 1   		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%BTP%' 					THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%HOUSING AUTHORITY%' 		THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%NEXT GEN%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%NEXT-GEN%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%NEXTGEN%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%BUILD TO PRESERVE%' 		THEN 1 
			ELSE 0 																										END	AS NYCHA_Flag,
		CASE 
			when a.project_id = 'P2018M0058'																			THEN 0 /*Nursing facility only small part of this project*/
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%CORRECTIONAL%' 			THEN 1   		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%NURSING%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '% MENTAL%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%DORMITOR%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%MILITARY%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%GROUP HOME%' 				THEN 1  		
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  like '%BARRACK%' 				THEN 1 
			ELSE 0 																										END	AS GQ_fLAG,


		/*Identifying definite senior housing projects*/
		CASE 
			when a.project_id in('P2012R0625','P2018M0058','P2016Q0306')												THEN 0 /*Three projects which only include
																									 								senior housing as a small portion*/
			when a.project_id = 'P2012M0285' 																			THEN 0 /*Existing site, not future site is senior home*/
			when a.project_id = 'P2014M0257' 																			THEN 1 /*Senior home in future site*/
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%SENIOR%' 				THEN 1
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%ELDERLY%' 				THEN 1 	
			WHEN concat(b.planner_input,a.project_description,a.project_brief)  		like '% AIRS%' 					THEN 1
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%A.I.R.S%' 				THEN 1 
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%CONTINUING CARE%' 		THEN 1
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%NURSING%' 				THEN 1
			WHEN concat(b.planner_input,a.project_description,a.project_brief)  		like '% SARA%' 					THEN 1
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%S.A.R.A%' 				THEN 1 
			else 0								 																		end	as Senior_Housing_Flag,
		CASE
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))  	like '%ASSISTED LIVING%' THEN 1 
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))	like '%LONG-TERM CARE%'  THEN 1 
			WHEN upper(concat(b.planner_input,a.project_description,a.project_brief))   like '%LONG TERM CARE%'	 THEN 1 
			else 0 																	 									end as Assisted_Living_Flag,

		coalesce(b.planner_input,'') 											as planner_input,
		row_number() over(partition by a.project_id) 							as project_id_instance
	from
		relevant_dcp_projects_housing_pipeline_ms_v4 a
	left join
		mapped_planner_inputs_consolidated_inputs_ms b
	on
		a.project_id = b.project_id or (a.map_id is not null and a.map_id = b.map_id)
	/*Joining on DCP Inputs from 2018 SCA Housing Pipeline to provide additional planner rationale where it is not available in 2019*/
) x
	where 
		project_id_instance = 1 								AND				/*Omitting duplicate project_id from relevant_dcp_projects_housing_pipeline_ms_v4*/
		not 
		(	
			(planner_input is not null and planner_input <> '') AND
			(
				upper(planner_input) like '%EXISTING UNITS%' 	OR				/*Omitting projects where we have not identified materialization but the planner has noted that 
																				the units identified are existing units*/ 
				upper(planner_input) like '%DUPLICATE%' 						/*Omitting project IDs which planner suggest are duplicates of others. Only eliminates P2017Q0385*/
			) 													
		)														AND
		not	(project_status is not null and upper(project_status) in('WITHDRAWN','RECORD CLOSED'))	/*Omitting one planner-added project with Record Closed*/
		AND NOT	project_id in('P2012Q0313','P2012K0231')											/*Planner-noted in 2018 that these units are already existing 
																									  for the former, and for the latter the planner has added
																									  an overriding polygon in the planner-added projects section*/


select cdb_cartodbfytable('capitalplanning', 'relevant_dcp_projects_housing_pipeline_ms_v5')


select
	*
into
	dcp_inputs_share_20190522
from
(
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		total_units
	from
		relevant_dcp_projects_housing_pipeline_ms_v5
	where
		project_id not like '%[ESD%'
) 	dcp_inputs_share_20190522
	order by
		project_id asc

select cdb_cartodbfytable('capitalplanning', 'dcp_inputs_share_20190522')


select
	*
into
	state_inputs_share_20190522
from
(
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		total_units
	from
		relevant_dcp_projects_housing_pipeline_ms_v5
	where
		project_id like '%[ESD%'
) 	state_inputs_share_20190522
	order by
		project_id asc


select cdb_cartodbfytable('capitalplanning', 'state_inputs_share_20190522')
