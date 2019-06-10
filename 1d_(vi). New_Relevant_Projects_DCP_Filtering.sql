/**************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Merging ZAP data cross borough, flagging residences, 
		and adding polygons
START DATE: 1/3/2018
COMPLETION DATE: 1/29/2018
SOURCE FILES:
1. dcp_zap_consolidated_ms: G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DCP_ZAP_Consolidated_MS.xlsx
	a. This is a consolidation of raw, by-borough ZAP project entities from: G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\ZAP 
2. HEIP ZAP Polygons: https://nycplanning.carto.com/u/capitalplanning/dataset/heip_zap_polygons
3. MAPPLUTO: https://nycplanning.carto.com/u/capitalplanning/dataset/mappluto_v_18v1_1
***************************************************************************************************************************************************************************************/
/**************************************************************************************************************************************************************************************
METHODOLOGY:
1. Omit areawide, non-residential, and historical projects, while including large rezonings like HY and WRY, as well as ZAP projects
   identified in Public Sites.
2. Identify potential residential projects and confirm whether they are residential. Merge lookup back in.
3. Assign consolidated total units value to each relevant project.
4. Deduplicate projects with multiple submissions such as permit renewals.

***************************************************************************************************************************************************************************************/



SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2_pre
from
(
	with relevant_dcp_projects as
	(
		select
			*,
			case 
			when
				project_id in 
						(
						'P2005M0053' /*Hudson Yards*/,
						'P2009M0294' /*Western Rail Yards*/,
						'P2014M0257' /*550 Washington St*/
						) then 1 else 0 end as Additional_Rezoning,
			
			/*Ensuring that additional Public Sites which have been manually found in ZAP are caught*/
			case 
				when project_id in
						(
					     	select 
				      			zap_project_id 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1	     
				when project_id in
						(
					     	select 
				      			zap_project_id_2 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1
				when project_id in
						(
					     	select 
				      			zap_project_id_3 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1
				when project_id in
						(
					     	select 
				      			zap_project_id_4
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1 else 0 end  as Public_Sites_Project,
		    case 
		    	when project_id like '%ESD%' then 1 else 0 end as State_Project
				
		from 
			dcp_project_flags_v2
		where
			(
				-- si_seat_cert = 0 and 
				(
				dwelling_units = 1 										or 
				potential_residential = 1
				) 														and 
				historical_project_pre_2012 =0 							and
				project_status not in('Record Closed', 'Terminated') 	and
				project_status not like '%Withdrawn%' 					and
				applicant_type <> 'DCP' 								and
				project_id <> 'P2016Q0238'  							and /*Omitting DTFR rezoning from ZAP*/
				project_id <> 'P2016R0149'								and	/*Omitting BSC rezoning from ZAP*/
				project_id <> 'P2012M0255'									/*Omitting Hudson Square rezoning from ZAP as it is no longer residential*/
				-- and pre_pas_flag<>1 and
				-- initiation_flag<>1
			) or
			/*Including large rezonings*/
			project_id in (
							'P2005M0053' /*Hudson Yards*/,
						   	'P2009M0294' /*Western Rail Yards*/,
						   	'P2014M0257' /*550 Washington St*/
				      ) or
			/*Including projects from Public Sites*/
			project_id in
				     (
				      select 
				      	zap_project_id 
				      from 
				      	capitalplanning.table_190510_public_sites_ms_v3 
				      where 
				      	zap_project_id not like 'P2016R0149' /*Omitting BSC rezoning identification in Public Sites*/
				     )  or
			project_id like '%[ESD Project]%' /*State project*/
		order by project_id

	),

	/*Export the following list of potential residential project IDs to create a lookup and merge back on, assessing whether these IDs are confirmed to be residential
	  and inputting in total units and source when possible*/

	potential_residential_projects as
	(
		select
			*
		from
			relevant_dcp_projects
		where
			dwelling_units=0 				and
			additional_rezoning = 0 		and
			public_sites_project = 0		and
			state_project = 0
	),

	/*Adding in manually collected data on projects flagged Potential_Residential with confirmation of whether residential, total units, and flag for further research*/

	relevant_dcp_projects_1 as
	(
		select 
				a.*,
				-- b.confirmed_potential_residential,
				coalesce(b.confirmed_potential_residential,c.confirmed_potential_residential) as confirmed_potential_residential,
				-- b.total_units_from_description as potential_residential_total_units,
				coalesce(b.total_units_from_description,c.units) as potential_residential_total_units
	 	from 
	 		relevant_dcp_projects a
	 	left join
	 		capitalplanning.potential_residential_zap_project_check_ms b
	 	on 
	 		a.project_id = b.project_id
	 	left join
	 	 	capitalplanning.table_20190510_potential_residential_project_check_ms_v2 c /*This is a list of projects identified as potentially residential, which
	 	 																			   existed in the new ZAP pull but not the old ZAP pull. Take the full
	 	 																			   potential residential project list and only selecting the projects
	 	 																			   which exist in the new list but not the old list*/ 
	 	on
	 		a.project_id = c.project_id
	),

	/*Limiting to projects which have confirmed dwelling units, or are the additional
	  large rezonings (HY, WRY, 550 Washington), Public Sites Projects, or State Projects*/
	relevant_dcp_projects_2 as
	(
		select
			*
		from
			relevant_dcp_projects_1
		where 
			dwelling_units = 1 or
			confirmed_potential_residential = 1 or
			additional_rezoning = 1 or
			public_sites_project=1 or
			state_project = 1
		order by
			project_id
	),

	relevant_dcp_projects_3 as
	(
		select
			row_number() over() as cartodb_id /*Important to generate CartoDB_ID for mapping purposes*/,
			a.the_geom,
			a.the_geom_webmercator,
			project_id,
			project_name,
			borough, 
			project_description,
			project_brief,
			applicant_type,
			case when substring(lead_action,1,2) = 'ZM' then 1 else 0 end as Rezoning_Flag,
			project_status,
			previous_project_status,
			process_stage_name_stage_id_process_stage as process_stage,
			previous_process_stage,
			anticipated_year_built,
			project_completed,
			certified_referred,
			dcp_target_certification_date,
			system_target_certification_date,
			coalesce(dcp_target_certification_date,system_target_certification_date) as target_certified_date,
			ulurp_non_ulurp as ULURP,
			project_status_date as latest_status_date,

		/*	The following field is calculated by coalescing total units taken from the description of projects deemed residential using text search and
			the total units listed in the original data. There is never a case where both fields exist because the lookup for projects deemed residential
			using text search was manually created by picking text-search projects with no associated units in the data. For projects with residential
			square feet but no associated units, 1 DU assigned per 850 SQFT. See first CHECKING QUERY below for justification.

			NOTE: 2 PROJECTS (P2014R0246 AND P2015Q0366) are flagged as dwelling unit projects but have new_dwelling_units listed as 0, residential
				  sqft >0, and all other unit fields as null. After reading the project descriptions, assuming these projects are correctly coded.

		*/

			case 
				when
					new_dwelling_units 							is null and
					total_dwelling_units_in_project 			is null and
					mih_dwelling_units_higher_number 			is null and
					voluntary_affordable_dwelling_units_non_mih is null and
					mih_dwelling_units_lower_number 			is null and
					potential_residential_total_units 			is null and
					residential_sq_ft							is null 	then 	null




				else
					coalesce(
							potential_residential_total_units, /*Preferencing manual lookup additions by MS*/
							case when new_dwelling_units <> 0 then new_dwelling_units else null end, 
							total_dwelling_units_in_project,
							case when 
									coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) <> 0 then 
									coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) else null end,
									/*	Two observations where voluntary affordable units is listed and total_dwelling_units is not, and the voluntary_affordable units are confirmed by the project
										description.*/
							case when residential_sq_ft/1000 < 1 and residential_sq_ft is not null then 1
								 when b.the_geom is not null then residential_sq_ft/850
								 else residential_sq_ft/1000 end /*1,000 sqft/du except for 850 sqft/du in Manhattan Core*/
							) end 

																																									as total_units,

			case
				when potential_residential_total_units is not null 																									then 'Potential Residential Total Units'
				when new_dwelling_units not in(null, 0) 																											then 'New Dwelling Units'
				when total_dwelling_units_in_project is not null 																									then 'Total Dwelling Units in Project'
				when coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) <> 0	then 'MIH + Voluntary Affordable Units'
				when residential_sq_ft is not null 																													then 'Residential Square Feet' end 

																																									as total_units_source,
			remaining_likely_to_be_built,
			rationale,
			si_seat_cert,
			dwelling_units as dwelling_units_flag,
			confirmed_potential_residential as potential_residential_flag,
			initiation_flag,
			pre_pas_flag,
			Diff_Between_Total_and_New_Units,
			Historical_Project_Pre_2012,
			Historical_Project_Pre_2008,
			match_heip_geom,
			match_dcp_2018_sca_inputs_share_geom,
			match_nyzma_geom,
			match_pluto_geom,
			match_impact_poly_latest
	from 
		relevant_dcp_projects_2 a
	left join
		capitalplanning.manhattan_cbd b
	on
		st_intersects(a.the_geom,b.the_geom) 
	)
	select * from relevant_dcp_projects_3
) x

SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2
from
(
	/*Identifying DCP projects which are permit renewals or changes of previous DCP projects. Omitting the outdated DCP project*/

	with	matching_projects as
	(
		select
			a.cartodb_id,
			a.project_id,
			b.project_id as match_project_id,
			a.project_name,
			b.project_name as matched_project_name,
			a.project_description,
			b.project_description as match_project_description,
			a.borough,
			b.borough as match_borough,
			coalesce(a.certified_referred,a.target_certified_date) as date,
			coalesce(b.certified_referred,b.target_certified_date) as match_date,
			a.total_units,
			b.total_units as match_total_units,
			st_distance(a.the_geom,b.the_geom) as Distance
		from
			relevant_dcp_projects_housing_pipeline_ms_v2_pre a
		inner join
			relevant_dcp_projects_housing_pipeline_ms_v2_pre b
		on 
			coalesce(a.certified_referred,a.target_certified_date) >= coalesce(b.certified_referred,b.target_certified_date) and
			a.project_id <> b.project_id and
			(
				st_intersects(a.the_geom,b.the_geom) 				or
				position(upper(b.project_name) in upper(a.project_name)) > 0 	or
				position(upper(a.project_name) in upper(b.project_name)) > 0
			) 									or
			(
				a.project_id = 'P2005M0053' and /*Matching Hudson Yards to DIB projects which are subsets of the total HY unit count*/ 
				b.project_name like '%DIB%'
			)									or
			(
				upper(a.project_name) like '%TWO BRIDGES%' and /*Matching Two Bridges parking and healthcare chaplaincy projects to their residential project counterparts. Will omit in next steps*/
				b.project_id in('P2012M0479','P2014M0022') and
				a.project_id <> b.project_id
			)

	),

		matching_projects_1 as
	(
		select
			*,
			case 
				when position('PERMIT RENEWAL' in upper(concat(project_name,project_description))) > 0 				then 'Permit renewal'
				
				/*Omitting selecting projects where total units are 1 due to small-project variance.*/
				when total_units = match_total_units and total_units <> 1 											then 'Same units'
				when 				
					left(
						upper(project_name),position('STREET' IN upper(project_name))-1
					) = 
					left(
						upper(matched_project_name),position('STREET' IN upper(matched_project_name))-1
					) and
					position('STREET' in upper(project_name)) > 0 and 
					position('STREET' in upper(matched_project_name)) >0											then 'Same project name' 
																		
				
				when
					upper(project_name) like '%TWO BRIDGES%' and 
					match_project_id in('P2012M0479','P2014M0022')													then 'Two Bridges duplicate'																									
				when
					project_id = 'P2005M0053' and matched_project_name like '%DIB%'									then 'HY DIB' end 	as Confirmed_Match_Reason_Automatic,
			null																														as Confirmed_Match_Reason_Manual
		from
			matching_projects
	),



	/*Export matching_projects_1 and review non-confirmed matches > 50 units. Apply same values in confirmed_match_reason field to matches which are manually identified.
	  Reupload this dataset as lookup_zap_overlapping_projects_ms and the updated ZAP pull (excluding projects which existed in the old ZAP pull) as lookup_zap_overlaps_ms_v2 .
	*/

		relevant_projects_4 as
	(
		select
			*
		from
			relevant_dcp_projects_housing_pipeline_ms_v2_pre
		where
			project_id not in
							(
								select match_project_id 
								from lookup_zap_overlapping_projects_ms 
								where 
									confirmed_match_reason_automatic 	in('Permit renewal','Same units','Same project name','Two Bridges duplicate') or
									confirmed_match_reason_manual	    = 1 
							) and
			project_id not in
							(
								select match_project_id 
								from lookup_zap_overlapping_projects_ms_v2 
								where 
								--	confirmed_match_reason_automatic 	in('Permit renewal','Same units','Same project name','Two Bridges duplicate') or
									confirmed_match_reason_manual	    = 1 
							)
		order by project_id
	),

	/*Subtracting DIB units from HY, and adding in total units for ZAP projects in Public Sites which 
	  do not have listed units in ZAP.*/
		relevant_projects_5 as
	(
		SELECT
			a.*,
			case 
				when a.project_id = 'P2005M0053' then a.total_units - coalesce(b.DIB_Units,0) 
				else a.total_units end 									as total_units_1
		FROM
			relevant_projects_4 a
		left join
			(
				select sum(match_total_units) as DIB_Units from lookup_zap_overlapping_projects_ms where Confirmed_Match_Reason_Automatic = 'HY DIB' 
			) b
		on
			a.project_id = 'P2005M0053'
/*		left join
			capitalplanning.public_sites_190410_ms c
		on
			a.project_id = c.zap_project_id
*/	)

	/**********************************************************************************************************************************************************************
	Create new table based on following query titled "relevant_dcp_projects_housing_pipeline_ms." Project_ID P2005M0053 included to include Hudson Yards.
	Project_ID P2009M0294 included to include Western Rail Yards. Note that 16 observations are missing Total_Units. These must be collected from planners. 
	**********************************************************************************************************************************************************************/

	/*Comparing the ZAP Pull of 5/10/2019 to the ZAP Pull of January 2019. Note to MQL about comparison below:

	We have unit counts for all but one new project and geometries for all but 3 new projects.

	The new ZAP pull omits 23 projects which existed in the old ZAP Pull. 16 of these are due to application withdrawals/record closures. 5 are due to overlaps (which have only recently been factored into the script). The remaining two are non site-specific projects which previously had listed unit counts but no longer do.

	The new ZAP pull has 24 new projects (which is within reason -- in the old pull, we have 585 projects over 7 years of data). At first glance, some of these projects are relevant to neighborhood rezonings, a public sites RFP we've been looking for more information for, and there's a BSC PLACES application for 2,557 units (which I assume should be omitted because it's areawide).

	9 existing projects have had changed unit counts > 20 in the last half year. All the changes look within reason. 58 existing projects have had status changes (primarily changes from Active to either On-Hold or Complete).*/

		select
			a.*,
			case when b.project_id is not null then 1 else 0 end 	as In_Previous_ZAP,
			b.total_units 											as previous_zap_total_units,
			b.project_status 										as previous_zap_project_status
		from
			relevant_projects_5 a
		left join
			relevant_dcp_projects_housing_pipeline_ms b
		on
			a.project_id = b.project_id

) AS DCP_FINAL

/**********************RUN IN REGULAR CARTO**************************/


select cdb_cartodbfytable('capitalplanning', 'relevant_dcp_projects_housing_pipeline_ms_v2')


