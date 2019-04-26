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
	relevant_dcp_projects_housing_pipeline_ms
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
						) then 1 end as Additional_Rezoning,
			case when
				project_id in
						(
					     	select 
				      			zap_project_id 
				      		from 
				      			capitalplanning.public_sites_190410_ms 
				     		) then 1 end as Public_Sites_Project,
		       case when
				project_id like '%ESD%' then 1 end as State_Project
						
				
		from 
			dcp_project_flags
		where
			(
				-- no_si_seat = 1 and 
				(
				dwelling_units = 1 or 
				potential_residential = 1
				) 							and 
				historical_project_pre_2012 =0 				and
				project_status not in('Record Closed', 'Terminated') 	and
				project_status not like '%Withdrawn%' 			and
				applicant_type <> 'DCP' 				and
				project_id <> 'P2016Q0238'  /*Omitting DTFR rezoning from ZAP*/
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
				     (select 
				      	zap_project_id 
				      from 
				      	capitalplanning.public_sites_190410_ms 
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
			dwelling_units=0 			and
			additional_rezoning = 0 		and
			public_sites_project = 0		and
			state_project = 0
	),

	/*Adding in manually collected data on projects flagged Potential_Residential with confirmation of whether residential, total units, and flag for further research*/

	relevant_dcp_projects_1 as
	(
		select 
				a.*,
				b.confirmed_potential_residential,
				b.total_units_from_description as potential_residential_total_units,
				b.need_manual_research as need_manual_research_flag
	 	from 
	 		relevant_dcp_projects a
	 	left join
	 		potential_residential_zap_project_check_ms b
	 	on a.project_id = b.project_id
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
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		applicant_type,
		case when substring(lead_action,1,2) = 'ZM' then 1 else 0 end as Rezoning_Flag,
		case when project_id = 'P2017R0349' then 1 else senior_housing_flag end as senior_housing_flag, --Flagging additional senior housing project
		case when project_id = 'P2018X0001' then 1 else Assisted_Living_Supportive_Housing_flag end as Assisted_Living_Supportive_Housing_flag, --Flagging additional supportive housing project
		project_status,
		previous_project_status,
		process_stage_name_stage_id_process_stage as process_stage,
		previous_process_stage,
		anticipated_year_built,
		project_completed,
		certified_referred,
		system_target_certification_date as target_certified_date,
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
						case when residential_sq_ft/850 < 1 and residential_sq_ft is not null then 1 else residential_sq_ft/850 end /*Average DUs/sqft. Confirmed by T. Smith*/
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
		no_si_seat,
		dwelling_units as dwelling_units_flag,
		confirmed_potential_residential as potential_residential_flag,
		need_manual_research_flag,
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
		relevant_dcp_projects_2
	),

	/*Identifying DCP projects which are permit renewals or changes of previous DCP projects. Omitting the outdated DCP project*/

		matching_projects as
	(
		select
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
			relevant_dcp_projects_3 a
		inner join
			relevant_dcp_projects_3 b
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
				when total_units = match_total_units and total_units <> 1 							then 'Same units'
				when 				
					left(
						upper(project_name),position('STREET' IN upper(project_name))-1
					) = 
					left(
						upper(matched_project_name),position('STREET' IN upper(matched_project_name))-1
					) and
					position('STREET' in upper(project_name)) > 0 and 
					position('STREET' in upper(matched_project_name)) >0							then 'Same project name' 
																		
				
				when
					upper(project_name) like '%TWO BRIDGES%' and 
					match_project_id in('P2012M0479','P2014M0022')								then 'Two Bridges duplicate'																									
				when
					project_id = 'P2005M0053' and matched_project_name like '%DIB%'						then 'HY DIB' end 	as Confirmed_Match_Reason_Automatic,
			null																											as Confirmed_Match_Reason_Manual
		from
			matching_projects
	),

	/*Export matching_projects_1 and review non-confirmed matches > 50 units. Apply same values in confirmed_match_reason field to matches which are manually identified.
	  Reupload this dataset as lookup_zap_overlapping_projects_ms.
	*/

		relevant_projects_4 as
	(
		select
			*
		from
			relevant_dcp_projects_3
		where
			project_id not in
							(
								select match_project_id 
								from lookup_zap_overlapping_projects_ms 
								where 
									confirmed_match_reason_automatic 	in('Permit renewal','Same units','Same project name','Two Bridges duplicate') or
									confirmed_match_reason_manual 		in('Permit renewal','Same units','Same project name','Two Bridges duplicate') 
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
				when a.project_id = 'P2005M0053' then a.total_units - b.DIB_Units 
				else coalesce(a.total_units,c.total_units,c.aff_units) end as total_units_1
		FROM
			relevant_projects_4 a
		left join
			(
				select sum(match_total_units) as DIB_Units from lookup_zap_overlapping_projects_ms where Confirmed_Match_Reason_Automatic = 'HY DIB' or Confirmed_Match_Reason_Manual = 'HY DIB'
			) b
		on
			a.project_id = 'P2005M0053'
		left join
			capitalplanning.public_sites_190410_ms c
		on
			a.project_id = c.zap_project_id
	)

	/**********************************************************************************************************************************************************************
	Create new table based on following query titled "relevant_dcp_projects_housing_pipeline_ms." Project_ID P2005M0053 included to include Hudson Yards.
	Project_ID P2009M0294 included to include Western Rail Yards. Note that 16 observations are missing Total_Units. These must be collected from planners. 
	**********************************************************************************************************************************************************************/

		select
			* 
		from
			relevant_projects_5
) AS DCP_FINAL

update capitalplanning.relevant_dcp_projects_housing_pipeline_ms a
set total_units = total_units_1;

alter table capitalplanning.relevant_dcp_projects_housing_pipeline_ms
drop column total_units_1;

select cdb_cartodbfytable('capitalplanning', 'relevant_dcp_projects_housing_pipeline_ms')

/**********************************************************************************************************************************************************************
CHECKING QUERY: SQUARE FEET AND DWELLING UNITS

There are multiple ZAP projects remaining with Residential SQ FT but no total units in the data. The following query shows that the avg sqft per unit
for projects that had both, by borough, was as follows:

SI: 1180
BX: 854
Queens: 828
Manhattan: 816
BK: 289 (likely due to poor data)

SELECT
	borough,
	sum(total_units) as total_units,
	sum(residential_sq_ft) as res_sq_ft,
	sum(residential_sq_ft)/sum(total_units) as sq_ft_per_unit,
	count(*) as obs
FROM
	 relevant_projects_4 
where
	total_units is not null and
	residential_sq_ft is not null
group by
	borough 

/**********************************************************************************************************************************************************************
CHECKING QUERY: All projects have polygons thanks to manual lookup.

1. Outdated but approximate: Show that 53/444 projects are missing polygons.
	a. 	SELECT 	
				count(case when the_geom is null then 1 end) as missing_geom,
	   			count(*) 
		FROM 
				capitalplanning.relevant_dcp_projects_housing_pipeline_ms
2. Show # of polygons sourcing from each dataset. HEIP: 50; DCP_2018_SCA_Inputs_Share: 188; NYZMA: 2; Pluto: 182; Poly_Latest: 3
	a.	SELECT 
				sum(match_heip_geom) as heip_geom,
			    sum(match_dcp_2018_sca_inputs_share_geom) as DCP_2018_SCA_Inputs_Share_Geom,
		        sum(match_nyzma_geom) as NYZMA_Geom,
		       	sum(match_pluto_geom) as Pluto_Geom,
		       	sum(match_impact_poly_latest) as Poly_Latest,
		       	count(*) 
		FROM 
				capitalplanning.relevant_dcp_projects_housing_pipeline_ms
3.	OUTDATED: Show matching information between relevant_dcp_projects_housing_pipeline_ms and HEIP_ZAP_Polygons residential dataset. When matching on Project ID,
	the two datasets show only 93 matches. 
	a.	SELECT a.project_id, 
			   b.projectid
		FROM 	capitalplanning.relevant_dcp_projects_housing_pipeline_ms a
		full outer join
				capitalplanning.heip_zap_polygons b
		on 	a.project_id = b.projectid and
			a.project_id is not null
**********************************************************************************************************************************************************************/
