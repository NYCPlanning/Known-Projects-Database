/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped ZAP dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate ZAP matches to DOB, HPD Projected Closings, HPD RFP, and EDC data.
2. Calculate ZAP increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

/*Remember to change the column ordering here to align with what you saw in the relevant projects list sent to SCA and DOE*/

select
	*
into
	zap_deduped
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		'DCP Applications' as Source,
		a.project_id,
		a.project_name,
		a.dcp_edit_project_status as status,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		greatest
				(
					0,
					a.total_units 										-
					coalesce(b.dob_units_net,0) 						-
					coalesce(c.hpd_project_incremental_units,0)			-
					coalesce(d.hpd_rfp_incremental_units,0)				-
					coalesce(e.edc_incremental_units,0)
				) as zap_incremental_units,
		a.applicant_type,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.ulurp,
		a.Anticipated_year_built as applicant_projected_build_year,
		a.early_stage_flag,
		a.si_seat_cert,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		case when a.Senior_Housing_Flag = 1 then 1 else 0 end as Senior_Housing_Flag,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then 1 else 0 end as Planner_Provided_Phasing,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2025,0) as portion_built_2025,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2035,0) as portion_built_2035,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2055,0) as portion_built_2055,
		a.planner_input,
		b.dob_job_numbers,
		b.dob_units_net,
		c.hpd_project_ids as hpd_projected_closings_ids,
		c.hpd_project_incremental_units as hpd_projected_closings_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_incremental_units,
		e.edc_project_ids,
		e.edc_incremental_units
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.zap_dob_final b
	on 
		a.project_id = b.project_id 
	left join
		capitalplanning.zap_hpd_projected_closings_final c 
	on
		a.project_id = c.project_id 
	left join
		capitalplanning.zap_hpd_rfps_final d
	on
		a.project_id = d.project_id 
	left join
		capitalplanning.zap_edc_final e
	on
		a.project_id = e.project_id
) zap_deduped



/********************************RUN IN REGULAR CARTO************************************************/

create table
	zap_deduped_build_year
as
(
	select
		row_number() over() as cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.Source,
		a.project_id,
		a.project_name,
		a.status,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		a.zap_incremental_units,
		a.applicant_type,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.ulurp,
		b.lead_action,
		a.applicant_projected_build_year,
		c.remaining_likely_to_be_built as remaining_likely_to_be_built_2018,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0
																												 	then
																												 	1
			else 																									0 end as Planner_Provided_Phasing,
		/*Assigning 2025 Portion Built*/
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0
																												 	then
																													coalesce(a.portion_built_2025,0)
			/*Adding in HY Phasing, taken from 2018 planner input*/
			when a.project_id = 'P2005M0053'																		then
																													.2
			/*Adding in WRY Phasing, taken from 2018 planner input*/			
			when a.project_id = 'P2009M0294'																		then
																													.2
			/*Adding in Pfizer Sites Phasing.*/			
			when a.project_id = 'P2013K0309'																		then
																													.5
			/*Adding in Peninsula phasing, taken from Peninsula EIS documents.*/			
			when a.project_id = 'P2016Q0306'																		then
																													862::float/2200::float

			when c.remaining_likely_to_be_built = 'No' and a.zap_incremental_units >= c.remaining_dcp_units
																													then 
																													0

			when (a.total_units > 10 and a.total_units::float*.2 > a.zap_incremental_units::float)
																													then 
																													1
			when (a.total_units <= 10 and a.total_units - a.zap_incremental_units > 3)
																													then 
																													1

			/*Adding in conditions for non-ULURP projects. Note that some non-ULURP FRESH projects are labeled
				as ULURP*/																													
			when concat(a.project_name,a.project_description,a.project_brief) 			like '%FRESH%'					then 1
								
			/*Adding in conditions for non-ULURP subdivisions and school seat certs*/
			when 	a.ulurp = 'Non-ULURP' and
					concat(a.project_name,a.project_description,a.project_brief) 			like '%SD%' or 								
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%SS%' or								
				 	(upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBDIVISION%' and
				  	a.project_id like '%R%')																			then 1
		
			/*Adding in conditions for non-ULURP subway and MTA projects*/
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBWAY%' or
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%MTA%'					then 0

			/*Adding in conditions for non-ULURP waterfront*/
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%WATERFRONT%'				then 1

			when 
				a.status in('Complete','Active, Certified')		and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2025)				then  
																													1
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year between 2025 and 2035												then  
																													0
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year > 2035																then  
																													0

			when a.status in ('Active, Initiation','Active, Pre-PAS')												then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				(a.applicant_projected_build_year <=2025 or a.applicant_projected_build_year is null)				then
																													1
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year between 2025 and 2035												then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year > 2035																then
																													0	
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				(a.applicant_projected_build_year	<=2035 or a.applicant_projected_build_year is null)				then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				a.applicant_projected_build_year	>2035															then
																													0
			when 
				a.status like '%On-Hold%'						and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2035)				then  
																													0
			when 
				a.status like '%On-Hold%'						and
				a.applicant_projected_build_year > 2035																then  
																													0
			else
																													null
			END 																									as portion_built_2025,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0
																												 	then
																													coalesce(a.portion_built_2035,0)
			/*Adding in HY Phasing, taken from 2018 planner input*/
			when a.project_id = 'P2005M0053'																		then
																													.8
			/*Adding in WRY Phasing, taken from 2018 planner input*/			
			when a.project_id = 'P2009M0294'																		then
																													.8
			/*Adding in Pfizer Sites Phasing.*/			
			when a.project_id = 'P2013K0309'																		then
																													.5
			/*Adding in Peninsula phasing, taken from Peninsula EIS documents.*/			
			when a.project_id = 'P2016Q0306'																		then
																													1338::float/2200::float
			
			when c.remaining_likely_to_be_built = 'No' and a.zap_incremental_units >= c.remaining_dcp_units
																													then 
																													0
			when (a.total_units > 10 and a.total_units::float*.2 > a.zap_incremental_units::float)
																													then 
																													0
			when (a.total_units <= 10 and a.total_units - a.zap_incremental_units > 3)								then
																													0
			/*Adding in conditions for non-ULURP projects. Note that some non-ULURP FRESH projects are labeled
				as ULURP*/																													 
			when concat(a.project_name,a.project_description,a.project_brief) 			like '%FRESH%'					then 0
								
			/*Adding in conditions for non-ULURP subdivisions and school seat certs*/
			when 	a.ulurp = 'Non-ULURP' and
					concat(a.project_name,a.project_description,a.project_brief)		like '%SD%' or 								
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%SS%' or								
				 	(upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBDIVISION%' and
				  	a.project_id like '%R%')																			then 0
		
			/*Adding in conditions for non-ULURP subway and MTA projects*/		
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBWAY%' or
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%MTA%'					then 1

			/*Adding in conditions for non-ULURP waterfront*/
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%WATERFRONT%'				then 0
																													
	
			when 
				a.status in('Complete','Active, Certified')		and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2025)				then  
																													0
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year between 2025 and 2035												then  
																													1
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year > 2035																then  
																													0

			when a.status in ('Active, Initiation','Active, Pre-PAS')												then
																													1
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				(a.applicant_projected_build_year <=2025 or a.applicant_projected_build_year is null)				then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year between 2025 and 2035												then
																													1
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year > 2035																then
																													0	
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				(a.applicant_projected_build_year	<=2035 or a.applicant_projected_build_year is null)				then
																													1
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				a.applicant_projected_build_year	>2035															then
																													0
			when 
				a.status like '%On-Hold%'						and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2035)				then  
																													1
			when 
				a.status like '%On-Hold%'						and
				a.applicant_projected_build_year > 2035																then  
																													0
			else
																													null
			END 																									as portion_built_2035,
		case
			when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0
																												 	then
																													coalesce(a.portion_built_2055,0)
			/*Adding in HY Phasing, taken from 2018 planner input*/
			when a.project_id = 'P2005M0053'																		then
																													0
			/*Adding in WRY Phasing, taken from 2018 planner input*/			
			when a.project_id = 'P2009M0294'																		then
																													0
			/*Adding in Pfizer Sites Phasing.*/			
			when a.project_id = 'P2013K0309'																		then
																													0
			/*Adding in Peninsula phasing, taken from Peninsula EIS documents.*/			
			when a.project_id = 'P2016Q0306'																		then
																													0
			when c.remaining_likely_to_be_built = 'No' and a.zap_incremental_units >= c.remaining_dcp_units
																													then 
																													1
			when (a.total_units > 10 and a.total_units::float*.2 > a.zap_incremental_units::float)
																													then 
																													0
			when (a.total_units <= 10 and a.total_units - a.zap_incremental_units > 3)								then
																													0
			/*Adding in conditions for non-ULURP projects. Note that some non-ULURP FRESH projects are labeled
				as ULURP*/																													 
			when concat(a.project_name,a.project_description,a.project_brief) 			like '%FRESH%'					then 0
								
			/*Adding in conditions for non-ULURP subdivisions and school seat certs*/
			when 	a.ulurp = 'Non-ULURP' and
					concat(a.project_name,a.project_description,a.project_brief) 			like '%SD%' or 								
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%SS%' or								
				 	(upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBDIVISION%' and
				  	a.project_id like '%R%')																			then 0
		
			/*Adding in conditions for non-ULURP subway and MTA projects*/
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%SUBWAY%' or
				 	concat(a.project_name,a.project_description,a.project_brief) 			like '%MTA%'					then 0

			/*Adding in conditions for non-ULURP waterfront*/
			when 	a.ulurp = 'Non-ULURP' and
					upper(concat(a.project_name,a.project_description,a.project_brief)) 	like '%WATERFRONT%'				then 0
																													
	
			when 
				a.status in('Complete','Active, Certified')		and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2025)				then  
																													0
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year between 2025 and 2035												then  
																													0
			when 
				a.status in('Complete','Active, Certified')		and
				a.applicant_projected_build_year > 2035																then  
																													1

			when a.status in ('Active, Initiation','Active, Pre-PAS')												then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				(a.applicant_projected_build_year <=2025 or a.applicant_projected_build_year is null)				then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year between 2025 and 2035												then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 
				a.dcp_target_certification_date is not null 	and
				a.applicant_projected_build_year > 2035																then
																													1	
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				(a.applicant_projected_build_year	<=2035 or a.applicant_projected_build_year is null)				then
																													0
			when 
				a.status = 'Active, Pre-Cert'					and 	
				a.dcp_target_certification_date is null 		and
				a.applicant_projected_build_year	>2035															then
																													1
			when 
				a.status like '%On-Hold%'						and
				(a.applicant_projected_build_year is null or a.applicant_projected_build_year <=2035)				then  
																													0
			when 
				a.status like '%On-Hold%'						and
				a.applicant_projected_build_year > 2035																then  
																													1
			else
																													null
			END 																									as portion_built_2055,
		a.early_stage_flag,
		a.si_seat_cert,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		a.Senior_Housing_Flag,
		a.planner_input,
		a.dob_job_numbers,
		a.dob_units_net,
		a.hpd_projected_closings_ids,
		a.hpd_projected_closings_incremental_units,
		a.hpd_rfp_ids,
		a.hpd_rfp_incremental_units,
		a.edc_project_ids,
		a.edc_incremental_units
	from
		zap_deduped a
	left join
		dcp_2018_sca_inputs_share c
	on
		a.project_id = c.project_id 
	left join
		dcp_project_flags_v2 b
	on
		a.project_id = b.project_id
	order by 
		a.project_id asc
)


/*RUN IN REGULAR CARTO*/

select cdb_cartodbfytable('capitalplanning','zap_deduped')


/*Talk to MQL! There are 8 projects, 6 of which have remaining units and 4 of which are complete, that planners in 2018 said would not materialize. Given their
  track record here, I suggest we omit this qualification*/

  select * from zap_deduped_1_test where remaining_likely_to_be_built_2018 = 'No'


/*
	There are 23 incomplete non-materialized non-ULURP projects. Only 12 projects > 50 units, I recommend we focus on these.
	3 of these projects are two bridges. THIS HELPED ME REALIZE THAT MAP ID 85339 IS THE BETTER POLYGON FOR ZAP PROJECT 2019K0219, which overlaps with
	P2012K0231. MAKES SENSE TO INCLUDE THE PLANNER ADDED PROJECT WITH THE BETTER POLYGON, LIKE LIC WATERFRONT. DELETE THESE OTHER TWO ZAP PROJECTS. YOU COULD
	RELABEL THEM TO 0 IN THE UNIDENTIFIED ZAP PROJECT LOOKUP, OR YOU COULD JUST MANUALLY DELETE. I'M INCLINED TO MANUALLY DELETE. I see no information on any other developments,
	so I'm inclined to do 50-50 2035/2055.

	WE ALSO NEED PHASING INFO FOR GREENPOINT LANDING.
*/ 

/*The rest of the projects > 50 units look appropriately assigned. Consider adding a criteria for FRESH in 2025 and MTA in 2035. MTA does not 
seem like a big deal here*/

  select * from zap_deduped_1_test where zap_incremental_units > 0 and ulurp = 'Non-ULURP' and status <> 'Complete'

  select * from zap_deduped_1_test where upper(concat(project_name,project_description,project_brief)) like '%MTA%'


 /*
 There are 8 projects where the planner did not provide phasing this year and last year the planner wrote that remaining units would likely not be built.
 2 of these projects have been completely built, and their matches have been confirmed (P2012X0215: Soundview Partners, P2016Q0479: Northeastern Towers Annex Rezoning). 2 of these projects, have
 materialized (but not completely) despite a planner saying they would not be built last year.
 P2015M0454: 207th Street Rezoning, has had ~600 units materialize in the DOB data since last year. 
 P2015M0488: West 108th St WSFSSH, has had ~230 units materialize in the DOB data since last year.
 
 Modifying the script above to only use 2018 planner "Remaining Likely to be Built" comment if no units have been built since.

*/
 select * from zap_deduped_1_test where planner_provided_phasing = 0 and remaining_likely_to_be_built_2018 = 'No'