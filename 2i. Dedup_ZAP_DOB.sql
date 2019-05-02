
/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from DOB projects
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to DOB projects using Address and spatial overlap. Proximity matches excluded due to low accuracy (2/35 accurate matches
	within 20 meters) and high number of manual exclusions.
2. If a DOB job maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select 
	* 
into 
	zap_dob
from
	(
	select
		a.*,
		case 
			when
				position(upper(b.address) in upper(a.project_name)) > 0 and
				case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) END 	then 'Address' /*The avg distance assocaited with address matches is .45 meters*/
			when
				st_intersects(a.the_geom,b.the_Geom)											then 'Spatial'
			when
				b.job_number is not null												then 'Proximity' end 	as DOB_Match_Type, /*Lookup shows that 2/35 proximity matches >=50 units are accurate. 
																																											  Given that <50 units makes a lookoup far more intensive, omitting proximity
																																											  matching*/

		st_distance(CAST(a.the_geom AS GEOGRAPHY),CAST(b.the_geom AS GEOGRAPHY))								as DOB_Distance,
		b.job_number 																as dob_job_number,
		b.units_prop 																as dob_prop_units,
		b.units_net 																as dob_net_units,
		b.job_type 																as dob_job_type,
		b.address 																as dob_address,
		b.pre_filing_year 															as dob_pre_filing_year,
		b.full_permit_issued_date																															
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on
		(
				st_intersects(a.the_geom,b.the_geom) or
			(
				position(upper(b.address) in upper(a.project_name)) > 0 and
				case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) end
			)
		) 
		and
		b.job_type <> 'Demolition' and not
		(a.project_id = 'P2012M0635' and b.job_number = 120481246) /*Manual removal -- previously this code had
									 matched 625 W 57th St DOB job to a 606 W 57th
									 street DCP rezoning. This was an inaccurate match,
									 but I cannot currently think through a logic to 
									 automate this.*/
	) as Raw_Merge




/*********************************************************************************
For DOB jobs with multiple matches, preferring the match using the following logic: 

1. If the DOB job is matched to multiple projects, but matched to some by address
	and others spatially, the address match is preferred.
2. If the above does not occur, then the match with the latest DCP update is
	preferred.
**********************************************************************************/
SELECT
	*
into
	zap_dob_2
from
(
	/*Filter to a list of instances where a DOB projects maps to multiple ZAP projects. Count
	the number of times this DOB projects map by address and spatially*/
	with multi_dob_matches as 
(
	select
		dob_job_number,
		count(*) as count,
		max(coalesce(latest_status_date,target_certified_date)) as latest_date,
		sum(case when DOB_Match_Type = 'Address' then 1 end) 	as Address_Matches,
		sum(case when DOB_Match_Type = 'Spatial' then 1 end) 	as Spatial_Matches
	from
		zap_dob
	where
		dob_job_number is not null
	group by
		dob_job_number
	having
		count(*) > 1
),

	/*Use the above created lookup to preference address matches over spatial matches, and new projects over older projects*/
	zap_dob_1 as
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.project_description,
		a.project_brief,
		a.borough,		
		a.applicant_type,
		a.rezoning_flag,
		a.project_status,
		a.process_stage,
		a.project_completed,
		a.certified_referred,
		a.target_certified_date,
		a.latest_status_date,
		a.dwelling_units_flag,
		a.potential_residential_flag,
		a.pre_pas_flag,
		a.initiation_flag,
		case when a.Diff_Between_Total_and_New_Units > 0 then 1 end as Total_DNE_New_Units_Flag,
		a.total_units,
		a.dob_pre_filing_year,
		a.full_permit_issued_date,
		a.dob_address,
		a.anticipated_year_built as projected_build_year,
		/*The following lines erase matches for multi-matched DOB jobs that are not preferenced*/
		case 
			when a.dob_job_number is not null and b.address_matches < b.spatial_matches and b.address_matches<>0 and a.dob_match_type <> 'Address'
			then null
			when a.dob_job_number is not null and coalesce(a.latest_status_date,a.target_certified_date) <> b.latest_date
			then null 
			else a.dob_job_number end as dob_job_number,
		case 
			when a.dob_job_number is not null and b.address_matches < b.spatial_matches and b.address_matches<>0 and a.dob_match_type <> 'Address'
			then null
			when a.dob_job_number is not null and coalesce(a.latest_status_date,a.target_certified_date) <> b.latest_date
			then null 
			else a.dob_prop_units end as dob_prop_units,
		case 
			when a.dob_job_number is not null and b.address_matches < b.spatial_matches and b.address_matches<>0 and a.dob_match_type <> 'Address'
			then null
			when a.dob_job_number is not null and coalesce(a.latest_status_date,a.target_certified_date) <> b.latest_date
			then null
			else a.dob_net_units end as dob_net_units,
		case 
			when a.dob_job_number is not null and b.address_matches < b.spatial_matches and b.address_matches<>0 and a.dob_match_type <> 'Address'
			then null
			when a.dob_job_number is not null and coalesce(a.latest_status_date,a.target_certified_date) <> b.latest_date
			then null
			else a.dob_job_type end as dob_job_type,
		a.dob_match_type,
		a.dob_distance		
	from
		zap_dob a
	left join
		multi_dob_matches b
	on
		a.dob_job_number = b.dob_job_number
),

	/*Aggregate matches to the ZAP project level*/
	zap_dob_2 as
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.project_description,
		a.project_brief,
		a.borough,		
		array_to_string(array_agg(a.dob_address),', ')  	as dob_addresses, /*Diagnostic field*/
		a.applicant_type,
		a.rezoning_flag,
		a.project_status,
		a.process_stage,
		a.project_completed,
		a.certified_referred,
		a.target_certified_date,
		a.latest_status_date,
		a.dwelling_units_flag,
		a.potential_residential_flag,
		a.pre_pas_flag,
		a.initiation_flag,
		a.Total_DNE_New_Units_Flag,
		a.total_units,
		a.projected_build_year,
		array_to_string(array_agg(a.dob_job_number),', ')  	as dob_job_numbers,
		sum(
			coalesce(
						CASE WHEN a.dob_net_units < 0 then a.dob_prop_units else a.dob_net_units end,
						0
					) /*Converting <0 net unit jobs to units_prop because the three jobs where this is the case (all alterations-- 421204967, 110170378, 320912367) match with ZAP projects which
						list total_units as not the change in units, but the resulting units of the proposed project. This change will allow these ZAP projects
						to be correctly deduplicated.
					  */   
			) as dob_net_units
	from
		zap_dob_1 a
	group by
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.project_description,
		a.project_brief,
		a.borough,		
		a.applicant_type,
		a.rezoning_flag,
		a.project_status,
		a.process_stage,
		a.project_completed,
		a.certified_referred,
		a.target_certified_date,
		a.latest_status_date,
		a.dwelling_units_flag,
		a.potential_residential_flag,
		a.pre_pas_flag,
		a.initiation_flag,
		a.Total_DNE_New_Units_Flag,
		a.total_units,
		a.projected_build_year

)

select * from zap_dob_2

) as zap_dob_final

