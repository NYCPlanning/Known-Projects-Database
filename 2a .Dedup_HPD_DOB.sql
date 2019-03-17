/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD data with DOB, EDC, ZAP, and DEP data
START DATE: 1/8/2019
COMPLETION DATE: 
Sources: dob_2018_sca_inputs_ms, hpd_2018_sca_inputs_ms , address_checking_for_hpd_dob_matches
		 relevant_dcp_projects_housing_pipeline_ms, dep_ndf_by_site,
		 edc_2018_sca_input_1_limited
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Join DOB data to HPD data by address, bbl, and spatially.
2. Create lookup to examine matches which do not intersect nor have the same address, but are within 20 meters of each other.
	Manually confirm the accuracy of these matches.
3. Import the lookup from step 2 and modify dataset from step 1 accordingly.
*************************************************************************************************************************************************************************************/
/**********************RUN THE FOLLOWING QUERIES IN CARTO BATCH******************************/
select
	*
into
	hpd_dob_match
from
(
	select
		a.*,
		case when b.address is not null 								then 1 end	as Address_Match,
		case when c.bbl is not null 									then 1 end	as bbl_match,
		case when 	st_intersects(a.the_geom,d.the_geom)						then 1 end	as spatial_match,
		case when 	st_dwithin(cast(a.the_geom as geography),cast(d.the_geom as geography),20) 	then 1 end	as proximity_match,
		coalesce(b.the_geom,c.the_geom,d.the_geom) 									as dob_geom,
		coalesce
			(	
				st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)),
				st_distance(cast(a.the_geom as geography),cast(c.the_geom as geography)),
				st_distance(cast(a.the_geom as geography),cast(d.the_geom as geography))
			) 																								as Geom_Distance,
		coalesce(b.job_number,c.job_number,d.job_number) 								as dob_job_number,
		coalesce(b.job_type,c.job_type,d.job_type) 									as dob_job_type,
		coalesce(b.address,c.address,d.address) 									as dob_address,
		coalesce(b.status,c.status,d.status) 										as dob_status,
		coalesce(b.units_init,c.units_init,d.units_init) 								as units_init,
		coalesce(b.units_prop,c.units_prop,d.units_prop) 								as units_prop,
		coalesce(b.units_net,c.units_net,d.units_net) 									as units_net,
		coalesce(b.units_incomplete,c.units_incomplete,d.units_incomplete) 						as units_incomplete,
		coalesce(b.latest_cofo,c.latest_cofo,d.latest_cofo) 								as latest_cofo,
		coalesce(b.most_recent_status_date,c.most_recent_status_date,d.most_recent_status_date) 			as most_recent_status_date,
		coalesce(b.completed_application_date,c.completed_application_date,d.completed_application_date) 		as completed_application_date,
		coalesce(b.full_permit_issued_date,c.full_permit_issued_date,d.full_permit_issued_date) 			as full_permit_issued_date,
		coalesce(b.partial_permit_issued_date,c.partial_permit_issued_date,d.partial_permit_issued_date) 		as partial_permit_issued_date,
		coalesce(b.job_completion_date,c.job_completion_date,d.job_completion_date) 					as job_completion_date
	from 
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join 
		capitalplanning.address_checking_for_hpd_dob_matches e
	on
		a.address= e.hpd_unmatched_in_construction_addresses 	and
		e.dob_address <> '' 					and 
		a.project_name = e.hpd_project_name
	left join
		capitalplanning.dob_2018_sca_inputs_ms b 
	on 
		upper(concat(coalesce(e.dob_address,a.address),' ',a.borough)) =
		upper(concat(b.address,' ',b.borough)) 			and
		a.address is not null 					and
		b.job_type<>'Demolition'
	left join
		capitalplanning.dob_2018_sca_inputs_ms c
	on 
		a.bbl = c.bbl 		and
		a.bbl is not null 	and 
		b.address is null 	and
		c.job_type<>'Demolition'
	left join
		capitalplanning.dob_2018_sca_inputs_ms d
	on
		st_dwithin(cast(a.the_geom as geography),cast(d.the_geom as geography),20) /*Meters*/ 	and
		a.latitude 	is not null 																and
		b.address 	is null 																	and
		c.bbl 		is null 																	and
		d.job_type = 'New Building' 
	where 
		a.source = 'HPD Projects'
	order by
		a.project_id
) as HPD_DOB_Merge


/*EXPORT THE FOLLOWING QUERY AS HPD_DOB_PROXIMATE_MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
(
	select
		*
	from
		hpd_dob_match
	where
		proximity_match = 1 and
		units_net<>total_units
	order by
		geom_distance asc
)

/*End of lookup creation*/

/*Use above lookup to (reimported from Excel) to delete inaccurate proximity matches.*/			  
			  
select
		      *
into
			  hpd_dob_match_2
from
(	      
	select
		the_geom,
		the_geom_webmercator,
		project_id 							as unique_project_id,
		hpd_project_id,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null

			when Address_Match 		= 1	then 'Address'
			when BBL_Match 			= 1	then 'BBL'
			when spatial_match 		= 1	then 'Spatial' 
			when proximity_match 	= 1	then 'Proximity' end	
										as DOB_Match_Type,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null
										else dob_job_number end 
										as dob_job_number,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null
										else units_net end 
										as DOB_Units_Net,
		address,
		borough,
		latitude,
		longitude,
		bbl
	from
		hpd_dob_match
	order by
		project_id
) as HPD_DOB_Match_2

