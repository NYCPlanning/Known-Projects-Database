/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping EDC data with DOB, HPD Project, HPD RFP data
START DATE: 2/1/2019
COMPLETION DATE: 2/1/2019
Sources: edc_2018_sca_input_1_limited, dob_2018_sca_inputs_ms,
		 hpd_projects_dob_edc_hpdrfp_zap_dep_match, hpd_2018_sca_inputs_ms
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match EDC projects to DOB jobs. 
2. Omit inaccurate matches within 20 meters which do not overlap.
3. Calculate incremental units.
************************************************************************************************************************************************************************************/

select
	*
into
	edc_dob
from
(
	select
		a.the_geom,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing,
		case 
			when st_intersects(a.the_geom,b.the_geom) 	then 'Spatial'
			when b.job_number is not null 			then 'Proximity' end 	as DOB_Match_Type,
		st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)) 	as DOB_Distance, 
		b.job_number 									as dob_job_number,
		b.address,
		b.pre_filing_year 								as dob_pre_filing_year,
		b.units_net
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) and
		b.job_type = 'New Building'
) as edc_dob


/*CREATE LOOKUP CALLED EDC_DOB_PROXIMATE_MATCHES
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
select
   	*
from
   	edc_dob 
where
 	DOB_Match_Type = 'Proximity'
/*END OF LOOKUP CREATION*/			    
			    
	/*Omitting inaccurate, non-overlapping matches*/
select
	*
into
	edc_dob_1
from
(
  	select 
 		a.the_geom,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing,
		array_to_string(array_agg(
					case when b.match= 0 	then null else a.dob_job_number end),', ')  	as dob_job_numbers,
		sum(			case when b.match= 0 	then null else a.units_net      end) 		as dob_units_net
	from edc_dob a 
	left join
		capitalplanning.lookup_proximity_edc_dob_matches b
	on
		concat(a.edc_project_id,', ',a.dob_job_number) = b.match_id
   	group by
 		a.the_geom,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing
) as edc_dob_1

