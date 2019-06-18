/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping EDC data with DOB
Sources: edc_2018_sca_input_1_limited, dob_2018_sca_inputs_ms,
		 hpd_projects_dob_edc_hpdrfp_zap_dep_match, hpd_2018_sca_inputs_ms
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match EDC projects to DOB jobs. 
2. Omit inaccurate matches within 20 meters which do not overlap.
3. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
drop table if exists edc_dob;
drop table if exists edc_dob_final;

select
	*
into
	edc_dob
from
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		a.geom_source,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.comments_on_phasing,
		a.build_year,
		a.total_units,
		a.cartodb_id,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		a.Senior_Housing_Flag,
		case 
			when st_intersects(a.the_geom,b.the_geom) then 									'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 			'Proximity' end as DOB_Match_Type,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status,
		st_distance(a.the_geom::geography,b.the_geom::geography) 	as DOB_Distance
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	and
		b.job_type = 'New Building'													and 
		b.status <> 'Complete' /*Omitting completed matches for Stapleton Phase I, given that the unit count for Stapleton Phase I is only what is remaining/unbuilt*/
) as edc_dob;


/*CREATE LOOKUP CALLED edc_dob_proximate_matches_190524_v2
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
select
   	*
from
   	edc_dob 
where
 	DOB_Match_Type = 'Proximity' and
 	dob_units_net <> total_units
/*END OF LOOKUP CREATION*/			    
			    
	/*Omitting inaccurate, non-overlapping matches*/
select
	*
into
	edc_dob_final
from
(
  	select 
		a.the_geom,
		a.the_geom_webmercator,
		a.geom_source,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.comments_on_phasing,
		a.build_year,
		a.total_units,
		a.cartodb_id,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		a.Senior_Housing_Flag,
		array_to_string(array_agg(
						case when b.match= 0 	then null else concat_ws(', ',a.dob_job_number,nullif(a.dob_address,'')) end),' | ')  	as dob_job_numbers,
		sum(			case when b.match= 0 	then null else a.dob_units_net      end)												as dob_units_net
	from edc_dob a 
	left join
		capitalplanning.edc_dob_proximate_matches_190524_v2 b
	on
		concat(a.edc_project_id,a.dob_job_number) = concat(b.edc_id,b.dob_job_number)
   	group by
		a.the_geom,
		a.the_geom_webmercator,
		a.geom_source,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.comments_on_phasing,
		a.build_year,
		a.total_units,
		a.cartodb_id,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		a.Senior_Housing_Flag
	order by
		a.edc_project_id asc
) as edc_dob_1;


