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
1. Spatially match DOB, HPD Project, and HPD RFP data. Sum matched units.
************************************************************************************************************************************************************************************/

select
	*
into
	edc_deduped
from
(
	with edc_dob as 
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
		case when st_intersects(a.the_geom,b.the_geom) 	then 'Spatial'
			 when b.job_number is not null 				then 'Proximity' end as DOB_Match_Type,
		st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)) as DOB_Distance, 
		b.job_number as dob_job_number,
		b.address,
		b.pre_filing_year as dob_pre_filing_year,
		b.units_net
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) and
		b.job_type = 'New Building'
	),
  
  


		edc_dob_1 as
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
			array_to_string(array_agg(	case when b.match= 0 	then null else a.dob_job_number end),', ')  as dob_job_numbers,
			sum(						case when b.match= 0 	then null else a.units_net      end) 		as dob_units_net
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
	),

		edc_dob_hpd_projects as
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
			a.dob_job_numbers,
			a.dob_units_net,
			b.unique_project_id as hpd_unique_project_id,
			b.hpd_incremental_units as HPD_Matched_Units,
			b.projected_completion_date as HPD_Projected_Completion_Date
		from 
			edc_dob_1 a
		left join
			capitalplanning.hpd_deduped b
		on 
			case 
				when
					position(',' in b.edc_project_ids) = 0 then concat(a.edc_project_id) = b.edc_project_ids
				else 
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',1)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',2)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',3)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',4)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',5)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',6)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',7)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',8)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',9)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',10)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',11)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',12)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',13)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',14)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',15)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',16)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',17)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',18)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',19)) or
					concat(a.edc_project_id)=trim(split_part(b.edc_project_ids,',',20)) end
	),

		edc_dob_hpd_projects_1 as
	(  
	  select 
	 		the_geom,
			edc_project_id,
			dcp_project_id,
			project_name,
			project_description,
			total_units,
			build_year,
			comments_on_phasing,
			dob_job_numbers,
			dob_units_net,
			array_to_string(array_agg(hpd_unique_project_id),', ')  as hpd_unique_project_ids,
			sum(HPD_Matched_Units) as HPD_Matched_Units
		from 
			edc_dob_hpd_projects 
		group by
			the_geom,
			edc_project_id,
			dcp_project_id,
			project_name,
			project_description,
			total_units,
			build_year,
			comments_on_phasing,
			dob_job_numbers,
			dob_units_net
	),

		edc_dob_hpd_projects_rfps as
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
			a.dob_job_numbers,
			a.dob_units_net,
			a.hpd_unique_project_ids,
			a.HPD_Matched_Units,
			case 	when st_intersects(a.the_geom,b.the_geom) 	then 'Spatial'
					when b.rfp_id is not null	 				then 'Proximity' end as HPD_RFP_Match_Type,
			st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)) as HPD_RFP_Distance,
			b.rfp_id as HPD_RFP_Unique_ID,
			b.project_name as HPD_RFP_Project_Name,
			b.Incremental_HPD_RFP_Units as HPD_RFP_Units
		from
			edc_dob_hpd_projects_1 a
		left join
	  		capitalplanning.hpd_rfp_deduped b 
		on
			st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) /*Accurate matches at this distance*/
	),

	/*CREATE LOOKUP CALLED EDC_DOB_PROXIMATE_MATCHES
	  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
	  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
	  -- select
	  -- 	*
	  -- from
	  -- 	edc_dob_hpd_projects_rfps 
	  -- where
	  -- 	HPD_RFP_Match_Type = 'Proximity'
	/*END OF LOOKUP CREATION*/

		edc_dob_hpd_projects_rfps1 as
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
			a.dob_job_numbers,
			a.dob_units_net,
			a.hpd_unique_project_ids,
			a.HPD_Matched_Units,
			array_to_string(array_agg(	case when b.match = 0 then null else a.HPD_RFP_Unique_ID end),', '	) as HPD_RFP_Unique_IDs,
			sum(						case when b.match = 0 then null else a.HPD_RFP_Units 	 end 		) as HPD_RFP_Units
		from 
			edc_dob_hpd_projects_rfps a
		left join
			capitalplanning.lookup_proximity_edc_hpdrfp_matches b
		on
			concat(a.edc_project_id,', ',a.HPD_RFP_Unique_ID) = b.match_id
		group by 
	 		a.the_geom,
			a.edc_project_id,
			a.dcp_project_id,
			a.project_name,
			a.project_description,
			a.total_units,
			a.build_year,
			a.comments_on_phasing,
			a.dob_job_numbers,
			a.dob_units_net,
			a.hpd_unique_project_ids,
			a.HPD_Matched_Units
		order by
			a.edc_project_id
	),

select * from edc_dob_hpd_projects_rfps1
) as edc_deduped
