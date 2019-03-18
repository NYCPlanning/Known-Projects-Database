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
1. Spatially match DOB, HPD Project, and HPD RFP data. 
2. Omit inaccurate matches within 20 meters which do not overlap.
3. Calculate incremental units.
************************************************************************************************************************************************************************************/

select
	*
into
	HPD_Projects_DOB_EDC_HPDRFP_Match 
from
(
SELECT 
		a.the_geom,
		a.the_geom_webmercator,
		a.unique_project_id,
		a.hpd_project_id,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units,
		a.DOB_Match_Type,
		a.dob_job_number,
		a.DOB_Units_Net,
		c.project_id 											as hpd_rfp_id,
		c.project_name 											as hpd_rfp_project_name, 
		c.total_units 											as hpd_rfp_units,
		st_distance(cast(a.the_geom as geography),cast(c.the_geom as geography)) 			as HPD_RFP_Distance,
		case when a.bbl is not null and a.bbl=c.bbl		then 'BBL'
			 when st_intersects(a.the_geom,c.the_geom) 	then 'Spatial' 
			 when c.project_id is not null 				then 'Proximity' end 		as HPD_RFP_Match_Type,
  		b.edc_project_id,
  		b.total_units 											as edc_project_units,
  		st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)) 			as EDC_Distance, 
  		case when st_intersects(a.the_geom,c.the_geom) 	then 'Spatial'
  			 when b.edc_project_id is not null 			then 'Proximity' end 		as EDC_Match_Type,
		a.address,
		a.borough,
		a.latitude,
		a.longitude,
		a.bbl

FROM 
  	hpd_dob_match_2 a
left join
  	capitalplanning.edc_2018_sca_input_1_limited b
on 
  st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 
/*20 meter distance chosen because larger increments would start including incorrectly matched projects.
  This distance correctly matches HPD jobs 660 and 668 to EDC ID 3, and HPD jobs 676 and 677 to EDC ID 4. */ 
left join
  	capitalplanning.hpd_2018_sca_inputs_ms c
on
	c.source = 'HPD RFPs' and
	(
	a.bbl = c.bbl or
	st_dwithin(cast(a.the_geom as geography),cast(c.the_geom as geography),20)
	)
order by a.unique_project_id
) as HPD_Projects_DOB_EDC_HPDRFP_Match



/*EXPORT THE FOLLOWING QUERY AS HPD_DOB_PROXIMATE_MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */			    
select
	*
from
	HPD_Projects_DOB_EDC_HPDRFP_Match
where
	edc_match_type = 'Proximity'
order by
	edc_distance asc
/*END OF LOOKUP CREATION*/
			    

/*Omitting inaccurate non-overlapping matches*/
select
			    *
into
			    HPD_Projects_DOB_EDC_HPDRFP_Match_2 
from
(
	select
		the_geom,
		the_geom_webmercator,
		unique_project_id,
		hpd_project_id,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		DOB_Match_Type,
		dob_job_number,
		DOB_Units_Net,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_rfp_id 				end as hpd_rfp_id,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_rfp_project_name 	end as hpd_rfp_project_name, 
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_rfp_units 			end as hpd_rfp_units,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else HPD_RFP_Distance 		end as HPD_RFP_Distance,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else HPD_RFP_Match_Type 		end as HPD_RFP_Match_Type,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_project_id 			end as edc_project_id,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_project_units		end as edc_project_units,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_distance 			end as EDC_Distance, 
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else EDC_Match_Type 			end as EDC_Match_Type,
		address,
		borough,
		latitude,
		longitude,
		bbl
	from
		HPD_Projects_DOB_EDC_HPDRFP_Match
) as HPD_Projects_DOB_EDC_HPDRFP_Match_2


			    
/*
1. Merging EDC projects with DOB jobs spatially. Create a lookup for non-overlapping matches.
2. Merging EDC projects with HPD jobs by transporting the cleaned matches made in the previous step.
3. Merging EDC projects with HPD RFP jobs spatially. Create a lookup for non-overlapping matches.
*/
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
