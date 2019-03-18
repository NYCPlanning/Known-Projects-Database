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
1. Spatially match HPD Projects to EDC jobs. 
2. Omit inaccurate matches within 20 meters which do not overlap.
3. Transpose these matches onto EDC data.
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



/*EXPORT THE FOLLOWING QUERY AS HPD_EDC_PROXIMATE_MATCHES.
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
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches 	where match = 0) then null else hpd_rfp_id 		end as hpd_rfp_id,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches 	where match = 0) then null else hpd_rfp_project_name 	end as hpd_rfp_project_name, 
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches 	where match = 0) then null else hpd_rfp_units 		end as hpd_rfp_units,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches 	where match = 0) then null else HPD_RFP_Distance 	end as HPD_RFP_Distance,
		case when concat(unique_project_id,', ',hpd_rfp_id) 	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches	where match = 0) then null else HPD_RFP_Match_Type 	end as HPD_RFP_Match_Type,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_project_id 		end as edc_project_id,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_project_units	end as edc_project_units,
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else edc_distance 		end as EDC_Distance, 
  		case when concat(unique_project_id,', ',edc_project_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_edc_matches 	where match = 0) then null else EDC_Match_Type 		end as EDC_Match_Type,
		address,
		borough,
		latitude,
		longitude,
		bbl
	from
		HPD_Projects_DOB_EDC_HPDRFP_Match
) as HPD_Projects_DOB_EDC_HPDRFP_Match_2

select
	*
into
	edc_dob_hpd_projects_1
from
(
with	edc_dob_hpd_projects as
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
	)
	select * from edc_dob_hpd_projects_1
) as edc_dob_hpd_projects_1
