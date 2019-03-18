/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with HPD Project Data
START DATE: 2/10/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Match HPD RFP projects to HPD projects by BBL and spatially. Confirm proximity matches.
2. Merge HPD Deduped file with HPD RFP projects
3. Calculate incremental units
************************************************************************************************************************************************************************************/
/**********************RUN IN CARTO BATCH***********************/

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

/*EXPORT THE FOLLOWING QUERY AS HPD_HPDRFP_PROXIMATE_MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */

SELECT
	*
from
	HPD_Projects_DOB_EDC_HPDRFP_Match
where
	hpd_rfp_match_type = 'Proximity' 
	--and	total_units <> HPD_RFP_Units will include when we have units for HPD RFPs
order by
	hpd_rfp_distance asc
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


/*Transposing matches from HPD_Projects_DOB_EDC_HPDRFP_Match_2 onto HPD RFP dataset*/
select
	*
into
	hpd_rfp_deduped
from
(
with hpd_rfp_1 as
(
	select
		a.the_geom,
		a.project_id 			as RFP_ID,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units,
		b.unique_project_id 		as HPD_Project_ID,
		b.total_units 			as HPD_Project_Total_Units,
		b.hpd_incremental_units 	as HPD_Project_Incremental_Units
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.hpd_deduped b
	on
	 		trim(split_part(b.hpd_rfp_ids,',',1)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',1)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',2)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',2)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',3)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',3)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',4)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',4)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',5)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',5)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',6)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',6)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',7)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',7)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',8)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',8)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',9)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',9)) 	<> '' or
	 		trim(split_part(b.hpd_rfp_ids,',',10)) 	= concat(a.project_id) 	and	trim(split_part(b.hpd_rfp_ids,',',10)) 	<> '' 

	where
		a.source = 'HPD RFPs'
)

	select
		the_geom,
		RFP_ID,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		array_to_string(array_agg(HPD_Project_ID),', ')  				as HPD_Project_IDs,
		sum(HPD_Project_Incremental_Units) 						as HPD_Project_Incremental_Units,
		greatest(0,total_units - sum(coalesce(HPD_Project_Incremental_Units,0)))	as Incremental_HPD_RFP_Units
	from
		hpd_rfp_1 a
	group by
		a.the_geom,
		a.RFP_ID,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units
	order by
		cast(RFP_ID as integer)
) as HPD_RFP_Deduped
