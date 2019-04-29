/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping EDC data with DOB HPD Projected Closings
Sources: edc_2018_sca_input_1_limited, dob_2018_sca_inputs_ms,
		 hpd_projects_dob_edc_hpdrfp_zap_dep_match, hpd_2018_sca_inputs_ms
*************************************************************************************************************************************************************************************/
/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match HPD Projects to EDC jobs. 
2. Omit inaccurate matches within 20 meters which do not overlap.
************************************************************************************************************************************************************************************/

/**********************RUN THE FOLLOWING QUERY IN CARTO BATCH******************************/

select
	*
into
	edc_hpd
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
		b.unique_project_id 								as HPD_Project_ID,
		b.project_name 									as HPD_Project_Name,
		b.status									as HPD_Status,
		b.project_start_date								as HPD_Project_Start_Date,
		b.projected_completion_date							as HPD_Projected_Completion_Date
		,b.total_units									as HPD_Total_Units
		,b.hpd_incremental_units
		,b.address									as HPD_Address
		,b.borough									as HPD_Borough
		,b.bbl										as HPD_BBL
		case
			when st_intersects(a.the_geom,b.the_geom)			then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography)	then 'Proximity'
											end 	as Match_Type
		,st_distance(a.the_geom::geography,b.the_geom::geography)			as distance
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.hpd_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
	order by
		edc_project_id asc
) as edc_hpd

/**********************RUN THE FOLLOWING QUERY IN REGULAR CARTO******************************/
/*EXPORT THE FOLLOWING QUERY AS HPD_EDC_PROXIMATE_MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */

select
	*
WHERE
	match_type = 'Proximity' and
	total_units <> hpd_total_units
from 
	edc_hpd
order by
	distance
	
/**********************RUN THE FOLLOWING QUERY IN CARTO BATCH******************************/

select
	*
into
	edc_hpd_1
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
		array_to_string(array_agg(case when b.match = 0 then null else a.hpd_project_id end),', ') as hpd_project_ids,
		sum(case when b.match = 0 then null else a.hpd_incremental_units) as HPD_Incremental_Units
	from
		edc_hpd a
	left join
		lookup_proximity_hpd_edc_matches b
	on 
		concat(a.hpd_project_id,', ',a.edc_project_id) = b.match_id
) as edc_hpd_1
order by
	edc_project_id asc
