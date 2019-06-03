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
			when st_intersects(a.the_geom,b.the_geom)						then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 'Proximity'
																			end 			as Match_Type,
		b.project_id 					as HPD_Project_ID,
		b.address						as HPD_Address,
		b.bbl 							as HPD_BBL,
		b.total_units 					as HPD_Project_Total_Units,
		b.hpd_incremental_units 		as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography)			as distance
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
/*If there are any proximity-based matches, EXPORT THE FOLLOWING QUERY AS edc_hpd_proximate_matches_190524_v2. THERE ARE NO PROXIMITY-BASED MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */

select
	*
from 
	edc_hpd
WHERE
	match_type = 'Proximity' and
	total_units <> HPD_Project_Total_Units
order by
	distance
	
/**********************RUN THE FOLLOWING QUERY IN CARTO BATCH******************************/

select
	*
into
	edc_hpd_final
from
(
	select
		the_geom,
		the_geom_webmercator,
		geom_source,
		edc_project_id,
		dcp_project_id,
		project_name,
		project_description,
		comments_on_phasing,
		build_year,
		total_units,
		cartodb_id,
		NYCHA_Flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag,
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_project_id,''),nullif(hpd_address,'')),'')),' | ') 	as hpd_project_ids,
		sum(HPD_Project_Total_Units)																					as HPD_Project_Total_Units,
		sum(HPD_Project_Incremental_Units) 																				as HPD_Project_Incremental_Units
	from
		edc_hpd
	group by
		the_geom,
		the_geom_webmercator,
		geom_source,
		edc_project_id,
		dcp_project_id,
		project_name,
		project_description,
		comments_on_phasing,
		build_year,
		total_units,
		cartodb_id,
		NYCHA_Flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag
	order by
		edc_project_id asc
) x

