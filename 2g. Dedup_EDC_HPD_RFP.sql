    
/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping EDC data with HPD RFPs
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match EDC projects to HPD RFP jobs. 
2. Omit inaccurate matches within 20 meters which do not overlap.
3. Aggregate.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
select
	*
into
	edc_hpd_rfp
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
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_project_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance,
		case 
			when st_intersects(a.the_geom::geography,b.the_geom::geography) then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20) then 'Proximity'
																							end as Match_Type
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.hpd_rfp_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
) as edc_hpd_rfp
order by
	edc_project_id asc
	
	
/**********************************RUN IN REGULAR CARTO************************/
/*If there are any proximity-based matches, EXPORT THE FOLLOWING QUERY AS edc_hpd_rfp_proximate_matches_190524_v2
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
  
  select
  	*
  from
   	edc_hpd_rfp
  where
   	Match_Type = 'Proximity' and total_units <> hpd_rfp_total_units
  order by
  	distance asc

/*END OF LOOKUP CREATION*/

/*************************RUN IN CARTO BATCH********************/

select
	*
into
	edc_hpd_rfp_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_rfp_id,''),nullif(hpd_rfp_project_name,'')),'')),' | ') 		as hpd_rfp_ids,
		sum(hpd_rfp_total_units)																								as HPD_rfp_Total_Units,
		sum(hpd_rfp_incremental_units) 																							as HPD_rfp_Incremental_Units
	from
		capitalplanning.edc_hpd_rfp a
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
) as edc_hpd_rfp_final
order by
	edc_project_id asc
