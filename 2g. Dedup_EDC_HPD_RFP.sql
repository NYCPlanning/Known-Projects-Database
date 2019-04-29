    
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
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing,
		b.rfp_id as hpd_rfp,
		b.project_name as hpd_rfp_project_name,
		b.total_units as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance,
		case 
			when a.bbl = b.bbl						then 'BBL'
			when st_intersects(a.the_geom::geography,b.the_geom::geography) then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20) then 'Proximity'
											end as Match_Type
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.hpd_rfp_deduped b
	on
		(a.bbl = b.bbl and a.bbl is not null and a.bbl <> '') or
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
) as edc_hpd_rfp
order by
	edc_project_id asc
	
	
/**********************************RUN IN REGULAR CARTO************************/
/*CREATE LOOKUP CALLED EDC_DOB_PROXIMATE_MATCHES
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
	edc_hpd_rfp_1
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
		array_to_string(array_agg(case when b.match = 0 then null else a.hpd_rfp_id end),', '	) as HPD_RFP_IDs,
		sum(case when b.match = 0 then null else a.hpd_rfp_incremental_units end 		) as HPD_RFP_Units
	from
		capitalplanning.edc_hpd a
	left join
		capitalplanning.lookup_proximity_edc_hpdrfp_matches b
	on
		concat(a.edc_project_id,', ',a.hpd_rfp_id) = b.match_id
	group by 
		a.the_geom,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing
) as edc_hpd_rfp_1
order by
	edC_project_id
