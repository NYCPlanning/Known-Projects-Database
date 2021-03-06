/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with HPD RFPs
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with HPD RFPs.
2. If an HPD RFPs maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_hpd_rfps
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)					then 'Proximity'
			end																				as match_type,
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.hpd_rfp_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_hpd_rfps

/*There is only 1 match, and it is proximity-based. It is between Public Site Pipeline 29 (Hunters Point D+E) and HPD RFP ID 13 (Hunters Point F+G), ~15 meters apart.
  These are clearly separate projects. For expedience, not creating a lookup for this 1 project. Instead just joining by spatial overlap, not proximity. See query below.*/

select
	*
into
	public_sites_hpd_rfps_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			end																				as match_type,
		b.project_id 																		as hpd_rfp_id,
		b.project_name 																		as hpd_rfp_name,
		b.total_units 																		as hpd_rfp_total_units,
		b.hpd_rfp_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.hpd_rfp_deduped b
	on
		st_intersects(a.the_geom,b.the_geom)		
--		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_hpd_rfps_1


select
	*
into
	public_sites_hpd_rfps_final
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		public_sites_id,
		project,
		boro,
		lead,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input,
		array_to_string(array_agg(nullif(concat_ws(', ',hpd_rfp_id,nullif(hpd_rfp_name,'')),'')),' | ') 			as HPD_RFP_IDs,
		sum(HPD_RFP_Total_Units) 																					as HPD_RFP_Total_Units,
		sum(HPD_RFP_Incremental_Units) 																				as HPD_RFP_Incremental_Units
	from
		public_sites_hpd_rfps_1
	group by
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		public_sites_id,
		project,
		boro,
		lead,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input
	order by
		public_sites_id asc
) public_sites_hpd_rfps_final