/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with HPD Projected Closings
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with HPD Projected Closings jobs.
2. If an HPD Projected Closing maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_hpd_projected_closings
from
(
	select
		a.*,
		case
			when st_intersects(a.the_geom,b.the_geom)						then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 'Proximity'
																			end 			as Match_Type,
		b.project_id 																		as HPD_Project_ID,
		b.address																			as HPD_Address,
		b.bbl 																				as HPD_BBL,
		b.total_units 																		as HPD_Project_Total_Units,
		b.hpd_incremental_units 															as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.hpd_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
) public_sites_hpd_projected_closings

/*There is only one match, so avoiding 1-1 match filtering process.*/

/*Checking proximity matches. There is 1 match by proximity. 
  Create lookup public_sites_hpd_projected_closings_proximate_matches_190529_v2 with manual
  checks on the accuracy of each proximity match. */

	select
		*
	from
		public_sites_hpd_projected_closings
	where
		match_type = 'Proximity'
	order by
		distance asc


/*Omitting inaccurate proximity-based matches*/

select
	*
into
	public_sites_hpd_projected_closings_1_pre
from
(
	select
		a.public_sites_id as public_sites_id_temp,
		a.match_type,
		a.HPD_Project_ID,
		a.HPD_Address,
		a.HPD_BBL,
		a.HPD_Project_Total_Units,
		a.HPD_Project_Incremental_Units,
		a.distance
	from
		public_sites_hpd_projected_closings a
	left join
		public_sites_hpd_projected_closings_proximate_matches_190529_v2 b
	on
		concat(a.public_sites_id,a.hpd_project_id) = concat(b.public_sites_id,b.hpd_projected_closing_id) and
		b.accurate_match = 0
	where
		b.public_sites_id is null
) public_sites_hpd_projected_closings_1_pre

select
	*
into
	public_sites_hpd_projected_closings_1
from
(
	select
		a.*,
		b.*
	from
		public_sites_2018_sca_inputs_ms_1 a
	left join
		public_sites_hpd_projected_closings_1_pre b
	on
		a.public_sites_id = b.public_sites_id_temp
) public_sites_hpd_projected_closings_1

select
	*
into
	public_sites_hpd_projected_closings_final 
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
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input,
		nullif(array_to_string(array_agg(nullif(concat_ws(', ',nullif(hpd_project_id,''),nullif(hpd_address,'')),'')),' | '),'') 	as HPD_Project_IDs,
		sum(HPD_Project_Total_Units) 																								as HPD_Project_Total_Units,		
		sum(HPD_Project_Incremental_Units) 																							as HPD_Project_Incremental_Units
	from
		public_sites_hpd_projected_closings_1
	group by
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		public_sites_id,
		project,
		boro, 
		lead,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input
) public_sites_hpd_projected_closings_final
