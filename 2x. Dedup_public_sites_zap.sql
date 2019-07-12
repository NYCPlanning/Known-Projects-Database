/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with ZAP projects
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with EDC projects.
2. If an EDC project maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/
drop table if exists public_sites_zap;
drop table if exists public_sites_zap_1;
drop table if exists public_sites_zap_final;

select
	*
into
	public_sites_zap
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
		b.project_id 																		as ZAP_Project_ID,
		b.project_name 																		as ZAP_Project_Name,
		b.status 																	as ZAP_Project_Status,
		b.project_description																as ZAP_Project_Description,
		b.project_brief 																	as ZAP_Project_Brief,
		b.applicant_type 																	as ZAP_Applicant_Type,
		b.total_units 																		as ZAP_Total_Units,
		b.zap_incremental_units 															as zap_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.zap_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_zap;

/*There is only 1 match, and it is proximity-based. It is between Public Site Pipeline 28 (351 Powers Avenue) and ZAP ID 2019X0196 (WIN Powers), ~19 meters apart.
  These are clearly separate projects -- the project description shows WIN Powers targeting 346 Powers Avenue, and the lot of 351 Powers is currently owned by DCAS/DOE. 
  For expedience, not creating a lookup for this 1 project. Instead just joining by spatial overlap, not proximity. See query below.*/


select
	*
into
	public_sites_zap_1
from
(
	select
		a.*,
		case
			when
				st_intersects(a.the_geom,b.the_geom)										then 'Spatial'
			end																				as match_type,
		b.project_id 																		as ZAP_Project_ID,
		b.project_name 																		as ZAP_Project_Name,
		b.status 																	as ZAP_Project_Status,
		b.project_description																as ZAP_Project_Description,
		b.project_brief 																	as ZAP_Project_Brief,
		b.applicant_type 																	as ZAP_Applicant_Type,
		b.total_units 																		as ZAP_Total_Units,
		b.zap_incremental_units 															as zap_incremental_units,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.zap_deduped b
	on
		st_intersects(a.the_geom,b.the_geom)
--		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_zap_1;


/*Aggregating projects*/


select
	*
into
	public_sites_zap_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',nullif(zap_project_id,''),nullif(zap_project_name,'')),'')),' | ') 	as zap_project_ids,
		sum(zap_total_units)																								as zap_total_units,
		sum(zap_incremental_units)																							as zap_incremental_units
	from
		public_sites_zap_1
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
) public_sites_zap_final;
