/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with EDC projects
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with EDC projects.
2. If an EDC project maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_edc
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
		b.edc_project_id,
		b.project_name 											as edc_project_name,
		b.project_description 									as edc_project_description,
		b.comments_on_phasing									as edc_comments_on_phasing,
		b.build_year											as edc_build_year,
		b.total_units 											as edc_total_units,
		b.edc_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.edc_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_edc


/*There are no matches. For expedience, not creating a lookup for proximity-based matching, a lookup for 1-1 filtering, or aggregating.*/

select
	*
into
	public_sites_edc_1
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
		b.edc_project_id,
		b.project_name 											as edc_project_name,
		b.project_description 									as edc_project_description,
		b.comments_on_phasing									as edc_comments_on_phasing,
		b.build_year											as edc_build_year,
		b.total_units 											as edc_total_units,
		b.edc_incremental_units,
	 	st_distance(a.the_geom::geography,b.the_geom::geography) as distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.edc_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_edc_1


/*Aggregating projects*/

select
	*
into
	public_sites_edc_final
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
		array_to_string(array_agg(nullif(concat_ws(', ',edc_project_id,nullif(edc_project_name,'')),'')),' | ') 				as edc_project_ids,
		sum(edc_total_units) 																									as edc_total_units,
		sum(edc_incremental_units) 																								as edc_incremental_units
	from
		public_sites_edc_1
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
) public_sites_edc_final
