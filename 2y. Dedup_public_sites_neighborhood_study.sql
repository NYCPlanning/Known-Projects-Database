/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with Neighborhood Study Commitments
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with Neighborhood Study Commitments.
2. If a Neighborhood Study Commitments maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_nstudy
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
		b.project_id 																		as nstudy_project_id,
		b.project_name																		as nstudy_project_name,
		b.total_units																		as nstudy_units,
		b.nstudy_incremental_units,
		b.planner_input																		as nstudy_planner_input
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.nstudy_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 
	order by
		public_sites_id asc
)  public_sites_nstudy


/*No matches. For expedience, not creating a lookup for proximity-based matching, a lookup for 1-1 filtering, or aggregating.*/

select
	*
into
	public_sites_nstudy_final
from
(
	select
		*
	from
		public_sites_nstudy
) public_sites_nstudy_final
