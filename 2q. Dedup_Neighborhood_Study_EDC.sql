/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate Neighborhood Study Rezoning Commitments from EDC
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match neighborhood study rezoning commitments to EDC projected projects.
2. If an EDC project maps to multiple neighborhood study rfps, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	nstudy_edc
from
(
	select
		a.*,
		case
			/*Not performing BBL match because geometries for both these sources are taken from PLUTO. Therefore, a spatial match is an implicit BBL match*/
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
		(select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment') a
	left join
		capitalplanning.edc_deduped b
	on 
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
	order by 
		project_id asc
) nstudy_edc


/*There is only one match between EDC and neighborhood study rezoning commitments -- the 126th Bus Street Depot in East Harlem. No need to consider
  proximity-based matches or filtering to create 1-1 matches.*/

select
	*
into
	nstudy_edc_final
from
(
	select
		*
	from
		nstudy_edc
) nstudy_edc_final


