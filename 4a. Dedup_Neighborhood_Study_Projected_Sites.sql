/************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicating projected and potential sites from adopted neighborhood rezonings for SCA
START DATE: 6/9/2019
************************************************************/

/*********************************************************
METHODOLOGY:
1. Deduplicate aggregated dataset against
	projected sites in adopted rezonings
2. As an alternative, deduplicate projected
   sites when aggregated using the neighborhood 
   rezoning's polygon
3. Compare impact
*********************************************************/

/*Create aggregated dataset using site-specific identified geometries*/
select
	*
into
	nstudy_projected_potential
from
(
	select
		st_union(the_geom) as the_geom,
		st_union(the_geom_webmercator) as the_geom_webmercator,
		concat(neighborhood,' ',status,' Development Sites') as Project_ID,
		status,
		neighborhood,
		borough,
		ROUND(sum(units),0) as total_units
	from
		(select * from dep_ndf_by_site where status <> 'Rezoning Commitment') a 
	group by 
		concat(neighborhood,' ',status,' Development Sites'),
		status,
		neighborhood,
		borough
	order by
		neighborhood,
		borough,
		status
) nstudy_projected_potential




/*Create aggregated dataset using neighborhood-wide identified geometries*/
select
	*
into
	nstudy_projected_potential_areawide
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		concat(a.neighborhood,' ',a.status,' Development Sites') as Project_ID,
		a.status,
		a.neighborhood,
		a.borough,
		ROUND(sum(a.units),0) as total_units
	from
		(select * from dep_ndf_by_site where status <> 'Rezoning Commitment') a
	left join
		(
		select
			*
		from
			capitalplanning.nyc_rezonings
			WHERE admin = 'deblasio'
			AND NOT (study = 'Gowanus' AND shapefile = 'context area')
			AND NOT (study = 'Bay Street Corridor' AND shapefile = 'context area')
			AND NOT (study = 'East Harlem' AND shapefile = 'context area') 
		) b
	on
		a.neighborhood = b.study
	group by 
		b.the_geom,
		b.the_geom_webmercator,
		concat(a.neighborhood,' ',a.status,' Development Sites'),
		a.status,
		a.neighborhood,
		a.borough
	order by
		a.neighborhood,
		a.borough,
		a.status
) nstudy_projected_potential_areawide



/*Deduplicating using site-specific geometries*/

select
	*
into
	nstudy_projected_potential_deduped
from
(
	select
		a.*,
		b.source,
		b.project_id as match_project_id,
		b.project_name_address,
		b.dob_job_type,
		b.status as match_status,
		b.deduplicated_units
	from
		nstudy_projected_potential a
	left join
		(
			select 
				* 
			from 
				known_projects_db_20190609_v2 
			where 
				dob_job_type in('','New Building')  
				and total_units>0
		) b
	on
		st_intersects(a.the_geom,b.the_geom)
) nstudy_projected_potential_deduped

/*Aggregating matches to calculate incremental units*/

select
	*
into
	nstudy_projected_potential_deduped_final
from
(
	select
		row_number() over() as cartodb_id,
		the_geom,
		the_geom_webmercator,
		'Neighborhood Study Projected and Potential Development Sites' as Source,
		project_id,
		status,
		neighborhood,
		borough,
		total_units,
		total_units-coalesce(sum(deduplicated_units),0) 																			as nstudy_projected_potential_incremental_units,
		array_to_string(array_agg(nullif(concat_ws(', ',source,match_project_id,nullif(project_name_address,'')),'')),' | ') 		as project_matches,
		sum(deduplicated_units)																										as matched_incremental_units
	from
		nstudy_projected_potential_deduped
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		status,
		neighborhood,
		borough,
		total_units
) nstudy_projected_potential_deduped_final