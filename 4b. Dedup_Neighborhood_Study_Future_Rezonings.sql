/************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicating estimated incremental units from neighborhood rezonings for SCA
START DATE: 6/9/2019
************************************************************/

/*********************************************************
METHODOLOGY:
1. Use imported units counts from future rezonings and apply geography information
2. Deduplicate from aggregated dataset
*********************************************************/

drop table if exists nstudy_future;

select
	*
into 
	nstudy_future
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		'Future Neighborhood Studies' as Source,
		concat(a.neighborhood,' ',a.status,' Development') as Project_ID,
		a.neighborhood,
		a.borough,
		a.status,
		a.effective_year,
		a.build_period,
		a.certainty,
		a.certainty_factor,
		a.incremental_units as total_units,
		a.incremental_units_with_certainty_factor,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055
	from
		table_20190609_future_rezoning_inputs_ms a
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
	order by
		a.neighborhood,
		a.borough,
		a.status,
		a.certainty
) nstudy_future






































/************************************************SUPERSEDED***********************************************/
select
	*
into
	nstudy_future_deduped
from
(
	select
		a.*,
		b.source as match_source,
		b.project_id as match_project_id,
		b.project_name_address,
		b.dob_job_type,
		b.status as match_status,
		b.deduplicated_units
	from
		nstudy_future a
	left join
		(
			select 
				* 
			from 
				known_projects_db_20190609_v2 
			where 
				source not in('Neighborhood Study Development Sites', 'EDC', 'HPD RFPs','DOB') 	and
				dob_job_type in('','New Building') 
				and total_units>0																and
				status <> 'Complete'
		) b
	on
		st_intersects(a.the_geom,b.the_geom)
) nstudy_future_deduped


select
	*
into
	nstudy_future_deduped_final
from
(
	select
		row_number() over() as cartodb_id,
		the_geom,
		the_geom_webmercator,
		Source,
		project_id,
		status,
		neighborhood,
		borough,
		total_units,
		total_units-coalesce(sum(deduplicated_units),0) 																			as nstudy_projected_potential_incremental_units,
		array_to_string(array_agg(nullif(concat_ws(', ',source,match_project_id,nullif(project_name_address,'')),'')),' | ') 		as project_matches,
		sum(deduplicated_units)																										as matched_incremental_units
	from
		nstudy_future_deduped
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		status,
		neighborhood,
		borough,
		total_units
) nstudy_future_deduped_final