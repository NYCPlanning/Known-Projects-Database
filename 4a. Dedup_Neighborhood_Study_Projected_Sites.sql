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
		concat(a.neighborhood,' Projected Development Sites') as Project_ID,
		'Projected Development' as status,
		a.neighborhood,
		a.borough,
		case
			when a.neighborhood 	= 'East New York' 			then '04-20-2016'::date
			when a.neighborhood 	= 'East Harlem' 			then '11-30-2017'::date
			when a.neighborhood 	= 'Downtown Far Rockaway' 	then '09-07-2017'::date
			when a.neighborhood 	= 'Inwood'					then '03-22-2018'::date
			when a.neighborhood 	= 'Jerome'					then '01-16-2018'::date
			when a.neighborhood 	= 'Bay Street Corridor'		then '07-01-2019'::date 	end as effective_date,
		ROUND(sum(a.units),0) as total_units 
	from
		(select * from dep_ndf_by_site where status = 'Projected') a
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
		a.neighborhood = b.study or
		a.neighborhood = 'Jerome' and b.study = 'Jerome Avenue'
	group by 
		b.the_geom,
		b.the_geom_webmercator,
		concat(a.neighborhood,' Projected Development Sites'),
		a.neighborhood,
		a.borough,
		case
			when a.neighborhood 	= 'East New York' 			then '04-20-2016'::date
			when a.neighborhood 	= 'East Harlem' 			then '11-30-2017'::date
			when a.neighborhood 	= 'Downtown Far Rockaway' 	then '09-07-2017'::date
			when a.neighborhood 	= 'Inwood'					then '03-22-2018'::date
			when a.neighborhood 	= 'Jerome'					then '01-16-2018'::date
			when a.neighborhood 	= 'Bay Street Corridor'		then '07-01-2019'::date 	end	
	order by
		a.neighborhood,
		a.borough
) nstudy_projected_potential_areawide




/*Deduping using areawide geometries*/
select
	*
into
	nstudy_projected_potential_areawide_deduped
from
(
	select
		a.*,
		b.source,
		b.project_id as match_project_id,
		b.project_name_address,
		b.dob_job_type,
		b.status as match_status,
		b.deduplicated_units,
		coalesce
			(
				nullif(c.pre_filing_date,'')::date,
				d.certified_referred::date
			) as DOB_ZAP_Date
	from
		nstudy_projected_potential_areawide a
	left join
		(
			select 
				* 
			from 
				known_projects_db_20190609_v2 
			where 
				dob_job_type in('','New Building')  
				and total_units>0 and
				source <> 'Neighborhood Study Development Sites'
		) b
	on
		st_intersects(a.the_geom,b.the_geom)
	left join
		capitalplanning.dob_2018_sca_inputs_ms c
	on
		b.source = 'DOB' and b.project_id = concat(c.job_number)
	left join
		capitalplanning.zap_deduped_build_year d
	on
		b.source = 'DCP Applications' and b.project_id = d.project_id
) nstudy_projected_potential_areawide_deduped
where
	dob_zap_date is null or
	dob_zap_date >= effective_date

/*Aggregating matches to calculate incremental units*/

select
	*
into
	nstudy_projected_potential_areawide_deduped_final
from
(
	select
		row_number() over() as cartodb_id,
		the_geom,
		the_geom_webmercator,
		'Neighborhood Study Projected Development Sites' as Source,
		project_id,
		status,
		neighborhood,
		borough,
		effective_date,
		total_units,
		greatest(total_units-coalesce(sum(deduplicated_units),0),0) 	as nstudy_projected_potential_incremental_units,
		case
			when neighborhood 	= 'East New York' 			then 1
			when neighborhood 	= 'East Harlem' 			then .8
			when neighborhood 	= 'Downtown Far Rockaway' 	then .8
			when neighborhood 	= 'Inwood'					then .8
			when neighborhood 	= 'Jerome'					then .8
			when neighborhood 	= 'Bay Street Corridor'		then .7
															end																		as portion_built_2025,
		case
			when neighborhood 	= 'East New York' 			then 0
			when neighborhood 	= 'East Harlem' 			then .2
			when neighborhood 	= 'Downtown Far Rockaway' 	then .2
			when neighborhood 	= 'Inwood'					then .2
			when neighborhood 	= 'Jerome'					then .2
			when neighborhood 	= 'Bay Street Corridor'		then .3
															end																		as portion_built_2035,
		case
			when neighborhood 	= 'East New York' 			then 0
			when neighborhood 	= 'East Harlem' 			then 0
			when neighborhood 	= 'Downtown Far Rockaway' 	then 0
			when neighborhood 	= 'Inwood'					then 0
			when neighborhood 	= 'Jerome'					then 0
			when neighborhood 	= 'Bay Street Corridor'		then 0 	
															end																		as portion_built_2055,						
		array_to_string(array_agg(nullif(concat_ws(', ',source,match_project_id,nullif(project_name_address,'')),'')),' | ') 		as project_matches,
		sum(deduplicated_units)																										as matched_incremental_units
	from
		nstudy_projected_potential_areawide_deduped
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		status,
		neighborhood,
		borough,
		effective_date,
		total_units,
		case
			when neighborhood 	= 'East New York' 			then 1
			when neighborhood 	= 'East Harlem' 			then .8
			when neighborhood 	= 'Downtown Far Rockaway' 	then .8
			when neighborhood 	= 'Inwood'					then .8
			when neighborhood 	= 'Jerome'					then .8
			when neighborhood 	= 'Bay Street Corridor'		then .7
															end,
		case
			when neighborhood 	= 'East New York' 			then 0
			when neighborhood 	= 'East Harlem' 			then .2
			when neighborhood 	= 'Downtown Far Rockaway' 	then .2
			when neighborhood 	= 'Inwood'					then .2
			when neighborhood 	= 'Jerome'					then .2
			when neighborhood 	= 'Bay Street Corridor'		then .3
															end,
		case
			when neighborhood 	= 'East New York' 			then 0
			when neighborhood 	= 'East Harlem' 			then 0
			when neighborhood 	= 'Downtown Far Rockaway' 	then 0
			when neighborhood 	= 'Inwood'					then 0
			when neighborhood 	= 'Jerome'					then 0
			when neighborhood 	= 'Bay Street Corridor'		then 0
															end						
) nstudy_projected_potential_areawide_deduped_final




/************************SUPERSEDED*****************************************/

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
		'Neighborhood Study Projected Development Sites' as Source,
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
	WHERE
		status = 'Projected'
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		status,
		neighborhood,
		borough,
		total_units
) nstudy_projected_potential_deduped_final



/****************************************
SOURCE-SPECIFIC OUTPUT
****************************************/
select * from nstudy_projected_potential_areawide_deduped_final order by effective_date asc