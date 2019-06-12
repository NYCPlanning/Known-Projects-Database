/************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Geocode projected and sites from adopted neighborhood rezonings for SCA
START DATE: 6/12/2019
************************************************************/

drop table if exists nstudy_projected_csd;
drop table if exists nstudy_projected_csd_final;


/*CSD*/

select
	*
into 
	nstudy_projected_csd
from
(
	select
		a.*,
		b.schooldist as csd,
		st_area(st_intersection(a.the_geom,b.the_geom))/st_area(a.the_geom) as proportion_in_csd,
		round((st_area(st_intersection(a.the_geom,b.the_geom))/st_area(a.the_geom) * nstudy_projected_potential_incremental_units)::numeric,0) as units_in_csd
	from
		nstudy_projected_potential_areawide_deduped_final a
	left join
		nyc_school_districts b
	on
		st_intersects(a.the_geom,b.the_geom)
	where
		a.status = 'Projected Development'
) nstudy_projected_csd;

select
	*
into
	nstudy_projected_csd_final
from
(
	select
		source,
		project_id,
		neighborhood,
		borough,
		effective_date,
		status,
		total_units,
		nstudy_projected_potential_incremental_units,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(concat(CSD),''),
						concat(units_in_csd,' units')
					),
				'')),
		' | ') 	as CSD 
	from
		nstudy_projected_csd
	group by
		source,
		project_id,
		neighborhood,
		borough,
		effective_date,
		status,
		total_units,
		nstudy_projected_potential_incremental_units
	order by
		effective_date::date asc
) nstudy_projected_csd_final;



/*Subdistrict*/
select
	*
into 
	nstudy_projected_subdistrict
from
(
	select
		a.*,
		b.distzone as subdistrict,
		st_area(st_intersection(a.the_geom,b.the_geom))/st_area(a.the_geom) as proportion_in_subdistrict,
		round((st_area(st_intersection(a.the_geom,b.the_geom))/st_area(a.the_geom) * nstudy_projected_potential_incremental_units)::numeric,0) as units_in_subdistrict
	from
		nstudy_projected_potential_areawide_deduped_final a
	left join
		dcpadmin.doe_schoolsubdistricts b
	on
		st_intersects(a.the_geom,b.the_geom)
	where
		a.status = 'Projected Development'
) nstudy_projected_csd;

select
	*
into
	nstudy_projected_subdistrict_final
from
(
	select
		source,
		project_id,
		neighborhood,
		borough,
		effective_date,
		status,
		total_units,
		nstudy_projected_potential_incremental_units,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(concat(subdistrict),''),
						concat(units_in_subdistrict,' units')
					),
				'')),
		' | ') 	as Subdistrict
	from
		nstudy_projected_subdistrict
	group by
		source,
		project_id,
		neighborhood,
		borough,
		effective_date,
		status,
		total_units,
		nstudy_projected_potential_incremental_units
	order by
		effective_date::date asc
) nstudy_projected_subdistrict_final;
