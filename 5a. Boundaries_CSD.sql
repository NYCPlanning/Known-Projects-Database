/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding CSD boundaries to aggregated pipeline
START DATE: 6/10/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

select
	*
into
	aggregated_csd_v1
from
(
	with aggregated_boundaries_csd as
(
	select
		a.*,
		b.the_geom as csd_geom,
		b.schooldist,
		st_distance(a.the_geom::geography,b.the_geom::geography) as csd_Distance
	from
		capitalplanning.known_projects_db_20190610_v4 a
	left join
		nyc_school_districts b
	on 
	case
		when st_area(a.the_geom::geography)>4000 	then
			st_intersects(a.the_geom,b.the_geom) and CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1
		when st_area(a.the_geom) > 0 				then
			st_intersects(st_centroid(a.the_geom),b.the_geom) 
		else
			st_intersects(a.the_geom,b.the_geom) 	end
																									/*Only matching if at least 10% of the polygon
		                           																	is in the boundary. Otherwise, the polygon will be
		                           																	apportioned to its other boundaries only*/
),

	multi_geocoded_projects as
(
	select
		source,
		project_id
	from
		aggregated_boundaries_csd
	group by
		source,
		project_id
	having
		count(*)>1
),

	aggregated_boundaries_csd_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.project_id) in(select concat(source,project_id) from multi_geocoded_projects) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_Intersection(a.the_geom,a.csd_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_csd
	from
		aggregated_boundaries_csd a
),

	aggregated_boundaries_csd_3 as
(
	SELECT
		source,
		project_id,
		sum(proportion_in_csd) as total_proportion
	from
		aggregated_boundaries_csd_2
	group by
		source,
		project_id
),

	aggregated_boundaries_csd_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_csd/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_csd_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_csd/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_csd_2 a
	left join
		aggregated_boundaries_csd_3 b
	on
		a.project_id = b.project_id and a.source = b.source
)

	select * from aggregated_boundaries_csd_4

) as _1

select
	*
into
	ungeocoded_projects_csd
from
(
	with ungeocoded_projects_csd as
(
	select
		a.*,
		coalesce(a.schooldist,b.schooldist) as school_dist1,
		coalesce(a.csd_distance,st_distance(a.the_geom::geography,b.the_geom::geography)) as csd_distance1
	from
		aggregated_csd_test1 a 
	left join
		nyc_school_districts b
	on 
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) and
		a.csd_distance is null
)
	select * from ungeocoded_projects_csd
) as _2


select
	*
into
	aggregated_csd_3
from
(
	with	min_distances as
(
	select
		project_id,
		min(csd_distance1) as min_distance
	from
		ungeocoded_projects_csd
	group by 
		project_id
),

	all_projects_csd as
(
	select
		a.*
	from
		ungeocoded_projects_csd a 
	inner join
		min_distances b
	on
		a.project_id = b.project_id and
		a.csd_distance1=b.min_distance
)

	select 
		a.*, 
		b.school_dist1 as CSD, 
		b.proportion_in_csd_1 as proportion_in_csd,
		a.counted_units * b.proportion_in_csd_1 as counted_units_in_CSD 
	from 
		known_projects_db_20190610_v4 a 
	left join 
		all_projects_csd b 
	on 
		a.source = b.source and 
		a.project_id = b.project_id 
	order by 
		source, 
		project_id
) as _3

