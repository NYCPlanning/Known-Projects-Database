/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding Census Tract boundaries to aggregated pipeline
START DATE: 6/10/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/


/*There are 150 projects > 10K square meters, average unit count of 816. These projects should certainly be treated as polygons.
  There are 94 projects > 200 units and <10K square meters. These projects are mostly distinct buildings and can be based on 
  centroid. Choosing polygons for projects <10K square meters if they are >600 units (18 projects)*/

SELECT * FROM capitalplanning.known_projects_db_20190610_v4 where st_area(the_geom::geography)<10000 and total_units > 500 and source in('DCP Applications','DCP Planner-Added Projects')

select
	*
into
	aggregated_ct
from
(
	with aggregated_boundaries_ct as
(
	select
		a.*,
		b.the_geom as ct_geom,
		b.boro_ct201,
		st_distance(a.the_geom::geography,b.the_geom::geography) as ct_Distance
	from
		capitalplanning.known_projects_db_20190610_v4 a
	left join
		capitalplanning.census_tract_2010_190412_ms b
	on 
	case
		when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added Projects') 		then
			st_intersects(a.the_geom,b.the_geom) and CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1
		when st_area(a.the_geom) > 0 																											then
			st_intersects(st_centroid(a.the_geom),b.the_geom) 
		else
			st_intersects(a.the_geom,b.the_geom) 																								end
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
		aggregated_boundaries_ct
	group by
		source,
		project_id
	having
		count(*)>1
),

	aggregated_boundaries_ct_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.project_id) in(select concat(source,project_id) from multi_geocoded_projects) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_Intersection(a.the_geom,a.ct_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_ct
	from
		aggregated_boundaries_ct a
),

	aggregated_boundaries_ct_3 as
(
	SELECT
		source,
		project_id,
		sum(proportion_in_ct) as total_proportion
	from
		aggregated_boundaries_ct_2
	group by
		source,
		project_id
),

	aggregated_boundaries_ct_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_ct/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_ct_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_ct/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_ct_2 a
	left join
		aggregated_boundaries_ct_3 b
	on
		a.project_id = b.project_id and a.source = b.source
)

	select * from aggregated_boundaries_ct_4

) as _1

select
	*
into
	ungeocoded_projects_ct
from
(
	with ungeocoded_projects_ct as
(
	select
		a.*,
		coalesce(a.boro_ct201,b.boro_ct201) as boro_ct201_1,
		coalesce(
					a.ct_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added Projects') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as ct_distance1
	from
		aggregated_ct a 
	left join
		capitalplanning.census_tract_2010_190412_ms b
	on 
		a.ct_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added Projects') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	select * from ungeocoded_projects_ct
) as _2


select
	*
into
	aggregated_ct_final
from
(
	with	min_distances as
(
	select
		project_id,
		min(ct_distance1) as min_distance
	from
		ungeocoded_projects_ct
	group by 
		project_id
),

	all_projects_ct as
(
	select
		a.*
	from
		ungeocoded_projects_ct a 
	inner join
		min_distances b
	on
		a.project_id = b.project_id and
		a.ct_distance1=b.min_distance
)

	select 
		a.*, 
		b.boro_ct201_1 as ct, 
		b.proportion_in_ct_1 as proportion_in_ct,
		round(a.counted_units * b.proportion_in_ct_1) as counted_units_in_ct 
	from 
		known_projects_db_20190610_v4 a 
	left join 
		all_projects_ct b 
	on 
		a.source = b.source and 
		a.project_id = b.project_id 
	order by 
		source asc,
		project_id asc,
		project_name_address asc,
		status asc
) as _3

