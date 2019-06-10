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


alter table capitalplanning.aggregated_working_pipeline_cpf4_nta
drop column ntaname,
drop column ntacode,
drop column counted_units,
drop column nta_distance,
drop column nta_distance1,
drop column nta_geom;

alter table capitalplanning.aggregated_working_pipeline_cpf4_nta
rename column ntaname_1 to nta_name
alter table capitalplanning.aggregated_working_pipeline_cpf4_nta
rename column ntacode_1 to nta_code
alter table capitalplanning.aggregated_working_pipeline_cpf4_nta
rename column counted_units_1 to counted_units;


select cdb_cartodbfytable('capitalplanning','aggregated_working_pipeline_cpf4_nta')

SELECT source, sum(deduplicated_units) FROM (select distinct source, project_id, deduplicated_units from capitalplanning.aggregated_working_pipeline_cpf4_nta) a group by source

SELECT 
		SOURCE, 
		SUM(TOTAL_UNITS) AS TOTAL,
		SUM(DEDUPLICATED_UNITS) AS DEDUP, 
		SUM(COUNTED_UNITS) AS COUNTED 
FROM 
	 capitalplanning.aggregated_working_pipeline_cpf4_nta

with multi_nta_projects as
(
	select
		source,
		project_id,
		project_name,
		deduplicated_units
	from 
		capitalplanning.aggregated_working_pipeline_cpf4_nta
	GROUP BY
		source,
		project_id,
		project_name,
		deduplicated_units
	HAVING COUNT (*) > 1

),

	sum_counted_units as
(
	SELECT
		a.*,
		sum(counted_units) as counted_units
	from
		multi_nta_projects a 
	left join 
		capitalplanning.aggregated_working_pipeline_cpf4_nta b
	on
		a.project_id = b.project_id and a.source = b.source
	group by
		a.source,
		a.project_id,
		a.project_name,
		a.deduplicated_units

)

select * from sum_counted_units


/*SUPERSEDED*/
select
	*
into
	aggregated_boundaries
from
(
	select
		a.*,
		b.ntaname,
		b.ntacode,
		c.borocd,
		d.schooldist,
		f.distzone 	as schoolsubdist,
		g.label 	as es_zone,
		h.label 	as ms_zone,
		e.objectid 	as Drainage_Planning_Area
	from
		capitalplanning.aggregated_working_pipeline_ms a 
	left join
		dcpadmin.support_admin_ntaboundaries b
	on 
		st_intersects(a.the_geom,b.the_geom)
	/*Address how some DOB projects are coded into the water*/
	left join
		capitalplanning.ny_community_districts c 
	on
		st_intersects(a.the_geom,c.the_geom)
	left join
		capitalplanning.nyc_school_districts d
	on
		st_intersects(a.the_geom,d.the_geom)
	left join
		capitalplanning.dep_data_dep_sewer e
	on
		st_intersects(a.the_geom,e.the_geom)
	left join
		dcpadmin.doe_schoolsubdistricts f
	on
		st_intersects(a.the_geom,f.the_geom)
	left join
		capitalplanning.doe_school_zones_es_2019 g 
	on
		st_intersects(a.the_geom,g.the_geom)
	left join
		capitalplanning.doe_school_zones_ms_2019 h 
	on
		st_intersects(a.the_geom,h.the_geom)
	order by
		a.source,
		a.project_id
) as boundaries

select cdb_cartodbfytable('capitalplanning','aggregated_pipeline_boundaries')
