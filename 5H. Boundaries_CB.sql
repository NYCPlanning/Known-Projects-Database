/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding Census Block boundaries to aggregated pipeline
START DATE: 8/7/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/


drop table if exists aggregated_cb;
drop table if exists ungeocoded_PROJECTs_cb;
drop table if exists aggregated_cb_longform;
drop table if exists aggregated_cb_PROJECT_level;

select
	*
into
	aggregated_cb
from
(
	with aggregated_boundaries_cb as
(
	select
		a.*,
		b.the_geom as cb_geom,
		b.bctcb2010,
		st_distance(a.the_geom::geography,b.the_geom::geography) as cb_distance
	from
		capitalplanning.known_projects_db_20190917_v6_cp_assumptions a
	left join
		dcpadmin.dcp_nycbctcb2010 b
	on 
	case
		/*Treating large developments as polygons*/
		when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('EDC Projected Projects','DCP Applications','DCP Planner-Added PROJECTs')	then
			st_intersects(a.the_geom,b.the_geom) and 
			(
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1 or
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(b.the_geom) AS DECIMAL) >=.5
			)

		/*Treating subdivisions in SI across many lots as polygons*/
		when a.project_id in(select project_id from zap_projects_many_bbls) and a.project_name_address like '%SD %'								then
			st_intersects(a.the_geom,b.the_geom) and 
			(
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1 or
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(b.the_geom) AS DECIMAL) >=.5
			)

		/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
		when a.PROJECT_name_address like '%Resilient Housing%' and a.source in('DCP Applications','DCP Planner-Added PROJECTs')									then
			st_INTERSECTs(a.the_geom,b.the_geom) and 
			(
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1 or
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(b.the_geom) AS DECIMAL) >=.5
			)
		/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
		when (a.PROJECT_name_address like '%NIHOP%' or a.PROJECT_name_address like '%NCP%' )and a.source in('DCP Applications','DCP Planner-Added PROJECTs')	then
			st_INTERSECTs(a.the_geom,b.the_geom) and 
			(
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1 or
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(b.the_geom) AS DECIMAL) >=.5
			)
	/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
		when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') 														then
			st_INTERSECTs(a.the_geom,b.the_geom) and 
			(
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1 or
				CAST(ST_Area(ST_Intersection(a.the_geom,b.the_geom))/ST_Area(b.the_geom) AS DECIMAL) >=.5
			)
		/*Treating other polygons as points, using their centroid*/
		when st_area(a.the_geom) > 0 																															then
			 st_INTERSECTs(st_centroid(a.the_geom),b.the_geom) 

		/*Treating points as points*/
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
		aggregated_boundaries_cb
	group by
		source,
		project_id
	having
		count(*)>1
),

	aggregated_boundaries_cb_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.project_id) in(select concat(source,project_id) from multi_geocoded_projects) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_Intersection(a.the_geom,a.cb_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_cb
	from
		aggregated_boundaries_cb a
),

	aggregated_boundaries_cb_3 as
(
	SELECT
		source,
		project_id,
		sum(proportion_in_cb) as total_proportion
	from
		aggregated_boundaries_cb_2
	group by
		source,
		project_id
),

	aggregated_boundaries_cb_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_cb/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_cb_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_cb/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_cb_2 a
	left join
		aggregated_boundaries_cb_3 b
	on
		a.project_id = b.project_id and a.source = b.source
)

	select * from aggregated_boundaries_cb_4

) as _1;

select
	*
into
	ungeocoded_projects_cb
from
(
	with ungeocoded_projects_cb as
(
	select
		a.*,
		coalesce(a.bctcb2010,b.bctcb2010) as bctcb2010_1,
		coalesce(
					a.cb_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added Projects') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as cb_distance1
	from
		aggregated_cb a 
	left join
		dcpadmin.dcp_nycbctcb2010 b
	on 
		a.cb_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added Projects') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	select * from ungeocoded_projects_cb
) as _2;


select
	*
into
	aggregated_cb_longform
from
(
	with	min_distances as
(
	select
		project_id,
		min(cb_distance1) as min_distance
	from
		ungeocoded_projects_cb
	group by 
		project_id
),

	all_projects_cb as
(
	select
		a.*
	from
		ungeocoded_projects_cb a 
	inner join
		min_distances b
	on
		a.project_id = b.project_id and
		a.cb_distance1=b.min_distance
)

	select 
		a.*, 
		b.bctcb2010_1 as cb, 
		b.proportion_in_cb_1 as proportion_in_cb,
		round(a.counted_units * b.proportion_in_cb_1) as counted_units_in_cb 
	from 
		known_projects_db_20190917_v6_cp_assumptions a 
	left join 
		all_projects_cb b 
	on 
		a.source = b.source and 
		a.project_id = b.project_id 
	order by 
		source asc,
		project_id asc,
		project_name_address asc,
		status asc,
		b.bctcb2010_1 asc
) as _3
;

select
	*
into
	aggregated_cb_project_level
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name_address,
		dob_job_type,
		status,
		borough,
		total_units,
		deduplicated_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		planner_input,
		dob_matches,
		dob_matched_units,
		hpd_projected_closing_matches,
		hpd_projected_closing_matched_units,
		hpd_rfp_matches,
		hpd_rfp_matched_units,
		edc_matches,
		edc_matched_units,
		dcp_application_matches,
		dcp_application_matched_units,
		state_project_matches,
		state_project_matched_units,
		neighborhood_study_matches,
		neighborhood_study_units,
		public_sites_matches,
		public_sites_units,
		planner_projects_matches,
		planner_projects_units,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(cb,''),
						concat(round(100*proportion_in_cb,0),'%')
					),
				'')),
		' | ') 	as Census_Tract 
	from
		aggregated_cb_longform
	group by
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name_address,
		dob_job_type,
		status,
		borough,
		total_units,
		deduplicated_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		planner_input,
		dob_matches,
		dob_matched_units,
		hpd_projected_closing_matches,
		hpd_projected_closing_matched_units,
		hpd_rfp_matches,
		hpd_rfp_matched_units,
		edc_matches,
		edc_matched_units,
		dcp_application_matches,
		dcp_application_matched_units,
		state_project_matches,
		state_project_matched_units,
		neighborhood_study_matches,
		neighborhood_study_units,
		public_sites_matches,
		public_sites_units,
		planner_projects_matches,
		planner_projects_units,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag
) x
;


drop table if exists longform_cb_output;
SELECT
	*
into
	longform_cb_output
from
(
	SELECT 
		*  
	FROM 
		capitalplanning.aggregated_cb_longform 
	where 
		not (source = 'DOB' and status in('Complete','Complete (demolition)')) 
		and
		source not in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites')
) x;


drop table if exists cb_average_unit_size;
select
	*
into
	cb_average_unit_size
from
(

	with _1 as
	(
	select
		cb,
		count(*) as cb_dob_count,
		avg(total_units) as cb_average_unit_size
	from
		longform_cb_output
	where
		source = 'DOB' and
		dob_job_type = 'New Building'
	group by
		cb
	),

	_2 as
	(
		select
			row_number() over() as cartodb_id,
			b.the_geom,
			b.the_geom_webmercator,
			a.cb,
			a.cb_dob_count,
			a.cb_average_unit_size
		from
			_1 a
		left join
			dcpadmin.dcp_housingbctcb_2010 b
		on
			a.cb = b.bctcb::text
	)

	select * from _2
) x;

select cdb_cartodbfytable('capitalplanning','cb_average_unit_size');

update cb_average_unit_size a
set
	the_geom = b.the_geom,
	the_geom_webmercator = b.the_geom_webmercator
from
	dcpadmin.dcp_housingbctcb_2010 b
where
	a.cb = b.bctcb::text
	