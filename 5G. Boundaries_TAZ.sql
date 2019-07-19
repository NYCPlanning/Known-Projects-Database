/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding TAZ boundaries to aggregated pipeline
START DATE: 7/2/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_taz;
drop table if exists ungeocoded_projects_taz;
drop table if exists aggregated_taz_longform;
drop table if exists aggregated_taz_PROJEct_level;


SELECT
	*
into
	aggregated_taz
from
(
	with aggregated_boundaries_taz as
(
	SELECT
		a.*,
		b.the_geom as taz_geom,
		b.bpm2012taz AS taz,
		b.distname as distname,
		st_distance(a.the_geom::geography,b.the_geom::geography) as taz_Distance
	from
		capitalplanning.known_projects_db_20190712_v5 a
	left join
		capitalplanning.nybpm2012_tazboundaryrev2 b
	on 
	case
		/*Treating large developments as polygons*/
		when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('EDC Projected Projects','DCP Applications','DCP Planner-Added PROJECTs') 		then
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating subdivisions in SI across many lots as polygons*/
		when a.PROJECT_id in(SELECT PROJECT_id from zap_PROJECTs_many_bbls) and a.PROJECT_name_address like '%SD %'								then
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
		when a.PROJECT_name_address like '%Resilient Housing%' and a.source in('DCP Applications','DCP Planner-Added PROJECTs')									then
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
		when (a.PROJECT_name_address like '%NIHOP%' or a.PROJECT_name_address like '%NCP%' )and a.source in('DCP Applications','DCP Planner-Added PROJECTs')	then
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

			/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
		when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') 														then
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1
	

		/*Treating other polygons as points, using their centroid*/
		when st_area(a.the_geom) > 0 																											then
			st_INTERSECTs(st_centroid(a.the_geom),b.the_geom) 

		/*Treating points as points*/
		else
			st_INTERSECTs(a.the_geom,b.the_geom) 																								end
																									/*Only matching if at least 10% of the polygon
		                           																	is in the boundary. Otherwise, the polygon will be
		                           																	apportioned to its other boundaries only*/
),

	multi_geocoded_PROJECTs as
(
	SELECT
		source,
		PROJECT_id
	from
		aggregated_boundaries_taz
	group by
		source,
		PROJECT_id
	having
		count(*)>1
),

	aggregated_boundaries_taz_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.PROJECT_id) in(SELECT concat(source,PROJECT_id) from multi_geocoded_PROJECTs) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_INTERSECTion(a.the_geom,a.taz_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_taz
	from
		aggregated_boundaries_taz a
),

	aggregated_boundaries_taz_3 as
(
	SELECT
		source,
		PROJECT_id,
		sum(proportion_in_taz) as total_proportion
	from
		aggregated_boundaries_taz_2
	group by
		source,
		PROJECT_id
),

	aggregated_boundaries_taz_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_taz/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_taz_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_taz/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_taz_2 a
	left join
		aggregated_boundaries_taz_3 b
	on
		a.PROJECT_id = b.PROJECT_id and a.source = b.source
)

	SELECT * from aggregated_boundaries_taz_4

) as _1;

SELECT
	*
into
	ungeocoded_PROJECTs_taz
from
(
	with ungeocoded_PROJECTs_taz as
(
	SELECT
		a.*,
		coalesce(a.taz,b.bpm2012taz) 	as taz_1,
		coalesce(a.distname,b.distname) as distname_1,
		coalesce(
					a.taz_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as taz_distance1
	from
		aggregated_taz a 
	left join
		capitalplanning.nybpm2012_tazboundaryrev2 b
	on 
		a.taz_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	SELECT * from ungeocoded_PROJECTs_taz
) as _2;


SELECT
	*
into
	aggregated_taz_longform
from
(
	with	min_distances as
(
	SELECT
		PROJECT_id,
		min(taz_distance1) as min_distance
	from
		ungeocoded_PROJECTs_taz
	group by 
		PROJECT_id
),

	all_PROJECTs_taz as
(
	SELECT
		a.*
	from
		ungeocoded_PROJECTs_taz a 
	inner join
		min_distances b
	on
		a.PROJECT_id = b.PROJECT_id and
		a.taz_distance1=b.min_distance
)

	SELECT 
		a.*, 
		b.taz_1 as taz,
		b.distname_1 as distname,
		b.proportion_in_taz_1 							as proportion_in_taz,
		round(a.counted_units * b.proportion_in_taz_1) 	as counted_units_in_taz
	from 
		known_projects_db_20190712_v5 a 
	left join 
		all_PROJECTs_taz b 
	on 
		a.source = b.source and 
		a.PROJECT_id = b.PROJECT_id 
	order by 
		source asc,
		PROJECT_id asc,
		PROJECT_name_address asc,
		status asc,
		b.taz_1 asc
) as _3;


SELECT
	*
into
	aggregated_taz_PROJECT_level
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		source,
		PROJECT_id,
		PROJECT_name_address,
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
		hpd_PROJECTed_closing_matches,
		hpd_PROJECTed_closing_matched_units,
		hpd_rfp_matches,
		hpd_rfp_matched_units,
		edc_matches,
		edc_matched_units,
		dcp_application_matches,
		dcp_application_matched_units,
		state_PROJECT_matches,
		state_PROJECT_matched_units,
		neighborhood_study_matches,
		neighborhood_study_units,
		public_sites_matches,
		public_sites_units,
		planner_PROJECTs_matches,
		planner_PROJECTs_units,
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
						taz,
						concat(round(100*proportion_in_taz,0),'%')
					),
				'')),
		' | ') 	as taz,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(distname,''),
						concat(round(100*proportion_in_taz,0),'%')
					),
				'')),
		' | ') 	as tazname 
	from
		aggregated_taz_longform
	group by
		the_geom,
		the_geom_webmercator,
		source,
		PROJECT_id,
		PROJECT_name_address,
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
		hpd_PROJECTed_closing_matches,
		hpd_PROJECTed_closing_matched_units,
		hpd_rfp_matches,
		hpd_rfp_matched_units,
		edc_matches,
		edc_matched_units,
		dcp_application_matches,
		dcp_application_matched_units,
		state_PROJECT_matches,
		state_PROJECT_matched_units,
		neighborhood_study_matches,
		neighborhood_study_units,
		public_sites_matches,
		public_sites_units,
		planner_PROJECTs_matches,
		planner_PROJECTs_units,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag
) x;

drop table if exists longform_taz_output;
SELECT
	*
into
	longform_taz_output
from
(
	SELECT 
		*  
	FROM 
		capitalplanning.aggregated_taz_longform 
	where 
		not (source = 'DOB' and status in('Complete','Complete (demolition)')) and
		source not in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites')
) x;
