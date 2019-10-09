/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding elementary school zone boundaries to aggregated pipeline
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_ms_zone;
drop table if exists ungeocoded_projects_ms_zone;
drop table if exists aggregated_ms_zone_longform;
drop table if exists aggregated_ms_zone_PROJEct_level;


SELECT
	*
into
	aggregated_ms_zone
from
(
	with aggregated_boundaries_ms_zone as
(
	SELECT
		a.*,
		b.the_geom as ms_zone_geom,
		b.dbn AS ms_zone,
		b.remarks as ms_remarks,
		st_distance(a.the_geom::geography,b.the_geom::geography) as ms_zone_Distance
	from
		capitalplanning.known_projects_db_20190917_v6_cp_assumptions a
	left join
		capitalplanning.doe_school_zones_ms_2019 b
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
		aggregated_boundaries_ms_zone
	group by
		source,
		PROJECT_id
	having
		count(*)>1
),

	aggregated_boundaries_ms_zone_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.PROJECT_id) in(SELECT concat(source,PROJECT_id) from multi_geocoded_PROJECTs) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_INTERSECTion(a.the_geom,a.ms_zone_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_ms_zone
	from
		aggregated_boundaries_ms_zone a
),

	aggregated_boundaries_ms_zone_3 as
(
	SELECT
		source,
		PROJECT_id,
		sum(proportion_in_ms_zone) as total_proportion
	from
		aggregated_boundaries_ms_zone_2
	group by
		source,
		PROJECT_id
),

	aggregated_boundaries_ms_zone_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_ms_zone/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_ms_zone_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_ms_zone/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_ms_zone_2 a
	left join
		aggregated_boundaries_ms_zone_3 b
	on
		a.PROJECT_id = b.PROJECT_id and a.source = b.source
)

	SELECT * from aggregated_boundaries_ms_zone_4

) as _1;

SELECT
	*
into
	ungeocoded_PROJECTs_ms_zone
from
(
	with ungeocoded_PROJECTs_ms_zone as
(
	SELECT
		a.*,
		coalesce(a.ms_zone,b.dbn) 			as ms_zone_1,
		coalesce(a.ms_remarks,b.remarks)	as ms_remarks_1,
		coalesce(
					a.ms_zone_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as ms_zone_distance1
	from
		aggregated_ms_zone a 
	left join
		capitalplanning.doe_school_zones_ms_2019 b
	on 
		a.ms_zone_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	SELECT * from ungeocoded_PROJECTs_ms_zone
) as _2;


SELECT
	*
into
	aggregated_ms_zone_longform
from
(
	with	min_distances as
(
	SELECT
		PROJECT_id,
		min(ms_zone_distance1) as min_distance
	from
		ungeocoded_PROJECTs_ms_zone
	group by 
		PROJECT_id
),

	all_PROJECTs_ms_zone as
(
	SELECT
		a.*
	from
		ungeocoded_PROJECTs_ms_zone a 
	inner join
		min_distances b
	on
		a.PROJECT_id = b.PROJECT_id and
		a.ms_zone_distance1=b.min_distance
)

	SELECT 
		a.*, 
		b.ms_zone_1 as ms_zone, 
		b.ms_remarks_1 as ms_remarks,
		coalesce(
				b.ms_zone_1,
				case 
					when b.ms_remarks_1 like '%Contact %' then substring(b.ms_remarks_1,1,position('Contact' in b.ms_remarks_1) - 1)
					else b.ms_remarks_1 end
				)											as ms_zone_remarks,
		b.proportion_in_ms_zone_1 as proportion_in_ms_zone,
		round(a.counted_units * b.proportion_in_ms_zone_1) as counted_units_in_ms_zone 
	from 
		known_projects_db_20190917_v6_cp_assumptions a 
	left join 
		all_PROJECTs_ms_zone b 
	on 
		a.source = b.source and 
		a.PROJECT_id = b.PROJECT_id 
	order by 
		source asc,
		PROJECT_id asc,
		PROJECT_name_address asc,
		status asc,
		b.ms_zone_1 asc
) as _3;


SELECT
	*
into
	aggregated_ms_zone_PROJECT_level
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
						nullif(
								coalesce(
										ms_zone,
										case 
											when ms_remarks like '%Contact %' then substring(ms_remarks,1,position('Contact' in ms_remarks) - 1)
											else ms_remarks end
										)
							,''),
						concat(round(100*proportion_in_ms_zone,0),'%')
					),
				'')),
		' | ') 	as ms_zone 
	from
		aggregated_ms_zone_longform
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
) x
;

drop table if exists longform_ms_zone_output;
SELECT
	*
into
	longform_ms_zone_output
from
(
SELECT *  FROM capitalplanning.aggregated_ms_zone_longform where not (source = 'DOB' and status in('Complete','Complete (demolition)'))
) x;

select cdb_cartodbfytable('capitalplanning','longform_ms_zone_output');
