/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding nta boundaries to aggregated pipeline
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_nta_cp_assumptions;
drop table if exists ungeocoded_PROJEcts_nta_cp_assumptions;
drop table if exists aggregated_nta_longform_cp_assumptions;
drop table if exists aggregated_nta_PROJEct_level_cp_assumptions;


SELECT
	*
into
	aggregated_nta_cp_assumptions
from
(
	with aggregated_boundaries_nta as
(
	SELECT
		a.*,
		b.the_geom as nta_geom,
		b.ntaname,
		b.ntacode,
		st_distance(a.the_geom::geography,b.the_geom::geography) as nta_Distance
	from
		capitalplanning.known_projects_db_20190712_v5_cp_assumptions a
	left join
		dcpadmin.support_admin_ntaboundaries b
	on 
	case
		/*Treating large developments as polygons*/
		when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('EDC Projected Projects','DCP Applications','DCP Planner-Added PROJECTs')	then
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
		aggregated_boundaries_nta
	group by
		source,
		PROJECT_id
	having
		count(*)>1
),

	aggregated_boundaries_nta_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.PROJECT_id) in(SELECT concat(source,PROJECT_id) from multi_geocoded_PROJECTs) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_INTERSECTion(a.the_geom,a.nta_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_nta
	from
		aggregated_boundaries_nta a
),

	aggregated_boundaries_nta_3 as
(
	SELECT
		source,
		PROJECT_id,
		sum(proportion_in_nta) as total_proportion
	from
		aggregated_boundaries_nta_2
	group by
		source,
		PROJECT_id
),

	aggregated_boundaries_nta_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_nta/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_nta_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_nta/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_nta_2 a
	left join
		aggregated_boundaries_nta_3 b
	on
		a.PROJECT_id = b.PROJECT_id and a.source = b.source
)

	SELECT * from aggregated_boundaries_nta_4

) as _1;

SELECT
	*
into
	ungeocoded_PROJECTs_nta_cp_assumptions
from
(
	with ungeocoded_PROJECTs_nta as
(
	SELECT
		a.*,
		coalesce(a.ntaname,b.ntaname) as ntaname_1,
		coalesce(a.ntacode,b.ntacode) as ntacode_1,
		coalesce(
					a.nta_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as nta_distance1
	from
		aggregated_nta_cp_assumptions a 
	left join
		dcpadmin.support_admin_ntaboundaries b
	on 
		a.nta_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	SELECT * from ungeocoded_PROJECTs_nta
) as _2;



drop table if exists aggregated_nta_longform_cp_assumptions;

SELECT
	*
into
	aggregated_nta_longform_cp_assumptions
from
(
	with	min_distances as
(
	SELECT
		PROJECT_id,
		min(nta_distance1) as min_distance
	from
		ungeocoded_PROJECTs_nta_cp_assumptions
	group by 
		PROJECT_id
),

	all_PROJECTs_nta as
(
	SELECT
		a.*
	from
		ungeocoded_PROJECTs_nta_cp_assumptions a 
	inner join
		min_distances b
	on
		a.PROJECT_id = b.PROJECT_id and
		a.nta_distance1=b.min_distance
)

	SELECT 
		a.*, 
		b.ntaname_1 as ntaname,
		b.ntacode_1 as ntacode,
		b.proportion_in_nta_1 as proportion_in_nta,
		round(a.counted_units * b.proportion_in_nta_1) as counted_units_in_nta
	from 
		known_projects_db_20190712_v5_cp_assumptions a 
	left join 
		all_PROJECTs_nta b 
	on 
		a.source = b.source and 
		a.PROJECT_id = b.PROJECT_id 
	order by 
		source asc,
		PROJECT_id asc,
		PROJECT_name_address asc,
		status asc,
		b.ntaname_1 asc,
		b.ntacode_1 asc
) as _3;


SELECT
	*
into
	aggregated_nta_PROJECT_level_cp_assumptions
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
						nullif(ntaname,''),
						concat(round(100*proportion_in_nta,0),'%')
					),
				'')),
		' | ') 	as ntaname,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(ntacode,''),
						concat(round(100*proportion_in_nta,0),'%')
					),
				'')),
		' | ') 	as ntacode 
	from
		aggregated_nta_longform_cp_assumptions
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


drop table if exists longform_nta_output_cp_assumptions;
SELECT
	*, row_number() over() as cartodb_id_replacement
into
	longform_nta_output_cp_assumptions
from
(
SELECT *  FROM capitalplanning.aggregated_nta_longform_cp_assumptions where not (source = 'DOB' and status in('Complete','Complete (demolition)'))
) x;
