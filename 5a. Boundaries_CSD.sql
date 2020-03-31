/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding CSD boundaries to aggregated pipeline
START DATE: 6/10/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_csd;
drop table if exists ungeocoded_PROJECTs_CSD;
drop table if exists aggregated_CSD_longform;
drop table if exists aggregated_CSD_PROJECT_level;


SELECT
	*
into
	aggregated_CSD
from
(
	with aggregated_boundaries_CSD as
(
	SELECT
		a.*,
		b.the_geom as CSD_geom,
		b.SCHOOLDIST AS CSD,
		st_distance(a.the_geom::geography,b.the_geom::geography) as CSD_Distance
	from
		capitalplanning.known_projects_db_20190917_v6_cp_assumptions a
	left join
		capitalplanning.nyc_school_districts b
	on 
	case
		/*Treating large developments as polygons*/
		when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('EDC Projected Projects','DCP Applications','DCP Planner-Added Projects')	then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating subdivisions in SI across many lots as polygons*/
		when a.PROJECT_id in(SELECT PROJECT_id from zap_PROJECTs_many_bbls) and a.PROJECT_name_address like '%SD %'												then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
		when a.PROJECT_name_address like '%Resilient Housing%' and a.source in('DCP Applications','DCP Planner-Added PROJECTs')									then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
		when (a.PROJECT_name_address like '%NIHOP%' or a.PROJECT_name_address like '%NCP%' )and a.source in('DCP Applications','DCP Planner-Added PROJECTs')	then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.the_geom,b.the_geom) and CAST(ST_Area(ST_INTERSECTion(a.the_geom,b.the_geom))/ST_Area(a.the_geom) AS DECIMAL) >= .1

		/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
		when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') 														then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
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

	/*Identify projects geocoded to multiple CSDs*/
	multi_geocoded_PROJECTs as
(
	SELECT
		source,
		PROJECT_id
	from
		aggregated_boundaries_CSD
	group by
		source,
		PROJECT_id
	having
		count(*)>1
),

	/*Calculate the proportion of each project in each CSD that it overlaps with*/
	aggregated_boundaries_CSD_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.PROJECT_id) in(SELECT concat(source,PROJECT_id) from multi_geocoded_PROJECTs) and st_area(a.the_geom) > 0	then 
					CAST(ST_Area(ST_INTERSECTion(a.the_geom,a.CSD_geom))/ST_Area(a.the_geom) AS DECIMAL) 										else
					1 end																														as proportion_in_CSD
	from
		aggregated_boundaries_CSD a
),

	/*
	  If <10% of a project falls into a particular CSD, then the sum of all proportions of a project in each CSD would be <100%, because
	  projects with less than 10% in a CSD are not assigned to that CSD. The next two steps ensure that 100% of each project's units are
	  allocated to a CSD.
	*/
	aggregated_boundaries_CSD_3 as
(
	SELECT
		source,
		PROJECT_id,
		sum(proportion_in_CSD) as total_proportion
	from
		aggregated_boundaries_CSD_2
	group by
		source,
		PROJECT_id
),

	aggregated_boundaries_CSD_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_CSD/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_CSD_1,
		case when b.total_proportion is not null then round(a.counted_units * cast(a.proportion_in_CSD/b.total_proportion as decimal)) 
			 else a.counted_units end as counted_units_1
	from
		aggregated_boundaries_CSD_2 a
	left join
		aggregated_boundaries_CSD_3 b
	on
		a.PROJECT_id = b.PROJECT_id and a.source = b.source
)

	SELECT * from aggregated_boundaries_CSD_4

) as _1;


/*Identify projects which did not geocode to any CSD*/
SELECT
	*
into
	ungeocoded_PROJECTs_CSD
from
(
	with ungeocoded_PROJECTs_CSD as
(
	SELECT
		a.*,
		coalesce(a.CSD,b.schooldist) as CSD_1,
		coalesce(
					a.CSD_distance,
					st_distance(
								b.the_geom::geography,
								case
									when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 	then a.the_geom::geography
									when st_area(a.the_geom) > 0 																										then st_centroid(a.the_geom)::geography
									else a.the_geom::geography 																											end
								)
				) as CSD_distance1
	from
		aggregated_CSD a 
	left join
		capitalplanning.nyc_school_districts b
	on 
		a.CSD_distance is null and
		case
			when (st_area(a.the_geom::geography)>10000 or total_units > 500) and a.source in('DCP Applications','DCP Planner-Added PROJECTs') 		then
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)
			when st_area(a.the_geom) > 0 																											then
				st_dwithin(st_centroid(a.the_geom)::geography,b.the_geom::geography,500)
			else
				st_dwithin(a.the_geom::geography,b.the_geom::geography,500)																			end
)
	SELECT * from ungeocoded_PROJECTs_CSD
) as _2;

/*Assign ungeocoded projects to their closest CSD*/
SELECT
	*
into
	aggregated_CSD_longform
from
(
	with	min_distances as
(
	SELECT
		PROJECT_id,
		min(CSD_distance1) as min_distance
	from
		ungeocoded_PROJECTs_CSD
	group by 
		PROJECT_id
),

	all_PROJECTs_CSD as
(
	SELECT
		a.*
	from
		ungeocoded_PROJECTs_CSD a 
	inner join
		min_distances b
	on
		a.PROJECT_id = b.PROJECT_id and
		a.CSD_distance1=b.min_distance
)

	SELECT 
		a.*, 
		b.CSD_1 as CSD, 
		b.proportion_in_CSD_1 as proportion_in_CSD,
		round(a.counted_units * b.proportion_in_CSD_1) as counted_units_in_CSD 
	from 
		known_projects_db_20190917_v6_cp_assumptions a 
	left join 
		all_PROJECTs_CSD b 
	on 
		a.source = b.source and 
		a.PROJECT_id = b.PROJECT_id 
	order by 
		source asc,
		PROJECT_id asc,
		PROJECT_name_address asc,
		status asc,
		b.CSD_1 asc
) as _3
	order by
		csd asc;

/*Aggregate all results to the project-level, because if a project matches with multiple CSDs, it'll appear in multiple rows*/
SELECT
	*
into
	aggregated_CSD_PROJECT_level
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
						nullif(concat(CSD),''),
						concat(round(100*proportion_in_csd,0),'%')
					),
				'')),
		' | ') 	as CSD 
	from
		(select * from aggregated_CSD_longform order by csd asc) a
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

/*
	Output final CSD-based KPDB. This is not at the project-level, but rather the project & CSD-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.
*/
drop table if exists longform_csd_output;
SELECT
	*
into
	longform_csd_output
from
(
SELECT *  FROM capitalplanning.aggregated_csd_longform where not (source = 'DOB' and status in('Complete','Complete (demolition)'))
	order by 
		source asc,
		PROJECT_id asc,
		PROJECT_name_address asc,
		status asc
) x;
