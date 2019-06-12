/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Summarizing 2025, 2035, and 2055 growth by ES_Zone
START DATE: 6/11/2019
*************************************************************************************************************************************************************************************/


select
	row_number() over() as cartodb_id,
	*
into
	ES_Zone_Growth_Summary_Known_Projects_20190611
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		a.ES_Zone,
		sum(a.portion_built_2025*a.counted_units) as Units_2025,
		sum(a.portion_built_2035*a.counted_units) as Units_2025_2035,
		sum(a.portion_built_2055*a.counted_units) as Units_2035_2055,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'DOB' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as dob_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'HPD Projected Closings' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as hpd_projected_closings_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'HPD RFPs' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as hpd_rfp_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'EDC Projected Projects' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as edc_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'DCP Applications' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as dcp_applications_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'Empire State Development Projected Projects' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as state_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'Neighborhood Study Rezoning Commitments' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as neighborhood_study_rezoning_commitment_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'Future City-Sponsored RFPs/RFEIs' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as public_sites_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'DCP Planner-Added Projects' then
											nullif
											(
												concat_ws
												(
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as planner_added_projects_matches
	from
		aggregated_es_zone_longform a
	left join
		doe_school_zones_es_2019 b
	on
		a.es_zone = 
								coalesce(
										b.dbn,
										case 
											when b.remarks like '%Contact %' then substring(b.remarks,1,position('Contact' in b.remarks) - 1)
											else b.remarks end
										)
	group by
		b.the_geom,
		b.the_geom_webmercator,
		a.es_zone
	order by 
		a.es_zone asc 
) es_zone_Growth_Summary_Known_Projects_20190611
order by
	es_zone asc