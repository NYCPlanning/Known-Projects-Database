/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Summarizing 2025, 2035, and 2055 growth by ms_zone
START DATE: 6/11/2019
*************************************************************************************************************************************************************************************/

drop table if exists ms_zone_Growth_Summary_Known_Projects_20190611;

select
	row_number() over() as cartodb_id,
	*
into
	ms_zone_Growth_Summary_Known_Projects_20190611
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		a.ms_zone,
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
		doe_school_zones_ms_2019 b
	left join
		(select * from aggregated_mS_zone_longform where not(source = 'DOB' and status in('Complete','Complete (demolition)'))) a
	on
		a.ms_zone = 
								coalesce(
										b.dbn,
										case 
											when b.remarks like '%Contact %' then substring(b.remarks,1,position('Contact' in b.remarks) - 1)
											else b.remarks end
										)
	group by
		b.the_geom,
		b.the_geom_webmercator,
		a.ms_zone
	order by 
		a.ms_zone asc 
) ms_zone_Growth_Summary_Known_Projects_20190611
order by
	ms_zone asc