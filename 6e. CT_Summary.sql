/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Summarizing 2025, 2035, and 2055 growth by Census Tract
START DATE: 6/11/2019
*************************************************************************************************************************************************************************************/

drop table if exists ct_Growth_Summary_Known_Projects_20190611;

select distinct
	row_number() over() as cartodb_id,
	*
into
	ct_Growth_Summary_Known_Projects_20190611
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		a.ct,
		sum(a.portion_built_2025*a.counted_units_in_ct) as Units_2025,
		sum(a.portion_built_2035*a.counted_units_in_ct) as Units_2025_2035,
		sum(a.portion_built_2055*a.counted_units_in_ct) as Units_2035_2055,
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
		census_tract_2010_190412_ms b
	left join
		(select * from aggregated_ct_longform where not(source = 'DOB' and status in('Complete','Complete (demolition)'))) a
	on
		a.ct = b.boro_ct201
	group by
		b.the_geom,
		b.the_geom_webmercator,
		a.ct
	order by 
		a.ct asc 
) ct_Growth_Summary_Known_Projects_20190611
order by
	ct asc;


select cdb_cartodbfytable('capitalplanning','CT_Growth_Summary_Known_Projects_20190611') ;