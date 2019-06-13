/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Summarizing 2025, 2035, and 2055 growth by Subdistrict
START DATE: 6/11/2019
*************************************************************************************************************************************************************************************/

drop table if exists subdistrict_Growth_Summary_Known_Projects_20190611;

select
	row_number() over() as cartodb_id,
	*
into
	subdistrict_Growth_Summary_Known_Projects_20190611
from
(
	select
		b.the_geom,
		b.the_geom_webmercator,
		concat('"',a.subdistrict,'"') as subdistrict,
		case
			when b.boro = 'M' then 'Manhattan'
			when b.boro = 'K' then 'Brooklyn'
			when b.boro = 'X' then 'Bronx'
			when b.boro = 'Q' then 'Queens'
			when b.boro = 'R' then 'Staten Island' end			 as borough,
		sum(a.portion_built_2025*a.counted_units_in_subdistrict) as Units_2025,
		sum(a.portion_built_2035*a.counted_units_in_subdistrict) as Units_2025_2035,
		sum(a.portion_built_2055*a.counted_units_in_subdistrict) as Units_2035_2055,
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
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
													': ',concat(a.project_id,', ',a.project_name_address),concat(a.counted_units_in_subdistrict,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as planner_added_projects_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'Neighborhood Study Projected Development Sites' then
											nullif
											(
												concat_ws
												(
													': ',a.project_id,concat(a.counted_units_in_subdistrict,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as nstudy_projected_development_matches,
		array_to_string
		(
			array_agg
			(
				case
					when a.source = 'Future Neighborhood Studies' then
											nullif
											(
												concat_ws
												(
													': ',a.project_id,concat(a.counted_units_in_subdistrict,' units')
												)
												,''
											)
				else null end
			),
		' | '
		) 	as future_nstudy_matches
	from
		dcpadmin.doe_schoolsubdistricts b
	left join
		(select * from aggregated_subdistrict_longform where not(source = 'DOB' and status in('Complete','Complete (demolition)'))) a
	on
		a.subdistrict = b.distzone
	group by
		b.the_geom,
		b.the_geom_webmercator,
		a.subdistrict,
		b.boro
	order by 
	substring(a.subdistrict,1,position('/' in a.subdistrict) -1)::numeric asc,
	substring(a.subdistrict,position('/' in a.subdistrict) +1,2)::numeric asc
) subdistrict_Growth_Summary_Known_Projects_20190611;


select cdb_cartodbfytable('capitalplanning','subdistrict_Growth_Summary_Known_Projects_20190611') ;	