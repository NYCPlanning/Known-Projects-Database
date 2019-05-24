/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from HPD Projected Closings
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to HPD projects using Address, spatial, and overlap.
2. If an HPD job maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	zap_hpd_projected_closings
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		a.total_unit_source,
		a.ZAP_Unit_Source,
		a.applicant_type,
		a.project_status,
		a.previous_project_status,
		a.process_stage,
		a.previous_process_stage,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.Anticipated_year_built,
		a.remaining_units_likely_to_be_built_2018,
		a.rationale_2018,
		a.rationale_2019,
		a.phasing_notes_2019,
		a.additional_notes_2019,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.si_seat_cert,
		case 
			when
				position(upper(b.address) in upper(a.project_name)) > 0 and
				case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) END 	then 'Address' 
			when
				st_intersects(a.the_geom,b.the_geom)																				then 'Spatial'
			when
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)															then 'Proximity'
																																	end as Match_Type,
		b.project_id 					as HPD_Project_ID,
		b.address						as HPD_Address,
		b.bbl 							as HPD_BBL,
		b.total_units 					as HPD_Project_Total_Units,
		b.hpd_incremental_units 		as HPD_Project_Incremental_Units,
		st_distance(a.the_geom::geography,b.the_geom::geography)			as distance
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.hpd_deduped b
	on
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) or 
		(
			position(upper(b.address) in upper(a.project_name)) > 0 and
			case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) end
		)
) zap_hpd_projected_closings