/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped ZAP dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate ZAP matches to DOB, HPD Projected Closings, HPD RFP, and EDC data.
2. Calculate ZAP increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

/*Remember to change the column ordering here to align with what you saw in the relevant projects list sent to SCA and DOE*/

select
	*
into
	zap_deduped
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		'DCP Applications' as Source,
		a.project_id,
		a.project_name,
		a.project_status,
		a.process_stage,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		greatest
				(
					0,
					a.total_units 										-
					coalesce(b.dob_units_net,0) 						-
					coalesce(c.hpd_project_incremental_units,0)			-
					coalesce(d.hpd_rfp_incremental_units,0)				-
					coalesce(e.edc_incremental_units,0)
				) as zap_incremental_units,
		a.applicant_type,
		a.ulurp_non_ulurp,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.Anticipated_year_built as applicant_projected_build_year,
		a.early_stage_flag,
		a.si_seat_cert,
		a.NYCHA_Flag,
		a.gq_flag,
		a.Assisted_Living_Flag,
		case when a.Senior_Housing_Flag = 1 then 1 else 0 end as Senior_Housing_Flag,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.planner_input,
		b.dob_job_numbers,
		b.dob_units_net,
		c.hpd_project_ids,
		c.hpd_project_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_incremental_units,
		e.edc_project_ids,
		e.edc_incremental_units
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	left join
		capitalplanning.zap_dob_final b
	on 
		a.project_id = b.project_id 
	left join
		capitalplanning.zap_hpd_projected_closings_final c 
	on
		a.project_id = c.project_id 
	left join
		capitalplanning.zap_hpd_rfps_final d
	on
		a.project_id = d.project_id 
	left join
		capitalplanning.zap_edc_final e
	on
		a.project_id = e.project_id
) zap_deduped


/*RUN IN REGULAR CARTO*/

select cdb_cartodbfytable('capitalplanning','zap_deduped')
