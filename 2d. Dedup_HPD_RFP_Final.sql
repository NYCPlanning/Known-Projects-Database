/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Finalizing deduped HPD RFP data
Sources: 
*************************************************************************************************************************************************************************************/

/*********************************************
		RUN THE FOLLOWING QUERY IN
		CARTO BATCH
*********************************************/

select
	*
into
	hpd_rfp_deduped
from
(
	select
		a.the_geom,
		a.the_geom_webmercator,
		'HPD RFPs' as Source,
		a.project_id,
		a.project_name,
		a.lead_agency,
		a.status,
		a.borough,
		a.total_units,
		greatest
			(
				a.total_units - coalesce(a.dob_units_net,0) - coalesce(b.hpd_projected_closings_incremental_units,0)
				,0
			) as HPD_RFP_Incremental_Units,
		a.likely_to_be_built_by_2025_flag,
		a.excluded_project_flag,
		a.rationale_for_exclusion,
		a.nycha_flag,
		a.gq_flag,
		case when a.senior_housing_flag = 1 then 1 else 0 end as senior_housing_flag,
		a.assisted_living_flag,
		a.dob_job_numbers,
		a.dob_units_net,
		b.hpd_projected_closings_ids,
		b.hpd_projected_closings_incremental_units
	from
		capitalplanning.hpd_rfp_dob_final a
	left join
		capitalplanning.hpd_rfp_hpd_final b
	on
		a.project_id = b.project_id
) as x
order by project_id::numeric asc

/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_rfp_deduped')