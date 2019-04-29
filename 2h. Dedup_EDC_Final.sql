
/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped EDC dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate EDC matches to DOB, HPD Projected Closings, and HPD RFP data.
2. Calculate EDC increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	edc_deduped
from
(
	select
		a.the_geom,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.total_units,
		a.build_year,
		a.comments_on_phasing,
		b.dob_job_numbers,
		b.dob_units_net,
		c.hpd_project_ids,
		c.hpd_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_units,
		greatest
			(
				0,
				a.total_units - coalesce(b.dob_units_net,0) 	- 
				coalesce(c.hpd_incremental_units,0) 		-
				coalesce(d.hpd_rfp_units,0)
			) as EDC_Incremental_Units
	from
		capitalplanning.edc_2018_sca_input_1_limited a
	left join
		capitalplanning.edc_dob_1 b
	on
		a.edc_project_id = b.edc_project_id
	left join
		capitalplanning.edc_hpd_1 c
	on
		a.edc_project_id = c.edc_project_id
	left join
		capitalplanning.edc_hpd_rfp_1 d
	on
		a.edc_project_id = d.edc_project_id
) as edc_deduped
order by
	edc_project_id asc
	
