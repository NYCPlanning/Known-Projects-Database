/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped Public Sites dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate Public Sites matches to DOB, HPD Projected Closings, HPD RFP, EDC, ZAP, and Neighborhood Studies data.
2. Calculate Public Sites Increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_deduped
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.public_sites_id 	as project_id,
		a.project 			as project_name,
		a.boro 				as borough,
		a.lead,
		a.total_units,
		CASE
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) > 0 THEN 1 ELSE 0 END AS PLANNER_PROVIDED_PHASING,
		CASE
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.nycha_flag = 1																THEN .5 /*PLACING NYCHA PROJECTS WITHOUT PROVIDED-PHASING*/
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.public_sites_id = 'Public Site Pipeline 23'										THEN 1  /*Placing specific HPD-owned project in 2025*/
			ELSE A.PORTION_BUILT_2025 															END AS portion_built_2025,
		CASE
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.nycha_flag = 1																THEN .5 /*PLACING NYCHA PROJECTS WITHOUT PROVIDED-PHASING*/
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.public_sites_id = 'Public Site Pipeline 23'										THEN 0  /*Placing specific HPD-owned project in 2025*/
			ELSE A.PORTION_BUILT_2035 															END AS portion_built_2035,
		CASE
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.nycha_flag = 1																THEN 0 /*PLACING NYCHA PROJECTS WITHOUT PROVIDED-PHASING*/
			WHEN COALESCE(A.portion_built_2025,A.PORTION_BUILT_2035,A.PORTION_BUILT_2055,0) = 0 AND
				 A.public_sites_id = 'Public Site Pipeline 23'										THEN 0  /*Placing specific HPD-owned project in 2025*/
			ELSE A.PORTION_BUILT_2055 															END AS portion_built_2055,
		a.planner_input,
		a.nycha_flag,
		a.gq_flag,
		a.assisted_living_flag,
		a.senior_housing_flag,
		greatest
		(
			0,
			a.total_units										-
			coalesce(b.dob_units_net,0) 						-
			coalesce(c.hpd_project_incremental_units,0)			-
			coalesce(d.hpd_rfp_incremental_units,0)				-
			coalesce(e.edc_incremental_units,0)					-
			coalesce(f.zap_incremental_units,0)					-
			coalesce(g.nstudy_incremental_units,0)
		) as public_sites_incremental_units,
		b.dob_job_numbers,
		b.dob_units_net,
		c.HPD_Project_IDs as hpd_projected_closings_ids,
		c.hpd_project_incremental_units as hpd_projected_closings_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_incremental_units,
		e.edc_project_ids,
		e.edc_incremental_units,
		f.zap_project_ids,
		f.zap_incremental_units,
		g.nstudy_project_ids,
		g.nstudy_incremental_units
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.public_sites_dob_final b
	on 
		a.public_sites_id = b.public_sites_id 
	left join
		capitalplanning.public_sites_hpd_projected_closings_final c 
	on
		a.public_sites_id = c.public_sites_id 
	left join
		capitalplanning.public_sites_hpd_rfps_final d
	on
		a.public_sites_id = d.public_sites_id 
	left join
		capitalplanning.public_sites_edc_final e
	on
		a.public_sites_id = e.public_sites_id
	left join
		capitalplanning.public_sites_zap_final f
	on
		a.public_sites_id = f.public_sites_id
	left join
		capitalplanning.public_sites_nstudy_final g
	on
		a.public_sites_id = g.public_sites_id
	order by
		a.public_sites_id asc
) public_sites_deduped

/*RUN IN REGULAR CARTO*/
select cdb_cartodbfytable('capitalplanning','public_sites_deduped')
