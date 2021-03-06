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

drop table if exists public_sites_deduped;

select
	*
into
	public_sites_deduped
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		borough,
		lead,
		total_units,
		public_sites_incremental_units,
		case
			when total_units::float*.2>public_sites_incremental_units 									then 0
			when public_sites_incremental_units<=2 and public_sites_incremental_units<>total_units		then 0
			else public_sites_incremental_units 														end							as counted_units,
		PLANNER_PROVIDED_PHASING,
		case
			when total_units::float*.2>public_sites_incremental_units 									then null
			when public_sites_incremental_units<=2 and public_sites_incremental_units<>total_units		then null
			else 																						portion_built_2025 end		as portion_built_2025,
		case
			when total_units::float*.2>public_sites_incremental_units 									then null
			when public_sites_incremental_units<=2 and public_sites_incremental_units<>total_units		then null
			else 																						portion_built_2035 end		as portion_built_2035,
		case
			when total_units::float*.2>public_sites_incremental_units 									then null
			when public_sites_incremental_units<=2 and public_sites_incremental_units<>total_units		then null
			else 																						portion_built_2055 end		as portion_built_2055,
		planner_input,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		dob_job_numbers,
		dob_units_net,
		hpd_projected_closings_ids,
		hpd_projected_closings_incremental_units,
		hpd_rfp_ids,
		hpd_rfp_incremental_units,
		edc_project_ids,
		edc_incremental_units,
		zap_project_ids,
		zap_incremental_units,
		nstudy_project_ids,
		nstudy_incremental_units
	from
	(
		select
			a.cartodb_id,
			a.the_geom,
			a.the_geom_webmercator,
			'Future City-Sponsored RFPs/RFEIs'							as source,
			a.public_sites_id 	as project_id,
			a.project 			as project_name,
			a.boro 				as borough,
			a.lead,
			a.total_units,
			A.PLANNER_PROVIDED_PHASING,
			A.portion_built_2025,
			A.portion_built_2035,
			A.portion_built_2055,
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
			capitalplanning.public_sites_2018_sca_inputs_ms a
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
	) x
) public_sites_deduped;

select cdb_cartodbfytable('capitalplanning','public_sites_deduped');


/**********************************
SOURCE-SPECIFIC OUTPUT
**********************************/

select * from public_sites_deduped  order by PROJECT_ID asc
