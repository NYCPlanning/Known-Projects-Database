/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped Planner-Added Projects Dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate Planner-Added Projects matches to DOB, HPD Projected Closings, HPD RFP, EDC, ZAP, Neighborhood Studies, and Public Sites data.
2. Calculate Public Sites Increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_deduped
from(
	select
		a.the_geom,
		a.the_geom_webmercator,
		'DCP Planner-Added Projects' as Source,
		a.map_id as project_id,
		a.project_name,
		a.boro as borough,
		a.total_units,
		greatest
		(
			0,
			a.total_units										-
			coalesce(b.dob_units_net,0) 						-
			coalesce(c.hpd_project_incremental_units,0)			-
			coalesce(d.hpd_rfp_incremental_units,0)				-
			coalesce(e.edc_incremental_units,0)					-
			coalesce(f.zap_incremental_units,0)					-
			coalesce(g.nstudy_incremental_units,0)				-
			coalesce(h.public_sites_incremental_units,0)				
		) as planner_projects_incremental_units,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2025,0)
				else 0 end as portion_built_2025,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2035,0)
				else 0 end as portion_built_2035,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2055,0)
				else 1 end as portion_built_2055,
		a.planner_input,
		a.nycha_flag,
		a.gq_flag,
		a.assisted_living_flag,
		a.senior_housing_flag,
		b.dob_job_numbers,
		b.dob_units_net,
		c.hpd_project_ids as hpd_projected_closings_ids,
		c.hpd_project_incremental_units as hpd_projected_closings_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_incremental_units,
		e.edc_project_ids,
		e.edc_incremental_units,
		f.zap_project_ids,
		f.zap_incremental_units,
		g.nstudy_project_ids,
		g.nstudy_incremental_units,
		h.public_sites_project_ids,
		h.public_sites_incremental_units
	from
		capitalplanning.mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.planner_projects_dob_final b
	on 
		a.map_id = b.map_id 
	left join
		capitalplanning.planner_projects_hpd_projected_closings_final c 
	on
		a.map_id = c.map_id 
	left join
		capitalplanning.planner_projects_hpd_rfps_final d
	on
		a.map_id = d.map_id 
	left join
		capitalplanning.planner_projects_edc_final e
	on
		a.map_id = e.map_id
	left join
		capitalplanning.planner_projects_zap_final f
	on
		a.map_id = f.map_id
	left join
		capitalplanning.planner_projects_nstudy_final g
	on
		a.map_id = g.map_id
	left join
		capitalplanning.planner_projects_public_sites_final h
	on
		a.map_id = h.map_id
	order by
		a.map_id asc
) planner_projects_deduped


/*RUN IN REGULAR CARTO*/
select cdb_cartodbfytable('capitalplanning','planner_projects_deduped')
