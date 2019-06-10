/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped Neighborhood Study Rezoning Commitment dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate Neighborhood Study matches to DOB, HPD Projected Closings, HPD RFP, EDC, and ZAP data.
2. Calculate Neighborhood Study Rezoning Commitment increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	nstudy_deduped
from
(
	select
		ROW_NUMBER() OVER() AS CARTODB_ID,
		'Neighborhood Study Development Sites' as Source,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.neighborhood,
		a.borough,
		a.status,
		a.units as total_units,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2025,0)
				else 0 end as portion_built_2025,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2035,0)
				else 1 end as portion_built_2035,
		case
				when coalesce(a.portion_built_2025,0)+coalesce(a.portion_built_2035,0)+coalesce(a.portion_built_2055,0) > 0 then coalesce(a.portion_built_2055,0)
				else 0 end as portion_built_2055,
		a.planner_input,
		a.nycha_flag,
		a.gq_flag,
		a.assisted_living_flag,
		a.senior_housing_flag,
		greatest
				(
					0,
					a.units 											-
					coalesce(b.dob_units_net,0) 						-
					coalesce(c.hpd_project_incremental_units,0)			-
					coalesce(d.hpd_rfp_incremental_units,0)				-
					coalesce(e.edc_incremental_units,0)					-
					coalesce(f.zap_incremental_units,0)
				) as nstudy_incremental_units,
		b.dob_job_numbers,
		b.dob_units_net,
		c.hpd_project_ids as hpd_projected_closings_ids,
		c.hpd_project_incremental_units as hpd_projected_closings_incremental_units,
		d.hpd_rfp_ids,
		d.hpd_rfp_incremental_units,
		e.edc_project_ids,
		e.edc_incremental_units,
		f.zap_project_ids,
		f.zap_incremental_units
	from
		(select * from capitalplanning.dep_ndf_by_site where status = 'Rezoning Commitment')  a
	left join
		capitalplanning.nstudy_dob_final b
	on 
		a.project_id = b.project_id 
	left join
		capitalplanning.nstudy_hpd_projected_closings_final c 
	on
		a.project_id = c.project_id 
	left join
		capitalplanning.nstudy_hpd_rfp_final d
	on
		a.project_id = d.project_id 
	left join
		capitalplanning.nstudy_edc_final e
	on
		a.project_id = e.project_id
	left join
		capitalplanning.nstudy_zap_final f
	on
		a.project_id = f.project_id
) nstudy_deduped


/*RUN IN REGULAR CARTO*/
select cdb_cartodbfytable('capitalplanning','nstudy_deduped')