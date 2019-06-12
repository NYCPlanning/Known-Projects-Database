/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Finalizing deduped HPD RFP data
Sources: 
*************************************************************************************************************************************************************************************/

/*********************************************
		RUN THE FOLLOWING QUERY IN
		CARTO BATCH
*********************************************/
drop table if exists hpd_rfp_deduped;

select
	*
into
	hpd_rfp_deduped
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		borough,
		bbl,
		lead_agency,
		status,
		total_units,
		HPD_RFP_Incremental_Units,
		case
			when total_units::float*.2>hpd_rfp_incremental_units 								then 0
			when hpd_rfp_incremental_units<=2 and hpd_rfp_incremental_units<>total_units		then 0
			else hpd_rfp_incremental_units 														end							as counted_units,
		case
			when total_units::float*.2>hpd_rfp_incremental_units 								then null
			when hpd_rfp_incremental_units<=2 and hpd_rfp_incremental_units<>total_units		then null
			else 																				portion_built_2025 end		as portion_built_2025,
		case
			when total_units::float*.2>hpd_rfp_incremental_units 								then null
			when hpd_rfp_incremental_units<=2 and hpd_rfp_incremental_units<>total_units		then null
			else 																				portion_built_2035 end		as portion_built_2035,
		case
			when total_units::float*.2>hpd_rfp_incremental_units 								then null
			when hpd_rfp_incremental_units<=2 and hpd_rfp_incremental_units<>total_units		then null
			else 																				portion_built_2055 end		as portion_built_2055,
		excluded_project_flag,
		rationale_for_exclusion,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		dob_job_numbers,
		dob_units_net,
		hpd_projected_closings_ids,
		hpd_projected_closings_incremental_units
	from
		(
			select
				c.the_geom,
				c.the_geom_webmercator,
				c.Source,
				c.project_id,
				c.project_name,
				c.borough,
				c.bbl,
				c.lead_agency,
				c.status,
				c.total_units,
				case 
					when c.total_units is null then null
					else 
					greatest
					(
						c.total_units - coalesce(a.dob_units_net,0) - coalesce(b.hpd_projected_closings_incremental_units,0)
						,0
					) end as HPD_RFP_Incremental_Units,
				case when c.likely_to_be_built_by_2025_flag = 1 then 1 else 0 end as portion_built_2025,
				0 as portion_built_2035,
				0 as portion_built_2055,
				c.excluded_project_flag,
				c.rationale_for_exclusion,
				c.nycha_flag,
				c.gq_flag,
				case when c.senior_housing_flag = 1 then 1 else 0 end as senior_housing_flag,
				c.assisted_living_flag,
				a.dob_job_numbers,
				a.dob_units_net,
				b.hpd_projected_closings_ids,
				b.hpd_projected_closings_incremental_units
			from
				(select * from hpd_2018_sca_inputs_ms where source = 'HPD RFPs') c
			left join
				capitalplanning.hpd_rfp_dob_final a
			on
				c.project_id = a.project_id
			left join
				capitalplanning.hpd_rfp_hpd_final b
			on
				a.project_id = b.project_id
		) x
) as x
order by project_id::numeric asc;

select cdb_cartodbfytable('capitalplanning','hpd_rfp_deduped');


/**********************************
SOURCE-SPECIFIC OUTPUT
**********************************/

select * from hpd_rfp_deduped order by PROJECT_ID::numeric asc
