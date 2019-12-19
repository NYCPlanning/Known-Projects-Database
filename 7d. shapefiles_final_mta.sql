/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Creating shapefiles for MTA
START DATE: 11/27/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists dob_complete_shapefile_20191127_MTA;
drop table if exists dob_incomplete_shapefile_20191127_MTA;
drop table if exists dob_incomplete_shapefile_cp_assumptions_20191127_MTA;
drop table if exists hpd_projected_closings_shapefile_20191127_MTA;
drop table if exists hpd_rfps_shapefile_20191127_MTA;
drop table if exists edc_shapefile_20191127_MTA;
drop table if exists dcp_applications_shapefile_20191127_MTA;
drop table if exists esd_shapefile_20191127_MTA;
drop table if exists nstudy_rezoning_commitments_shapefile_20191127_MTA;
drop table if exists future_rfp_rfei_shapefile_20191127_MTA;
drop table if exists dcp_planner_added_projects_shapefile_20191127_MTA;
drop table if exists nstudy_projected_development_shapefile_20191127_MTA;
drop table if exists future_nstudy_shapefile_20191127_MTA;


select
	*
into
	dob_complete_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		job_number,
		address,
		units_net,
		units_net_incomplete,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		dob_2018_sca_inputs_ms_cp_build_year_3 
	where 
		status in('Complete','Complete (demolition)') and 
		borough in('Brooklyn','Queens')
) x
	order by 
		job_number asc;



select
	*
into
	dob_incomplete_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		job_number,
		job_type,
		address,
		units_net,
		units_net_incomplete,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		dob_2018_sca_inputs_ms_cp_build_year_3 
	where 
		status not in('Complete','Complete (demolition)') and 
		borough in('Brooklyn','Queens') 
) x
	order by 
		job_number asc;

select
	*
into
	hpd_rfps_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		hpd_rfp_deduped
	where
		borough in('Brooklyn','Queens')
) x
	order by 
		project_id::numeric asc;

select
	*
into
	edc_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		edc_deduped
	where
		borough in('Brooklyn','Queens') or
		project_name ilike '%coney%'
) x
	order by 
		project_id asc;

select
	*
into
	dcp_applications_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		zap_deduped_build_year
	where
		project_id not like '%ESD%' and
		borough in('Brooklyn','Queens')

) x
	order by 
		project_id asc;

select
	*
into
	esd_shapefile_20191127_MTA
from
(
	select
		the_geom,
		ST_Transform(the_geom,3857) as the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		zap_deduped_build_year
	where
		project_id like '%ESD%' and
		borough in('Brooklyn','Queens')
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_rezoning_commitments_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		nstudy_deduped
	where
		borough in('Brooklyn','Queens')
) x
	order by 
		project_id asc;

select
	*
into
	future_rfp_rfei_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		public_sites_deduped
	where
		borough in('Brooklyn','Queens')
) x
	order by 
		project_id asc;

select
	*
into
	dcp_planner_added_projects_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		total_units,
		counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from
		planner_projects_deduped
	where
		borough in('Brooklyn','Queens')
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_projected_development_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		nstudy_projected_potential_incremental_units as counted_units,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from	
		nstudy_projected_potential_areawide_deduped_final
	where
		borough in('Brooklyn','Queens')
) x
	order by
		project_id asc;


select
	*
into
	future_nstudy_shapefile_20191127_MTA
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		incremental_units_with_certainty_factor,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055
	from	
		nstudy_future
	where
		project_id ilike '%Gowanus%'
) x
	order by
		project_id asc;

select cdb_cartodbfytable('capitalplanning','dob_complete_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','dob_incomplete_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','hpd_rfps_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','edc_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','dcp_applications_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','esd_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','nstudy_rezoning_commitments_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','future_rfp_rfei_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','dcp_planner_added_projects_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','nstudy_projected_development_shapefile_20191127_MTA');
select cdb_cartodbfytable('capitalplanning','future_nstudy_shapefile_20191127_MTA');
