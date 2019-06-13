/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Creating shapefiles
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists dob_complete_shapefile_20190613;
drop table if exists dob_incomplete_shapefile_20190613;
drop table if exists hpd_projected_closings_shapefile_20190613;
drop table if exists hpd_rfps_shapefile_20190613;
drop table if exists edc_shapefile_20190613;
drop table if exists dcp_applications_shapefile_20190613;
drop table if exists esd_shapefile_20190613;
drop table if exists nstudy_rezoning_commitments_shapefile_20190613;
drop table if exists future_rfp_rfei_shapefile_20190613;
drop table if exists dcp_planner_added_projects_shapefile_20190613;
drop table if exists nstudy_projected_development_shapefile_20190613;
drop table if exists future_nstudy_shapefile_20190613;


select
	*
into
	dob_complete_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		job_number,
		units_net,
		units_net_incomplete
	from
		(select * from dob_2018_sca_inputs_ms where status in('Complete','Complete (demolition)')) a
) x
	order by 
		job_number asc;


select
	*
into
	dob_incomplete_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		job_number,
		units_net,
		units_net_incomplete
	from
		(select * from dob_2018_sca_inputs_ms where status not in('Complete','Complete (demolition)')) a 
) x
	order by 
		job_number asc;

select
	*
into
	hpd_projected_closings_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		hpd_deduped
) x
	order by 
		project_id asc;

select
	*
into
	hpd_rfps_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		hpd_rfp_deduped
) x
	order by 
		project_id::numeric asc;

select
	*
into
	edc_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		edc_deduped
) x
	order by 
		project_id asc;

select
	*
into
	dcp_applications_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		zap_deduped_build_year
	where
		project_id not like '%ESD%'

) x
	order by 
		project_id asc;

select
	*
into
	esd_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		zap_deduped_build_year
	where
		project_id like '%ESD%'
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_rezoning_commitments_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		nstudy_deduped
) x
	order by 
		project_id asc;

select
	*
into
	future_rfp_rfei_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		public_sites_deduped
) x
	order by 
		project_id asc;

select
	*
into
	dcp_planner_added_projects_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		counted_units as deduplicated_units
	from
		planner_projects_deduped
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_projected_development_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		nstudy_projected_potential_incremental_units as deduplicated_units
	from	
		nstudy_projected_potential_areawide_deduped_final
) x
	order by
		project_id asc;


select
	*
into
	future_nstudy_shapefile_20190613
from
(
	select
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		total_units,
		incremental_units_with_certainty_factor
	from	
		nstudy_future
) x
	order by
		project_id asc;

select cdb_cartodbfytable('capitalplanning','dob_complete_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','dob_incomplete_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','hpd_projected_closings_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','hpd_rfps_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','edc_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','dcp_applications_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','esd_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','nstudy_rezoning_commitments_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','future_rfp_rfei_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','dcp_planner_added_projects_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','nstudy_projected_development_shapefile_20190613');
select cdb_cartodbfytable('capitalplanning','future_nstudy_shapefile_20190613');
