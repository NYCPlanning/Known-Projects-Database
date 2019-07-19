/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding boundaries to project-level, source-specific sheets
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists dob_2018_sca_inputs_ms_complete_geo;
drop table if exists dob_2018_sca_inputs_ms_incomplete_geo;
drop table if exists dob_2018_sca_inputs_ms_incomplete_geo_cp_assumptions;
drop table if exists hpd_deduped_geo;
drop table if exists hpd_rfp_deduped_geo;
drop table if exists edc_deduped_geo;
drop table if exists zap_deduped_geo;
drop table if exists state_deduped_geo;
drop table if exists nstudy_deduped_geo;
drop table if exists public_sites_deduped_geo;
drop table if exists planner_added_projects_deduped_geo;
drop table if exists nstudy_projected_areawide_deduped_geo;
drop table if exists future_nstudy_geo;



select
	*
into
	dob_2018_sca_inputs_ms_complete_geo
from
(
	select
		a.*,
		b.CSD 														as CSD,
		b.subdistrict 												as subdistrict,
		b.ES_Zone 													as es_zone,
		b.ms_zone 													as ms_zone,
		b.Census_Tract  											as ct,
		b.taz
	from
		(select * from dob_2018_sca_inputs_ms_cp_build_year_3 where status in('Complete','Complete (demolition)')) a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'DOB' and
		concat(a.job_number) = b.project_id
) x
	order by 
		job_number asc;


select
	*
into
	dob_2018_sca_inputs_ms_incomplete_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz
	from
		(select * from dob_2018_sca_inputs_ms_2_1 where status not in('Complete','Complete (demolition)')) a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'DOB' and
		concat(a.job_number) = b.project_id
) x
	order by 
		job_number asc;


select
	*
into
	dob_2018_sca_inputs_ms_incomplete_geo_cp_assumptions
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		(select * from dob_2018_sca_inputs_ms_cp_build_year_3 where status not in('Complete','Complete (demolition)')) a
	left join
		Known_Projects_DB_Project_Level_Boundaries_cp_assumptions b
	on
		b.source = 'DOB' and
		concat(a.job_number) = b.project_id
) x
	order by 
		job_number asc;


select
	*
into
	hpd_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		hpd_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'HPD Projected Closings' and
		a.project_id = b.project_id
) x
	order by 
		project_id asc;

select
	*
into
	hpd_rfp_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		hpd_rfp_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'HPD RFPs' and
		concat(a.project_id) = b.project_id
) x
	order by 
		project_id::numeric asc;
select
	*
into
	edc_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		edc_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'EDC Projected Projects' and
		concat(a.project_id) = b.project_id
) x
	order by 
		project_id asc;

drop table if exists zap_deduped_geo;
select
	*
into
	zap_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		zap_deduped_build_year a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'DCP Applications' and
		a.project_id = b.project_id
	where 
		a.project_id not like '%ESD%'
) x
	order by 
		project_id asc;

select
	*
into
	state_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		zap_deduped_build_year a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'Empire State Development Projected Projects' and
		a.project_id = b.project_id
	where 
		a.project_id like '%ESD%'
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		nstudy_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'Neighborhood Study Rezoning Commitments' and
		a.project_id = b.project_id
) x
	order by 
		project_id asc;

select
	*
into
	public_sites_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		public_sites_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'Future City-Sponsored RFPs/RFEIs' and
		a.project_id = b.project_id
) x
	order by 
		project_id asc;

select
	*
into
	planner_added_projects_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from
		planner_projects_deduped a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'DCP Planner-Added Projects' and
		concat(a.project_id) = b.project_id
) x
	order by 
		project_id asc;


select
	*
into
	nstudy_projected_areawide_deduped_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from	
		nstudy_projected_potential_areawide_deduped_final a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'Neighborhood Study Projected Development Sites' and
		a.project_id = b.project_id
) x
	order by
		project_id asc;


select
	*
into
	future_nstudy_geo
from
(
	select
		a.*,
		b.CSD,
		b.Subdistrict,
		b.ES_Zone,
		b.MS_Zone,
		b.Census_Tract,
		b.taz 
	from	
		nstudy_future a
	left join
		Known_Projects_DB_Project_Level_Boundaries b
	on
		b.source = 'Future Neighborhood Studies' and
		a.project_id = b.project_id
) x
	order by
		project_id asc;