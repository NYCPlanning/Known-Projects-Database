/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Preparing Public Sites data for relevant projects
Source file: "G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\Public Sites\190410_Public_Sites_MS.xlsx"
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY:
1. Remove non-project rows
2. Select relevant fields
3. Select relevant rows
4. Geocode
*************************************************************************************************************************************************************************************/

/***********************************RUN IN CARTO BATCH*****************************/

select
	*
into
	public_sites_2018_sca_inputs_ms
from
(
	select
		st_union(b.the_geom),
		a.unique_project_id,
		a.project as project_name,
		a.boro as borough,
		a.lead as agency_lead,
		a.city_planning_comments,
		a.total_units,
		a.aff_units as affordable_units
	from
		capitalplanning.public_sites_190410_ms a
	left join
		capitalplanning.mappluto_v_18v_2 b
	on
		position(b.bbl in a.bbl) > 0 and b.bbl is not null
	group by
		a.unique_project_id,
		a.project,
		a.boro,
		a.lead,
		a.city_planning_comments,
		a.total_units,
		a.aff_units
	where
		a.zap_project_id = 'No ZAP ID'
) as public_sites_2018_sca_inputs_ms

/***********************************RUN IN REGULAR CARTO*****************************/

select cdb_cartodbfytable('capitalplanning', 'public_sites_2018_sca_inputs_ms')
		
