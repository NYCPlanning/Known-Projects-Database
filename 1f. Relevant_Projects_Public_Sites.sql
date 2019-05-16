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
		a.cartodb_id,
		coalesce(b.the_geom,c.the_geom) as the_geom,
		a.unique_project_id,
		concat('Public Site Pipeline ',a.project_id) as Public_Sites_ID,
		a.boro,
		a.lead,
		a.project,
		a.zap_project_id,
		a.zap_project_id_2,
		a.zap_project_id_3,
		a.zap_project_id_4,
		a.city_planning_comments,
		b.total_units_from_planner as total_units,
		a.aff_units,
		a.cm,
		a.developer,
		a.program,
		a.current_agency
	from
		(select * from capitalplanning.table_190510_public_sites_ms_v3 where unique_project_id in('Pipeline 1','Pipeline 13','Pipeline 17','Pipeline 22','Pipeline 26','Pipeline 29') /*Selecting public sites not accounted for in other sources*/) a
	left join
		capitalplanning.mapped_planner_inputs_consolidated_inputs_ms b
	on
		(a.unique_project_id = b.project_id and a.unique_project_id <> 'Pipeline 22') or 
		(a.unique_project_id = 'Pipeline 22' and b.map_id = 85353)
	left join
		capitalplanning.mappluto_v_18v1_1 c
	on
		a.unique_project_id = 'Pipeline 1' and c.bbl = 1004910016
) x

/***********************************RUN IN REGULAR CARTO*****************************/

select cdb_cartodbfytable('capitalplanning', 'public_sites_2018_sca_inputs_ms')
		
