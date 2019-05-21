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
		coalesce
			(
				b.total_units_from_planner,
				case 
					when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
					else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end
			) as total_units,
		,a.aff_units,
		a.cm,
		a.developer,
		a.program,
		a.current_agency
	from
		(select * from capitalplanning.table_190510_public_sites_ms_v3_1 where project_found_in = '' and omit_from_public_sites_relevant_projects = 0 /*Selecting public sites not accounted for in other sources*/) a
	left join
		capitalplanning.mapped_planner_inputs_consolidated_inputs_ms b
	on
		(a.unique_project_id = b.project_id and a.unique_project_id <> 'Pipeline 22') or 
		(a.unique_project_id = 'Pipeline 22' and b.map_id = 85353)
	left join
		capitalplanning.mappluto_v_18v1_1 c
	on
		a.bbls_if_not_in_pipeline_or_planner_inputs =  c.bbl and c.bbl is not null
) x

/***********************************RUN IN REGULAR CARTO*****************************/

select cdb_cartodbfytable('capitalplanning', 'public_sites_2018_sca_inputs_ms')
		
