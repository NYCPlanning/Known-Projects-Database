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
		coalesce(b.the_geom_webmercator,c.the_geom_webmercator) as the_geom_webmercator,
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
		a.aff_units,
		a.cm,
		a.developer,
		a.program,
		a.current_agency,
		b.planner_input,
		b.portion_built_2025,
		b.portion_built_2035,
		b.portion_built_2055,
		/*Identifying NYCHA Projects*/
		CASE 
			WHEN upper(concat(a.project,b.planner_input))  like '%NYCHA%' THEN 1   		
			WHEN upper(concat(a.project,b.planner_input))  like '%BTP%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%HOUSING AUTHORITY%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%NEXT GEN%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%NEXT-GEN%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%NEXTGEN%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%BUILD TO PRESERVE%' THEN 1 ELSE 0 END 		AS NYCHA_Flag,

		CASE 
			WHEN upper(concat(a.project,b.planner_input))  like '%CORRECTIONAL%' THEN 1   		
			WHEN upper(concat(a.project,b.planner_input))  like '%NURSING%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '% MENTAL%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%DORMITOR%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%MILITARY%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%GROUP HOME%' THEN 1  		
			WHEN upper(concat(a.project,b.planner_input))  like '%BARRACK%' THEN 1 ELSE 0 END 		AS GQ_fLAG,

		/*Identifying definite senior housing projects*/
		CASE 
			WHEN upper(concat(a.project,b.planner_input))  	like '%SENIOR%' THEN 1
			WHEN upper(concat(a.project,b.planner_input))  	like '%ELDERLY%' THEN 1 	
			WHEN concat(a.project,b.planner_input)  		like '% AIRS%' THEN 1
			WHEN upper(concat(a.project,b.planner_input))  	like '%A.I.R.S%' THEN 1 
			WHEN upper(concat(a.project,b.planner_input))  	like '%CONTINUING CARE%' THEN 1
			WHEN upper(concat(a.project,b.planner_input))  	like '%NURSING%' THEN 1
			WHEN concat(a.project,b.planner_input)  		like '% SARA%' THEN 1
			WHEN upper(concat(a.project,b.planner_input))  	like '%S.A.R.A%' THEN 1 else 0 end as Senior_Housing_Flag,
		CASE
			WHEN upper(concat(a.project,b.planner_input))  like '%ASSISTED LIVING%' THEN 1 else 0 end as Assisted_Living_Flag

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
		

select
	*
into
	public_sites_2018_sca_inputs_ms_1
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.unique_project_id,
		a.public_sites_id,
		a.boro,
		a.lead,
		a.project,
		a.city_planning_comments,
		a.total_units,
		a.planner_input,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.NYCHA_Flag,
		a.GQ_fLAG,
		a.Senior_Housing_Flag,
		a.Assisted_Living_Flag
	from
		public_sites_2018_sca_inputs_ms a
) x

select cdb_cartodbfytable('capitalplanning', 'public_sites_2018_sca_inputs_ms_1')



select
	*
into
	public_sites_inputs_ms_share_20190521
from
(
	select
		the_geom,
		the_geom_webmercator,
		unique_project_id,
		project_name,
		total_units
	from
		public_sites_2018_sca_inputs_ms_1
)
	order by
		unique_project_id asc
