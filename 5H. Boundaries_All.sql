/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Collecting boundaries and joining to project-level Known Projects DB
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists Known_Projects_DB_Project_Level_Boundaries;

select
	*
into
	Known_Projects_DB_Project_Level_Boundaries
from
(
	select
		a.*
		,b.CSD
		,c.Subdistrict
		,d.ES_Zone
		,e.MS_Zone
		-- ,i.ntacode
		-- ,i.ntaname
		,f.Census_Tract
		,g.taz 
		-- ,h.Community_District
	from
		known_projects_db_20190917_v6 a
	left join
		aggregated_CSD_PROJECT_level b
	on
		a.source 		= b.source 		and
		a.project_id 	= b.project_id 
	left join
		aggregated_subdistrict_PROJECT_level c
	on
		a.source 		= c.source 		and
		a.project_id 	= c.project_id 
	left join
		aggregated_es_zone_PROJECT_level d
	on
		a.source 		= d.source 		and
		a.project_id 	= d.project_id 
	left join
		aggregated_ms_zone_PROJECT_level e
	on
		a.source 		= e.source 		and
		a.project_id 	= e.project_id 
	left join
		aggregated_ct_PROJECT_level f
	on
		a.source 		= f.source 		and
		a.project_id 	= f.project_id 
	left join
		aggregated_taz_project_level g
	on
		a.source 		= g.Source 		and
		a.project_id  	= g.project_id 
	-- left join
	-- 	aggregated_cd_project_level h
	-- on
	-- 	a.source 		= h.Source 		and
	-- 	a.project_id  	= h.project_id 
	-- left join
	-- 	aggregated_nta_project_level_cp_assumptions i
	-- on
	-- 	a.source 		= h.Source 		and
	-- 	a.project_id  	= h.project_id 

) x
order by
	source,
	project_id asc;