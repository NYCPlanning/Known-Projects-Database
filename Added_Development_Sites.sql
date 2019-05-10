/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Appending added planner development sites from KS and RL
*************************************************************************************************************************************************************************************/


/**********************************RUN IN CARTO BATCH*****************/

select
	*
into
	added_development_sites_20190510_MS
from
	(
		select
			the_geom,
			mapid,
			objectid,
			shape_length,
			shape_area,
			area_sqft,
			'KS' as source
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_ks_1
		union
		select
			the_geom,
			mapid,
			objectid,
			shape_length,
			shape_area,
			null,
			'RL' as source
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_rl_1			
	) as added_development_sites
order by
	mapid asc


/*************************************RUN IN REGULAR CARTO****************/

select cdb_cartodbfytable('capitalplanning', 'added_development_sites_20190510_MS')
