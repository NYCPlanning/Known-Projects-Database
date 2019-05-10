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
			cartodb_id,
			the_geom,
			mapid,
			objectid,
			shape_length,
			shape_area,
			area_sqft
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_ks_1
		union
		select
			cartodb_id,
			the_geom,
			mapid,
			objectid,
			shape_length,
			shape_area,
			null
		from
			capitalplanning.addeddevelopmentsites_addeddevsites_rl_1			
	) as added_development_sites
order by
	mapid asc


/*************************************RUN IN REGULAR CARTO****************/

select cdb_cartodbfytable('capitalplanning', 'added_development_sites_20190510_MS')
