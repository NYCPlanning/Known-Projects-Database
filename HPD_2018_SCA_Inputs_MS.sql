/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Preparing HPD's HPD_2018_SCA_Inputs_geo_pts dataset for joining
START DATE: 1/10/2019
COMPLETION DATE: 1/10/2019
Source file: "G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\HPD\hpd_2018_sca_inputs_geo_pts.csv"
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY:
1. Rearrange fields and append HPD RFPs to HPD projects.
2. Complete missing lat long fields
3. Omit completed projects.
*************************************************************************************************************************************************************************************/

with HPD_2018_SCA_Inputs_MS as
(
	select 
		the_geom,
		project_id,
		hpd_project_id,
		building_id,
		project_name,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		address,
		borough,
		total_units,
		latitude,
		longitude,
		bin,
		bbl,
		source
	from(
		select
			the_geom,
			concat(project_id, building_id) as project_id,
			project_id as hpd_project_id,
			building_id,
			project_name,
			primary_program_at_start,
			construction_type,
			status,
			coalesce(project_start_date,projected_start_date) as project_start_date,
			coalesce(building_completion_date,projected_completion_date) as projected_completion_date,
			concat(house_number,' ', street_name) as address,
			borough,
			total_units,
			case when lat_geoclient 	=0 then st_y(the_geom)
				else lat_geoclient 		end as latitude, /*Some observations with filled geoms but no lat - adding in lat from the_geom*/
			case when long_geoclient 	=0 then st_x(the_geom)
				else  long_geoclient	end as longitude, /*Some observations with filled geoms but no long - adding in long from the_geom*/
			bin_geoclient as bin, /*Looks to be an inaccurate field*/
			bbl_geoclient as bbl,
			'HPD Projects' as Source
		from
			capitalplanning.HPD_2018_SCA_Inputs_geo_pts
		where
			status <> 'Completed' /*Omitting ~17K completed units out of ~65K total units*/
		union
		select
			the_geom,
			concat(cartodb_id) as project_id,
			null as hpd_project_id,
			null as building_id,
			rq__p_n as project_name,
			null as primary_program_at_start,
			null as construction_type,
			null as status,
			null as project_start_date,
			null as projected_completion_date,
			null as address,
			borough,
			null as total_units,
			null as latitude,
			null as longitude,
			null as bin,
			bbl as bbl,
			'HPD RFPs' as Source
		from
			capitalplanning.hpd_rfps_1
		order by hpd_project_id
		) as compilation
	order by source, cast(project_id as integer)
)

select * from HPD_2018_SCA_Inputs_MS

/*Create new dataset from query titled HPD_2018_SCA_Inputs_MS */ 

/******************************************************************************
 The following query shows 741 projects from HPD In Construction + Projected data,
 and 138 projects from HPD RFP data

	SELECT 
		source,
	    count(*) 
	FROM capitalplanning.hpd_2018_sca_inputs_ms
	group by source
******************************************************************************/
/*********************************GEOCODING ASSEMENT**********************************************
Average distance between the_geom and lat/longs from HPD_2018_SCA_Inputs_Geo_Pts <.0005 meters:

with distance_between_geom_and_lat_longs as
(
select *, st_distance(the_geom,st_setsrid(st_makepoint(longitude,latitude),4326)) as distance from hpd_2018_sca_inputs_ms where source = 'HPD Projects'
)
select avg(distance) from distance_between_geom_and_lat_longs

There is no observations from HPD_2018_SCA_Inputs_Geo_Pts where distance b/w the_geom and lat/longs < .001 meters

select * from distance_between_geom_and_lat_longs where distance > .001
**************************************************************************************************/
