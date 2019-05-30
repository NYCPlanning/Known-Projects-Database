/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping Planner-Added Projects with DOB
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match Planner-Added Projects with DOB jobs.
2. Clean incorrect matches by tailored removal of alterations and complete projects.
3. If a DOB job maps to multiple Planner-Added Projects, create a preference methodology to make 1-1 matches.
4. Omit inaccurate proximity-based matches within 20 meters.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	planner_projects_dob
from
(
	select
		a.*,
		case
			when st_intersects(a.the_geom,b.the_Geom)				then 'Spatial'
			when b.job_number is not null							then 'Proximity' end 	as DOB_Match_Type,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as DOB_Distance,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	and
		b.job_type <> 'Demolition'
	order by
		a.map_id asc 													 
) planner_projects_dob

/*******************************DIAGNOSTICS******************************************************/

/*There are 33 matches with Alteration job type. Only the match between Planner Project 85408 and DOB job number 420664310 looks to be correct, based on the fact
  that they share the same unit count. Otherwise, DOB job matches are for small or negative unit counts which do not fit into planner added projects. The next step will
  omit alterations which don't match with the planner-added project's unit count.*/

select
	*
from
	planner_projects_dob
where
	DOB_JOB_TYPE = 'Alteration'


/*There are 49 matches with DOB status Permit Issued or Complete. These are unlikely matches -- given that these planner projects are assumed to not have materialized. 
  After manual review of the New Building Complete/Permit Issued projects, the statuses of the Planner-Added projects, and planner inputs, there are no accurate matches. Flushing
  Commons has a proximity match to its complete first phase, but the planner-added unit count only includes future units*/


  select
  	*
  from
  	planner_projects_dob
  where
  	dob_status in('Complete','Permit issued')

/****************************END OF DIAGNOSTICS************************************************/

select
	*
into
	planner_projects_dob_1
from
(
	select
		a.*,
		case
			when st_intersects(a.the_geom,b.the_Geom)				then 'Spatial'
			when b.job_number is not null							then 'Proximity' end 	as DOB_Match_Type,
		st_distance(a.the_geom::geography,b.the_geom::geography)							as DOB_Distance,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status
	from
		mapped_planner_inputs_added_projects_ms_1 a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(cast(a.the_geom as geography),cast(b.the_geom as geography),20) 	and
		b.job_type <> 'Demolition'													and
		case when b.job_type = 'Alteration' then a.total_units = b.units_net end    and 
		b.status not in('Complete','Permit issued')
	order by
		a.map_id asc 													 
) planner_projects_dob


/*There is only one match, between map_id 85408 and DOB Job Number 420664310. This is a spatial overlap and both share a unit count of 202, so deeming this accurate*/

select
	*
into
	planner_projects_dob_final
from
(
	select
		map_id,
		project_name,
		boro as borough,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input,
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') 	as dob_job_numbers,
		sum(dob_units_net) 																					as dob_units_net
	from
		planner_projects_dob_1
	group by
		map_id,
		project_name,
		boro,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input
	order by 
		map_id asc
)	planner_projects_dob_final	

