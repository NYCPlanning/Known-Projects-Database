/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping City Hall Public Sites data with DOB
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Spatially match City Hall Public Sites with DOB jobs.
2. If a DOB job maps to multiple sites, create a preference methodology to make 1-1 matches
3. Omit inaccurate proximity-based matches within 20 meters.
4. Calculate incremental units.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select
	*
into
	public_sites_dob
from
(
	select
		a.*,
		case 
			when st_intersects(a.the_geom,b.the_geom) 						then			'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 			'Proximity' end as DOB_Match_Type,
		b.job_number 												as dob_job_number,
		b.job_description											as dob_project_description,
		b.units_net 												as dob_units_net,
		b.address 													as dob_address,
		b.job_type													as dob_job_type,
		b.status 													as dob_status,
		st_distance(a.the_geom::geography,b.the_geom::geography) 	as DOB_Distance
	from
		capitalplanning.public_sites_2018_sca_inputs_ms_1 a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on 
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20) 	and
		b.job_type = 'New Building' /*3 alterations matched and all are inaccurate -- only matching with NBs*/													 
) public_sites_dob


/*There is 1 match and it is spatial. Seaview Healthy Community (500 units) and DOB Job # 520136040 (198 units, filed application). 
  Surprising, given that Public Sites does not list a timeline for this project. As expected, other projects do not match with DOB data.
  All the included Pipeline Public Sites are at most in RFP issuance stage, if there is a listed timeline at all. */ 

/*Aggregating projects*/

select
	*
into
	public_sites_dob_final
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		public_sites_id,
		project,
		boro,
		lead,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input,
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') 	as dob_job_numbers,
		sum(dob_units_net) 																					as dob_units_net
	from
		public_sites_dob
	group by
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		public_sites_id,
		project,
		boro,
		lead,
		total_units,
		nycha_flag,
		gq_flag,
		assisted_living_flag,
		senior_housing_flag,
		planner_input
	order by
		public_sites_id asc
) public_sites_dob_final