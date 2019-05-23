/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD data with DOB data
Sources: dob_2018_sca_inputs_ms, hpd_2018_sca_inputs_ms , address_checking_for_hpd_dob_matches
		 relevant_dcp_projects_housing_pipeline_ms, dep_ndf_by_site,
		 edc_2018_sca_input_1_limited
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Join DOB data to HPD data by address, bbl, and spatially.
2. Create lookup to examine matches which do not intersect nor have the same address, but are within 20 meters of each other.
	Manually confirm the accuracy of these matches.
3. Import the lookup from step 2 and modify dataset from step 1 accordingly.
*************************************************************************************************************************************************************************************/
/**********************RUN THE FOLLOWING QUERY IN CARTO BATCH******************************/
select
	*
into
	hpd_dob_match
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.construction_type,
		a.status,
		a.projected_fiscal_year_range,
		a.min_of_projected_units,
		a.max_of_projected_units,
		a.total_units,
		a.address,
		a.borough,
		a.bbl,
		a.nycha_flag,
		a.gq_flag,
		a.senior_housing_flag,
		a.assisted_living_flag,
		case 
			when b.address 		is not null 													then 'Address'
			when c.bbl 			is not null														then 'BBL'
			when st_intersects(a.the_geom,d.the_geom)											then 'Spatial'
			when st_dwithin(cast(a.the_geom as geography),cast(d.the_geom as geography),20)		then 'Proximity' end as DOB_Match_Type,
		coalesce(b.the_geom,c.the_geom,d.the_geom) 																as dob_geom,
		coalesce
			(	
				st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography)),
				st_distance(cast(a.the_geom as geography),cast(c.the_geom as geography)),
				st_distance(cast(a.the_geom as geography),cast(d.the_geom as geography))
			) 																									as Geom_Distance,
		coalesce(b.job_number,c.job_number,d.job_number) 														as dob_job_number,
		coalesce(b.job_type,c.job_type,d.job_type) 																as dob_job_type,
		coalesce(b.address,c.address,d.address) 																as dob_address,
		coalesce(b.status,c.status,d.status) 																	as dob_status,
		coalesce(b.units_init,c.units_init,d.units_init) 														as units_init,
		coalesce(b.units_prop,c.units_prop,d.units_prop) 														as units_prop,
		coalesce(b.units_net,c.units_net,d.units_net) 															as units_net,
		coalesce(b.units_incomplete,c.units_incomplete,d.units_incomplete) 										as units_incomplete,
		coalesce(b.latest_cofo,c.latest_cofo,d.latest_cofo) 													as latest_cofo,
		coalesce(b.pre_filing_date,c.pre_filing_date,d.pre_filing_date) 										as pre_filing_date,	
		coalesce(b.most_recent_status_date,c.most_recent_status_date,d.most_recent_status_date) 				as most_recent_status_date,
		coalesce(b.completed_application_date,c.completed_application_date,d.completed_application_date) 		as completed_application_date,
		coalesce(b.full_permit_issued_date,c.full_permit_issued_date,d.full_permit_issued_date) 				as full_permit_issued_date,
		coalesce(b.partial_permit_issued_date,c.partial_permit_issued_date,d.partial_permit_issued_date) 		as partial_permit_issued_date,
		coalesce(b.job_completion_date,c.job_completion_date,d.job_completion_date) 							as job_completion_date
	from 
		(SELECT * FROM capitalplanning.hpd_2018_sca_inputs_ms WHERE SOURCE = 'HPD Projected Closings') a
	left join 
		capitalplanning.address_checking_for_hpd_dob_matches e
	on
		a.address= e.hpd_unmatched_in_construction_addresses 	and
		e.dob_address <> '' 									and 
		a.project_name = e.hpd_project_name
	left join
		capitalplanning.dob_2018_sca_inputs_ms b 
	on 
		upper(concat(coalesce(e.dob_address,a.address),' ',a.borough)) =
		upper(concat(b.address,' ',b.borough)) 					and
		a.address is not null 									and
		b.job_type = 'New Building'								and
		extract(year from b.pre_filing_date::date) >= 2017
	left join
		capitalplanning.dob_2018_sca_inputs_ms c
	on 
		a.bbl::bigint = c.bbl									and
		a.bbl is not null 										and 
		b.address is null 										and
		c.job_type = 'New Building'								and
		extract(year from c.pre_filing_date::date) >= 2017
	left join
		capitalplanning.dob_2018_sca_inputs_ms d
	on
		st_dwithin(cast(a.the_geom as geography),cast(d.the_geom as geography),20) /*Meters*/ 	and
		b.address 	is null 																	and
		c.bbl 		is null 																	and
		d.job_type = 'New Building' 															and
		extract(year from d.pre_filing_date::date) >= 2017
	order by
		a.project_id
) as HPD_DOB_Merge


/*Identifying DOB jobs which matched to more than 1 HPD job and preferencing matches by address, then, bbl, then spatial, then proximity*/

SELECT
	*
into
	multi_hpd_dob_matches
from
(
	SELECT 
		dob_job_number, 
		count(*) as count, 
		count(case when dob_match_type = 'Address' then 1 end) as address, 
		count(case when dob_match_type = 'BBL' then 1 end) as BBL, 
		count(case when dob_match_type = 'Spatial' then 1 end) as spatial, 
		count(case when dob_match_type = 'Proximity' then 1 end) as proximity 
	FROM 
		capitalplanning.hpd_dob_match 
	group by
		dob_job_number
	having count(*) > 1
) multi_hpd_dob_matches

/*Limiting matches of the DOB jobs identified in multi_hpd_dob_matches*/

SELECT
	*
into
	hpd_dob_match_1
from
(
	SELECT
		*
	from
		hpd_dob_match
	where
		dob_job_number is null or
		(
			not(dob_job_number in(select dob_job_number from capitalplanning.multi_hpd_dob_matches where address>=1) 	and dob_match_type in('BBL','Spatial','Proximity')) 	and
			not(dob_job_number in(select dob_job_number from capitalplanning.multi_hpd_dob_matches where bbl>=1) 		and dob_match_type in('Spatial','Proximity')) 			and
			not(dob_job_number in(select dob_job_number from capitalplanning.multi_hpd_dob_matches where spatial>=1) 	and dob_match_type ='Proximity') 			
		)
) hpd_dob_match_1

/*Some DOB jobs are still matched to multiple HPD projects (primarily because HPD projects are geocoded to the lot-level, while DOB jobs are points). Preferencing the
  matches which are closest in unit count to the HPD projected closings*/

SELECT
	*
into
	multi_hpd_dob_matches_1
from
(
	SELECT 
		dob_job_number, 
		count(*) as count,
		min(abs(units_net-total_units)) as min_unit_difference
	FROM 
		capitalplanning.hpd_dob_match 
	where
		dob_job_number is not null
	group by
		dob_job_number		
	having count(*) > 1
) multi_hpd_dob_matches_1

/*Limiting matches of the DOB jobs identified in multi_hpd_dob_matches_1 to their closest match by unit count*/

SELECT
	*
into
	hpd_dob_match_2
from
(
	SELECT
		a.*
	from
		hpd_dob_match_1 a
	left join
		multi_hpd_dob_matches_1 b
	on
		a.dob_job_number = b.dob_job_number and
		abs(a.units_net-a.total_units) <> b.min_unit_difference
	where
		b.dob_job_number is null
) hpd_dob_match_2
	order by


/*
	After these steps, there are still 10 jobs remaining which match to more than 1 HPD Project. The list of these jobs is below.
	AFter manually examining these matches, they are all developments in vacant lots geocoded to the same point and matching to HPD
	projects of the same characteristics. Therefore, there is no inaccurate unit impact to attributing these developments arbitrarily.
	Export multi_hpd_dob_matches_2, arbitrarily create one-to-one-matches, and reimport as hpd_dob_one_to_one_matching_20190523_ms.
	Use hpd_dob_one_to_one_matching_20190523_ms as a lookup with which to delete the last multi-dob-matches.
*/

select
	*
into
	multi_hpd_dob_matches_2
from
(
	select
		*
	from
		hpd_dob_match_2
	where
		dob_job_number in
		(
			select
				dob_job_number
			from
				hpd_dob_match_2
			group by
				dob_job_number
			having
				count(*)>1
		)
	order by
		dob_job_number asc,
		project_id asc
) x


select
	*
into
	hpd_dob_match_3
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		project_id,
		construction_type,
		status,
		projected_fiscal_year_range,
		min_of_projected_units,
		max_of_projected_units,
		total_units,
		address,
		borough,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_match_type 	end 	as dob_match_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_job_number 	end 	as dob_job_number,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_job_type   	end 	as dob_job_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_status		   	end 	as dob_status,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_address	   	end 	as dob_address,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else units_net 			end		as units_net,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else geom_distance 		end 	as geom_distance
	from
		hpd_dob_match_2
) hpd_dob_match_3 



/**********************RUN THE FOLLOWING QUERY IN REGULAR CARTO******************************/

/*EXPORT THE FOLLOWING QUERY AS HPD_DOB_PROXIMATE_MATCHES.
  IDENTIFY WHETHER THE MATCHES IN THIS DATASET ARE ACCURATE BY FLAGGING.
  REIMPORT AS A LOOKUP AND OMIT INACCURATE MATCHES. */
(
	select
		*
	from
		hpd_dob_match_3
	where
		dob_match_type = 'Proximity' and
		units_net<>total_units
	order by
		project_id asc,
		geom_distance asc
)

/*End of lookup creation*/

/**********************RUN THE FOLLOWING QUERIES IN CARTO BATCH******************************/
		      
		      
/*Use above lookup to (reimported from Excel) to delete inaccurate proximity matches.*/			  
			  
select
		      *
into
			  hpd_dob_match_2
from
(	      
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		construction_type,
		status,
		projected_fiscal_year_range,
		min_of_projected_units,
		max_of_projected_units,
		total_units,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null

		else dob_match_type end
										as DOB_Match_Type,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null
										else dob_job_number end 
										as dob_job_number,
		case 
			when concat(project_id,', ',dob_job_number) in
									(
										select
											match_id
										from
											capitalplanning.lookup_proximity_hpd_dob_matches
										where
											match = 0
									)	then null
										else units_net end 
										as DOB_Units_Net,
		address,
		borough,
		latitude,
		longitude,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag
	from
		hpd_dob_match
	order by
		project_id
) as HPD_DOB_Match_2


/*Group matches to the HPD Project-level*/
		      
select
		      *
into
			  HPD_Deduped
from
(	      
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		construction_type,
		status,
		projected_fiscal_year_range,
		min_of_projected_units,
		max_of_projected_units,
		case 
			when array_to_string(array_agg(dob_job_number),', ') like '%, ,%' 	then null
			when array_to_string(array_agg(dob_job_number),', ') = ', ' 		then null
			else array_to_string(array_agg(dob_job_number),', ') 			end 	as dob_job_numbers	
		sum(units_net)										as DOB_Units_Net,
		greatest(total_units - sum(units_net),0) as hpd_incremental_units
		address,
		borough,
		latitude,
		longitude,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag
	from
		hpd_dob_match_2
	group by
		the_geom,
		the_geom_webmercator,
		project_id,
		construction_type,
		status,
		projected_fiscal_year_range,
		min_of_projected_units,
		max_of_projected_units,
		address,
		borough,
		latitude,
		longitude,
		bbl,
		nycha_flag,
		gq_flag,
		senior_housing_flag,
		assisted_living_flag		
	order by
		project_id
) as hpd_deduped


		      
/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_deduped')

