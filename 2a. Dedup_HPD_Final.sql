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
			when 	upper(concat(coalesce(e.dob_address,a.address),' ',a.borough)) =
					upper(concat(b.address,' ',b.borough)) 	and
					a.address is not null 														then 'Address'
			when	a.bbl::bigint = b.bbl and b.bbl is not null									then 'BBL'
			when st_intersects(a.the_geom,b.the_geom)											then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)						then 'Proximity' end as DOB_Match_Type,

		b.the_geom 																								as dob_geom,
		st_distance(cast(a.the_geom as geography),cast(b.the_geom as geography))								as Geom_Distance,
		b.job_number 																							as dob_job_number,
		b.job_type 																								as dob_job_type,
		b.job_description 																						as dob_job_description,
		b.address 								 																as dob_address,
		b.status 							 																	as dob_status,
		b.units_init									 														as units_init,
		b.units_prop									 														as units_prop,
		b.units_net 								 															as units_net,
		b.units_net_incomplete 																					as units_net_incomplete,
		b.latest_cofo										 													as latest_cofo,
		b.pre_filing_date 																						as pre_filing_date,	
		b.most_recent_status_date 																				as most_recent_status_date,
		b.completed_application_date		 															 		as completed_application_date,
		b.full_permit_issued_date 																 				as full_permit_issued_date,
		b.partial_permit_issued_date 																	 		as partial_permit_issued_date,
		b.job_completion_date 														 							as job_completion_date
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
		b.job_type = 'New Building'									and
		extract(year from b.pre_filing_date::date) >= 2017			and
		(

			(
				upper(concat(coalesce(e.dob_address,a.address),' ',a.borough)) =
				upper(concat(b.address,' ',b.borough)) 					and
				a.address is not null
			)																		or 
			(
				a.bbl::bigint = b.bbl and
				b.bbl is not null
			) 																		or
			(
				st_intersects(a.the_geom,b.the_geom)
			)																		or
			(
				st_dwithin(a.the_geom::geography,b.the_geom::geography,20)			and
				not st_intersects(a.the_geom,b.the_geom)							and
				(
					(a.total_units > 10 and abs(a.total_units-b.units_net)::float/a.total_units::float <=.5) or
					(a.total_units <=10 and abs(a.total_units - b.units_net) <=5)
				)																							/*Limiting proximity-matches to those that are close with unit count,
																			  								  with a threshold for smaller buildings*/
			)
		)
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
		sum(case when dob_match_type = 'Address' 				then 1 else 0 end) as address, 
		sum(case when dob_match_type = 'BBL' 					then 1 else 0 end) as BBL, 
		sum(case when dob_match_type = 'Spatial' 				then 1 else 0 end) as spatial, 
		sum(case when dob_match_type = 'Proximity' 			then 1 else 0 end) as proximity 
	FROM 
		capitalplanning.hpd_dob_match 
	group by
		dob_job_number
	having count(*) > 1
) multi_hpd_dob_matches

/*Limiting matches of the DOB jobs identified in multi_hpd_dob_matches. There are no cases when a DOB job matches both by more than 1 of address, BBL, and spatial overlap to multiple
  HPD Projected Closings.*/

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
		capitalplanning.hpd_dob_match_1
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
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_one_to_one_matching_20190523_ms where accurate_match =0) then null else dob_job_description end 	as dob_job_description,
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
  REIMPORT AS A LOOKUP (hpd_dob_proximate_matches_190603_v3) AND OMIT INACCURATE MATCHES. */
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
			  hpd_dob_match_4
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
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_match_type 		end 	as dob_match_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_job_number 		end 	as dob_job_number,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_job_type   		end 	as dob_job_type,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_job_description end 	as dob_job_description,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_status		   	end 	as dob_status,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else dob_address	   		end 	as dob_address,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else units_net 			end		as units_net,
		case when concat(project_id,dob_job_number) in(select concat(project_id,dob_job_number) from hpd_dob_proximate_matches_190603_v3 where match =0) then null else geom_distance 		end 	as geom_distance
	from
		hpd_dob_match_3
	order by
		project_id
) as HPD_DOB_Match_4




/*Group matches to the HPD Project-level*/
		      
select
		      *
into
			  HPD_Deduped_pre
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
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') 	as dob_job_numbers,	
		sum(units_net)																						as DOB_Units_Net,
		greatest(total_units - coalesce(sum(units_net),0),0) 												as hpd_incremental_units
	from
		HPD_DOB_Match_4
	group by
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
		assisted_living_flag
	order by
		project_id
) as hpd_deduped_pre


/*
  For HPD Project ID 53680, many DOB projects are geocoded to the same building, despite actually being duplicates of unmatched buildings in HPD project 53680. Creating a lookup
  to correct these matches.
*/

SELECT
	*
into
	hpd_project_53680_lookup
from
(
	SELECT
		*
	from
		hpd_deduped_pre
	where
		project_id like '53680%'
) x


/*Correcting matched for project_id 53680*/

SELECT
	*
into
	hpd_deduped_pre_1
from
(
	SELECT
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.address,
		a.borough,
		a.total_units,
		greatest(a.total_units - coalesce(c.units_net,a.DOB_Units_Net,0),0) 	as hpd_incremental_units,
		a.nycha_flag,
		a.gq_flag,
		a.senior_housing_flag,
		a.assisted_living_flag,
		coalesce(
					nullif
					(	
						concat_ws(
									', ',
									b.corrected_dob_job_number_match,
									nullif(c.address,'')
								),
					''
					),
					a.dob_job_numbers
				) 																as dob_job_numbers,
		coalesce(c.units_net,a.DOB_Units_Net) 									as dob_units_net
	from
		hpd_deduped_pre a
	left join
		hpd_projected_closings_53680_lookup_190523_ms b
	on
		a.project_id = b.hpd_project_id and
		b.accurate_match = 0
	left join
		dob_2018_sca_inputs_ms c
	on
		c.job_number = b.corrected_dob_job_number_match
	order by
		a.project_id asc
) hpd_deduped_pre_1


/*Joining onto full list of HPD Projected Closings*/

SELECT
	*
into
	hpd_deduped
from
(
	SELECT
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.Source,
		a.project_id,
		a.address,
		'Projected' as Status,
		a.projected_fiscal_year_range,
		a.borough,
		a.bbl,
		a.total_units,
		greatest(a.total_units - coalesce(b.dob_units_net,0),0) 	as hpd_incremental_units,
		a.nycha_flag,
		a.gq_flag,
		'Unknown' as senior_housing_flag,
		a.assisted_living_flag,
		b.dob_job_numbers,
		b.dob_units_net
	from
		(select * from hpd_2018_sca_inputs_ms where source = 'HPD Projected Closings') a
	left join
		hpd_deduped_pre_1 b
	on
		a.project_id = b.project_id
) hpd_deduped
order by
	project_id asc

		      
/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_deduped')

/************************************************************************DIAGNOSTICS****************************************************************************************************/

/*
70/182 projects have materialized in DOB data.
*/
SELECT
	COUNT(CASE WHEN dob_job_numbers <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_deduped

/*
49/67 projects expected to close by FY 2020 have materialized. 18/102 projects expected to close by FY2021 have materialized. 3 projects past FY 2021 have materialized. 
*/
SELECT
	projected_fiscal_year_range,
	COUNT(CASE WHEN dob_job_numbers <> '' then 1 end) as matched_projects,
	count(*) as all_projects
from
	hpd_deduped
group by
	projected_fiscal_year_range


/*Of the 70 materialized jobs, 55 match exactly with their DOB unit count. 7 match between 1-5 units, 2 b/w 5-10, 2 b/w 10-15, 3 b/w 35-40, and 2 > 50.*/

	select
		case
			when abs(total_units-dob_units_net) < 1 then '<1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' end
															 	as HPD_Units_minus_DOB_Units,
		count(*) as Count
	from 
		hpd_deduped
	where
		dob_job_numbers <> ''
	group by 
		case
			when abs(total_units-dob_units_net) < 1 then '<1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' end

/*
	The >50 unit difference matches below are correct. For 202-258 WEST 124 STREET, see the DOB address is 206 West 124th St.
	For the match at 3875 9 Avenue, see https://therealdeal.com/2018/08/31/maddd-equities-planning-614-unit-project-for-inwood/
*/
SELECT
	*
from
	hpd_deduped
where
	dob_job_numbers <> '' and
	abs(total_units - dob_units_net) > 50