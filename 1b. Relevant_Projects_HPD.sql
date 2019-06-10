/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Preparing HPD's data for In Construction projects, projected closings, and RFPs Issued and Designated
Source file: "G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\HPD\hpd_2018_sca_inputs_geo_pts.csv"
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY:
1. Rearrange fields and create a distinct project ID field for HPD In Construction Projects
2. Geocode and append HPD Projected Closings with the same fields as above. 
3. Geocode updated HPD RFPs using outdated HPD RFP data with polygons. Assign polygons to ungeocoded projects.
   Aggregate polygons to the project level.
*************************************************************************************************************************************************************************************/

/***********************************RUN IN REGULAR CARTO*****************************
Shows that 77 of ~180 Projected Closings received from HPD on 4/2/2019 included in
2018 HPD Projected Closings Data. Majority of projects in old data which had no match
were started in 2018. No matches to complete or in construction projects, as expected.

	select
		a.*,
		case when b.project_id is not null then 1 end as match
	from
		capitalplanning.HPD_2018_SCA_Inputs_geo_pts a
	left join
		capitalplanning.hpd_projected_closings_190409_ms b
	on
		concat(a.project_id, a.building_id) = concat(b.project_id, b.building_id)
	where
		case when b.project_id is not null then 1 end = 1

*************************************************************************************/

/***********************************RUN IN CARTO BATCH*****************************/

SELECT
	*
into
	hpd_2018_sca_inputs_ms
from

(
	select
		row_number() over() as cartodb_id, 
		the_geom,
		the_geom_webmercator,
		project_id,
		hpd_project_id,
		building_id,
		project_name,
		construction_type,
		lead_agency,
		status,
		projected_fiscal_year_range,
		address,
		borough,
		min_of_projected_units,
		max_of_projected_units,
		total_units,
		bbl,
		/*Identifying NYCHA*/
		CASE 
			WHEN lead_agency 		  like '%NYCHA%' then 1
			WHEN upper(project_name)  like '%NYCHA%' THEN 1   		
			WHEN upper(project_name)  like '%BTP%' THEN 1  		
			WHEN upper(project_name)  like '%HOUSING AUTHORITY%' THEN 1  		
			WHEN upper(project_name)  like '%NEXT GEN%' THEN 1  		
			WHEN upper(project_name)  like '%NEXT-GEN%' THEN 1  		
			WHEN upper(project_name)  like '%NEXTGEN%' THEN 1  		
			WHEN upper(project_name)  like '%BUILD TO PRESERVE%' THEN 1 ELSE 0 END 	AS NYCHA_Flag,

		/*Identifying group quarters*/
		CASE 
			WHEN upper(project_name)  like '%CORRECTIONAL%' THEN 1   		
			WHEN upper(project_name)  like '%NURSING%' THEN 1  		
			WHEN upper(project_name)  like '% MENTAL%' THEN 1  		
			WHEN upper(project_name)  like '%DORMITOR%' THEN 1  		
			WHEN upper(project_name)  like '%MILITARY%' THEN 1  		
			WHEN upper(project_name)  like '%GROUP HOME%' THEN 1  		
			WHEN upper(project_name)  like '%BARRACK%' THEN 1 ELSE 0 END 			AS GQ_fLAG,

		/*Identifying definite senior housing projects*/
		CASE 
			WHEN upper(project_name)  like '%SENIOR%' THEN 1
			WHEN upper(project_name)  like '%ELDERLY%' THEN 1 	
			WHEN project_name  		  like '% AIRS %' THEN 1
			WHEN upper(project_name)  like '%A.I.R.S%' THEN 1 
			WHEN upper(project_name)  like '%CONTINUING CARE%' THEN 1
			WHEN upper(project_name)  like '%NURSING%' THEN 1
			WHEN project_name  		  like '% SARA %' THEN 1
			WHEN upper(project_name)  like '%S.A.R.A%' THEN 1 ELSE 0 end 			as Senior_Housing_Flag,
		/*Identifying assisted living projects*/
		CASE
			WHEN upper(project_name)  like '%ASSISTED LIVING%' THEN 1 else 0 end 	as Assisted_Living_Flag,
		CASE
			when status_for_sca = 'Children Unlikely'		   then 1 else 0 end as HPD_Children_Unlikely_Flag,
			Likely_to_be_Built_by_2025_Flag,
			Excluded_Project_Flag,
			rationale_for_exclusion,
			source
			,building_instance
	from(
		SELECT
			b.the_geom													as the_geom,
			b.the_geom_webmercator 										as the_geom_webmercator,
			concat(a.project_id,'/',a.building_id) 						as project_id,
			a.project_id 												as hpd_project_id,
			a.building_id,
			null 														as project_name,
			a.reporting_construction_type 								as construction_type,
			null 														as lead_agency,
			'Projected' 												as Status,
			a.projected_fiscal_year_range,
			concat(a.house_number, ' ',a.street_name) 					as address,
			a.boro_full_name 											as borough,
			a.min_of_projected_units,
			a.max_of_projected_units,
			(a.min_of_projected_units+a.max_of_projected_units)/2 		as total_units, /*We have been given a range for total units, and have chosen the avg of the high and low*/
			concat(a.bbl) 												as bbl,
			c.status_for_sca,
			null as Likely_to_be_Built_by_2025_Flag,
			null as Excluded_Project_Flag,
			null as rationale_for_exclusion,
			'HPD Projected Closings'									as Source,
		/*There are two projects, building IDs 985433 & 985432, which have duplicates of the same building ID but different project ID. The unit counts and lots
		  represent the same building, so the below field will be used to arbitrarily select one project for building 985433 and one project for building 985432*/
			row_number() over(partition by a.building_id order by (a.min_of_projected_units+a.max_of_projected_units)/2 desc) 				as building_instance
		from
			capitalplanning.hpd_projected_closings_190409_ms a
		left join
			capitalplanning.mappluto_v_18v2 b
		on
			a.bbl 	= b.bbl or
			(a.bbl 	= 2027380037 and b.bbl = 2027380035) /*Accounting for project at 720 Tiffany Street which will clearly be on lot with BBL 2027380035, but is listed as a nonexistent BBL 2027380037*/
		left join
			capitalplanning.table_2019_4_2_dcp_nc_pipeline_sca_status c
		on
			concat(a.project_id,'/',a.building_id) = concat(c.project_id,'/',c.building_id)
		union
		select
			st_union(coalesce(b.the_geom,c.the_geom)) 						as the_geom,
			st_union(coalesce(b.the_geom_webmercator,c.the_geom_webmercator)) as the_geom_webmercator,
			concat(a.cartodb_id) 											as project_id,
			null 															as hpd_project_id,
			null															as building_id,
			a.rfp_project_name 												as project_name,
			null 															as construction_type,			
			a.lead_agency,
			concat	
				(
					case when a.designated 	is true then 'RFP designated' 		else 'RFP issued'		end,
					case when a.closed 	is true then '; financing closed' 	else '; financing not closed' 	end
				) 															as status,
			null															as projected_fiscal_year_range,
			null 															as address,
			a.borough,
			null,
			null,
			case when a.announced_unit_count = 0 then null else a.announced_unit_count end	as total_units,
			array_to_string(array_agg(concat(coalesce(b.bbl,c.bbl))),', ') 					as bbl,
			'' as status_for_sca,
			case
				when rfp_project_name = 'NYCHA Harborview Terrace' then 0 /*Project no longer in NYCHA pipeline*/
				when estimated_build_year_by_2025 is true then 1 else 0 end as Likely_to_be_Built_by_2025_Flag,
			case
				when announced_unit_count = 0 then 1 else 0 end 			as Excluded_Project_Flag,
			rationale_for_exclusion,
			'HPD RFPs' 														as Source,
			null															as building_instance
		from
			capitalplanning.hpd_rfps_2019_05_16 a
		left join
			capitalplanning.hpd_rfps_1 b
		on 
			(a.rfp_project_name = b.rq__p_n) 																										or 
			(a.rfp_project_name = 'NYCHA Twin Park West'										and b.bbl in (2031430234,2031430236,2031430240)) 	or 
			(a.rfp_project_name = 'MWBE Site D - 359 E. 157th Street / 784 Courtlandt Avenue' 	and b.bbl in(2024040001, 2024040002)) 				or 
			(a.rfp_project_name = 'NYCHA Betances V' 											and b.bbl in(2022870026, 2022870071)) 				or
			(a.rfp_project_name = 'NYCHA Betances VI'											and b.bbl =2022910001)								or
			(a.rfp_project_name = 'Broadway Triangle'											and upper(b.rq__p_n) like '%BROADWAY TRIANGLE%')
		left join
			capitalplanning.mappluto_v_18v1_1 c
		on
			trim(a.rfp_project_name) = '425 Grand Concourse' and c.bbl = 2023460001
		group by
			concat(a.cartodb_id),
			a.rfp_project_name,
			a.lead_agency,
			concat	(
					case when a.designated 	is true	then 'RFP designated' 	else 'RFP issued' 		end,
					case when a.closed 	is true	then '; financing closed' 	else '; financing not closed' 	end
				),
			a.borough,
			a.announced_unit_count,
			case
				when rfp_project_name = 'NYCHA Harborview Terrace' then 0 
				when estimated_build_year_by_2025 is true then 1 else 0 end,
			case
				when announced_unit_count = 0 then 1 else 0 end,
			rationale_for_exclusion
		) as compilation
	order by 
		source, 
		project_id
) hpd_2018_sca_inputs_ms
/*There are two HPD Projected Closings, building IDs 985433 & 985432, which have duplicates of the same building ID but different project ID. The unit counts and lots
  represent the same building, so the below where statement will be used to arbitrarily select one project for building 985433 and one project for building 985432*/
where
	building_instance is null or
	building_instance = 1
order by
	source,
	project_id asc


/***********************************RUN IN REGULAR CARTO*****************************/


select cdb_cartodbfytable('capitalplanning', 'hpd_2018_sca_inputs_ms')

SELECT
	*
into
	hpd_closings_inputs_share_20190522
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		project_id,
		address,
		total_units
	from
		hpd_2018_sca_inputs_ms
	where
		source = 'HPD Projected Closings'
) hpd_closings_inputs_share_20190522
	order by
		project_id

select cdb_cartodbfytable('capitalplanning', 'hpd_closings_inputs_share_20190522')
															     

SELECT
	*
into
	hpd_rpfs_inputs_share_20190522
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		total_units
	from
		hpd_2018_sca_inputs_ms
	where
		source = 'HPD RFPs'
) hpd_rpfs_inputs_share_20190522
	order by
		project_id
															     
select cdb_cartodbfytable('capitalplanning', 'hpd_rpfs_inputs_share_20190522')

															     
															     
/***********************************SUPERSEDED**************************************/
															     
/*  OMITTING CODE FOR IN CONSTRUCTION PROJECTS 
		
		select
			the_geom,
			concat(project_id, building_id) 				as project_id,
			project_id 							as hpd_project_id,
			building_id,
			project_name,
			primary_program_at_start,
			construction_type,
			null 								as lead_agency,
			status,
			coalesce(project_start_date,projected_start_date) 		as project_start_date,
			coalesce(building_completion_date,projected_completion_date) 	as projected_completion_date,
			null 								as projected_fiscal_year_range,
			concat(house_number,' ', street_name) 				as address,
			borough,
			total_units,
			case 
				when lat_geoclient 		=0 then st_y(the_geom)
				else lat_geoclient 		end 			as latitude, 
																			/*Some observations with filled geoms but no lat - adding in lat from the_geom*/
			case 
				when long_geoclient 	=0 then st_x(the_geom)
				else  long_geoclient	end 				as longitude, 
																			/*Some observations with filled geoms but no long - adding in long from the_geom*/
			bin_geoclient 							as bin, /*Looks to be an inaccurate field*/
			concat(bbl_geoclient) 						as bbl,
			'HPD Projects' 							as Source
		from
			capitalplanning.HPD_2018_SCA_Inputs_geo_pts
		where
			status = 'In Construction' /*Limiting to In Construction projects as we have received a more recent update of projected closings from HPD*/
		union
*/
