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
		the_geom,
		project_id,
		hpd_project_id,
		building_id,
		project_name,
	--	primary_program_at_start,
		construction_type,
		lead_agency,
		status,
	--	project_start_date,
	--	projected_completion_date,
		projected_fiscal_year_range,
		address,
		borough,
		min_of_projected_units,
		max_of_projected_units,
		total_units,
		latitude,
		longitude,
		bin,
		bbl,
		source
	from(

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
		SELECT
			b.the_geom,
			concat(a.project_id,a.building_id) 				as project_id,
			a.project_id 							as hpd_project_id,
			a.building_id,
			null 								as project_name,
	--		null 								as primary_program_at_start,
			a.reporting_construction_type 					as construction_type,
			null 								as lead_agency,
			'Projected' 							as Status,
	--		null 								as project_start_date,
	--		null 								as projected_completion_date,
			a.projected_fiscal_year_range,
			concat(a.house_number, ' ',a.street_name) 			as address,
			a.boro_full_name 						as borough,
			a.min_of_projected_units,
			a.max_of_projected_units,
			(a.min_of_projected_units+a.max_of_projected_units)/2 		as total_units, /*We have been given a range for total units, and have chosen the avg of the high and low*/
			null		 						as latitude,
			null		 						as longitude,
			null 								as bin, 
			concat(a.bbl) 							as bbl,
			'HPD Projected Closings'					as Source
		from
			capitalplanning.hpd_projected_closings_190409_ms a
		left join
			capitalplanning.mappluto_v_18v2 b
		on
			a.bbl 	= b.bbl or
			(a.bbl 	= 2027380037 and b.bbl = 2027380035) /*Accounting for project at 720 Tiffany Street which will clearly be on lot with BBL 2027380035, but is listed as a nonexistant lot 2027380037*/
		union
		select
			st_union(coalesce(b.the_geom,c.the_geom)) 			as the_geom,
			concat(a.cartodb_id) 						as project_id,
			null 								as hpd_project_id,
			null								as building_id,
			a.rfp_project_name 						as project_name,
	--		null 								as primary_program_at_start,
			null 								as construction_type,			
			a.lead_agency,
			concat	(
					case when a.designated 	is true then 'RFP designated' 		else 'RFP issued'		end,
					case when a.closed 	is true then '; financing closed' 	else '; financing not closed' 	end
				) 							as status,
	--		null 								as project_start_date,
	--		null 								as projected_completion_date,
			null								as projected_fiscal_year_range,
			null 								as address,
			a.borough,
			null,
			null,
			a.announced_unit_count 						as total_units,
			null 								as latitude,
			null 								as longitude,
			null 								as bin,
			array_to_string(array_agg(concat(coalesce(b.bbl,c.bbl))),', ') 	as bbl,
			'HPD RFPs' 							as Source
		from
			capitalplanning.hpd_rfps_2019_03_18 a
		left join
			capitalplanning.hpd_rfps_1 b
		on 
			(a.rfp_project_name = b.rq__p_n) 																										or 
			(a.rfp_project_name = 'NYCHA Twin Park West'						and b.bbl in (2031430234,2031430236,2031430240)) 	or 
			(a.rfp_project_name = 'MWBE Site D - 359 E. 157th Street / 784 Courtlandt Avenue' 	and b.bbl in(2024040001, 2024040002)) 			or 
			(a.rfp_project_name = 'NYCHA Betances V' 						and b.bbl in(2022870026, 2022870071)) 			or
			(a.rfp_project_name = 'NYCHA Betances VI'						and b.bbl =2022910001)
		left join
			capitalplanning.mappluto_v_18v1_1 c
		on
			trim(a.rfp_project_name) = '425 Grand Concourse' and c.bbl = 2023460001
		group by
			concat(a.cartodb_id),
			a.rfp_project_name,
			a.lead_agency,
			concat	(
					case when a.designated 	is true	then 'RFP designated' 		else 'RFP issued' 		end,
					case when a.closed 	is true	then '; financing closed' 	else '; financing not closed' 	end
				),
			a.borough,
			a.announced_unit_count
		) as compilation
	order by 
		source, 
		cast(project_id as bigint)
) as hpd_2018_sca_inputs_ms


/***********************************RUN IN REGULAR CARTO*****************************/


select cdb_cartodbfytable('capitalplanning', 'hpd_2018_sca_inputs_ms')
