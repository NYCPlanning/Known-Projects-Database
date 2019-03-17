/**************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Merging ZAP data cross borough, flagging residences, 
		and adding polygons
START DATE: 1/3/2018
COMPLETION DATE: 1/29/2018
SOURCE FILES:
1. dcp_zap_consolidated_ms: G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DCP_ZAP_Consolidated_MS.xlsx
	a. This is a consolidation of raw, by-borough ZAP project entities from: G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\ZAP 
2. HEIP ZAP Polygons: https://nycplanning.carto.com/u/capitalplanning/dataset/heip_zap_polygons
3. MAPPLUTO: https://nycplanning.carto.com/u/capitalplanning/dataset/mappluto_v_18v1_1
***************************************************************************************************************************************************************************************/
/**************************************************************************************************************************************************************************************
METHODOLOGY:
1. Create table called dcp_zap_consolidated_ms.
  Append in ZAP project pulls from each borough. Files included:
  DCP_ZAP_QUEENS_MS.csv
  DCP_ZAP_MX_MS.csv
  DCP_ZAP_BX_MS.csv
  DCP_ZAP_BK_MS.csv
  DCP_ZAP_SI_MS.csv
2. Merge in polygons to ZAP data using HEIP polygon data,
   previous SCA 2018 inputs share, NYZMA, and ZAP BBL polygon data
3. Create flags for residential, potential residential, and 
 	projects to exclude 
***************************************************************************************************************************************************************************************/

ALTER TABLE CAPITALPLANNING.dcp_zap_consolidated_ms
ADD COLUMN match_heip_geom numeric,
ADD COLUMN match_dcp_2018_sca_inputs_share_geom numeric,
ADD COLUMN match_nyzma_geom numeric,
ADD COLUMN match_pluto_geom numeric,
ADD COLUMN match_impact_poly_latest numeric, 
ADD COLUMN match_lookup_pluto_geom numeric; 

/*Merge in available polygons from HEIP's polygon data. 102 relevant project_ids joined successfully.*/
update capitalplanning.dcp_zap_consolidated_ms
set the_geom = a.the_geom, 
	THE_GEOM_WEBMERCATOR = a.the_geom_webmercator,
	match_heip_geom = 1
from capitalplanning.heip_zap_polygons a
where project_id = a.projectid and a.projectid is not null;


/*Merge in available Polygons from DCP 2018 SCA Inputs Share. 293 relevant project_ids joined successfully*/
update capitalplanning.dcp_zap_consolidated_ms a
set the_geom = 					coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR =		coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_dcp_2018_sca_inputs_share_geom = 1
from capitalplanning.dcp_2018_sca_inputs_share b
where a.project_id = b.project_id and 
	  a.project_id is not null and
	  match_heip_geom is null;

/*Merge in ULURP # and match by Project IDs to then join polygons from
NYZMA using ULURP #. Note that less than 1% of observations we are able to match
less than 1% of observations with this attempt.*/
update capitalplanning.dcp_zap_consolidated_ms
set ulurp_number = a.ulurp_number
from capitalplanning.dcp_project_actions_zap_ms a
where 
		(project_id = a.project and project_id <> 'P2005M0053');
		/*Hudson Yards has two designated ULURP #s: 040499 and 040500. Despite that the ZAP data only includes 040500, ZOLA and NYZMA
	  	  only attribute 040499 to Hudson Yards. I am not matching Hudson Yards, P2005M0053, to avoid assigning it ULURP # 040500. I am adding on the polygon from 040499 to geocode Hudson Yards.*/

update capitalplanning.dcp_zap_consolidated_ms
set ulurp_number = a.ulurp_number
from capitalplanning.dcp_project_actions_zap_ms a
where (project_id = 'P2005M0053' and a.ulurp_number = 'C040499AZMM');
		/*Hudson Yards has two designated ULURP #s: 040499 and 040500. Despite that the ZAP data only includes 040500, ZOLA and NYZMA
	  	  only attribute 040499 to Hudson Yards. I am adding on the polygon from 040499 to geocode Hudson Yards.*/

update capitalplanning.dcp_zap_consolidated_ms a
set the_geom = 				coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_nyzma_geom = 1
from capitalplanning.nyzma_december2018 b
where 
	  case when 
	  				substring(a.ulurp_number,1,1) = '1' then substring(a.ulurp_number,1,6)
	  													else substring(a.ulurp_number,2,6) end
	  			
	  																			 = substring(b.ulurpno,1,6) and 
	  a.ulurp_number is not null and
	  a.match_heip_geom is null and
	  a.match_dcp_2018_sca_inputs_share_geom is null
	  ;

/********************************************************************************************
Merge in polygons from PLUTO to BBLs included in each Project ID (exported from ZAP).
Then merge in polygons by Project_ID using this updated BBL file. 
**********************************************************************************************/

update capitalplanning.dcp_project_bbls_zap_ms a
set the_geom = b.the_geom,
	the_geom_webmercator = b.the_geom_webmercator
from capitalplanning.mappluto_v_18v1_1 b
where a.bbl_number = b.bbl and a.bbl_number is not null;

with dcp_project_bbls_zap_ms_consolidated as
(
	select
		st_union(the_geom) as the_geom,
		st_union(the_geom_webmercator) as the_geom_webmercator,
		project
	from
		capitalplanning.dcp_project_bbls_zap_ms
	group by
		project
	order by
		project
)

update capitalplanning.dcp_zap_consolidated_ms a
set the_geom = 				coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_pluto_geom = 1
from dcp_project_bbls_zap_ms_consolidated b
where a.project_id = b.project and 
	  a.project_id is not null and
	  b.the_geom is not null and
	  match_heip_geom is null and
	  match_dcp_2018_sca_inputs_share_geom is null and
	  match_nyzma_geom is null;



/********************************************************************************************
Merging in polygon data from Impact_Poly_Latest, last updated in April 2018.
**********************************************************************************************/

update capitalplanning.dcp_zap_consolidated_ms a
set the_geom = 					coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=		coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_impact_poly_latest = 1
from capitalplanning.Impact_Poly_Latest b
where a.project_id = b.projectid and 
	  a.project_id is not null and
	  b.the_geom is not null and
	  match_heip_geom is null and
	  match_dcp_2018_sca_inputs_share_geom is null and
	  match_nyzma_geom is null and
	  match_pluto_geom is null;

/********************************************************************************************
The following query should be run in Carto Batch. It does the following.
Merge in PLUTO geometries to projects in a manually created lookup using BBLs.
Then merge in polygons to the ZAP project file using BBL. Because some Project IDs have multiple
BBLs, the first merge creates doubles. I then use st_union to aggregate these doubles.

26 projects matched using the following method.
**********************************************************************************************/

select
	project_id,
	count(*) as Match_Count,
	st_union(the_geom) 				as the_geom,
	st_union(the_geom_webmercator) 	as the_geom_webmercator
into
	zap_project_missing_geom_lookup_1
from
(
	select
		a.project_id,
		b.the_geom				as the_geom,	
		a.THE_GEOM_WEBMERCATOR 	as the_geom_webmercator
	from
		capitalplanning.zap_project_missing_geom_lookup a
	left join
		capitalplanning.mappluto_v_18v1_1 b
	on
		case when 	a.associated_bbl is not null then a.associated_bbl = b.bbl
			 else 	a.block is not null and
			 	  	a.block = b.block and
			 	  	concat(b.lot) in(
									trim(split_part(a.lot,',',1)),
									trim(split_part(a.lot,',',2)),
									trim(split_part(a.lot,',',3))
									) end
) as Lookup_Pluto_Merge
group by 
	project_id


update capitalplanning.dcp_zap_consolidated_ms a
set the_geom = 					coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=		coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_lookup_pluto_geom = 1
from zap_project_missing_geom_lookup_1 b
where a.project_id = b.project_id and 
	  a.project_id is not null and
	  b.the_geom is not null and
	  match_heip_geom is null and
	  match_dcp_2018_sca_inputs_share_geom is null and
	  match_nyzma_geom is null and
	  match_pluto_geom is null and
	  match_impact_poly_latest is null;



/*The following step eliminates a false data entry where the input lists 
  the residential sqft figure as both in residential sqft and clearly 
  inaccurately in the voluntary_affordable_dwelling_units_non_mih as 
  well.*/
update capitalplanning.dcp_zap_consolidated_ms a
set voluntary_affordable_dwelling_units_non_mih = null
where project_id = '2019K0093';


/*The following step adds a total units estimate for various
  state-wide projects and manually-included old neighborhood rezonings*/

/*Hudson Yards: Units taken from MQL's last June 2018 SCA Input areawide data.*/	
update capitalplanning.dcp_zap_consolidated_ms a
set total_dwelling_units_in_project = 13508
	new_dwelling_units 				= 13508

where project_id = 'P2005M0053';

/*Western Rail Yards: Units taken from Table S-5 of the Western Rail Yards EIS.*/	
update capitalplanning.dcp_zap_consolidated_ms a
set total_dwelling_units_in_project = 6074
	new_dwelling_units 				= 6074
where project_id = 'P2009M0294';


/*Adding in Statewide projects from lookup and their polygons from PLUTO.*/
with State_Developments_Geom as
(
	select
		b.the_geom as the_geom,
		a.project_name,
		a.total_units as new_dwelling_units,
		a.borough,
		a.bbl
	from
		capitalplanning.state_developments_for_housing_pipeline a
	left join
		capitalplanning.mappluto_v_18v1_1 b
	on
		a.bbl = b.bbl
),

	State_Developments_Geom_1 as
(
	select
		st_union(the_geom) as the_geom,
		project_name,
		new_dwelling_units,
		borough,
		count(case when bbl is not null then 1 end) as BBL_Count,
		count(case when the_geom is not null then 1 end) as Polygon_Count
		/*Polygon count for both projects is less than BBL count, indicating that
		  PLUTO does not include some BBLs listed for these projects*/
	FROM
		State_Developments_Geom
	group by
		project_name,
		new_dwelling_units,
		borough
)

insert into
	 capitalplanning.dcp_zap_consolidated_ms
	 	(the_geom, project_id, project_name, borough, new_dwelling_units)
	 select
	 	the_geom,
	 	concat(row_number() over(), ' [ESD Project]') as project_id,
	 	project_name,
	 	borough,
	 	new_dwelling_units
	 from
	 	State_Developments_Geom_1;

/*************************************************************************************
RUN THE REST OF THIS SCRIPT IN CARTO BATCH

Identify DCP projects which could be relevant to the SCA housing pipeline.
*************************************************************************************/ 
SELECT
	*
into
	dcp_project_flags
FROM
(
		select 
			a.*,
			b.project_status 				as previous_project_status,
			b.process_stage 				as previous_process_stage,
			b.remaining_likely_to_be_built 	as remaining_likely_to_be_built,
			b.rationale 					as rationale,
			case when (
						a.si_school_seat <> 'true' or a.si_school_seat is null
					  )
					  and upper(concat(a.project_description,' ',a.project_brief)) not like '%SCHOOL SEAT CERT%' 
					  and upper(substring(a.project_name,1,3)) <> 'SS ' 			   then 1 ELSE 0 end as No_SI_Seat, /*Potential exclusion if null. A few instances in project brief where school seat certification is mentioned. Also omitting 'SS ' from Project_Name.*/
			case when 
					(
						(	
							case when a.total_dwelling_units_in_project 				is null then 0 else a.total_dwelling_units_in_project 				end 	+ 
						  	case when a.mih_dwelling_units_higher_number				is null then 0 else a.mih_dwelling_units_higher_number  			end 	+ 
						  	case when a.mih_dwelling_units_lower_number					is null then 0 else a.mih_dwelling_units_lower_number				end 	+ 
						  	case when a.new_dwelling_units  							is null then 0 else a.new_dwelling_units 							end 	+ 
						  	case when a.voluntary_affordable_dwelling_units_non_mih		is null then 0 else a.voluntary_affordable_dwelling_units_non_mih 	end
						) > 0 											or
					   	(a.residential_sq_ft > 0 and upper(concat(a.project_description,a.project_brief)) not like '%APPLICATION FOR PARKING%') /*Eliminating Parking application for large building (P2015M0047)*/			or
					   	a.project_id in('P2005M0053','P2009M0294') 		/*Adding in Hudson Yards and Western Rail Yards*/
					)													and
						upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING SINGLE-FAMILY%' 	and
						upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING ONE-FAMILY%' 		and
						upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING 1-FAMILY%' 		and
						upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING HOME%'
																									   	THEN 1 else 0 
																										END AS Dwelling_Units, /*Identified as residential projects*/
			/*~12/14 Projects without associated unit count but WITH residential square feet had the above text catches.*/ 


			/*Text match as potential residential projects*/	 																																													
		coalesce(
		CASE	 when upper(concat(a.project_description,' ',a.project_brief)) like '%AFFORDABLE%' 	then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%RESID%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%RESIDENCE%' 	then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%APARTM%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%APT%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%DWELL%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%LIVING%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%HOUSI%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%MIH%' 		then 1 
				 when upper(concat(a.project_description,' ',a.project_brief)) like '%HOMES%' 		then 1  /*Consider omitting this text search as it searches for single residences*/
				 when (concat(a.project_description,' ',a.project_brief)) 	   like '%DUs%'			then 1 
				 																							END  - 
		CASE 	 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENTIAL TO COMMERCIAL%' THEN 1
			 	 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE-FAMILY%' THEN 1 
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE FAMILY%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1-FAMILY%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE FAMILY%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE-FAMILY%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1 FAMILY%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FLOATING%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%TRANSITIONAL%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FOSTER%' THEN 1
				 -- WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%PARKING%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ILLUMIN%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENCE DISTRICT%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%LANDMARKS PRESERVATION COMMISSION%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOME%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOUSE%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NUMBER OF BEDS%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EATING AND DRINKING%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NO INCREASE%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ENLARGEMENT%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NON-RESIDENTIAL%' THEN 1
				 WHEN upper(concat(a.project_description,' ',a.project_brief)) like  '%LIVINGSTON%' THEN 1 
				 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' THEN 1 ELSE 0 END
			,0) 																												AS Potential_Residential,
			CASE																													
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SENIOR%' THEN 1 ELSE 0 END 				AS SENIOR_HOUSING_flag,

			CASE																											
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NURSING%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%MEDICAL%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AIRS%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%A.I.R.S%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%CONTINUING CARE%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ASSISTED LIVING%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ELDERLY%' THEN 1
					  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SHELTER%' THEN 1 ELSE 0 END as Assisted_Living_Supportive_Housing_flag,
			

	/*Incorporating diagnostic code for judging the accuracy of text searching by text-catching criteria*/

			CASE 	 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%AFFORDABLE%' 	then 'AFFORDABLE' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%RESID%' 		then 'RESID' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%RESIDENCE%' 	then 'RESIDENCE' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%APARTM%' 		then 'APARTM' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%APT%' 		then 'APT' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%DWELL%' 		then 'DWELL' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%LIVING%' 		then 'LIVING' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%HOUSI%' 		then 'HOUSI' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%MIH%' 		then 'MIH' 
					 when upper(concat(a.project_description,' ',a.project_brief)) 	like '%HOMES%' 		then 'HOMES'  /*Consider omitting this text search as it searches for single residences*/
					 when 		(concat(a.project_description,' ',a.project_brief))	like '%DUs%'			then 'DUs' END as Potential_Res_Catch,
			CASE 	 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENTIAL TO COMMERCIAL%' THEN 'RESIDENTIAL TO COMMERCIAL'
				 	 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE-FAMILY%' THEN 'SINGLE-FAMILY' 
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE FAMILY%' THEN 'SINGLE FAMILY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1-FAMILY%' THEN '1-FAMILY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE FAMILY%' THEN 'ONE FAMILY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE-FAMILY%' THEN 'ONE-FAMILY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1 FAMILY%' THEN '1 FAMILY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FLOATING%' THEN 'FLOATING'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%TRANSITIONAL%' THEN 'TRANSITIONAL'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FOSTER%' THEN 'FOSTER'
					 -- WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%PARKING%' THEN 'PARKING'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ILLUMIN%' THEN 'ILLUMIN'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENCE DISTRICT%' THEN 'RESIDENCE DISTRICT'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%LANDMARKS PRESERVATION COMMISSION%' THEN 'LANDMARKS PRESERVATION COMMISSION'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOME%' THEN 'EXISTING HOME'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOUSE%' THEN 'EXISTING HOUSE'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NUMBER OF BEDS%' THEN 'NUMBER OF BEDS'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EATING AND DRINKING%' THEN 'EATING AND DRINKING'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NO INCREASE%' THEN 'NO INCREASE'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ENLARGEMENT%' THEN 'ENLARGEMENT'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NON-RESIDENTIAL%' THEN 'NON-RESIDENTIAL'
					 WHEN upper(concat(a.project_description,' ',a.project_brief)) like  '%LIVINGSTON%' THEN 'LIVINGSTON' 
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' THEN 'AMBULATORY' END as Non_Res_Catch,
			CASE																											
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NURSING%' THEN 'NURSING'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' THEN 'AMBULATORY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%MEDICAL%' THEN 'MEDICAL'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AIRS%' THEN 'AIRS'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%A.I.R.S%' THEN 'A.I.R.S'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%CONTINUING CARE%' THEN 'CONTINUING CARE'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ASSISTED LIVING%' THEN 'ASSISTED LIVING'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ELDERLY%' THEN 'ELDERLY'
					 WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SHELTER%' THEN 'SHELTER' END as Assisted_Supportive_Catch,

	/*End of text-catching diagnostic script*/

			case when a.process_stage_name_stage_id_process_stage = 'Initiation' then 1 else 0 end as Initiation_Flag, /*Potential exclusion if 1*/
			case when a.process_stage_name_stage_id_process_stage = 'Pre-Pas' then 1 else 0 end as Pre_PAS_Flag, /*Potential exclusion if 1*/
			case when date_part('year',cast(a.project_completed as date)) < 2012 or date_part('year',cast(a.certified_referred as date)) < 2012 then 1 else 0 end as Historical_Project_Pre_2012, /*Assessing recency of the project. Potential exclusion.*/ 
			case when date_part('year',cast(a.project_completed as date)) < 2008 or date_part('year',cast(a.certified_referred as date)) < 2008 then 1 else 0 end as Historical_Project_Pre_2008, /*Assessing recency of the project. Potential exclusion.*/ 
			abs(coalesce(total_dwelling_units_in_project,0) - coalesce(new_dwelling_units,0)) as Diff_Between_Total_and_New_Units,
			case when a.project_id in('P2012M0255') then 1 end as Areawide_Flag /*Identifying Hudson Square to be treated as areawide*/
		from
			capitalplanning.dcp_zap_consolidated_ms a
		left join
			capitalplanning.knownprojects_dcp_final b
		on
			a.project_id = b.project_id and a.project_id is not null
		where
				a.project_id in (
									'P2005M0053' /*Hudson Yards*/,
					   				'P2009M0294' /*Western Rail Yards*/,
					   				'P2014M0257' /*550 Washington St*/
					  			)	or
				a.project_id like '%[ESD Project]%' or /*State project*/
			(
				(
				a.project_status not in('Record Closed','Terminated') and
				a.project_status not like '%Withdrawn%' /*Excluding discontinued projects*/ and
				a.applicant_type <> 'DCP' /*Excluding DCP-initiated projects*/ 
				)
				-- and not
				-- (
				-- a.applicant_type = 'Other Public Agency' and (a.project_status = 'Complete' or a.process_stage_name_stage_id_process_stage = 'Completed') and not a.applicant_administrator = 'NYC EDC' 
				-- 																																				/*Including EDC-led projects*/
				-- ) 
			)  and
			a.project_id not in('P2016Q0238') /*Omitting Downtown Far Rockaway Rezoning*/
) as DCP_Project_Flagging

SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms
from
(
	with relevant_dcp_projects as
	(
		select
			*
		from 
			dcp_project_flags
		where
			Areawide_Flag is null and
		(
			(
				-- no_si_seat = 1 and 
				(
				dwelling_units = 1 or 
				potential_residential = 1
				) and 
				historical_project_pre_2012 =0 
				-- and pre_pas_flag<>1 and
				-- initiation_flag<>1
			) or
			project_id in (
							'P2005M0053' /*Hudson Yards*/,
						   	'P2009M0294' /*Western Rail Yards*/,
						   	'P2014M0257' /*550 Washington St*/
						  ) or
			project_id like '%[ESD Project]%' /*State project*/
		)
		order by project_id

	),

	/*Export the following list of potential residential project IDs to create a lookup and merge back on, assessing whether these IDs are confirmed to be residential
	  and inputting in total units and source when possible*/

	potential_residential_projects as
	(
		select
			*
		from
			relevant_dcp_projects
		where
			dwelling_units=0 and
			project_id not like '%[ESD Project]%' and 
			project_id not in 
							(
							'P2005M0053' /*Hudson Yards*/,
						   	'P2009M0294' /*Western Rail Yards*/,
						   	'P2014M0257' /*550 Washington St*/
						  	) 
	),

	/*Adding in manually collected data on projects flagged Potential_Residential with confirmation of whether residential, total units, and flag for further research*/

	relevant_dcp_projects_1 as
	(
		select 
				a.*,
				b.confirmed_potential_residential,
				b.total_units_from_description as potential_residential_total_units,
				b.need_manual_research as need_manual_research_flag
	 	from 
	 		relevant_dcp_projects a
	 	left join
	 		potential_residential_zap_project_check_ms b
	 	on a.project_id = b.project_id
	),


	relevant_dcp_projects_2 as
	(
		select
			*
		from
			relevant_dcp_projects_1
		where 
			dwelling_units = 1 or
			confirmed_potential_residential = 1
		order by
			project_id
	),

	relevant_dcp_projects_3 as
	(
		select
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		applicant_type,
		case when substring(lead_action,1,2) = 'ZM' then 1 else 0 end as Rezoning_Flag,
		case when project_id = 'P2017R0349' then 1 else senior_housing_flag end as senior_housing_flag, --Flagging additional senior housing project
		case when project_id = 'P2018X0001' then 1 else Assisted_Living_Supportive_Housing_flag end as Assisted_Living_Supportive_Housing_flag, --Flagging additional supportive housing project
		project_status,
		previous_project_status,
		process_stage_name_stage_id_process_stage as process_stage,
		previous_process_stage,
		anticipated_year_built,
		project_completed,
		certified_referred,
		system_target_certification_date as target_certified_date,
		project_status_date as latest_status_date,

		/*	The following field is calculated by coalescing total units taken from the description of projects deemed residential using text search and
			the total units listed in the original data. There is never a case where both fields exist because the lookup for projects deemed residential
			using text search was manually created by picking text-search projects with no associated units in the data. For projects with residential
			square feet but no associated units, 1 DU assigned per 850 SQFT. See first CHECKING QUERY below for justification.

			NOTE: 2 PROJECTS (P2014R0246 AND P2015Q0366) are flagged as dwelling unit projects but have new_dwelling_units listed as 0, residential
				  sqft >0, and all other unit fields as null. After reading the project descriptions, assuming these projects are correctly coded.

		*/

		case 
			when
				new_dwelling_units 							is null and
				total_dwelling_units_in_project 			is null and
				mih_dwelling_units_higher_number 			is null and
				voluntary_affordable_dwelling_units_non_mih is null and
				mih_dwelling_units_lower_number 			is null and
				potential_residential_total_units 			is null and
				residential_sq_ft							is null 	then 	null




			else
				coalesce(
						potential_residential_total_units, /*Preferencing manual lookup additions by MS*/
						case when new_dwelling_units <> 0 then new_dwelling_units else null end, 
						total_dwelling_units_in_project,
						case when 
								coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) <> 0 then 
								coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) else null end,
								/*	Two observations where voluntary affordable units is listed and total_dwelling_units is not, and the voluntary_affordable units are confirmed by the project
									description.*/
						case when residential_sq_ft/850 < 1 and residential_sq_ft is not null then 1 else residential_sq_ft/850 end /*Average DUs/sqft. Confirmed by T. Smith*/
						) end 

																																								as total_units,

		case
			when potential_residential_total_units is not null 																									then 'Potential Residential Total Units'
			when new_dwelling_units not in(null, 0) 																											then 'New Dwelling Units'
			when total_dwelling_units_in_project is not null 																									then 'Total Dwelling Units in Project'
			when coalesce(mih_dwelling_units_higher_number,mih_dwelling_units_lower_number,0) + coalesce(voluntary_affordable_dwelling_units_non_mih,0) <> 0	then 'MIH + Voluntary Affordable Units'
			when residential_sq_ft is not null 																													then 'Residential Square Feet' end 

																																								as total_units_source,
		remaining_likely_to_be_built,
		rationale,
		no_si_seat,
		dwelling_units as dwelling_units_flag,
		confirmed_potential_residential as potential_residential_flag,
		need_manual_research_flag,
		initiation_flag,
		pre_pas_flag,
		Diff_Between_Total_and_New_Units,
		Historical_Project_Pre_2012,
		Historical_Project_Pre_2008,
		match_heip_geom,
		match_dcp_2018_sca_inputs_share_geom,
		match_nyzma_geom,
		match_pluto_geom,
		match_impact_poly_latest
	from 
		relevant_dcp_projects_2
	),

	/*Identifying DCP projects which are permit renewals or changes of previous DCP projects. Omitting the outdated DCP project*/

		matching_projects as
	(
		select
			a.project_id,
			b.project_id as match_project_id,
			a.project_name,
			b.project_name as matched_project_name,
			a.project_description,
			b.project_description as match_project_description,
			a.borough,
			b.borough as match_borough,
			coalesce(a.certified_referred,a.target_certified_date) as date,
			coalesce(b.certified_referred,b.target_certified_date) as match_date,
			a.total_units,
			b.total_units as match_total_units,
			st_distance(a.the_geom,b.the_geom) as Distance
		from
			relevant_dcp_projects_3 a
		inner join
			relevant_dcp_projects_3 b
		on 
			coalesce(a.certified_referred,a.target_certified_date) >= coalesce(b.certified_referred,b.target_certified_date) and
			a.project_id <> b.project_id and
			(
				(
				st_distance(a.the_geom,b.the_geom) = 0 and
				a.the_geom is not null and
				b.the_geom is not null
				) or
				position(upper(b.project_name) in upper(a.project_name)) > 0 or
				position(upper(a.project_name) in upper(b.project_name)) > 0
			) 
	),

	/*Omitting selecting projects where total units are 1.*/


/*FLAG FOR REVIEW*/
		matching_projects_filtered as
	(
		select
			* 
		from
			matching_projects
		where 
			position('PERMIT RENEWAL' in upper(concat(project_name,project_description))) > 0 or
			(total_units = match_total_units and total_units <> 1) or
			(
				left(
						upper(project_name),position('STREET' IN upper(project_name))-1
					) = 
				left(
						upper(matched_project_name),position('STREET' IN upper(matched_project_name))-1
					) 
				and
				position('STREET' in upper(project_name)) > 0 and 
				position('STREET' in upper(matched_project_name)) >0
			)																				 /* Accounting for the match of Project IDs P2015M007 and P2012M0256,
																								which show the same address in the project_name field
																								but have slightly different coordinates that are
																								more distant than other, inaccurate matches.*/
	),

		relevant_projects_4 as
	(
		select
			*
		from
			relevant_dcp_projects_3
		where
			project_id not in(select match_project_id from matching_projects_filtered)
		order by project_id
	)


	/**********************************************************************************************************************************************************************
	Create new table based on following query titled "relevant_dcp_projects_housing_pipeline_ms." Project_ID P2005M0053 included to include Hudson Yards.
	Project_ID P2009M0294 included to include Western Rail Yards. Note that 16 observations are missing Total_Units. These must be collected from planners. 
	**********************************************************************************************************************************************************************/

		select
			* 
		from
			relevant_projects_4
) AS DCP_FINAL

select cdb_cartodbfytable('capitalplanning', 'relevant_dcp_projects_housing_pipeline_ms')

/**********************************************************************************************************************************************************************
CHECKING QUERY: SQUARE FEET AND DWELLING UNITS

There are multiple ZAP projects remaining with Residential SQ FT but no total units in the data. The following query shows that the avg sqft per unit
for projects that had both, by borough, was as follows:

SI: 1180
BX: 854
Queens: 828
Manhattan: 816
BK: 289 (likely due to poor data)

SELECT
	borough,
	sum(total_units) as total_units,
	sum(residential_sq_ft) as res_sq_ft,
	sum(residential_sq_ft)/sum(total_units) as sq_ft_per_unit,
	count(*) as obs
FROM
	 relevant_projects_4 
where
	total_units is not null and
	residential_sq_ft is not null
group by
	borough 

/**********************************************************************************************************************************************************************
CHECKING QUERY: 

1. Outdated but approximate: Show that 53/444 projects are missing polygons.
	a. 	SELECT 	
				count(case when the_geom is null then 1 end) as missing_geom,
	   			count(*) 
		FROM 
				capitalplanning.relevant_dcp_projects_housing_pipeline_ms
2. Show # of polygons sourcing from each dataset. HEIP: 50; DCP_2018_SCA_Inputs_Share: 188; NYZMA: 2; Pluto: 182; Poly_Latest: 3
	a.	SELECT 
				sum(match_heip_geom) as heip_geom,
			    sum(match_dcp_2018_sca_inputs_share_geom) as DCP_2018_SCA_Inputs_Share_Geom,
		        sum(match_nyzma_geom) as NYZMA_Geom,
		       	sum(match_pluto_geom) as Pluto_Geom,
		       	sum(match_impact_poly_latest) as Poly_Latest,
		       	count(*) 
		FROM 
				capitalplanning.relevant_dcp_projects_housing_pipeline_ms
3.	OUTDATED: Show matching information between relevant_dcp_projects_housing_pipeline_ms and HEIP_ZAP_Polygons residential dataset. When matching on Project ID,
	the two datasets show only 93 matches. 
	a.	SELECT a.project_id, 
			   b.projectid
		FROM 	capitalplanning.relevant_dcp_projects_housing_pipeline_ms a
		full outer join
				capitalplanning.heip_zap_polygons b
		on 	a.project_id = b.projectid and
			a.project_id is not null
**********************************************************************************************************************************************************************/
