/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Consolidating ZAP projects by borough
Sources: edc_2018_sca_input_1_limited, dob_2018_sca_inputs_ms,
		 hpd_projects_dob_edc_hpdrfp_zap_dep_match, hpd_2018_sca_inputs_ms
*************************************************************************************************************************************************************************************/

/**************************************RUN IN CARTO BATCH*********************************/

/*Consolidate ZAP Project pull from each borough. Requires manual field type modifications which are mentioned in the query.*/
	SELECT 
		* 
	into
		dcp_zap_consolidated_20190510_ms
	FROM 
		(
			select
				*
			from
				capitalplanning.v2_20190510_dcp_zap_mn_ms 
			union 
			select 
				* 
			from 
				capitalplanning.v2_20190510_dcp_zap_bx_ms 
			union
			select 
				* 
			from 
				capitalplanning.v2_20190510_dcp_zap_bk_ms  
			union 
			select 
				*  
			from 
				capitalplanning.v2_20190510_dcp_zap_qn_ms /*Requires, in Carto, manually converting the Anticipated Year Built field to numeric*/
			union 
			select 
				* 
			from 
				capitalplanning.v2_20190510_dcp_zap_si_ms /*Requires, in Carto, manually converting all the unit count and square footage fields to numeric*/
		) as citywide


/********************************RUN IN REGULAR CARTO*******************************************/

ALTER TABLE CAPITALPLANNING.dcp_zap_consolidated_20190510_ms
ADD COLUMN ULURP_NUMBER TEXT,
ADD COLUMN match_heip_geom numeric,
ADD COLUMN match_dcp_2018_sca_inputs_share_geom numeric,
ADD COLUMN match_nyzma_geom numeric,
ADD COLUMN match_pluto_geom numeric,
ADD COLUMN match_impact_poly_latest numeric, 
ADD COLUMN match_lookup_pluto_geom numeric; 

/*Merge in available polygons from HEIP's polygon data. 102 relevant project_ids joined successfully.*/
update capitalplanning.dcp_zap_consolidated_20190510_ms
set 
	the_geom 		= a.the_geom, 
	THE_GEOM_WEBMERCATOR 	= a.the_geom_webmercator,
	match_heip_geom 	= 1
from capitalplanning.heip_zap_polygons a
where 	project_id = a.projectid and 
	a.projectid is not null;


/*Merge in available Polygons from DCP 2018 SCA Inputs Share. 293 relevant project_ids joined successfully*/
update capitalplanning.dcp_zap_consolidated_20190510_ms a
set the_geom = 						coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR =			coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_dcp_2018_sca_inputs_share_geom = 	1
from capitalplanning.dcp_2018_sca_inputs_share b
where 
	a.project_id = b.project_id and 
	 a.project_id is not null and
	 match_heip_geom is null;

update capitalplanning.dcp_zap_consolidated_20190510_ms
set ulurp_number = a.ulurp_number
from capitalplanning.v2_20190510_project_action_advanced_find_view_ms a
where 
	(project_id = a.project and project_id <> 'P2005M0053');
	/*Hudson Yards has two designated ULURP #s: 040499 and 040500. Despite that the ZAP data only includes 040500, ZOLA and NYZMA
	  only attribute 040499 to Hudson Yards. I am not matching Hudson Yards, P2005M0053, to avoid assigning it ULURP # 040500. 
	  I am adding on the polygon from 040499 to geocode Hudson Yards.*/


update capitalplanning.dcp_zap_consolidated_20190510_ms
set ulurp_number = a.ulurp_number
from capitalplanning.v2_20190510_project_action_advanced_find_view_ms a
where 
	(project_id = 'P2005M0053' and a.ulurp_number = 'C040499AZMM');
	/*Hudson Yards has two designated ULURP #s: 040499 and 040500. Despite that the ZAP data only includes 040500, ZOLA and NYZMA
  	  only attribute 040499 to Hudson Yards. I am adding on the polygon from 040499 to geocode Hudson Yards.*/

update capitalplanning.dcp_zap_consolidated_20190510_ms a
set the_geom = 			coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_nyzma_geom = 	1
from capitalplanning.nyzma_december2018 b
where 
	  case when 
		substring(a.ulurp_number,1,1) = '1' 	then substring(a.ulurp_number,1,6)
							else substring(a.ulurp_number,2,6) end				 = substring(b.ulurpno,1,6) and 
	  a.ulurp_number is not null and
	  a.match_heip_geom is null and
	  a.match_dcp_2018_sca_inputs_share_geom is null
	  ;

/********************************************************************************************
Merge in polygons from PLUTO to BBLs included in each Project ID (exported from ZAP).
Then merge in polygons by Project_ID using this updated BBL file. 
**********************************************************************************************/

update capitalplanning.v2_20190510_project_bbl_advanced_find_view_ms a
set 
	the_geom 		= b.the_geom,
	the_geom_webmercator 	= b.the_geom_webmercator
from capitalplanning.mappluto_v_18v1_1 b
where 
	a.bbl_number = b.bbl and 
	a.bbl_number is not null;

with dcp_project_bbls_zap_ms_consolidated as
(
	select
		st_union(the_geom) 		as the_geom,
		st_union(the_geom_webmercator) 	as the_geom_webmercator,
		project
	from
		capitalplanning.v2_20190510_project_bbl_advanced_find_view_ms
	group by
		project
	order by
		project
)

update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 
	the_geom = 		coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_pluto_geom = 	1
from dcp_project_bbls_zap_ms_consolidated b
where 
	a.project_id 				= b.project	and 
	a.project_id 				is not null	and
	b.the_geom 				is not null	and
	match_heip_geom 			is null		and
	match_dcp_2018_sca_inputs_share_geom 	is null		and
	match_nyzma_geom 			is null;




/********************************************************************************************
Merging in polygon data from Impact_Poly_Latest, last updated in April 2018.
**********************************************************************************************/

update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 
	the_geom = 			coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR =		coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_impact_poly_latest = 	1
from capitalplanning.Impact_Poly_Latest b
where 
	a.project_id 				= b.projectid 				and 
	a.project_id 							is not null 	and
	b.the_geom 								is not null 	and
	match_heip_geom 						is null 		and
	match_dcp_2018_sca_inputs_share_geom 	is null 		and
	match_nyzma_geom 						is null 		and
	match_pluto_geom 						is null;


/********************************************************************************************
The following query should be run in Carto Batch. It does the following.
Merge in PLUTO geometries to projects in a manually created lookup using BBLs.
Then merge in polygons to the ZAP project file using BBL. Because some Project IDs have multiple
BBLs, the first merge creates doubles. I then use st_union to aggregate these doubles.

26 projects matched using the following method.
**********************************************************************************************/

select
	project_id,
	count(*) 						as Match_Count,
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
		case when 	a.associated_bbl is not null 
							then a.associated_bbl = b.bbl
			 				else 	a.block is not null and
			 	  				a.block = b.block and
			 	  				concat(b.lot) in
									(
									trim(split_part(a.lot,',',1)),
									trim(split_part(a.lot,',',2)),
									trim(split_part(a.lot,',',3))
									) end
) as Lookup_Pluto_Merge
group by 
	project_id


update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 
	the_geom = 			coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=		coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_lookup_pluto_geom = 	1
from zap_project_missing_geom_lookup_1 b
where 
	a.project_id 								= b.project_id 	and 
	a.project_id 								is not null		and
	b.the_geom 									is not null		and
	match_heip_geom 							is null 		and
	match_dcp_2018_sca_inputs_share_geom 		is null 		and
	match_nyzma_geom 							is null 		and
	match_pluto_geom 							is null 		and
	match_impact_poly_latest 					is null;


							
/*****************************************************************
				DATA CORRECTION
*****************************************************************/									
											

/*The following step deletes the target cert date where it is 1/1/2022. This is a default system entry and does not represent
  actual inputs.*/

update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 	system_target_certification_date 	= null
where 	system_target_certification_date::date 	= '2022-01-01'						
											

/*The following step eliminates a false data entry where the input lists 
  the residential sqft figure as both in residential sqft and clearly 
  inaccurately in the voluntary_affordable_dwelling_units_non_mih.*/
update capitalplanning.dcp_zap_consolidated_20190510_ms a
set voluntary_affordable_dwelling_units_non_mih = null
where project_id = '2019K0093';


/*The following step adds a total units estimate for various
  state-wide projects and manually-included old neighborhood rezonings*/

/*Hudson Yards: Units taken from MQL's last June 2018 SCA Input areawide data.*/	
update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 	total_dwelling_units_in_project = 13508,
		new_dwelling_units		= 13508
where project_id = 'P2005M0053';

/*Western Rail Yards: Units taken from Table S-5 of the Western Rail Yards EIS.*/	
update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 	total_dwelling_units_in_project = 6074,
		new_dwelling_units				= 6074
where project_id = 'P2009M0294';


/*****************************************************************
		Adding Empire State Development Projects
*****************************************************************/																

							
with State_Developments_Geom as
(
	select
		b.the_geom 		as the_geom,
		a.project_name,
		a.total_units 	as new_dwelling_units,
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
		st_union(the_geom) 									as the_geom,
		project_name,
		new_dwelling_units,
		borough,
		count(case when bbl is not null then 1 end) 		as BBL_Count,
		count(case when the_geom is not null then 1 end) 	as Polygon_Count
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
	 capitalplanning.dcp_zap_consolidated_20190510_ms
	 	(the_geom, project_id, project_name, borough, new_dwelling_units)
	 select
	 	the_geom,
	 	concat(row_number() over(), ' [ESD Project]') as project_id,
	 	project_name,
	 	borough,
	 	new_dwelling_units
	 from
	 	State_Developments_Geom_1;


SELECT
	*
into
	dcp_project_flags_v2
FROM
(
	select 
		a.*,
		b.project_status 				as previous_project_status,
		b.process_stage 				as previous_process_stage,
		b.remaining_likely_to_be_built 	as remaining_likely_to_be_built,
		b.rationale 					as rationale,
		case when 
		(a.si_school_seat <> 'true' or a.si_school_seat is null) 								and
		upper(concat(a.project_description,' ',a.project_brief)) not like '%SCHOOL SEAT CERT%' 	and 
		upper(substring(a.project_name,1,3)) <> 'SS '
							then 1 ELSE 0 end as No_SI_Seat, 
							/*Potential exclusion if null. 
							A few instances in project brief where school seat certification is mentioned. 
							Also omitting 'SS ' from Project_Name.*/
		
		/*****************************************
		IDENTIFYING DEFINITE RESIDENTIAL PROJECTS
		BY CHECKING WHETHER ANY RESIDENTIAL
		UNIT COUNTS ARE LISTED OR RESIDENTIAL
		SQUARE FEET IS INCLUDED.
		******************************************/
		case when 
		(
			(	
				coalesce(a.total_dwelling_units_in_project,0) 	+ 
				coalesce(a.mih_dwelling_units_higher_number,0)	+ 
				coalesce(a.mih_dwelling_units_lower_number,0) 	+ 
				coalesce(a.new_dwelling_units,0)		+ 
				coalesce(a.voluntary_affordable_dwelling_units_non_mih,0)
			) > 0 													or
					
			(
				a.residential_sq_ft > 0 									and 
				/*Eliminating Parking application for large building (P2015M0047)*/
				upper(concat(a.project_description,a.project_brief)) not like '%APPLICATION FOR PARKING%') 	or
				/*Adding in Hudson Yards, Western Rail Yards, and 550 Washington*/							
				a.project_id in('P2005M0053','P2009M0294','P2014M0257') 		
			)													and
				/*Omitting applications for modifications to existing single-family homes*/
				upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING SINGLE-FAMILY%' 	and
				upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING ONE-FAMILY%'	and
				upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING 1-FAMILY%' 	and
				upper(concat(a.project_description,' ',a.project_brief)) not like '%EXISTING HOME%'
																THEN 1 else 0 
																END AS Dwelling_Units, 

		/*****************************************
		IDENTIFYING POTENTIAL PROJECTS BY CHECKING 
		WHETHER PROJECT DESCRIPTIONS INCLUDE TEXT 
		THAT INDICATES RESIDENTIAL DEVELOPMENT.
		******************************************/

	coalesce(
		/* +1 for text which indicates a potential residence*/
		CASE
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%AFFORDABLE%' 	then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%RESID%' 		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%RESIDENCE%' 	then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%APARTM%'		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%APT%' 		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%DWELL%' 		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%LIVING%'		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%HOUSI%' 		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%MIH%' 		then 1 
			when upper(concat(a.project_description,' ',a.project_brief)) 	like '%HOMES%' 		then 1  
			when (concat(a.project_description,' ',a.project_brief)) 	like '%DUs%'		then 1 
														END  - 
		/* -1 for text which indicates that the project is not residential, or simply a modification of a single-homes.*/
		CASE 	 
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENTIAL TO COMMERCIAL%' 	THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE-FAMILY%' 			THEN 1 
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SINGLE FAMILY%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1-FAMILY%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE FAMILY%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ONE-FAMILY%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%1 FAMILY%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FLOATING%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%TRANSITIONAL%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%FOSTER%' 				THEN 1
			-- WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%PARKING%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ILLUMIN%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%RESIDENCE DISTRICT%' 		THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%LANDMARKS PRESERVATION COMMISSION%' THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOME%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EXISTING HOUSE%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NUMBER OF BEDS%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%EATING AND DRINKING%' 		THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NO INCREASE%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ENLARGEMENT%' 			THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NON-RESIDENTIAL%' 		THEN 1
			WHEN upper(concat(a.project_description,' ',a.project_brief)) like  '%LIVINGSTON%' 			THEN 1 
			WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' 			THEN 1 
					  									ELSE 0
					  									END
		,0) 														AS Potential_Residential,

		/*Identifying senior housing projects*/
		CASE WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SENIOR%' THEN 1 ELSE 0 END 		AS SENIOR_HOUSING_flag,

		/*IDENTIFYING SUPPORTIVE HOUSING AND ASSISTED LIVING PROJECTS.*/
		CASE																											
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%NURSING%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AMBULATORY%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%MEDICAL%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%AIRS%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%A.I.R.S%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%CONTINUING CARE%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ASSISTED LIVING%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%ELDERLY%' THEN 1
		  WHEN upper(concat(a.project_description,' ',a.project_brief))  like '%SHELTER%' THEN 1 ELSE 0 END 		as Assisted_Living_Supportive_Housing_flag,

		case when a.process_stage_name_stage_id_process_stage = 'Initiation' then 1 else 0 end 				as Initiation_Flag, /*Potential exclusion if 1*/
		case when a.process_stage_name_stage_id_process_stage = 'Pre-Pas' then 1 else 0 end 				as Pre_PAS_Flag, /*Potential exclusion if 1*/
		case when date_part('year',cast(a.project_completed as date)) < 2012 or date_part('year',cast(a.certified_referred as date)) < 2012 then 1 else 0 end 
													      			as Historical_Project_Pre_2012, /*Assessing recency of the project. Potential exclusion if 1.*/ 
		case when date_part('year',cast(a.project_completed as date)) < 2008 or date_part('year',cast(a.certified_referred as date)) < 2008 then 1 else 0 end 
													      			as Historical_Project_Pre_2008, /*Assessing recency of the project. Potential exclusion if 1.*/ 
		abs(coalesce(total_dwelling_units_in_project,0) - coalesce(new_dwelling_units,0)) 				as Diff_Between_Total_and_New_Units /*Flag for future BO input.*/
	from
		capitalplanning.dcp_zap_consolidated_20190510_ms a
	left join
		capitalplanning.knownprojects_dcp_final b
	on
		a.project_id = b.project_id and 
		a.project_id is not null
) as DCP_Project_Flagging



SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2
from
(
	with relevant_dcp_projects as
	(
		select
			*,
			case 
			when
				project_id in 
						(
						'P2005M0053' /*Hudson Yards*/,
						'P2009M0294' /*Western Rail Yards*/,
						'P2014M0257' /*550 Washington St*/
						) then 1 else 0 end as Additional_Rezoning,
			
			/*Ensuring that additional Public Sites which have been manually found in ZAP are caught*/
			case 
				when project_id in
						(
					     	select 
				      			zap_project_id 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1	     
				when project_id in
						(
					     	select 
				      			zap_project_id_2 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1
				when project_id in
						(
					     	select 
				      			zap_project_id_3 
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1
				when project_id in
						(
					     	select 
				      			zap_project_id_4
				      		from 
				      			capitalplanning.table_190510_public_sites_ms_v3 
				     	) 
				     		then 1 else 0 end  as Public_Sites_Project,
		    case 
		    	when project_id like '%ESD%' then 1 else 0 end as State_Project
				
		from 
			dcp_project_flags_v2
		where
			(
				-- no_si_seat = 1 and 
				(
				dwelling_units = 1 										or 
				potential_residential = 1
				) 														and 
				historical_project_pre_2012 =0 							and
				project_status not in('Record Closed', 'Terminated') 	and
				project_status not like '%Withdrawn%' 					and
				applicant_type <> 'DCP' 								and
				project_id <> 'P2016Q0238'  							and /*Omitting DTFR rezoning from ZAP*/
				project_id <> 'P2016R0149'									/*Omitting BSC rezoning from ZAP*/
				-- and pre_pas_flag<>1 and
				-- initiation_flag<>1
			) or
			/*Including large rezonings*/
			project_id in (
							'P2005M0053' /*Hudson Yards*/,
						   	'P2009M0294' /*Western Rail Yards*/,
						   	'P2014M0257' /*550 Washington St*/
				      ) or
			/*Including projects from Public Sites*/
			project_id in
				     (select 
				      	zap_project_id 
				      from 
				      	capitalplanning.table_190510_public_sites_ms_v3 
				     )  or
			project_id like '%[ESD Project]%' /*State project*/
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
			dwelling_units=0 				and
			additional_rezoning = 0 		and
			public_sites_project = 0		and
			state_project = 0
	),

	/*Adding in manually collected data on projects flagged Potential_Residential with confirmation of whether residential, total units, and flag for further research*/

	relevant_dcp_projects_1 as
	(
		select 
				a.*,
				b.confirmed_potential_residential,
				-- coalesce(b.confirmed_potential_residential,c.confirmed_potential_residential),
				b.total_units_from_description as potential_residential_total_units,
				-- coalesce(b.total_units_from_description,c.units) as potential_residential_total_units,
				b.need_manual_research as need_manual_research_flag
	 	from 
	 		relevant_dcp_projects a
	 	left join
	 		capitalplanning.potential_residential_zap_project_check_ms b
	 	on 
	 		a.project_id = b.project_id
	 	-- left join
	 	-- 	capitalplanning.20190510_potential_residential_zap_project_check_ms_v2 c
	 	-- on
	 	-- 	a.project_id = c.project_id
	),

	/*ADD IN NEW POTENTIAL RESIDENTIAL LOOKUP HERE*/

	/*Limiting to projects which have confirmed dwelling units, or are the additional
	  large rezonings (HY, WRY, 550 Washington), Public Sites Projects, or State Projects*/
	relevant_dcp_projects_2 as
	(
		select
			*
		from
			relevant_dcp_projects_1
		where 
			dwelling_units = 1 or
			confirmed_potential_residential = 1 or
			additional_rezoning = 1 or
			public_sites_project=1 or
			state_project = 1
		order by
			project_id
	),

	relevant_dcp_projects_3 as
	(
		select
			row_number() over() as cartodb_id /*Important to generate CartoDB_ID for mapping purposes*/,
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
			dcp_target_certification_date,
			system_target_certification_date,
			coalesce(dcp_target_certification_date,system_target_certification_date) as target_certified_date,
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
			a.cartodb_id,
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
				st_intersects(a.the_geom,b.the_geom) 				or
				position(upper(b.project_name) in upper(a.project_name)) > 0 	or
				position(upper(a.project_name) in upper(b.project_name)) > 0
			) 									or
			(
				a.project_id = 'P2005M0053' and /*Matching Hudson Yards to DIB projects which are subsets of the total HY unit count*/ 
				b.project_name like '%DIB%'
			)									or
			(
				upper(a.project_name) like '%TWO BRIDGES%' and /*Matching Two Bridges parking and healthcare chaplaincy projects to their residential project counterparts. Will omit in next steps*/
				b.project_id in('P2012M0479','P2014M0022') and
				a.project_id <> b.project_id
			)

	),

		matching_projects_1 as
	(
		select
			*,
			case 
				when position('PERMIT RENEWAL' in upper(concat(project_name,project_description))) > 0 				then 'Permit renewal'
				
				/*Omitting selecting projects where total units are 1 due to small-project variance.*/
				when total_units = match_total_units and total_units <> 1 											then 'Same units'
				when 				
					left(
						upper(project_name),position('STREET' IN upper(project_name))-1
					) = 
					left(
						upper(matched_project_name),position('STREET' IN upper(matched_project_name))-1
					) and
					position('STREET' in upper(project_name)) > 0 and 
					position('STREET' in upper(matched_project_name)) >0											then 'Same project name' 
																		
				
				when
					upper(project_name) like '%TWO BRIDGES%' and 
					match_project_id in('P2012M0479','P2014M0022')													then 'Two Bridges duplicate'																									
				when
					project_id = 'P2005M0053' and matched_project_name like '%DIB%'									then 'HY DIB' end 	as Confirmed_Match_Reason_Automatic,
			null																														as Confirmed_Match_Reason_Manual
		from
			matching_projects
	),



	/*Export matching_projects_1 and review non-confirmed matches > 50 units. Apply same values in confirmed_match_reason field to matches which are manually identified.
	  Reupload this dataset as lookup_zap_overlapping_projects_ms and the updated ZAP pull (excluding projects which existed in the old ZAP pull) as lookup_zap_overlaps_ms_v2 .
	*/

		relevant_projects_4 as
	(
		select
			*
		from
			relevant_dcp_projects_3
		where
			project_id not in
							(
								select match_project_id 
								from lookup_zap_overlapping_projects_ms 
								where 
									confirmed_match_reason_automatic 	in('Permit renewal','Same units','Same project name','Two Bridges duplicate') or
									confirmed_match_reason_manual	    = 1 
							) and
			project_id not in
							(
								select match_project_id 
								from lookup_zap_overlapping_projects_ms_v2 
								where 
								--	confirmed_match_reason_automatic 	in('Permit renewal','Same units','Same project name','Two Bridges duplicate') or
									confirmed_match_reason_manual	    = 1 
							)
		order by project_id
	),

	/*Subtracting DIB units from HY, and adding in total units for ZAP projects in Public Sites which 
	  do not have listed units in ZAP.*/
		relevant_projects_5 as
	(
		SELECT
			a.*,
			case 
				when a.project_id = 'P2005M0053' then a.total_units - coalesce(b.DIB_Units,0) 
				else a.total_units end 									as total_units_1
		FROM
			relevant_projects_4 a
		left join
			(
				select sum(match_total_units) as DIB_Units from lookup_zap_overlapping_projects_ms where Confirmed_Match_Reason_Automatic = 'HY DIB' 
			) b
		on
			a.project_id = 'P2005M0053'
/*		left join
			capitalplanning.public_sites_190410_ms c
		on
			a.project_id = c.zap_project_id
*/	)

	/**********************************************************************************************************************************************************************
	Create new table based on following query titled "relevant_dcp_projects_housing_pipeline_ms." Project_ID P2005M0053 included to include Hudson Yards.
	Project_ID P2009M0294 included to include Western Rail Yards. Note that 16 observations are missing Total_Units. These must be collected from planners. 
	**********************************************************************************************************************************************************************/

	/*Comparing the ZAP Pull of 5/10/2019 to the ZAP Pull of January 2019. Note to MQL about comparison below:

	We have unit counts for all but one new project and geometries for all but 3 new projects.

	The new ZAP pull omits 23 projects which existed in the old ZAP Pull. 16 of these are due to application withdrawals/record closures. 5 are due to overlaps (which have only recently been factored into the script). The remaining two are non site-specific projects which previously had listed unit counts but no longer do.

	The new ZAP pull has 24 new projects (which is within reason -- in the old pull, we have 585 projects over 7 years of data). At first glance, some of these projects are relevant to neighborhood rezonings, a public sites RFP we've been looking for more information for, and there's a BSC PLACES application for 2,557 units (which I assume should be omitted because it's areawide).

	9 existing projects have had changed unit counts > 20 in the last half year. All the changes look within reason. 58 existing projects have had status changes (primarily changes from Active to either On-Hold or Complete).*/

		select
			a.*,
			case when b.project_id is not null then 1 else 0 end 	as In_Previous_ZAP,
			b.total_units 											as previous_zap_total_units,
			b.project_status 										as previous_zap_project_status
		from
			relevant_projects_5 a
		left join
			relevant_dcp_projects_housing_pipeline_ms b
		on
			a.project_id = b.project_id

) AS DCP_FINAL

/**********************RUN IN REGULAR CARTO**************************/


select cdb_cartodbfytable('capitalplanning', 'relevant_dcp_projects_housing_pipeline_ms_v2')



/*********************MODIFYING THESE PROJECTS WITH PLANNER INPUTS****************/

/*Compile planner inputs*/

select
	*
into
	planner_inputs_consolidated_ms
from
(
	select
		boro,
		cd,
		map_id, /*Manually convert this field to numeric*/
		source,
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.bronx_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id, /*Manually convert this field to int*/
		source,		
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.brooklyn_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units_from_planner,
		notes_on_total_ks_assumed_units,
		ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.manhattan_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units as total_units_from_planner,
		notes_on_total_ks_assumed_units,
		null as ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.staten_island_planner_inputs_housing_pipeline
	union
	select
		boro,
		cd,
		map_id,
		source,
		project_id,
		project_name,
		status,
		total_units as total_units_from_planner,
		notes_on_total_ks_assumed_units,
		units_ks as ks_assumed_units,
		units_remaining_not_accounted_for_in_other_sources,
		lead_planner
		/*Manually convert all of the following fields to numeric in Carto*/
		,outdated_overlapping_project,
		non_residential_project_incl_group_quarters,
		withdrawn_project,
		inactive_project,
		other_reason_to_omit,
		corrected_existing_geometry,
		corrected_existing_unit_count,
		updated_unit_count,
		should_be_in_old_zap_pull,
		should_be_in_new_zap_pull,
		planner_added_project
	from 
		capitalplanning.queens_planner_inputs_housing_pipeline
) as planner_inputs


/**********************RUN IN REGULAR CARTO**************************/


select cdb_cartodbfytable('capitalplanning', 'planner_inputs_consolidated_ms')


/*Join the planner inputs to the mapped developments. Then do the intersect and delete those projects which are based on the flag*/

select
	*
into
	mapped_planner_inputs_consolidated_inputs_ms
from
(
	select
			row_number() over() as cartodb_id,
			a.the_geom,
			a.the_geom_webmercator,
			boro,
			cd,
			map_id, /*Manually convert this field to numeric*/
			b.source,
			project_id,
			project_name,
			status,
			total_units_from_planner,
			notes_on_total_ks_assumed_units,
			ks_assumed_units,
			units_remaining_not_accounted_for_in_other_sources,
			lead_planner
			/*Manually convert all of the following fields to numeric in Carto*/
			,outdated_overlapping_project,
			non_residential_project_incl_group_quarters,
			withdrawn_project,
			inactive_project,
			other_reason_to_omit,
			corrected_existing_geometry,
			corrected_existing_unit_count,
			updated_unit_count,
			should_be_in_old_zap_pull,
			should_be_in_new_zap_pull,
			planner_added_project,
			a.objectid,
			a.shape_length,
			a.shape_area,
			a.area_sqft
	from
		planner_inputs_consolidated_ms b
	left join
		added_development_sites_20190510_ms a
	on
		a.mapid = b.map_id	
) as mapped_planner_inputs_consolidated_inputs_ms


/**********************RUN IN REGULAR CARTO**************************/

select cdb_cartodbfytable('capitalplanning', 'mapped_planner_inputs_consolidated_inputs_ms')


SELECT
	a.* 
	,b.map_id /*Manually convert this field to numeric*/
	,b.source
	,b.project_id as planner_project_id
	,b.project_name as planner_project_name
	,b.status as planner_status
	,total_units_from_planner
	,notes_on_total_ks_assumed_units
	,case 
		when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
		else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end as ks_assumed_units
	,units_remaining_not_accounted_for_in_other_sources
	,lead_planner
	/*Manually convert all of the following fields to numeric in Carto*/
	,b.outdated_overlapping_project
	,non_residential_project_incl_group_quarters
	,withdrawn_project
	,inactive_project
	,other_reason_to_omit
	,corrected_existing_geometry
	,corrected_existing_unit_count
	,updated_unit_count
	,should_be_in_old_zap_pull
	,should_be_in_new_zap_pull
	,planner_added_project
from
	relevant_dcp_projects_housing_pipeline_ms_v2 a
inner join
	mapped_planner_inputs_consolidated_inputs_ms b
on
	st_intersects(a.the_geom,b.the_geom)
where
	outdated_overlapping_project = 1 or
	non_residential_project_incl_group_quarters = 1 or
	withdrawn_project = 1 or
	inactive_project = 1 or
	other_reason_to_omit = 1




/*Removing projects which planners indicated should be omitted due to overlaps, non-residential, withdrawals, inactivity, or otherwise*/

SELECT
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v2_1
from
(
	SELECT
		*,
		CASE 
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and outdated_overlapping_project 					= 1) then 'Planner Noted Overlap'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and non_residential_project_incl_group_quarters 	= 1) then 'Planner Noted Non-Residential'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and withdrawn_project 							= 1) then 'Planner Noted Withdrawn'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and inactive_project 								= 1) then 'Planner Noted Inactive'
			when project_id in(select project_id from capitalplanning.mapped_planner_inputs_consolidated_inputs_ms where source in('ZAP','DCP','DCP ZAP') and other_reason_to_omit 							= 1) then 'Planner Noted Other Reason to Omit'
			else null end as Planner_Noted_Omission
	from
		relevant_dcp_projects_housing_pipeline_ms_v2
) relevant_dcp_projects_housing_pipeline_ms_v2_1


/*Replace ZAP unit count and ZAP geom, where appropriate, with planner input. There are 60 planner inputs on unit count and all are within reason. Hudson Yards also has a planner input of 13,508 (EAS) joined on,
  but the unit count in relevant_dcp_projects_housing_pipeline_ms_v2_1 reflects HY after DIB deductions, so we are omitting this match. There are 11 location corrections and 1 location addition as well.*/


/*THIS IS WHERE YOU SHOULD UPDATE THE UNIT COUNT AND THE GEOMETRY. THEN YOU'RE DONE!!!!!!*/

SELECT
	a.*,
	case 
		when a.project_id <> 'P2005M0053' /*HY*/ then coalesce(
																	b.updated_unit_count,
																	b.total_units_from_planner,
																	a.total_units_1,
																	case 
																		when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
																		else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end
																) 
		else a.total_units_1 end 																														as total_units_2
	,b.map_id /*Manually convert this field to numeric*/
	,b.the_geom as planner_geom
	,b.source
	,b.project_id as planner_project_id
	,b.project_name as planner_project_name
	,b.status as planner_status
	,total_units_from_planner
	,notes_on_total_ks_assumed_units
	,case 
		when length(ks_assumed_units)<2 or position('units' in ks_assumed_units)<1 then null
		else substring(ks_assumed_units,1,position('units' in ks_assumed_units)-1)::numeric end as ks_assumed_units
	,units_remaining_not_accounted_for_in_other_sources
	,lead_planner
	/*Manually convert all of the following fields to numeric in Carto*/
	,b.outdated_overlapping_project
	,non_residential_project_incl_group_quarters
	,withdrawn_project
	,inactive_project
	,other_reason_to_omit
	,corrected_existing_geometry
	,corrected_existing_unit_count
	,updated_unit_count
	,should_be_in_old_zap_pull
	,should_be_in_new_zap_pull
	,planner_added_project
from
	relevant_dcp_projects_housing_pipeline_ms_v2_1 a
inner join
	mapped_planner_inputs_consolidated_inputs_ms b
on
	a.project_id = b.project_id


-- where b.project_id is not null and a.total_units_1 <> b.total_units_from_planner







SELECT




select
	*
into
	relevant_dcp_projects_housing_pipeline_ms_v3
from
	(
		select
	)