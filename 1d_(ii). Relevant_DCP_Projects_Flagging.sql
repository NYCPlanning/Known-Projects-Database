/**************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Flagging DCP Project Characteristics
***************************************************************************************************************************************************************************************/
/**************************************************************************************************************************************************************************************
METHODOLOGY:
1. Create flags for residential, potential residential, SI seat cert, initation, Pre-PAS, senior housing, and 
   projects to exclude from housing pipeline.
***************************************************************************************************************************************************************************************/
/*************************************************************************************
RUN THIS SCRIPT IN CARTO BATCH
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
		b.remaining_likely_to_be_built 			as remaining_likely_to_be_built,
		b.rationale 					as rationale,
		case when 
		(a.si_school_seat <> 'true' or a.si_school_seat is null) 				and
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
		abs(coalesce(total_dwelling_units_in_project,0) - coalesce(new_dwelling_units,0)) 				as Diff_Between_Total_and_New_Units, /*Flag for future BO input.*/
	from
		capitalplanning.dcp_zap_consolidated_ms a
	left join
		capitalplanning.knownprojects_dcp_final b
	on
		a.project_id = b.project_id and 
		a.project_id is not null
) as DCP_Project_Flagging

/***********************************************************SUPERSEDED*****************************************************/
		    
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

/*End of text-catching diagnostic script
******************************************************************************************************************************************/
