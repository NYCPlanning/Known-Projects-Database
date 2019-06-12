/************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Creating DEP NDF Dataset
START DATE: 1/4/2018
COMPLETION DATE:
Source Files/Folders: 
1. G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\DEP NDF
2. https://nycplanning.carto.com/u/capitalplanning/tables/mappluto_v_18v1_1
3. G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF
*************************************************************/
/************************************************************
METHODOLOGY:
1. Append relevant BBLs and expected units from DEP NDF projects
2. Geocode using MAPPLUTO
2. Join associated site values by BBL 
*************************************************************/

/**********************************RUN IN REGULAR CARTO*****************************/

ALTER TABLE capitalplanning.dep_ndf_polygon_matching_ms
ADD COLUMN PROJECT_ID TEXT, 
ADD COLUMN NEIGHBORHOOD TEXT,
ADD COLUMN STATUS TEXT,
ADD COLUMN UNITS NUMERIC,
ADD COLUMN BBL TEXT,
ADD COLUMN SITE TEXT,
DROP COLUMN NAME,
DROP COLUMN DESCRIPTION;

delete from capitalplanning.dep_ndf_polygon_matching_ms
where neighborhood is not null;

INSERT INTO capitalplanning.dep_ndf_polygon_matching_ms
(
		NEIGHBORHOOD,
		SITE,
		STATUS,
		UNITS,
		BBL
)

/*Add Jerome Neighborhood Study*/
SELECT

		'JEROME' as NEIGHBORHOOD,		
		null AS SITE,
		a.STATUS as STATUS,
		case when a.UNITS is null then null 
				else cast(a.UNITS as NUMERIC) end as UNITS,
		cast(a.BBL as TEXT) as BBL
 
FROM 
		capitalplanning.dep_rwcds_jerome a
UNION
/*Add DTFR Neighborhood Study*/
SELECT

		'DOWNTOWN FAR ROCKAWAY'	as NEIGHBORHOOD,	
		null AS SITE,
		b.PROJECTED_POTENTIAL AS STATUS,
		case when b.UNITS is null then null 
				else cast(b.UNITS as NUMERIC) end as UNITS,
		cast(b.BBL as TEXT) as BBL
	
FROM
		capitalplanning.dep_rwcds_far_rockaway b
UNION
/*Add East Harlem Neighborhood Study*/
SELECT

		'EAST HARLEM' as NEIGHBORHOOD,	
		null AS SITE,
		c.STATUS,
		case when c.UNITS is null then null 
				else cast(c.UNITS as NUMERIC) end as UNITS,
		cast(c.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_east_harlem c
UNION

/*Add Inwood Neighborhood Study*/
SELECT

		'INWOOD' as NEIGHBORHOOD,	
		null AS SITE,
		d.STATUS,
		case when d.UNITS is null then null 
				else cast(d.UNITS as NUMERIC) end as UNITS,
		cast(d.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_inwood d
UNION

/*Add ENY Neighborhood Study*/
SELECT

		'EAST NEW YORK' as NEIGHBORHOOD,	
		null AS SITE,
		e.STATUS,
		e.UNITS as UNITS,
		cast(e.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_east_new_york e
UNION

/*Add BSC Neighborhood Study*/
SELECT

		'BAY STREET CORRIDOR' as NEIGHBORHOOD,	
		null AS SITE,
		f.STATUS,
		case when f.UNITS is null then null 
				else cast(f.UNITS as NUMERIC) end as UNITS,
		cast(f.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_bay_st_corridor f
/***********************************************/
UNION

/*Add manually collected rezoning commitment sites.*/
SELECT		
		g.NEIGHBORHOOD_STUDY AS NEIGHBORHOOD,
		g.commitment_site as site,
		'Rezoning Commitment' AS STATUS,
		null as units,
		trim(concat(g.bbl)) as BBL
from
		capitalplanning.neighborhood_study_rezoning_commitments_ms g



/*Resolving a typographical error in the raw data*/
UPDATE capitalplanning.dep_ndf_polygon_matching_ms
set bbl = '1014255988'
where bbl = '10142.55989';

/*Omitting potentially inaccurate City Priority sites from RWCDS models and ghost rows pulled in from raw data*/
DELETE from capitalplanning.dep_ndf_polygon_matching_ms where status in('','City Priority');

/*Identifying various 'Projected' sites as actual Rezoning Commitments, based on matching BBLs.*/			    
with replacing_projected_with_commitment as
(
	SELECT
		a.bbl, case when b.bbl is not null then 1 end as Commitment_Match
	from
		capitalplanning.dep_ndf_polygon_matching_ms a
	left join
		capitalplanning.dep_ndf_polygon_matching_ms b
	on
		a.bbl 	= b.bbl and
		b.status=  'Rezoning Commitment'
	where
		a.status <> 'Rezoning Commitment' 
)

/*Deleting sites identified as above, as they are included in the Rezoning Commitments list*/
delete from capitalplanning.dep_ndf_polygon_matching_ms a
using replacing_projected_with_commitment b
where a.bbl = b.bbl and b.commitment_match = 1 and a.status <> 'Rezoning Commitment'


			    
/*************************
	GEOCODING
*************************/
with geom_match as
(
SELECT 
	a.cartodb_id,
    b.the_geom,
	b.the_geom_webmercator,
    a.bbl,
    a.units,
    a.status,
    a.neighborhood
FROM 
	capitalplanning.dep_ndf_polygon_matching_ms a
LEFT JOIN
	capitalplanning.mappluto_v_18v1_1 b
on a.bbl = cast(b.bbl as TEXT) and a.bbl is not null
)

UPDATE capitalplanning.dep_ndf_polygon_matching_ms
SET 
	the_geom 				= geom_match.the_geom,
	the_geom_webmercator 	= geom_match.the_geom_webmercator
FROM 
	geom_match
WHERE 
	dep_ndf_polygon_matching_ms.bbl = geom_match.bbl and 
	geom_match.bbl is not null;

UPDATE capitalplanning.dep_ndf_polygon_matching_ms a
SET
	Site = coalesce(a.site,b.Site)
from
	capitalplanning.list_of_bbls_by_development_site_2019_01_16 b
where (
		a.bbl = b.bbl and 
	  	a.bbl is not null
	  ) or
	  (
		a.bbl = '1014255988' and b.bbl = '10142.5598889' /*Addressing typographical error for BBL 10142.55989*/ 
	  );

/*Workaround for 15 BBLs which are not current and do not have BBLs. Assigning them a random polygon from another BBL on their site.*/
with distinct_site_geoms as
(
	select distinct site, neighborhood, the_geom
	from capitalplanning.dep_ndf_polygon_matching_ms
	where the_geom is not null
)

UPDATE capitalplanning.dep_ndf_polygon_matching_ms a
set the_geom = coalesce(a.the_geom,b.the_geom)
from distinct_site_geoms b
where 	a.the_geom is null and 
	 	a.site = b.site and 
	 	a.neighborhood = b.neighborhood and
	 	a.site is not null

/*Adding in project identifiers*/
update capitalplanning.dep_ndf_polygon_matching_ms
set Project_ID = cartodb_id


/*******************************************RUN IN CARTO BATCH******************************************/

/*Create Site-based polygons, create a dataset titled dep_ndf_by_site_pre with the following query: */
SELECT
	*
INTO
	dep_ndf_by_site_pre
FROM
(
	select
		trim(concat(site,' ',neighborhood,' ',upper(status))) as Project_ID,
		site,
		neighborhood,
		status,
		st_union(the_geom) as the_geom, 
		st_union(the_geom_webmercator) as the_geom_webmercator,
		sum(coalesce(units,0)) as Units,
		array_to_string(array_agg(bbl),', ') as included_bbls
	from
		capitalplanning.dep_ndf_polygon_matching_ms
	group by
		trim(concat(site,' ',neighborhood,' ',upper(status))),
		site,
		neighborhood,
		status
	order by
		trim(concat(site,' ',neighborhood,' ',upper(status))),
		neighborhood,
		site,
		status

) x
	

/*Integrating planner input on unit count and KS assumed unit calculations for where no unit count exists*/

drop table if exists dep_ndf_by_site_pre_1;
select
	*
INTO
	dep_ndf_by_site_pre_1
from
(
	SELECT	
		a.project_id,
		a.site as project_name,
		/*Add a status field*/
		a.neighborhood,
		a.status,
		a.the_geom,
		a.the_geom_webmercator
		,
		CASE 
			when A.PROJECT_ID LIKE '%REZONING COMMITMENT%' then	coalesce
																	(
																		case 
																			when a.project_id = 'Phipps House EAST NEW YORK REZONING COMMITMENT'											then 900
																			/*Inserting information from https://council.nyc.gov/land-use/wp-content/uploads/sites/53/2016/05/East-New-York-plan-summary.pdf*/
																			when a.project_id = '(Projected RFP) DSNY 123rd Street Parking Lot (Site 3) EAST HARLEM REZONING COMMITMENT' 	then 115 
																			/*Inserting information from planner Joseph Huennekens. See email at the following link:
																			"G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF\RE East Harlem Rezoning Commitment.msg"
																			*/
																			when a.project_id = '54 Central Avenue BAY STREET CORRIDOR REZONING COMMITMENT' 								then 64 
																			/*Inserting information from planner Joseph Helferty. See email at the following link:
																			"G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF\RE BSC housing commitment sites.msg"
																			*/
																			when a.project_id = 'Christopher-Glenmore EAST NEW YORK REZONING COMMITMENT' 									then 230 /*Inserting information from HPD RFP ID 21*/
																			when a.project_id = 'Beach 21st Street DOWNTOWN FAR ROCKAWAY REZONING COMMITMENT' 								then 224 /*Inserting information from HPD RFP ID 11*/
																			when a.project_id = 'Inwood Library INWOOD REZONING COMMITMENT' 												then 175 END/*Inserting information from HPD RFP ID 14*/,
																		c.total_units_ms,
																		b.total_units_from_planner, 
																		case 
																			when length(b.ks_assumed_units)<2 or position('units' in b.ks_assumed_units)<1 then null
																			else replace(substring(b.ks_assumed_units,1,position('units' in b.ks_assumed_units)-1),', ','')::numeric end
																	)
			else a.units end as units,
		a.included_bbls,
		coalesce(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input) as planner_input,
		coalesce(
					/*Inserting information from planner Joseph hELFERY. See email at the following link:
					"G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF\RE BSC housing commitment sites.msg"
					*/
					case when a.project_id = '54 Central Avenue BAY STREET CORRIDOR REZONING COMMITMENT' then .5 end,
					c.portion_built_2025,
					b.portion_built_2025,
					case when a.status = 'Rezoning Commitment' then 1 end
				) as portion_built_2025,
		coalesce(
					/*Inserting information from planner Joseph Helferty. See email at the following link:
					"G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF\RE BSC housing commitment sites.msg"
					*/
					case when a.project_id = '54 Central Avenue BAY STREET CORRIDOR REZONING COMMITMENT' then .5 end,
					/*Inserting information from planner Joseph Huennekens. See email at the following link:
					"G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Working Data\DEP NDF\RE East Harlem Rezoning Commitment.msg"
					*/
					case when a.project_id = '(Projected RFP) DSNY 123rd Street Parking Lot (Site 3) EAST HARLEM REZONING COMMITMENT' 	then 1 end,
					c.portion_built_2035,
					b.portion_built_2035,
					case when a.status = 'Rezoning Commitment' then 0 end
				) as portion_built_2035,
		coalesce(
				c.portion_built_2055,
				b.portion_built_2055,
				0
				) 
		as portion_built_2055,


		/*Identifying NYCHA Projects*/
		CASE 
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%NYCHA%' THEN 1   		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%BTP%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%HOUSING AUTHORITY%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%NEXT GEN%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%NEXT-GEN%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%NEXTGEN%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%BUILD TO PRESERVE%' THEN 1 ELSE 0 END 		AS NYCHA_Flag,

		CASE 
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%CORRECTIONAL%' THEN 1   		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%NURSING%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '% MENTAL%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%DORMITOR%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%MILITARY%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%GROUP HOME%' THEN 1  		
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%BARRACK%' THEN 1 ELSE 0 END 		AS GQ_fLAG,

		/*Identifying definite senior housing projects*/
		CASE 
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%SENIOR%' THEN 1
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%ELDERLY%' THEN 1 	
			WHEN concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input)  			like '% AIRS%' THEN 1
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%A.I.R.S%' THEN 1 
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%CONTINUING CARE%' THEN 1
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%NURSING%' THEN 1
			WHEN concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input)  			like '% SARA%' THEN 1
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  	like '%S.A.R.A%' THEN 1 else 0 end as Senior_Housing_Flag,
		CASE
			WHEN upper(concat(c.rationale_for_assignments_for_likelihood_to_be_built,b.planner_input))  like '%ASSISTED LIVING%' THEN 1 else 0 end as Assisted_Living_Flag

	from
		dep_ndf_by_site_pre a
	left join
		mapped_planner_inputs_consolidated_inputs_ms b
	on
		a.project_id = b.project_id or
		/*Performing manual matches based on planner inputs not labeled as the appropriate rezoning commitment*/
		(a.project_id = 'Phipps House EAST NEW YORK REZONING COMMITMENT' 		and b.project_id = '67 EAST NEW YORK REZONING COMMITMENT') or
		(a.project_id = 'Dinsmore - Chestnut EAST NEW YORK REZONING COMMITMENT'	and b.project_id = '66 EAST NEW YORK REZONING COMMITMENT') or
		(a.project_id = 'Jersey Street Garage BAY STREET CORRIDOR REZONING COMMITMENT' and b.project_id = 'Current 6')					
	left join
		table_190401_dtfr_and_inwood_commitment_sites_edc_input_v1_jd_m c
	on
		POSITION(UPPER(C.COMMITMENT_SITE) IN UPPER(A.PROJECT_ID)) > 0

) x 

drop table if exists dep_ndf_by_site;
select
	*
INTO
	dep_ndf_by_site
from
(
	SELECT
		ROW_NUMBER() OVER() AS cartodb_id,
		concat(initcap(a.neighborhood),' ', initcap(a.status),' ', row_number() over(partition by a.neighborhood, a.status )) 	as project_id,
		case
			when a.status = 'Rezoning Commitment' then a.project_name end 										as project_name,
		initcap(a.neighborhood) as neighborhood,
		case 
			when upper(neighborhood) in('BAY STREET CORRIDOR') 					then 'Staten Island'
			when upper(neighborhood) in('EAST NEW YORK') 						then 'Brooklyn'
			when upper(neighborhood) in('JEROME') 								then 'Bronx'
			when upper(neighborhood) in('DOWNTOWN FAR ROCKAWAY')				then 'Queens'
			when upper(neighborhood) in('INWOOD','EAST HARLEM') 				then 'Manhattan' 				end as borough,
		initcap(a.status) as status,
		a.the_geom,
		a.the_geom_webmercator,
		a.units,
		a.included_bbls,
		planner_input,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		nycha_flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag
	from
		dep_ndf_by_site_pre_1 a
	where
		not(status = 'Rezoning Commitment' and units is null) /*Omitting one rezoning cmomitment, 130 West 182nd St, which is unidentifiable*/
) x


/************************RUN IN REGULAR CARTO*********************/

select cdb_cartodbfytable('capitalplanning', 'dep_ndf_by_site')


/*******************************CHECKING QUERY**************************************************************************************************************************** 
15/1,047 sites are not geocoded after joining MAPPLUTO. They are then geocoded using a random polygon from other another BBL in their site.

Queries:
SELECT * FROM capitalplanning.dep_ndf_polygon_matching_ms where the_geom is null 
SELECT * FROM capitalplanning.dep_ndf_polygon_matching_ms where the_geom is not null 
*************************************************************************************************************************************************************************/

/*******************************CHECKING QUERY**************************************************************************************************************************** 
Shows that there are no site matches for 1 non-City Priority BBL: 

1. 2028580028 from Jerome (raw data does not provide a site designation for this BBL.)

Query:
SELECT * FROM capitalplanning.dep_ndf_polygon_matching_ms where site is null and status not like '%Priority%' and bbl is not null 
*************************************************************************************************************************************************************************/

/*******************************CHECKING QUERY**************************************************************************************************************************** 
Performed topline check comparing to RWCDS raw data using following query, comparing count and unit sums

select neighborhood, count(*) as observations, sum(units) as units from capitalplanning.dep_ndf_polygon_matching_ms group by neighborhood
*************************************************************************************************************************************************************************/

select
	*
into
	neighborhood_studies_inputs_share_20190522
from
(
	select
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		units as total_units
	from
		dep_ndf_by_site
	where
		status = 'Rezoning Commitment'
)	neighborhood_studies_inputs_share_20190522
	order by
		project_id asc


select cdb_cartodbfytable('capitalplanning', 'neighborhood_studies_inputs_share_20190522')


