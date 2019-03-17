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
1. Append relevant BBLs and expected units from Jerome, East Harlem, ENY, DTFR, Inwood, and BSC projects
2. Geocode using MAPPLUTO
3. Join associated site values by BBL 
*************************************************************/


ALTER TABLE capitalplanning.dep_ndf_polygon_matching_ms
ADD COLUMN PROJECT_ID TEXT, 
ADD COLUMN NEIGHBORHOOD TEXT,
ADD COLUMN STATUS TEXT,
ADD COLUMN UNITS NUMERIC,
ADD COLUMN BBL TEXT,
ADD COLUMN SITE TEXT,
DROP COLUMN NAME,
DROP COLUMN DESCRIPTION;

INSERT INTO capitalplanning.dep_ndf_polygon_matching_ms
(
		NEIGHBORHOOD,
		SITE,
		STATUS,
		UNITS,
		BBL
)
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
SELECT

		'EAST NEW YORK' as NEIGHBORHOOD,	
		null AS SITE,
		e.STATUS,
		e.UNITS as UNITS,
		cast(e.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_east_new_york e
UNION
SELECT

		'BAY ST CORRIDOR' as NEIGHBORHOOD,	
		null AS SITE,
		f.STATUS,
		case when f.UNITS is null then null 
				else cast(f.UNITS as NUMERIC) end as UNITS,
		cast(f.BBL as TEXT) as BBL

FROM
		capitalplanning.dep_rwcds_bay_st_corridor f
/***********************************************/
UNION
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
			    
/*
Omitting City Priority sites from RWCDS models and ghost rows pulled in from raw data.
City priority sites are replaced by Rezoning Commitment sites.
*/
DELETE from capitalplanning.dep_ndf_polygon_matching_ms 
where status in('','City Priority');

/*Omitting projected/potential projects if they are included in list of rezoning commitments.*/
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


delete from capitalplanning.dep_ndf_polygon_matching_ms a
using replacing_projected_with_commitment b
where a.bbl = b.bbl and b.commitment_match = 1 and a.status <> 'Rezoning Commitment'

/*Joining in polygon data from PLUTO*/
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
on 
	a.bbl = cast(b.bbl as TEXT) and 
	a.bbl is not null
)

UPDATE capitalplanning.dep_ndf_polygon_matching_ms
SET 
	the_geom = geom_match.the_geom,
	the_geom_webmercator = geom_match.the_geom_webmercator
FROM 
	geom_match
WHERE dep_ndf_polygon_matching_ms.bbl = geom_match.bbl and geom_match.bbl is not null;

UPDATE capitalplanning.dep_ndf_polygon_matching_ms a
SET
	Site = b.Site
from
	capitalplanning.list_of_bbls_by_development_site_2019_01_16 b
where 	
	a.bbl = b.bbl and 
	a.bbl is not null;

/*Workaround for 15 BBLs which are not current and do not have BBLs. Assigning them a random polygon from another BBL on their site.
List of relevant projects will be by site--therefore this reassignment provides no inaccurate information. However, there may be
missing geographic data from some site projects.*/
with distinct_site_geoms as
(
	select distinct site, the_geom
	from capitalplanning.dep_ndf_polygon_matching_ms
	where the_geom is not null
)

UPDATE capitalplanning.dep_ndf_polygon_matching_ms a
set the_geom = coalesce(a.the_geom,b.the_geom)
from distinct_site_geoms b
where 	a.the_geom is null and 
	 	a.site = b.site and 
	 	a.site is not null

/*Adding in project identifiers*/
update capitalplanning.dep_ndf_polygon_matching_ms
set Project_ID = cartodb_id


/*	Create Site-based polygons and create a dataset titled dep_ndf_by_site with the following query: */

select
	trim(concat(site,' ',neighborhood,' ',upper(status))) as Project_ID,
	site,
	neighborhood,
	st_union(the_geom) as the_geom, 
	st_union(the_geom_webmercator) as the_geom_webmercator,
	sum(coalesce(units,0)) as Units,
	array_to_string(array_agg(bbl),', ') as included_bbls
from
	capitalplanning.dep_ndf_polygon_matching_ms
group by
	trim(concat(site,' ',neighborhood,' ',upper(status))),
	site,
	neighborhood
order by
	neighborhood,
	project_id

	


/*******************************CHECKING QUERY**************************************************************************************************************************** 
15/1,047 BBLs are not geocoded after joining MAPPLUTO. They are then geocoded using a random polygon from other another BBL in their site.

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
