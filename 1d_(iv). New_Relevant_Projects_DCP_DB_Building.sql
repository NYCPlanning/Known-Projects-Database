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

select cdb_cartodbfytable('capitalplanning', 'dcp_zap_consolidated_20190510_ms')


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
set the_geom 				=	coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR 	=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_nyzma_geom 		= 	1
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
	the_geom 				= b.the_geom,
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
	the_geom = 				coalesce(a.the_geom,b.the_geom),
	THE_GEOM_WEBMERCATOR=	coalesce(a.THE_GEOM_WEBMERCATOR,b.THE_GEOM_WEBMERCATOR),
	match_pluto_geom = 	1
from dcp_project_bbls_zap_ms_consolidated b
where 
	a.project_id = b.project							and 
	a.project_id 							is not null	and
	b.the_geom 								is not null	and
	match_heip_geom 						is null		and
	match_dcp_2018_sca_inputs_share_geom 	is null		and
	match_nyzma_geom 						is null;




/********************************************************************************************
Merging in polygon data from Impact_Poly_Latest, last updated in April 2018.
**********************************************************************************************/

update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 
	the_geom = 					coalesce(a.the_geom,b.the_geom),
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

drop table if exists zap_project_missing_geom_lookup_20190609;

select
	project_id,
	count(*) 						as Match_Count,
	st_union(the_geom) 				as the_geom,
	st_union(the_geom_webmercator) 	as the_geom_webmercator
into
	zap_project_missing_geom_lookup_20190609
from
(
	select
		a.project_id,
		b.the_geom				as the_geom,	
		b.THE_GEOM_WEBMERCATOR 	as the_geom_webmercator
	from
		capitalplanning.zap_project_missing_geom_lookup a
	left join
		capitalplanning.mappluto_v_18v1_1 b
	on
		case when 			a.associated_bbl is not null 
							then a.associated_bbl = b.bbl
			 else 
		 					a.block is not null and
		 					(
		 						(a.project_borough 	= 'Brooklyn' 		and b.borough = 'BK') or
		 						(a.project_borough 	= 'Queens' 			and b.borough = 'QN') or
		 						(a.project_borough 	= 'Bronx' 			and b.borough = 'BX') or
		 						(a.project_borough 	= 'Staten Island'	and b.borough = 'SI') or
		 						(a.project_borough  = 'Manhattan'		and b.borough = 'MN')
		 					)					and
		 	  				a.block = b.block 	and
		 	  				concat(b.lot) in
								(
								trim(split_part(a.lot,',',1)),
								trim(split_part(a.lot,',',2)),
								trim(split_part(a.lot,',',3))
								) end
	union all
	select
		a.project_id,
		b.the_geom as the_geom,
		b.the_geom_webmercator
	from
		(
			select
				*
			from
				table_20190510_potential_residential_project_check_ms_v2 
			where
				bbl <> '' /*Adding in additional geometries provided by KS in HEIP on 6/8/2019*/
		) a
	left join
		capitalplanning.mappluto_v_18v1_1 b
	on
		(a.bbl = concat(b.bbl)) or position(concat(b.bbl) in a.bbl) > 0

) as Lookup_Pluto_Merge
group by 
	project_id;


update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 
	the_geom = 					coalesce(b.the_geom,a.the_geom),
	THE_GEOM_WEBMERCATOR=		coalesce(b.THE_GEOM_WEBMERCATOR,a.THE_GEOM_WEBMERCATOR),
	match_lookup_pluto_geom = 	1
from zap_project_missing_geom_lookup_20190609 b
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

/*Pfizer Sites: Units taken from EIS.*/	
update capitalplanning.dcp_zap_consolidated_20190510_ms a
set 	total_dwelling_units_in_project = 1146,
		new_dwelling_units				= 1146
where project_id = 'P2013K0309';



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



