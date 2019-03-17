/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Prepare EDC dataset for joining
START DATE: 1/17/2019
COMPLETION DATE: 1/17/2019
Source file: "G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\Jan 2019 SCA Housing Pipeline\Raw Data\EDC\edc_2018_sca_input.csv"
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY:
1. Remove projects with 0 total units.
2. Reorganize fields.
*************************************************************************************************************************************************************************************/

select
	*
into
	edc_2018_sca_input_1_limited
from(
with edc_2018_sca_input_1_limited as 
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		geom_source,
		edc_id as edc_project_id,
		dcp_project_id,
		project_name,
		project_description,
		comments_on_phasing,
		case when edc_id in(1,3) then 2025 else build_year end as build_year, /*R. Holbrook from EDC identifying Bedford-Union and Spofford as 2025 build years*/
		borough,
		total_units,
		senior_units,
		null as bbl,
		null as borough_code,
		null as block,
		null as lot
	from capitalplanning.edc_2018_sca_input_1
	union all
	select
		cartodb_id,
		null as the_geom,
		null as the_geom_webmercator,
		null as geom_source,
		edc_id as edc_project_id,
		null as dcp_project_id,
		project_name,
		null as project_description,
		comments_on_phasing,
		build_year,
		'Brooklyn' as borough,
		total_units,
		null as senior_units,
		bbl,
		borough_code,
		block,
		lot
	from 
		capitalplanning.edc_2018_sca_input_2
	where
		edc_id between 8 and 17
),
	
	edc_2018_sca_input_2_limited as
(
	select
		a.cartodb_id,
		coalesce(a.the_geom,b.the_geom) as the_geom,
		coalesce(a.the_geom_webmercator,b.the_geom_webmercator) as the_geom_webmercator,
		case when a.the_geom is null and b.the_geom is not null then 'PLUTO' else geom_source end as geom_source,
		a.edc_project_id,
		a.dcp_project_id,
		a.project_name,
		a.project_description,
		a.comments_on_phasing,
		a.build_year,
		a.borough,
		a.total_units,
		a.senior_units
	from
		edc_2018_sca_input_1_limited a
	left join
		capitalplanning.mappluto_v_18v1_1 b
	on 
		(a.bbl = concat(b.bbl) and a.bbl <> '') or 
		(split_part(a.bbl,';',1) = concat(b.bbl) and split_part(a.bbl,';',1) <> '' and position(';' in a.bbl)>0) or 
		(split_part(a.bbl,';',2) = concat(b.bbl) and split_part(a.bbl,';',2) <> '' and position(';' in a.bbl)>0) or 
		(a.borough_code = b.borocode and a.block = b.block and a.lot ='' 			and a.borough_code is not null) or
		(a.borough_code = b.borocode and a.block = b.block and a.lot =concat(b.lot) and a.borough_code is not null) 
),

	edc_2018_sca_input_3_limited as
(
	select
		st_union(the_geom) as the_geom,
		st_union(the_geom_webmercator) as the_geom_webmercator,
		geom_source,
		edc_project_id,
		dcp_project_id,
		project_name,
		project_description,
		comments_on_phasing,
		build_year,
		total_units,
		senior_units,
		cartodb_id
	from	
		edc_2018_sca_input_2_limited
	group by 
		geom_source,
		edc_project_id,
		dcp_project_id,
		project_name,
		project_description,
		comments_on_phasing,
		build_year,
		total_units,
		senior_units,
		cartodb_id
	order by
		edc_project_id
)

select * from edc_2018_sca_input_3_limited order by edc_project_id

) as raw_merge


select cdb_cartodbfytable('capitalplanning', 'edc_2018_sca_input_1_limited');

/************************************************************************************************************************************************************************************
Create new dataset named edc_2018_sca_input_1_limited. No observations eliminated from raw data, as all 7 observations have >0 total_units.

select * from edc_2018_sca_input_1_limited 
************************************************************************************************************************************************************************************/
