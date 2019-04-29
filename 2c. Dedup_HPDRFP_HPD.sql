/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with HPD Projected Closings Data
Sources: 
*************************************************************************************************************************************************************************************/

/*********************************************
		RUN THE FOLLOWING QUERY IN
		CARTO BATCH
*********************************************/

select
	*
into
	HPDRFP_HPD_Match
from
(
	select
		a.the_geom,
		a.rfp_id,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units,
		a.bbl,
--		a.dob_job_numbers,
--		a.dob_total_units,
		b.unique_project_id 		as HPD_Project_ID,
		b.total_units 			as HPD_Project_Total_Units,
		b.hpd_incremental_units 	as HPD_Project_Incremental_Units
		st_distance(a.the_geom::geography,b.the_geom::geography) as Distance,
		case 
			when a.bbl = b.bbl and b.bbl not in('','0')				then 'BBL'
			when st_intersects(a.the_geom,b.the_geom)				then 'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)		then 'Proximity'
												end as Match_Type		
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.HPD_Deduped b
	on
		(
			/*BBL-Match*/
			(a.bbl = b.bbl 	and b.bbl not in('','0')) or
			/*Spatial Match*/
			st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
		)
	where a.source = 'HPD RFPs'
) as HPDRFP_HPD_Match
									 
									 
/*****************************RUN THE FOLLOWING QUERY IN REGULAR CARTO************/
									 
/*Export the following query as HPD_HPDRFP_Proximate_Matches. Identify
  whether the matches in this dataset are accurate by flagging Reimport as
  a lookup and omit inaccurate matches*/
									 
select
	*
from
	hpdrfp_hpd_match
where
	match_type = 'Proximity' and
	total_units <> HPD_Project_Total_Units
									 
/***************************RUN THE FOLLOWING QUERY IN CARTO BATCH****************/

select
	*
into
	HPDRFP_HPD_Match_Filtered
FROM
(
	select
		the_geom,
		rfp_id,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		bbl,
--		dob_job_numbers,
--		dob_total_units,
		case when concat(HPD_Project_ID,', ',rfp_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_project_id 			end as hpd_project_id,
		case when concat(HPD_Project_ID,', ',rfp_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_project_total_units 		end as hpd_project_total_units,
		case when concat(HPD_Project_ID,', ',rfp_id)	in(select match_id from capitalplanning.lookup_proximity_hpd_hpdrfp_matches where match = 0) then null else hpd_project_incremental_units 	end as hpd_project_incremental_units
	from
		capitalplanning.HPDRFP_HPD_Match
) as HPDRFP_HPD_Match_filtered
									 
select
	*
into						
	hpd_rfp_hpd_1
from									 
(
	select							
		the_geom,
		rfp_id,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		bbl,
--		dob_job_numbers,
--		dob_total_units,
		case 
			when array_to_string(array_agg(hpd_project_id),', ') like '%, ,%' 	then null
			when array_to_string(array_agg(hpd_project_id),', ') = ', ' 		then null
			else array_to_string(array_agg(hpd_project_id),', ')			end as HPD_Project_IDs,
		sum(hpd_incremental_units) as HPD_Incremental_Units,
--		greatest(total_units - coalesce(dob_total_units,0) - coalesce(sum(hpd_incremental_units),0),0) as HPD_RFP_Incremental_Units
	from
		HPDRFP_HPD_Match_filtered
	group by
		the_geom,
		rfp_id,
		project_name,
		building_id,
		primary_program_at_start,
		construction_type,
		status,
		project_start_date,
		projected_completion_date,
		total_units,
		bbl,
--		dob_job_numbers,
--		dob_total_units
	order by
		rfp_id::numeric
) as hpd_rfp_hpd_1
									 
/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_rfp_deduped')
