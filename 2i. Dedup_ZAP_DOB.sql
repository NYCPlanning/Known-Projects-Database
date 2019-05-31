
/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduplicate ZAP projects from DOB projects
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge ZAP projects to DOB projects using Address and spatial overlap. Proximity matches excluded due to low accuracy (2/35 accurate matches
	within 20 meters) and high number of manual exclusions.
2. If a DOB job maps to multiple ZAP projects, create a preference methodology to make 1-1 matches
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

select distinct
	* 
into 
	zap_dob
from
	(
	select
		a.*,
		case 
			when
				position(upper(b.address) in upper(a.project_name)) > 0 and
				case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) END 	then 'Address' /*The avg distance assocaited with address matches is .45 meters*/
			when
				st_intersects(a.the_geom,b.the_Geom)																				then 'Spatial'
			when
				c.dob_job_number is not null and b.job_number is not null															then 'MQL Manual Match' 																					
			when
				b.job_number is not null																							then 'Proximity' end 	as DOB_Match_Type, /*Lookup shows that 2/35 proximity matches >=50 units are accurate. 
																																											  Given that <50 units makes a lookoup far more intensive, omitting proximity
																																											  matching*/

		st_distance(CAST(a.the_geom AS GEOGRAPHY),CAST(b.the_geom AS GEOGRAPHY))								as DOB_Distance,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status
	from
		capitalplanning.relevant_dcp_projects_housing_pipeline_ms_v5 a
	/*Adding in additional manual matches identified by MQL in the 2018 SCA Housing Pipeline*/
	left join
		capitalplanning.dcp_dob_dedupe_add c
	on
		a.project_id = c.project_id
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on
		(
			(
					st_intersects(a.the_geom,b.the_geom) or
				(
					position(upper(b.address) in upper(a.project_name)) > 0 and
					case when position('-' in a.project_name) = 0 then left(upper(a.project_name),5) = left(upper(b.address),5) end
				)
			) 
			and
			b.job_type <> 'Demolition' and not
			(a.project_id = 'P2012M0635' and b.job_number = 120481246) /*Manual removal -- previously this code had
										 matched 625 W 57th St DOB job to a 606 W 57th
										 street DCP rezoning. This was an inaccurate match,
										 but I cannot currently think through a logic to 
										 automate this.*/
			and b.units_net > 0						
		)  or
		b.job_number = c.dob_job_number
	) as Raw_Merge




/*********************************************************************************
For the 14 DOB jobs with multiple matches, all match spatially. The multiple matches are 
primarily because of Hudson Yards and Hudson Yards DIB overlaps. Selecting to deduplicate the DIB
unit count, which is already removed from the HY unit count. This will be done by
preferencing the match with the minimum unit difference. 
Also creating manual matching text for overlaps at the Kedem Winery (420 Kent) sites to appropriately
place the matches. 
**********************************************************************************/

select
	*
into
	multi_dcp_dob_matches
from
(
	select
		dob_job_number,
		dob_match_type,
		count(*) as match_count,
		min(abs(total_units-dob_units_net)) as min_unit_difference
	from
		zap_dob
	group by
		dob_job_number,
		dob_match_type
	having
		count(*)>1
) multi_dcp_dob_matches

Select
	*
into
	zap_dob_1
from
(
	select
		a.cartodb_id,
		a.the_geom,
		a.the_geom_webmercator,
		a.project_id,
		a.project_name,
		a.borough, 
		a.project_description,
		a.project_brief,
		a.total_units,
		a.total_unit_source,
		a.ZAP_Unit_Source,
		a.applicant_type,
		a.project_status,
		a.previous_project_status,
		a.process_stage,
		a.previous_process_stage,
		a.dcp_target_certification_date,
		a.certified_referred,
		a.project_completed,
		a.Anticipated_year_built,
		a.remaining_units_likely_to_be_built_2018,
		a.rationale_2018,
		a.rationale_2019,
		a.phasing_notes_2019,
		a.additional_notes_2019,
		a.portion_built_2025,
		a.portion_built_2035,
		a.portion_built_2055,
		a.si_seat_cert,
		a.dob_job_number,
		a.dob_units_net,
		a.dob_address,
		a.dob_job_type,
		a.dob_status
	from
		zap_dob a
	left join
		multi_dcp_dob_matches b
	on
	(	
		a.dob_job_number = b.dob_job_number and
		abs(a.total_units - a.dob_units_net) <> b.min_unit_difference and
		a.project_id not in('P2012K0103','P2015K0227') /*Kedem Winery and 420 Kent exception where I am not filtering to the closest match by unit count*/
	) or
		/*Identifying the following Kedem Winery and DOB jobs matches which should be omitted*/
	(
		(a.project_id = 'P2012K0103' and a.dob_job_number = 320597476 and b.dob_job_number = 320597476) or
		(a.project_id = 'P2015K0227' and a.dob_job_number = 320622616 and b.dob_job_number = 320622616)
	)
	where
		b.dob_job_number is null
) zap_dob_1


select
	*
into
	zap_dob_final
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		total_units,
		total_unit_source,
		ZAP_Unit_Source,
		applicant_type,
		project_status,
		previous_project_status,
		process_stage,
		previous_process_stage,
		dcp_target_certification_date,
		certified_referred,
		project_completed,
		Anticipated_year_built,
		remaining_units_likely_to_be_built_2018,
		rationale_2018,
		rationale_2019,
		phasing_notes_2019,
		additional_notes_2019,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		si_seat_cert,
		array_to_string(array_agg(nullif(concat_ws(', ',dob_job_number,nullif(dob_address,'')),'')),' | ') 	as dob_job_numbers,
		sum(dob_units_net) 																					as dob_units_net
	from
		zap_dob_1
	group by
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		project_id,
		project_name,
		borough, 
		project_description,
		project_brief,
		total_units,
		total_unit_source,
		ZAP_Unit_Source,
		applicant_type,
		project_status,
		previous_project_status,
		process_stage,
		previous_process_stage,
		dcp_target_certification_date,
		certified_referred,
		project_completed,
		Anticipated_year_built,
		remaining_units_likely_to_be_built_2018,
		rationale_2018,
		rationale_2019,
		phasing_notes_2019,
		additional_notes_2019,
		portion_built_2025,
		portion_built_2035,
		portion_built_2055,
		si_seat_cert
	order by
		project_id asc
) zap_dob_final


/***************************************************************************DIAGNOSTICS********************************************************************/

/*257 out of 387 certified projects have materialized*/

select 
	count(*)  as all, 
	count(case when dob_job_numbers<>'' then 1 end) as matched 
from 
	zap_dob_final 
where 
	certified_referred is not null

/*48 out of 228 non-certified projects have materialized*/

select 
	count(*)  as all, 
	count(case when dob_job_numbers<>'' then 1 end) as matched 
from 
	zap_dob_final 
where 
	certified_referred is null

/*Of the ~135 matches to non-certified projects, only 19 DOB jobs are complete. The rest are in progress, which makes sense given non-certification*/.

select 
	dob_status, 
	count(*) 
from 
	zap_dob_1 
where 
	certified_referred is null and 
	dob_job_number is not null 
group by
	dob_status  


/*
	Of the 315 projects with matches, 132 have an exact unit count match. Another 56 are b/w 1-5 units apart, and 18 are b/w 5-10 units apart.
	58 are > 50 units apart. These matches are for much larger rezonings which likely have multiple building counts.
*/
	select
		case
			when abs(total_units-dob_units_net) < 0 then '<0'
			when abs(total_units-dob_units_net) <= 1 then '<=1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' end
															 	as ZAP_Units_minus_DOB_Units,
		count(*) as Count
	from 
		zap_dob_final
	where
		dob_job_numbers <>'' and total_units is not null and dob_units_net is not null 
	group by 
		case
			when abs(total_units-dob_units_net) < 0 then '<0'
			when abs(total_units-dob_units_net) <= 1 then '<=1'
			when abs(total_units-dob_units_net) between 1 and 5 then 'Between 1 and 5'
			when abs(total_units-dob_units_net) between 5 and 10 then 'Between 5 and 10'
			when abs(total_units-dob_units_net) between 10 and 15 then 'Between 10 and 15'
			when abs(total_units-dob_units_net) between 15 and 20 then 'Between 15 and 20'
			when abs(total_units-dob_units_net) between 20 and 25 then 'Between 20 and 25'
			when abs(total_units-dob_units_net) between 25 and 30 then 'Between 25 and 30'
			when abs(total_units-dob_units_net) between 35 and 40 then 'Between 35 and 40'
			when abs(total_units-dob_units_net) between 40 and 45 then 'Between 40 and 45'
			when abs(total_units-dob_units_net) Between 45 and 50 then 'Between 45 and 50'
			when abs(total_units-dob_units_net) > 50 then '>50' 
															end
)


select
	*
from
	zap_dob_final
where
	abs(total_units - dob_units_net) > 50