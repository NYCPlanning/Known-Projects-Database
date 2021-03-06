
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
drop table if exists zap_dob;
drop table if exists multi_dcp_dob_matches;
drop table if exists zap_dob_1;
drop table if exists zap_dob_final;


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
				b.job_number is not null																							then 'Proximity' end 	as DOB_Match_Type, /*Lookup shows that 0/35 proximity matches >=50 units are accurate. 
																																											  Given that <50 units makes a lookoup far more intensive, omitting proximity
																																											  matching*/

		st_distance(CAST(a.the_geom AS GEOGRAPHY),CAST(b.the_geom AS GEOGRAPHY))								as DOB_Distance,
		b.job_number 						as dob_job_number,
		b.units_net 						as dob_units_net,
		b.address 							as dob_address,
		b.job_type							as dob_job_type,
		b.status 							as dob_status,
		b.job_completion_date				as dob_completion_date							
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
			/*Limiting potentially inaccurate Alterations matches.*/
			not (b.job_type = 'Alteration' and a.total_units > 10 		and b.units_net<=2) 		and
			not (b.job_type = 'Alteration' and a.early_stage_flag = 1 	and b.status = 'Complete') 	and
			/*Excluding demolitions*/
			b.job_type <> 'Demolition'																and
			/*Excluding an inaccurate match*/ 															
			not (a.project_id = 'P2012M0635' and b.job_number = 120481246) 							and
										/*Manual removal -- previously this code had
										 matched 625 W 57th St DOB job to a 606 W 57th
										 street DCP rezoning. This was an inaccurate match,
										 but I cannot currently think through a logic to 
										 automate this.*/
			b.units_net > 0																			and
			/*Omitting DOB matches where the ZAP job was Certified 3 or more years after the DOB job was completed. See Diagnostic section below.*/ 
			not(a.certified_referred is not null and b.job_completion_date <> '' and extract(year from a.certified_referred::date) - extract(year from b.job_completion_date::date) >=3) and
			/*Omitting DOB Complete matches to incomplete ZAP projects. Removes 4 matches, all of which are inaccurate. See Diagnostic section below.*/
			not(a.project_id not like '%ESD%' and a.certified_referred is null and a.project_status <> 'Complete' and b.status = 'Complete')					
		)  or
		b.job_number = c.dob_job_number or
		/*Manually matching Domino Sugar P2013K0179 to DOB Job Numbers 320917503 and 320916407. They should overlap, but do not due to DOB points
		  being geocoded past the shoreline and due to a flawed Domino Sugar polygon. Also manually matching 535 Carlton to Atlantic Yards ESD Project*/
		(
			(a.project_id = 'P2013K0179' and b.job_number in(320917503,320916407)) or
			(a.project_id =  '1 [ESD Project]' and b.job_number = 320626710)
		)
	) as Raw_Merge;




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
) multi_dcp_dob_matches;

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
		a.dob_status,
		a.dob_completion_date
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
) zap_dob_1;


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
) zap_dob_final;


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



/*
	Checking if any excessively small DOB jobs have matched with larger ZAP projects. 25 matches, 17 of which are correct. The rest
	can be solved by omitting alteration matches which are:
															< 50% of the ZAP project and
															cannot be Complete if the ZAP project is in Initiation or Pre-PAS stages and
															<=2 units if the ZAP project >10 units
*/ 


select * from zap_dob_1 where dob_units_net < total_units::float*.5 and dob_job_type = 'Alteration' 


/*
	Checking Complete DOB matches to ZAP projects certified 3 years or after DOB completion. There are only 3 matches and all are inaccurate.
*/

select * from zap_dob_1 where certified_referred is not null and dob_completion_date<>'' and extract(year from certified_referred::date) - extract(year from nullif(dob_completion_date,'')::date) >=3


/*
	Checking Complete DOB matches to incomplete ZAP projects where unit counts do not equal each other. There are only 4 matches. 
	project_id	project_name	dob_job_number	dob_address
	2019K0190	862-868 Kent Ave	310122621	133 TAAFFE PLACE
	2019K0211	Bedford Ave Overlay Extension	310159690	142 NORTH 1 STREET
	P2016Q0098	52nd Street Rezoning	402639971	52-01 QUEENS BOULEVARD
	P2018K0320	2892 Nostrand Avenue Rezoning	310146204	2910 NOSTRAND AVENUE

	All are inaccurate. Removing these matches in step zap_dob.
*/

select * from zap_dob_1 where (certified_referred is null and project_status <> 'Complete') and dob_completion_date <> '' and dob_completion_date is not null and total_units <> dob_units_net


/*
	Checking DOB matches to ZAP projects which are much larger. There are 31 projects where this is the case. At least 29 out of these 31 are accurate. For matches with the criteria below,
	and an assessment of whether they are accurate, see link below. Assessment made by comparing address of DOB job to ZAP project name, reading through ZAP application documents to see whether
	DOB lot was included in application, and assuming that multi-building ZAP developments could have small DOB matches.

	G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\01_Analysis\Jan 2019 SCA Housing Pipeline\Diagnostics\20190605_Small_DOB_Matches_to_ZAP.xlsx
*/

	select * from zap_dob_final where dob_units_net < .5*total_units::float and total_units > 10


