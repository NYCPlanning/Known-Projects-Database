/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Preparing DOB's devdb_housing_pts dataset for joining
START DATE: 1/10/2019
COMPLETION DATE: 1/10/2019
Source file: "G:\03. Schools Planning\01_Inputs to SCA CP\Housing pipeline\00_Data\
		Jan 2019 SCA Housing Pipeline\Raw Data\DOB\devdb_housing_pts.csv"
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY:
1. Remove inactive/closed and completed projects.
2. Remove non-residential projects.
3. Redefine "status" field to identify Partial Complete projects
4. Redefine "status" field to identify Complete NB projects without COs as Permit Issued
5. Rearrange fields.
*************************************************************************************************************************************************************************************/
/***********************************RUN IN CARTO BATCH*****************************/
select
	*
into
	dob_pipeline_ms 
from
(
select
	the_geom,
	job_number,
	job_type,
	address,
	boro 										as borough,
	occ_init,
	occ_prop,
	case 
		when 	/*Creating a Partial Complete status*/
			job_type = 'New Building' 	and 
			status = 'Complete' 		and 
			co_latest_certtype = 'T- TCO' 	and
			cast(co_latest_units as double precision)/cast(units_net as double precision) < .8 
											then 'Partial Complete'
		when	/*~100 NB jobs have been labeled as complete without associated COs. Reverting to status
			  'Permit Issued'*/	
			job_type = 'New Building' 	and
			status = 'Complete'		and
			co_latest_units is null		
											then 'Permit Issued'
											else status end
											as status,
	status_date 									as most_recent_status_date,
	right(status_a,4) 								as pre_filing_year,
	status_d 									as completed_application_date,
	status_r 									as full_permit_issued_date,
	status_q 									as partial_permit_issued_date,
	status_x 									as job_completion_date,
	co_earliest_effectivedate 							as earliest_cofo_date,
	co_latest_effectivedate 							as latest_cofo_date,
	stories_init,
	stories_prop,
	units_init,
	units_prop,
	units_net,
	units_incomplete,
	co_latest_units 								as latest_cofo,
	latitude,
	longitude,
	bin,
	bbl
from 
	capitalplanning.devdb_housing_pts_2019_02_08 
where
	status 		<>'Withdrawn' 	and 
	x_inactive 	= 'false' 	and /*Non-permitted job w/o update since two years ago*/
	(
		upper(occ_init) like '%RESIDENTIAL%' or
		upper(occ_prop) like '%RESIDENTIAL%'
	) /*Limiting to projects which were or at one point will be residential. Decreases count of uncompleted
		projects, limited by above fields, by 21 observations (875 units_net).*/
order by
	job_number
) as dob_pipeline_ms

/***********************************RUN IN REGULAR CARTO*****************************/

select cdb_cartodbfytable('capitalplanning', 'dob_pipeline_ms')

