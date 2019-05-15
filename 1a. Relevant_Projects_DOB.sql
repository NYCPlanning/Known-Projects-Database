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
	dob_2018_sca_inputs_ms 
from
(
select
	the_geom,
	job_number,
	job_type,
	job_description,
	address,
	boro 												as borough,
	occ_init,
	occ_prop,
	case 
		when 	/*Creating a Partial Complete status*/
			job_type = 'New Building' 		and 
			status = 'Complete' 			and 
			co_latest_certtype = 'T- TCO' 	and
			cast(co_latest_units as double precision)/cast(units_net as double precision) < .8 
											then 'Partial Complete'
		when	/*~100 NB jobs have been labeled as complete without associated COs. Reverting to status
			  'Permit Issued'*/	
			job_type = 'New Building' 	and
			status = 'Complete'			and
			co_latest_units is null		
											then 'Permit Issued'
											else status end
														as status,
	status_date 										as most_recent_status_date,
	right(status_a,4) 									as pre_filing_year,
	status_d 											as completed_application_date,
	status_r 											as full_permit_issued_date,
	status_q 											as partial_permit_issued_date,
	status_x 											as job_completion_date,
	co_earliest_effectivedate 							as earliest_cofo_date,
	co_latest_effectivedate 							as latest_cofo_date,
	stories_init,
	stories_prop,
	units_init,
	units_prop,
	units_net,
	units_incomplete,
	co_latest_units 									as latest_cofo,
	latitude,
	longitude,
	coalesce(geo_bin,bin)								as bin,
	coalesce(geo_bbl,bbl)								as bbl

from 
	capitalplanning.devdb_housing_pts_20190215
where
	status 		<>'Withdrawn' 				and 
	x_inactive 	= 'false' 					and /*Non-permitted job w/o update since two years ago*/
	(
		upper(occ_init) like '%RESIDENTIAL%' or
		upper(occ_prop) like '%RESIDENTIAL%'
	) /*Limiting to projects which were or at one point will be residential. Decreases count of uncompleted
		projects, limited by above fields, by 21 observations (875 units_net).*/ 
											and
	units_net <> 0
order by
	job_number
) as dob_2018_sca_inputs_ms

/***********************************RUN IN REGULAR CARTO*****************************/

select cdb_cartodbfytable('capitalplanning', 'dob_2018_sca_inputs_ms')

select
*
into
test
from(

	    SELECT 
	        tab1.job_number, 
	        tab1.job_type,
	        tab1.job_description,
	        tab1.status,
	        tab1.bin, 
	        tab1.bbl, 
	        tab1.units_net, 
	        tab1.earliest_cofo_date, 
	        tab1.most_recent_status_date,
	        tab2.job_number as dup_job_number, 
	        tab2.job_type as dup_job_type,
	        tab1.job_description,
	        tab2.status as dup_status,
	        tab2.bin as dup_used_bin, 
	        tab2.bbl as dup_used_bbl, 
	        tab2.units_net as dup_units_net,
	        tab2.earliest_cofo_date as dup_earliest_cofo_date,
	        tab2.most_recent_status_date as dup_status_date
	    FROM 
	        capitalplanning.dob_2018_sca_inputs_ms tab1
	    full outer join 
	        capitalplanning.dob_2018_sca_inputs_ms tab2
	    on
	        tab1.job_number is not null
	    WHERE 
	    	tab1.the_geom=tab2.the_geom and

	        -- tab1.geo_bin=tab2.geo_bin and
	        -- tab1.geo_bbl=tab2.geo_bbl and
	        tab1.job_number <> tab2.job_number                                  and not
	        (tab1.job_number = '220394552' and tab2.job_number = '220395515')   and not
	        (tab1.job_number = '210069753' and tab2.job_number = '210069432')   and not
	        (tab1.job_number = '420652225' and tab2.job_number = '420652537')   
) x


/*

If overlap and same job type and different status, pick the more recent. pull out if there are large unit count differences to look individually. I should use the
actual geometry right?
If overlap and not same job type, then read through a few job descriptions to see if they are the same or not.
Consider if this makes sense for demolitions as well. probably yes.
