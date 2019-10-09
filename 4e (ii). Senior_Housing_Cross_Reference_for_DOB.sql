/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Identifying previously unidentified senior housing in the DOB data by cross-referencing with HPD matches.
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Use intermediate DOB - HPD deduplication dataset to identify matches. If an HPD project is for senior housing, then the DOB project is set to senior housing as well.
2. repeat this step for both the dataset with HEIP build year assumptions, and the dataset with CP build year assumptions.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

drop table if exists dob_2018_sca_inputs_ms_2_1;

create table
		dob_2018_sca_inputs_ms_2_1
as
(
	select
		*
	from
		dob_2018_sca_inputs_ms_2
);


update dob_2018_sca_inputs_ms_2_1 a
set senior_housing_flag = 1
from capitalplanning.hpd_dob_match_4 b
where
	a.job_number = b.dob_job_number and
	b.dob_job_number is not null 	and
	b.senior_housing_flag = 1;



drop table if exists dob_2018_sca_inputs_ms_cp_build_year_2_1;
create table
		dob_2018_sca_inputs_ms_cp_build_year_2_1
as
(
	select
		*
	from
		dob_2018_sca_inputs_ms_cp_build_year_2
);


update dob_2018_sca_inputs_ms_cp_build_year_2_1 a
set senior_housing_flag = 1
from capitalplanning.hpd_dob_match_4 b
where
	a.job_number = b.dob_job_number and
	b.dob_job_number is not null 	and
	b.senior_housing_flag = 1;
