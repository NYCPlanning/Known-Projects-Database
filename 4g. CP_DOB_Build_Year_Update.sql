/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Updating build-year for inactive DOB jobs which have been matched to sources in HPD Projected Closings, HPD RFPs, or EDC projects. Assigning the build years of these projects
		because we have up-to-date information from DOB and HPD. Only performing this for CP build-year assumptions, as HEIP has included its own delay factor expectations.
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge DOB data with aggregated data.
2. If a DOB project matches with HPD or EDC projects, set build year to 2025 even if inactive.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

drop table if exists dob_2018_sca_inputs_ms_cp_build_year_3;
create table
		dob_2018_sca_inputs_ms_cp_build_year_3
as
(
	select
		*
	from
		dob_2018_sca_inputs_ms_cp_build_year_2_1
);

update dob_2018_sca_inputs_ms_cp_build_year_3 a
set
	portion_built_2025 = 1,
	portion_built_2035 = 0,
	portion_built_2055 = 0
from
	capitalplanning.known_projects_db_20190917_v6 b
where
	a.inactive_job is true 									and
	position(concat(a.job_number) in b.dob_matches) > 0		and
	(
		b.source in('HPD Projected Closings', 'HPD RFPs'					) or 
		(b.source = 'EDC Projected Projects' and b.project_id = '5'			) --Ensuring that an inactive DOB job for Stapleton Phase 1 (EDC Project ID #5) is placed to materialize in 2025.
	);


select cdb_cartodbfytable('capitalplanning','dob_2018_sca_inputs_ms_cp_build_year_3') ;
