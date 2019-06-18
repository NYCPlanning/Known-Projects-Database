/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Adding in DOB-provided addresses when Geosupport did not return addresses.
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Merge DOB raw DOB file with dob_2018_sca_inputs_ms. This merge only occurs after deduplication because we do not want to deduplicate using DOB-provided addresses. We only want
to provide DOB-provided addresses for context, if Geosupport was unable to return addresses.
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

drop table if exists dob_2018_sca_inputs_ms_2;

create table
		dob_2018_sca_inputs_ms_2
as
(
	select
		*,
		0 as DOB_Provided_Address_Flag
	from
		dob_2018_sca_inputs_ms
);

select cdb_cartodbfytable('capitalplanning','dob_2018_sca_inputs_ms_2') ;


update dob_2018_sca_inputs_ms_2 a
set address = b.address,
	DOB_Provided_Address_Flag = 1
from capitalplanning.devdb_housing_pts_20190215 b
where
	a.address = '' and
	a.job_number = b.job_number;



drop table if exists dob_2018_sca_inputs_ms_cp_build_year_2;
create table
		dob_2018_sca_inputs_ms_cp_build_year_2
as
(
	select
		*,
		0 as DOB_Provided_Address_Flag
	from
		dob_2018_sca_inputs_ms_cp_build_year
);

select cdb_cartodbfytable('capitalplanning','dob_2018_sca_inputs_ms_cp_build_year_2') ;


update dob_2018_sca_inputs_ms_cp_build_year_2 a
set address = b.address,
	DOB_Provided_Address_Flag = 1
from capitalplanning.devdb_housing_pts_20190215 b
where
	length(a.address) = 0 and
	a.job_number = b.job_number;