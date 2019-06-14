/**********************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Known Projects DB Final
START DATE: 6/11/2019
COMPLETION DATE: 
Sources: 
*************************************************************************************************************************************************************************************/

drop table if exists Known_Projects_DB_Project_Level_Final_cp_assumptions;

select
	*
into
	Known_Projects_DB_Project_Level_Final_cp_assumptions
from
(
	select
		*
	from
		Known_Projects_DB_Project_Level_Boundaries_cp_assumptions
	where
		not (source = 'DOB' and status in('Complete','Complete (demolition)')) and
		not (source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites'))
) x
	order by
		source asc,
		project_id asc;

select cdb_cartodbfytable('capitalplanning','Known_Projects_DB_Project_Level_Final_cp_assumptions') ;