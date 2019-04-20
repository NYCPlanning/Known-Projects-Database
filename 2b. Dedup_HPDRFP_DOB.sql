/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Deduping HPD RFP data with DOB Project Data
Sources: 
*************************************************************************************************************************************************************************************/

select
	*
into
	hpd_rfp_dob
from
(
	select
		a.the_geom,
		a.project_id as rfp_id,
		a.project_name,
		a.building_id,
		a.primary_program_at_start,
		a.construction_type,
		a.status,
		a.project_start_date,
		a.projected_completion_date,
		a.total_units,
		a.bbl,
		b.job_number as dob_job_number,
		b.units_net as dob_total_units,
		b.address as dob_address,
		b.job_type as dob_job_type,
		b.status as dob_status,
		st_distance(a.the_geom::geography,b.the_geom::geography) as DOB_Distance,
		case 
			when position(concat(b.bbl) in a.bbl)>0 and b.bbl is not null and b.bbl<>0 then 'BBL'
			when concat(b.bbl) = a.bbl and b.bbl is not null and b.bbl<>0 then				'BBL'
			when st_intersects(a.the_geom,b.the_geom) then 									'Spatial'
			when st_dwithin(a.the_geom::geography,b.the_geom::geography,20)	then 			'Proximity' end as DOB_Match_Type
	from
		capitalplanning.hpd_2018_sca_inputs_ms a
	left join
		capitalplanning.dob_2018_sca_inputs_ms b
	on
		b.job_type = 'New Building' and /*There were 9 alterations matches, all with -1 to 1 total units -- clearly not RFPs.*/
		b.status <> 'Complete' and /*2 complete matches and both are inaccurate*/
		(
		(
			b.bbl is not null and b.bbl<>0 and
				(
					concat(b.bbl) = a.bbl or
					position(concat(b.bbl) in a.bbl) > 0
				)
		) or
		st_dwithin(a.the_geom::geography,b.the_geom::geography,20)
		)
	where
		a.source = 'HPD RFPs' 
) as hpd_rfp_dob
