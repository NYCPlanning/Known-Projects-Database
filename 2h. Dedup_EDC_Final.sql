
/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Create a final deduped EDC dataset
*************************************************************************************************************************************************************************************/

/************************************************************************************************************************************************************************************
METHODOLOGY: 
1. Aggregate EDC matches to DOB, HPD Projected Closings, and HPD RFP data.
2. Calculate EDC increment
************************************************************************************************************************************************************************************/
/*************************RUN IN CARTO BATCH********************/

drop table if exists edc_deduped;

select
	*
into
	edc_deduped
from
(
	select
		cartodb_id,
		the_geom,
		the_geom_webmercator,
		source,
		project_id,
		project_name,
		project_description,
		comments_on_phasing,
		borough,
		build_year,
		status,
		total_units,
		EDC_Incremental_Units,
		case
			when total_units::float*.2>edc_incremental_units 								then 0
			when edc_incremental_units<=2 and edc_incremental_units<>total_units			then 0
			else edc_incremental_units 														end							as counted_units,
		case
			when total_units::float*.2>edc_incremental_units 								then null
			when edc_incremental_units<=2 and edc_incremental_units<>total_units			then null
			else 																				portion_built_2025 end		as portion_built_2025,
		case
			when total_units::float*.2>edc_incremental_units 								then null
			when edc_incremental_units<=2 and edc_incremental_units<>total_units			then null
			else 																				portion_built_2035 end		as portion_built_2035,
		case
			when total_units::float*.2>edc_incremental_units 								then null
			when edc_incremental_units<=2 and edc_incremental_units<>total_units			then null
			else 																				portion_built_2055 end		as portion_built_2055,
		NYCHA_Flag,
		gq_flag,
		Assisted_Living_Flag,
		Senior_Housing_Flag,
		dob_job_numbers,
		dob_units_net,
		hpd_projected_closings_ids,
		hpd_projected_closings_incremental_units,
		hpd_rfp_ids,
		hpd_rfp_incremental_units
	from
	(
		select
			a.cartodb_id,
			a.the_geom,
			a.the_geom_webmercator,
			'EDC' as source,
			a.edc_project_id as project_id,
			a.project_name,
			a.project_description,
			a.comments_on_phasing,
			case 
					when a.EDC_Project_ID 	= 1 		then 'Brooklyn'
					when a.EDC_Project_ID 	in(3,4) 	then 'Bronx'
					when a.EDC_Project_ID 	= 2 		then 'Manhattan'
					when a.EDC_Project_ID 	in(5,6,7) 	then 'Staten Island'
					when a.EDC_Project_ID   >7 			then 'Brooklyn'  end as borough,

			a.build_year,
			'Projected'															 as status,
			a.total_units,
			greatest
				(
					0,
					a.total_units - coalesce(b.dob_units_net,0) 		- 
					coalesce(c.hpd_project_incremental_units,0) 		-
					coalesce(d.hpd_rfp_incremental_units,0)
				) as EDC_Incremental_Units,
			case
				when a.build_year <=2025 then 1 else 0 end as portion_built_2025,
			case
				when a.build_year between 2026 and 2035 then 1 else 0 end as portion_built_2035,
			case
				when a.build_year >2035 then 1 else 0 end as portion_built_2055,	
			a.NYCHA_Flag,
			a.gq_flag,
			a.Assisted_Living_Flag,
			case when a.Senior_Housing_Flag = 1 then 1 else 0 end as Senior_Housing_Flag,
			b.dob_job_numbers,
			b.dob_units_net,
			c.hpd_project_ids as hpd_projected_closings_ids,
			c.hpd_project_incremental_units as hpd_projected_closings_incremental_units,
			d.hpd_rfp_ids,
			d.hpd_rfp_incremental_units
		from
			capitalplanning.edc_2018_sca_input_1_limited a
		left join
			capitalplanning.edc_dob_final b
		on
			a.edc_project_id = b.edc_project_id
		left join
			capitalplanning.edc_hpd_final c
		on
			a.edc_project_id = c.edc_project_id
		left join
			capitalplanning.edc_hpd_rfp_final d
		on
			a.edc_project_id = d.edc_project_id
	) x
) as edc_deduped
order by
	project_id asc;
		
select cdb_cartodbfytable('capitalplanning','edc_deduped');


/**********************************
SOURCE-SPECIFIC OUTPUT
**********************************/

select * from edc_deduped order by PROJECT_ID::numeric asc
