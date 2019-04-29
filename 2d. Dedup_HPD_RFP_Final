/************************************************************************************************************************************************************************************
AUTHOR: Mark Shapiro
SCRIPT: Finalizing deduped HPD RFP data
Sources: 
*************************************************************************************************************************************************************************************/

/*********************************************
		RUN THE FOLLOWING QUERY IN
		CARTO BATCH
*********************************************/

select
	*
into
	hpd_rfp_deduped
from
(
	select
		a.*
		,b.hpd_project_ids
		,b.hpd_incremental_units
		, greatest
			(
				a.total_units - coalesce(a.dob_total_units,0) - coalesce(b.hpd_incremental_units,0)
				,0
			) as HPD_RFP_Incremental_Units
	from
		capitalplanning.hpd_rfp_dob_1 a
	left join
		capitalplanning.hpd_rfp_hpd_1 b
	on
		a.rfp_id = b.rfp_id
) as x
order by rfp_id

/*Run in regular Carto to display table*/		      
select cdb_cartodbfytable('capitalplanning','hpd_rfp_deduped')



	
