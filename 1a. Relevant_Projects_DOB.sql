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
drop table if exists dob_2018_sca_inputs_ms_pre;
select
	*
into
	dob_2018_sca_inputs_ms_pre
from
(
select
	A.the_geom,
	A.the_geom_webmercator,
	'DOB' as Source,
	job_number,
	job_type,
	job_description,
	geo_address											as address,
	boro 												as borough,
	occ_init,
	occ_prop,
	case 
		when 	/*Creating a Partial Complete status*/
			job_type = 'New Building' 		and 
			status = 'Complete' 			and 
			co_latest_certtype = 'T- TCO' 	and

			(
				(cast(co_latest_units as double precision)/cast(units_net as double precision) < .8  	and units_net >= 20) or
				(units_net - co_latest_units >=5 														and units_net between 5 and 19)
			)
											then 'Partial Complete'
											else status end
														as status,
	status_date 										as most_recent_status_date,
	status_a 		 									as pre_filing_date,
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
	A.latitude,
	A.longitude,
	geo_bin												as bin,
	geo_bbl												as bbl,
	geo_ntacode_edit,
	geo_ntaname_edit,
	x_inactive											as inactive_job,

	/*Identifying NYCHA Projects*/
	CASE 
		WHEN upper(job_description)  like '%NYCHA%' THEN 1   		
		WHEN upper(job_description)  like '%BTP%' THEN 1  		
		WHEN upper(job_description)  like '%HOUSING AUTHORITY%' THEN 1  		
		WHEN upper(job_description)  like '%NEXT GEN%' THEN 1  		
		WHEN upper(job_description)  like '%NEXT-GEN%' THEN 1  		
		WHEN upper(job_description)  like '%NEXTGEN%' THEN 1  		
		WHEN upper(job_description)  like '%BUILD TO PRESERVE%' THEN 1 ELSE 0 END 				AS NYCHA_Flag,

	CASE 
		WHEN upper(job_description)  like '%CORRECTIONAL%' THEN 1   		
		WHEN upper(job_description)  like '%NURSING%' THEN 1  		
		WHEN upper(job_description)  like '% MENTAL%' THEN 1  		
		WHEN upper(job_description)  like '%DORMITOR%' THEN 1  		
		WHEN upper(job_description)  like '%MILITARY%' THEN 1  		
		WHEN upper(job_description)  like '%GROUP HOME%' THEN 1  		
		WHEN upper(job_description)  like '%BARRACK%' THEN 1 ELSE 0 END 						AS GQ_fLAG,


	/*Identifying definite senior housing projects*/
	CASE 
		WHEN B.PROJECT_ID IS NOT NULL 							THEN 1
		WHEN upper(job_description)  like '%SENIOR%' 			THEN 1
		WHEN upper(job_description)  like '%ELDERL%' 			THEN 1 	
		WHEN job_description  like '% AIRS %' 					THEN 1
		WHEN upper(job_description)  like '%A.I.R.S%' 			THEN 1 
		WHEN upper(job_description)  like '%CONTINUING CARE%' 	THEN 1
		WHEN upper(job_description)  like '%NURSING%' 			THEN 1
		WHEN job_description  like '% SARA %' 					THEN 1
		WHEN upper(job_description)  like '%S.A.R.A%' 			THEN 1  else 0 END				as Senior_Housing_Flag,
	CASE
		when UPPER(concat(occ_init,occ_prop)) like '%ASSISTED LIVING%' then 1
		WHEN upper(job_description)  like '%ASSISTED LIVING%' THEN 1 else 0 end 				as Assisted_Living_Flag,
	row_number() over(partition by job_number order by status_date::date desc, a.cartodb_id)	as job_number_instance /*Creating a flag to omit < 5 job numbers which appear more than once.
																														These jobs are all 0-1 units_net*/
from 
	capitalplanning.devdb_housing_pts_20190215 a
left join
	capitalplanning.hny_by_bldg_with_senior_20190429 b
on
	a.geo_bbl 	= b.bbl														and
	(extract(year from a.status_q::DATE)>=2014 or status_q is null)			and
	a.job_type 	= 'New Building'											and
	b.reporting_construction_type = 'New Construction' 						and
	b.contains_senior_units = 1


/*Filtering out jobs from pipeline*/	
where
	-- status 		<>'Withdrawn' 							and 
	-- x_inactive 	= 'false' 								and /*Non-permitted job w/o update since two years ago*/
	units_net 	<> 0									and /*Removing administrative and no work jobs which do not create units*/
	upper(job_description) not like '%NO WORK%' 		and
	upper(job_description) not like '%ADMINISTRATIVE%'  and
	job_number not in(220453168,220600446) 						/*Omitting two jobs at 29 Featherbed Lane which are duplicates of a third job, 220673162. All other significant duplicates
																have been already removed by HEIP and EDM prior to Housing DB publication*/
														and
	job_number <> 520137780								and		/*Omitting duplicate 475 Bay St Withdrawn Job*/
	job_number not in(420652948,420652966)						/*Omitting two duplicates for 131-02 40th Road. The two active jobs for this address are not duplicates*/

order by
	job_number
) as dob_2018_sca_inputs_ms_pre;

/*Deduplicating inactive jobs against other inactive jobs. HEIP has not accounted for potential duplicates which are inactive.
  Matching duplicates by geom and job_type, and preferencing the match with the more recent status date. This omits ~2,500 units.*/ 

drop table if exists inactive_deduped_against_inactive;
select
	*
into 
	inactive_deduped_against_inactive
from
(	
	select
		a.*,
		b.job_number 					as match_job_number,
		b.job_description				as match_job_description,
		b.job_type 						as match_job_type,  
		b.status 						as match_status,  
		b.address 						as match_address, 
		b.units_init					as match_units_init,
		b.units_prop					as match_units_prop, 
		b.units_net						as match_units_net,
		b.most_recent_status_date		as match_most_recent_status_date,
		b.pre_filing_date 				as match_pre_filing_date,
		b.completed_application_date	as match_completed_application_date,
		b.partial_permit_issued_date	as match_partial_permit_issued_date,
		b.full_permit_issued_date		as match_full_permit_issued_date,
		b.job_completion_date 			as match_job_completion_date,
		b.earliest_cofo_date			as match_earliest_cofo_date,
		b.latest_cofo_date				as match_latest_cofo_date
	from
		(select * from capitalplanning.dob_2018_sca_inputs_ms_pre where inactive_job is true) a
	left join
		(select * from capitalplanning.dob_2018_sca_inputs_ms_pre where inactive_job is true) b
	on
		a.the_geom = b.the_geom 												and
		a.most_recent_status_date::date > b.most_recent_status_date::date 		and
		a.job_type = b.job_type 												and
		a.job_number <> b.job_number
	order by
		a.units_net desc,
		b.units_net desc
) x;

/*Deduplicating the inactive jobs which haven't been deduplicated against another inactive job, against active jobs. Preferencing the active job. This omits ~10K units.*/ 

drop table if exists inactive_deduped_against_active;
select
	*
into
	inactive_deduped_against_active
from
(	
	select
		a.*,
		b.job_number 					as match_job_number,
		b.job_description				as match_job_description,
		b.job_type 						as match_job_type,  
		b.status 						as match_status,  
		b.address 						as match_address, 
		b.units_init					as match_units_init,
		b.units_prop					as match_units_prop, 
		b.units_net						as match_units_net,
		b.most_recent_status_date		as match_most_recent_status_date,
		b.pre_filing_date 				as match_pre_filing_date,
		b.completed_application_date	as match_completed_application_date,
		b.partial_permit_issued_date	as match_partial_permit_issued_date,
		b.full_permit_issued_date		as match_full_permit_issued_date,
		b.job_completion_date 			as match_job_completion_date,
		b.earliest_cofo_date			as match_earliest_cofo_date,
		b.latest_cofo_date				as match_latest_cofo_date
	from
		(select * from capitalplanning.dob_2018_sca_inputs_ms_pre where inactive_job is false) a
	left join
		(select * from capitalplanning.dob_2018_sca_inputs_ms_pre where inactive_job is true and job_number not in(select match_job_number from inactive_deduped_against_inactive where match_job_number is not null) ) b
	on
		a.the_geom = b.the_geom	and
		a.job_type = b.job_type
	order by
		a.units_net desc,
		b.units_net desc
) x;




drop table if exists dob_2018_sca_inputs_ms;

/*Omitting old dups from table*/
SELECT
	*
into
	dob_2018_sca_inputs_ms
from
(
	SELECT DISTINCT
		a.*,
	geo_ntacode_edit,
	geo_ntaname_edit,
		case
			when a.status like 'Complete%' 				then a.units_net
			when a.status like '%Partial Complete' 		then a.latest_cofo
			else null end 																			 	as units_net_complete, 
		case
			when a.status like 'Complete%' 				then null
			when a.status like '%Partial Complete' 		then a.units_net - a.latest_cofo
			else a.units_net end 																		as units_net_incomplete,
		case
			when a.status like 'Complete%' 				then null
			when a.status like '%Partial Complete' 		then a.units_net - a.latest_cofo
			else a.units_net end 																		as counted_units,
		case
			when a.status like 'Complete%' 				then null
			when a.status = 'Partial Complete'			then 1
			when c.completion_rate_2025 is not null		then completion_rate_2025
			when c.completion_rate_2025 is null 		then 0 											end as portion_built_2025,
		case
			when a.status like 'Complete%' 				then null
			when a.status = 'Partial Complete'			then 0
			when c.completion_rate_2025 is not null 	then round((1 -completion_rate_2025)::numeric,2)
			when c.completion_rate_2025 is null 		then 1 											end as portion_built_2035,
		case
			when a.status like 'Complete%' 				then null
			when a.status = 'Partial Complete'			then 0
			when c.completion_rate_2025 is not null 	then 0
			when c.completion_rate_2025 is null 		then 0 											end as portion_built_2055
	from
		(select * from capitalplanning.dob_2018_sca_inputs_ms_pre where job_number_instance = 1) a
	/*Adding in HEIP-developed phasing for DOB jobs.*/
	left join
		(select job_number, completion_rate_2025, row_number() over(partition by job_number order by cartodb_id) as job_number_instance from capitalplanning.housingdb_19v1_rl_test_0612) c
	on
		a.job_number = c.job_number and
		c.job_number_instance = 1 /*Using a created job_number_instance field to deduplicate housing-phasing data. Only omits one match.*/
	where
		--Omitting duplicate inactive jobs identified in the two steps above
		a.job_number not in(select match_job_number from inactive_deduped_against_inactive where match_job_number is not null) and
		a.job_number not in(select match_job_number from inactive_deduped_against_active   where match_job_number is not null)
) x
	order by
		x.job_number asc;


select cdb_cartodbfytable('capitalplanning', 'dob_2018_sca_inputs_ms');





drop table if exists dob_2018_sca_inputs_ms_cp_build_year;

/*Omitting old dups from table*/
SELECT
	*
into
	dob_2018_sca_inputs_ms_cp_build_year
from
(
	SELECT DISTINCT
		a.*,
		case
			when a.status like 'Complete%' 			then a.units_net
			when a.status like '%Partial Complete' 	then a.latest_cofo
			else null end 																			 	as units_net_complete, 
		case
			when a.status like 'Complete%' 			then null
			when a.status like '%Partial Complete' 	then a.units_net - a.latest_cofo
			else a.units_net end 																		as units_net_incomplete,
		case
			when a.status like 'Complete%' 			then null
			when a.status like '%Partial Complete' 	then a.units_net - a.latest_cofo
			else a.units_net end 																		as counted_units,
		case
			when a.status in('Complete','Complete (demolition)') 	then null
			when a.inactive_job is true								then 0
			when a.status like '%In progress%' 						then .5
			else 1 end 																					as portion_built_2025,
		case
			when a.status in('Complete','Complete (demolition)') 	then null
			when a.inactive_job is true								then 1 
			when a.status like '%In progress%' 						then .5
			else 0 end 																					as portion_built_2035,
		case
			when a.status in('Complete','Complete (demolition)') 	then null
			when a.inactive_job is true								then 0 
			when a.status like '%In progress%' 						then 0
			else 0 end 																					as portion_built_2055
	from
		capitalplanning.dob_2018_sca_inputs_ms_pre a
	where 
		a.job_number_instance = 1 																								and
		--Omitting duplicate inactive jobs identified in the two steps above
		a.job_number not in(select match_job_number from inactive_deduped_against_inactive where match_job_number is not null) 	and
		a.job_number not in(select match_job_number from inactive_deduped_against_active   where match_job_number is not null)
) x
	order by
		x.job_number asc;


select cdb_cartodbfytable('capitalplanning', 'dob_2018_sca_inputs_ms_cp_build_year');








/********************************SUPERSEDED**********************/

SELECT
	*
into
	dob_complete_inputs_share_20190522
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		job_number as project_id,
		address,
		units_net,
		units_net_complete,
		units_net_incomplete
	from
		dob_2018_sca_inputs_ms
	where
		status like '%Complete%'
) dob_inputs_ms_share_20190522

select cdb_cartodbfytable('capitalplanning', 'dob_complete_inputs_share_20190522')

SELECT
	*
into
	dob_incomplete_inputs_share_20190522
from
(
	SELECT
		the_geom,
		the_geom_webmercator,
		job_number as project_id,
		address,
		units_net,
		units_net_complete,
		units_net_incomplete
	from
		dob_2018_sca_inputs_ms
	where
		status not like '%Complete%'
) dob_inputs_ms_share_20190522

select cdb_cartodbfytable('capitalplanning', 'dob_incomplete_inputs_share_20190522')




/************************FURTHER SUPERSEDED ANALYSIS******************************/

/*Collecting DOB jobs which overlap with each other*/

select
	*
into
	DOB_Overlaps
from
(

	    SELECT 
	    	tab1.the_geom,
	        tab1.job_number, 
	        tab1.job_type,
	        tab1.address,
	        tab1.job_description,
	        tab1.status,
	        tab1.bin, 
	        tab1.bbl, 
	        tab1.units_net, 
	        tab1.earliest_cofo_date, 
	        tab1.most_recent_status_date,
	        tab2.the_geom					as dup_geom,
	        tab2.job_number 				as dup_job_number, 
	        tab2.job_type 					as dup_job_type,
	        tab2.address					as dup_address,
	        tab2.job_description 			as dup_job_description,
	        tab2.status 					as dup_status,
	        tab2.bin 						as dup_bin, 
	        tab2.bbl 						as dup_bbl, 
	        tab2.units_net 					as dup_units_net,
	        tab2.earliest_cofo_date 		as dup_earliest_cofo_date,
	        tab2.most_recent_status_date 	as dup_most_recent_status_date
	    FROM 
	        capitalplanning.dob_2018_sca_inputs_ms_pre tab1
	    left join 
	        capitalplanning.dob_2018_sca_inputs_ms_pre tab2
	    on
	        tab1.job_number <> 	tab2.job_number and
	        (
	        	(tab1.address = 	tab2.address and tab1.address<>'') 		or
	        	(tab1.bin=tab2.bin and tab1.bbl = tab2.bbl and tab1.bin is not null and tab1.bbl is not null)
	        ) 
	    WHERE 
	    	/*Limiting by Complete status jobs, known not to be overlaps, identified during multiplier analysis*/
	        not (tab1.job_number = '220394552' and tab2.job_number = '220395515')   and 
	        not (tab1.job_number = '210069753' and tab2.job_number = '210069432')   and 
	        not (tab1.job_number = '420652225' and tab2.job_number = '420652537')   
) x






/*Limiting the above list of overlaps to a potential list of dups based on job type and status date. Omitting BIN/BBL matches if they are "million BINs" and addresses do not match.*/
SELECT
	*
into
	dob_overlaps_lim
from
	(
		SELECT
			*
		from
			DOB_Overlaps
		where
		(
			(
				job_type = 'New Building' and dup_job_type = 'New Building' and most_recent_status_date::date > dup_most_recent_status_date::date 				
			)	or
			(
				job_type = 'New Building' and dup_job_type = 'New Building' and most_recent_status_date::date = dup_most_recent_status_date::date and job_number > dup_job_number				
			)	or
			(
				job_type = 'Demolition' and dup_job_type = 'Demolition' and most_recent_status_date::date > dup_most_recent_status_date::date 				
			)	or
			(
				job_type = 'Demolition' and dup_job_type = 'Demolition' and most_recent_status_date::date = dup_most_recent_status_date::date and job_number > dup_job_number				
			)
		)	and
		not (concat(bin) like '%00000%' and address<>dup_address)

	) x


/*Deduplicating list of DOB relevant projects*/

SELECT
	*
into
	dob_2018_sca_inputs_ms
from
(
	SELECT
		*
	from
		dob_2018_sca_inputs_ms_pre
	where
		job_number not in
		(
			SELECT
				dup_job_number
			from
				dob_overlaps_lim
			where
				dup_job_number not in
				(
					321063503, /*List of matched projects which actually represent different developments. See superseded section for more explanation*/
					321063512,
					321063521,
					321063530,
					321063549,
					321063558,
					321063567,
					321063576,
					321063585,
					321063594,
					321063601,
					321063610
				)	
		)
) x







/***********************************************************SUPERSEDED*************************************************/


/*Limiting to the list of overlaps which have been found in other data sources in the housing pipeline. Only 38 distinct dup_jobs.
  After review, find that only matches based on address as well are accurate. The following jobs are all part of 1 development (1560 60th street) and
  represent many structures, according to https://newyorkyimby.com/2015/01/permits-filed-for-large-borough-park-development-at-1560-60th-street.html. These
  are the only jobs which will from the above methodology which will not be excluded. ~1800 units are omitted due to this analysis.
		321063503
		321063512
		321063521
		321063530
		321063549
		321063558
		321063567
		321063576
		321063585
		321063594
		321063601
		321063610
  */

SELECT
	*
into
	dob_overlaps_lim_1
from
(
	SELECT
		*
	from
		dob_overlaps_lim
	where
		dup_job_number in
		--List of all job numbers currently found in other MVP data sources*/
		(
			320909433,320626710,321080833,321042304,320627103,320627112,320627096,321383702,321384480,321543291,321543282,321781942,421204967,520363179,520363160,520357809,321765700,420659326,421622621,220426475,201108933,
			520141971,321195050,120743311,110006243,110220289,121192397,123073805,121075772,121189819,120325594,103347870,122127055,121659400,122773784,104410727,104626031,121192618,103427926,121188972,104576246,122865319,
			122000486,122898926,121187697,121189882,121188767,121832428,121188099,104766479,122044233,121507208,121586060,121184912,110015894,121204268,120253714,122422226,121188008,120855761,104839980,120460811,121185760,
			121328116,103545022,120265685,121189132,123073814,103636059,120463202,123181350,122640375,121187483,121184208,104691861,320622750,320717140,321519068,320717168,321087131,321513171,321063530,321063629,321063567,
			321063558,321063549,321063521,321063512,321063585,321063610,321063601,321063576,321063503,321063594,321276445,321372411,301253989,320914953,320914917,321262977,321262441,320516571,320699366,321372420,321265929,
			321265938,320699357,320516964,321269845,320699348,320210632,320627256,320622616,320597476,320694744,310201616,320627327,320595192,320597449,320597430,320595209,320625999,302361227,310203302,320896349,320595003,
			320592328,320324207,121326341,110017151,120481246,120476163,120081614,120081400,121908132,121184869,121187036,121186938,121191110,121203937,121327395,121454773,121331674,121333627,110170378,121185626,121192351,
			121191968,121789671,121187125,121331120,122801272,110030485,120794499,120704416,120921002,121181416,121187456,122736762,121184645,121327233,121059861,121186386,121190585,122782186,121203866,121204053,421007387,
			421572257,420942396,420659139,421260110,421380268,421476931,421476940,410175215,401515017,420651912,421171993,421226024,421070450,421088762,421089413,520239812,520199543,520210370,520200238,520200247,520277520,
			520324015,520323962,520275005,520319726,500901857,520260814,520048975,520273249,520273276,520273285,520273267,520048966,520273301,520273258,520273230,520273294,520202995,520203654,520203002,520202986,520085121,
			520010657,520143746,520143728,520143755,520114974,520095405,520095851,520095290,220404612,220152144,220064436,220403882,220150226,210178546,210178635,210178573,210178626,220150217,210178555,210178591,210178582,
			210178617,210178608,220124381,220111689,220343420,220406889,220150440,220151859,201092897,220211937,320627121,321190670,321190634,320576792,320576809,320576783,320576818,321190652,321190625,320912170,320909727,
			320909736,321197478,320517151,320517106,310234207,320516189,321720349,320059066,121004028,121190424,104869509,121235289,121190861,121184734,121326494,421644867,421636732,421633432,421626672,421626654,421626663,
			421635109,421641101,421635449,421641094,421636714,421644858,421624380,421092105,520205457,520112477,520160432,520160441,520232873,520138379,520183907,520182793,520098368,520098386,520120798,520142925,520142916,
			520180438,520110273,520146066,520146057,520145049,520191710,520165687,520210496,520183079,520181605,520140400,520140455,520164009,520164116,520164410,520164429,520164394,520168381,520164401,520143924,520308284,
			520097635,520288073,520277012,520277030,520147305,520273631,520281882,520289777,520281828,520281891,520317611,520281819,520135862,520281908,520302636,520098803,520288082,520289768,520270732,520270741,520313839,
			520288199,220177092,220152233,220152625,220152634,220611504,220152643,200759795,220034558,220151868,220462381,220151519,220125335,220125326,320595183,320350678,320374064,320917772,320917978,320597378,320623250,
			320909996,321231412,321647811,321197316,321197307,301862456,321383784,321383775,321188031,320914070,320910822,121184173,122143787,121184431,121332815,121187562,121600062,121328321,121185261,122171041,121851834,
			121186812,121186607,121120768,121191432,421313377,421513927,420948782,421178987,410200367,420655017,420651770,520147868,520043774,520043756,520043765,520182613,520211422,520161547,520161538,520161556,520145496,
			520166775,520187556,520059258,520145030,520161459,520182828,520182819,520020799,520187547,520194646,520194664,520180214,520180278,520180189,520165703,520165749,520180250,520180198,520180232,520165721,520165767,520180312,520180296,520212225,520212207,520212181,520128692,520128718,520128745,520128727,520128763,520128754,510003381,520128709,520128736,500664444,500664453,520163652,520165883,520165874,520198857,520165865,520008875,520008973,520008928,520009179,520009071,520008937,520008946,520009197,520008848,520008955,520008991,520009099,520009062,520008731,520008759,520008839,520008857,520008866,520008777,520008964,520008900,520009124,520008820,520009035,520009053,520008893,520008982,520009204,520008811,520116785,520008884,520009044,520008740,520008795,520009213,520009026,520008802,520009080,520009188,520185932,520185941,520106377,520190873,520190882,520074428,520074419,520041721,520107278,520107553,520104743,520074400,520107544,520074437,520231188,520279635,520279626,520290088,520303797,520292781,520198624,520198615,520303760,520238136,520314437,520195208,520195191,520166374,520166347,520166329,520166365,520166338,520162136,520162109,520166613,520147831,520147822,520205670,520196038,520285799,520186557,520186548,520186566,520182962,520166105,520191667,520209408,520203048,520197563,520214713,520217685,520207491,520194405,520194432,520240739,220238017,220462363,220462407,220407860,220042246,220152750,321264396,321188317,320912367,320577531,320592462,321188567,321195979,302371369,321374936,321639642,301267769,310059129,321384140,321189735,121329053,121193074,121192770,121186171,122109743,121187660,121190273,122535630,121191557,121189141,121190601,121204320,121190781,121830439,121944227,420663669,421084454,520143871,520143899,520210922,520216356,520199829,520199810,520199311,520292781,520303797,520207543,520207552,520212715,520217658,520216310,520216338,520359585,520292282,520292264,520292273,220423879,220545952,220152572,220025835,220152563,321191394,321191410,320910617,320913703,320583891,321093598,321191198,321383383,320911670,321184302,321191214,121543632,121054303,122882853,122085206,120654531,123485308,122764589,122751450,122719273,120522906,122541231,122890434,122643121,122728655,123119160,120760471,120092684,122273244,122643372,123059251,122716784,121666713,120573209,121191423,121203973,120426379,121188847,121188856,121188918,122656367,122868138,121191049,121185813,121187241,420656230,420139843,402639971,421374845,421374881,420663776,420663981,520044568,520044577,520033525,220152705,210178001,220151948,220609410,220475322,210071429,220569999,320597083,321080539,321478548,320914347,321321636,320766051,320984208,321603608,121193234,121188491,122874906,421067767,520289321,510071039,510071020,510071048,510071011,520308523,520308514,520293370,520293389,210178500,220613717,310103278,320216798,321191349,320596495,320596501,321552459,310146204,120366282,122510924,420532908,420572963,402595358,420503495,420844581,421187520,421372339,410208038,410225885,421022985,421626976,420238497,421323400,421605105,420831238,421453821,402577751,402288289,421165517,421517415,421473854,420984457,421519137,421602224,421415365,420242730,420236701,420880620,410220265,401804874,402503359,421602420,420362921,420335024,420548376,410114255,420054988,420367383,420935000,421693233,420524515,421927240,421927259,520328048,520328057,520328039,520327673,520327655,520291096,520291167,520291041,520291069,520290881,520291158,520290872,520290845,520290943,520290827,520290925,520291121,520290934,520291149,520290952,520291130,520291078,520290961,520290863,520291087,520290890,520290836,520290989,520291050,520291103,520291176,520291014,520291005,520290970,520291032,520290854,520290916,520290907,520290998,210178751,220669453,210178902,210177841,210178047,210178939,200982687,220192404,321383383,320911670,321185695,321194088,320911689,210178500,520132972,520132963,121204464,121791775,121231327,321190126,321190117,320190324,320190333,320190342,320190351,320190360,320190379,320190388,320190404,320190397,220403882,122736762,321197726,320913810,320596413,320913829,320913785,320596262,320913801,320913838,321197717,321196567,320722606,320722624,320722615,320722599,321197307,321197316,320911705,320912429,121191968,321192348,321192348,321192302,220152144,321185542,321185588,321185631,321185560,321195782,321195791,321185551,321195808,321185579,321195719,321195773,321195700,321195899,121190022,321283188,320513342,320513351,320513379,320513388,220447078,220152572,220152563,104510218,220282628,220325510,320324207,220404612,121181915,122760654,220122445,220211964,121331059,320597378,220329366,220102092,320597092,320583891,320623250,320877627,220392858,220210929,121333342,210178939,320577746,220462381,220151868,220151859,121185760,220569999,420653457,220395515,121187660,121191432,121185626,121187045,121186938,320613494,121191646,420651379,320577862,121184253,220151323,121186386,121187410,320592587,220157700,320915649,220343420,320623884,320623919,420654508,420654517,320896349,220151680,320627050,320626765,320908997,220152180,320594200,321848774,321187434,321187443,321187452,321187461,321187470,321187559,321187540,321187531,321187522,321187513,321187504,321187498,321196040,321187648,321187639,321187620,321187611,321187595,321187586,321196022,321187577,321196059,321187684,321187675,321187719,321187666,321187657,321196031,321187700,321187693,321185604,321183161,321183170,321183223,321183232,321183241,321183250,321185640,321183045,321183036,321183027,321183018,321182992,321187988,321185622,321183054,321183063,321183072,321183081,321183090,321183107,321183116,321183125,321183134,321183143,321183152,321187407,321187755,321187764,321187773,321187782,321187791,321187808,321187880,321187915,321187924,321187933,321187942,321187951,321187817,321187826,321187844,321187835,321187862,321187899,321187871,321187906,321187960,321187979,321183009,321197977,321187425,321187489,320627121,220417840,310234207,320627256,320596397,320627103,121192155,121192182,320913703,121192388,121333440,121192547,121186849,220151760,320627390,321195540,321195559,321195835,321195577,321195504,321195522,321195531,321195513,321195568,321195498,321195489,321195675,321195666,321195657,321195639,321195648,321195602,321195611,321195595,321195620,321188031,122143457,220152019,121187483,110138271,220471503,320596565,321190634,321190670,321190625,121191557,121192574,220152055,220152046,321190527,321190536,321190518,220152171,220152117,220151975,420813285,220524476,320626710,320622616,320597476,320622787,220151911,220477981,220151895,420652074,321016333,520200461,320592328,121187456,321190108,320909816,321190260,320909852,321190091,320909861,320909898,320909905,320909889,320909914,321190082,320909834,320909870,320909825,320909843,321190242,321190251,420657113,220465841,420656230,220151840,220151877,320910617,321196102,320908041,220151966,321090788,210177690,321197174,421163877,321191394,321191410,420664481,420664524,420664490,220152849,420655703,220461774,321185695,321238264,320623214,121193065,121190139,321276454,121190111,321140706,220152082,420605893,220463558,121187447,220517689,220152260,220576589,320909727,320909736,220516207,220516190,220415012,220152395,321190652,210113250,520138379,321195096,220545952,321183287,320622750,321189888,220152420,220152705,121192342,121191110,122879233,121189711,321195979,220576721,220152670,220152634,220152625,220152643,321189735,321384140,210177841,121193458,121193181,121192976,220152607,220152750,320597083,220152698,321186729,220665340,121203937,121189141,321184446,321184446,321184099,220546639,220546620,220546648,220152233,220601980,220601999,321184302,220152616,421365784,220152803,321193025,122082030,210178500,121203919,321191214,210178047,121188856,320909772,321193301,123193212,220151831,320717168,320717140,220561853,121187795,420662303,220609410,321193944,210177681,420662410,420664203,210177663,121188847,220616288,320984208,321188317,421576299,302260158,121188491,320911340,220676757,421562286,321504555,121190978,321197389,123062078,121190424,220632233,220620576,421380268,121185751,220152876,320909479,310202152,122459375,321087131,321603608,321185828,321128999,321646215,421532755,121191049,321042527,321644538,320912312,220673162,220600446,220673126,421562268,420664739,210178911,321566934,321310951,122698241,321719100,210178225,220522682,321193944,220601695,210178127,321327863,122742835,421562286,121189141,321384337,321383702,220680065,321825959,122869397,321384177,121191432,321184446,121188491
		)

) x
