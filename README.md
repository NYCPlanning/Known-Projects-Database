# Known-Projects-Database

This repository contains the SQL and processing steps for creating New York City's Known Projects Database.

## Table of Contents
1. Introduction to Known Projects DB
2. Data sources
3. Prerequisites
4. Methodology

## Introduction to Known Projects DB
- The Known Projects DB contains information on future housing starts. It gathers data from 9-11 data sources (depending on use case, see below), compares the information in each data source to ensure that units existing in multiple sources are not counted more than once, and then aggregates these sources. It incorporates granular information at the project-level, including project statuses, estimated project phasing, and borough planner input on projects where available.
- The Known Projects DB presents housing information at the project-level. Therefore, it can be used for growth planning citywide as well as granular looks at neighborhood-level expected growth.
- The Known Projects DB is currently used to create the School Construction Authority's (SCA) Housing Pipeline.
- **Disclaimer** - This information does NOT represent a housing projection produced by DCP, nor can DCP attest to the certainty that each of these developments will lead to future housing starts
- **Disclaimer** - The Known Projects DB does not represent future as-of-right growth nor future growth from projects that have not yet materialized in the data sources below.

## Data Sources
### Primary Data Sources
- **[DCP Housing Developments Database](https://github.com/NYCPlanning/db-developments)** - This database is created by DCP using DOB permit and certificate of occupany data. It includes permits and applications for new buildings, building alterations, and demolitions.
- **HPD Projected Closings** - This is **confidential** data on HPD's pipeline of closings on privately-owned sites. Provided by HPD.
- **HPD Request for Proposals** - City-owned sites for which HPD has issued or designated RFPs for residential development. Provided by HPD.
- **EDC Projected Projects** - Projected projects expected by EDC to develop residential units with reasonable certainty. Provided by EDC.
- **DCP Applications**- Discretionary actions through DP that facilitate residential development. This data is generated by DCP. Several processing steps are required to identify projects facilitating residential development.
- **Empire State Development Projected Projects**- Known residential development projects by Empire State Development. These projects are collected by DCP.
- **Neighborhood study affordable housing commitments**- Affordable housing commitments made by the current administration in neighborhood rezoning areas. These are collected from each adopted neighborhood study's Points of Agreement.
- **Future City-Sponsored RFPs/RFEIs**- Additional future Request for Proposals or Requests for Expressions of Interest for affordable housing provided by City Hall. Note that many of these can be highly speculative conversations with developers, and not comprehensively provided by DCP planners.
- **DCP Planner-Added Projects**- Additional projects identified by the Department of City Planning borough planners which are not yet in the above data sources. Note that many of these can be highly speculative conversations with developers, and not comprehensively provided by DCP planners.
### Secondary Data Sources
- **Neighborhood study projected developments**- The count of units each adopted neighborhood study from the current administration is expected to develop. These projections are not site-specific, and are highly speculative. **These projects should not necessarily be included for planning purposes, depending on need.**
- **Future neighborhood studies**- The count of units each future neighborhood study is projected to develop. These projections are not site-specific, and are highly speculative. Because these rezonings have not yet been adopted, we include a certainty discount factor, and we do not deduplicate. **These projects should not necessarily be included for planning purposes.**

####Diagram
![sources](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/Capture.PNG)


### Prerequisites
- Obtain DCP Housing Developments database
- Obtain HPD Projected Closings and HPD RFP data from HPD
- Obtain EDC Projected Projects data from EDC
- Download DCP project data
  * Project data
  * Project actions
  * Project BBLs
- Obtain list of polygons create to represent applicant-owned sites in DCP applications from HEIP. Available **[here]**(https://nycplanning.carto.com/u/capitalplanning/dataset/heip_zap_polygons)
- Download **[NYC Zoning Map Amendments](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gis-zoning.page)** - This dataset contains project area polygons for all certified or approved projects seeking a zoning map amendment (ZM action)
- Obtain DCP's imPACT Visualization polygons - This dataset contains the polygons associated with all DCP projects. Because there are accuracy concerns with this dataset, nyzma was used where possible
- Research Empire State Development projects online and receive project input from to HEIP, borough planners
- Review Points of Agreement from adopted neighborhood studies
- Receive new list of future city-sponsored RFPs/RFEIs
- Perform borough office outreach to receive DCP Planner-Added Projects
- Perform borough office outreach to identify updates to DCP applications, Empire State project, affordable housing commitments, and future city-sponsored RFPs/RFEIs

## Limitations and future improvements
- **Limitations**
 *  Exercise discretion when deciding how to use inputs because data inputs vary in level of certainty. Projects within data inputs also   vary in level of certainty.
    - Higher certainty
      - DOB permit issued & permit apps - Historical analysis suggest 95% of permits issued are completed within 5 years and 75% of permit    apps in-progress or filed are completed within 5 years. While 2/3 of permit apps are disapproved or withdrawn in the first year, that figure drops sharply by year 2, averaging ~10-15% overall
      - HPD-financed projects - Projected projects listed are expected to close financing by FY2024
      - HPD RFPs - Projects listed are expected to be built by 2025
      - EDC-sponsored projects - Projected projects listed are expected to be complete given agreement with EDC
      - Empire State Development Projected Projects: These are public and planned projects.
      - Neighborhood Study Affordable Housing Commitments: These are administration commitments.
    - Lower certainty
      - DCP approved applications - Applicant may decide to not pursue proposed project or change use
      - DCP active and on-hold applications - Application may be on-hold, withdrawn, disapproved, or change use
      - Future City-Sponsored RFPs/RFEIs - These projects include highly speculative conversations with developers.
      - DCP Planner-Added Projects - These projects include highly speculative conversations with developers.
    - Lowest certainty
      - Neighborhood study projected developments - These are not known projects, nor are they site-specific.
      - Future neighborhood studies - These are neighborhood studies which may or may not be adopted.
      
## Methodology

### 1. Identify housing projects to be included from each data source

| Step  | Script | Description |
| :--- | :--- | :--- |
| DOB | [1a_Relevant_Projects_DOB.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1a.%20Relevant_Projects_DOB.sql) | Filter [DCP Housing Development Database](https://github.com/NYCPlanning/db-developments) to relevant residential jobs. Jobs completed before 2010 are excluded. Inactive jobs are included. Redefine project statuses where necessary. |
| HPD | [1b_Relevant_Projects_HPD.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1b.%20Relevant_Projects_HPD.sql) | Aggregate HPD projected closings and HPD RFPs. |
| EDC | [1c_Relevant_Projects_EDC.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1b.%20Relevant_Projects_EDC.sql) | Clean EDC data and add geometries. |
| DCP Applications -- DB Building | [1d_(iv). New_Relevant_Projects_DCP_Building.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1d_(iv).%20New_Relevant_Projects_DCP_Building.sql) | Consolidating DCP applications from various Zoning Application Portal exports. Adding geometries for all projects using previous HEIP work, NYZMA, imPACT, and PLUTO. | 
| DCP Applications -- Flagging | [1d_(v). New_Relevant_Projects_DCP_Flagging.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1d_(v).%20New_Relevant_Projects_DCP_Flagging.sql) | Flagging DCP applications based on characteristics including residential units, senior units, pre-2012 projects, and more. | 
| DCP Applications -- Filtering | [1d_(vi). New_Relevant_Projects_DCP_Filtering.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1d_(vi).%20New_Relevant_Projects_DCP_Filtering.sql) | Omit non-residential DCP applications and DCP applications from before 2012. Consolidate the many ZAP unit count fields into 1 "Total Units" field. Deduplicate applications such as Special Permit Renewals and District Improvement Bonuses.  | 
| DCP Applications -- Planner Mods | [1d_(vii). New_Relevant_Projects_DCP_Planner_Mods.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1d_(vii).%20New_Relevant_Projects_DCP_Planner_Mods.sql) | After receiving planner inputs, this step updates the filtered DCP applications dataset above. Projects can be removed or added based on updated characteristics from the planners. Unit counts and phasing may be added as well. | 
| Neighborhood Studies | [1e. Relevant_Projects_Neighborhood__Studies.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1e.%20Relevant_Projects_Neighborhood__Studies.sql) | Consolidating neighborhood studies projects, both rezoning commitments and projected developments sites from DCP's Reasonable Worst Case Development Scenario. | 
| Future City-Sponsored RFPs/RFEIs | [1f. Relevant_Projects_Public_Sites.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1f.%20Relevant_Projects_Public_Sites.sql) | Identifying future city-sponsored RFPs/RFEIs which haven't been manually identified in any other data sources. Limiting to these projects. | 
| Planner-Added Projects | [1g. Relevant_Planner_Added_Projects.sql](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/1g.%20Relevant_Planner_Added_Projects.sql) | Importing DCP planner-added projects and limiting to those which are not already included in the other data sources. | 


### 2. Deduplicate projects across each data source

Projects are deduplicated across each source using 4 types of matching, where data fields allow.

1. Address-matching (including parsing project names)
2. BBL-based matching
3. Matching by spatial overlap
4. Matching by spatial proximity (matches by spatial proximity are then reviewed to determine if they are accurate. The vast majority of proximity-based matches are inaccurate.)


### 3. Calculate incremental units count for pipeline

Once sources are deduplicated, incremental units for each project are calculated. If a project is not matched in any other sources, its full unit count is added to the pipeline. If a project is matched with other sources, it's full unit count is subtracted by the unit counts of projects to which it matches, from sources closer to materialized units. Diagram provided below.

A project's characteristics will change as it progresses, and this is reflected if it appears in multiple data sources. For instance, unit count estimates may change. We assume that if >80% of a project's listed new units have appeared in a source which indicates it is closer to materializing, we zero that project's contribution to our pipeline. 

![Materialization Diagram](https://github.com/NYCPlanning/Known-Projects-Database/blob/master/Capture1.PNG)

### 4. Borough planner inputs 

After steps 1-3, borough planners are solicited for input on developments with large unit counts, developments which have not updated their information despite registering before 2016, and developments with uncertain unit counts.

Borough planners clarified the information above. They also comprehensively reviewed projects to confirm residential status and deduplication results. They have also provided build year estimates (by 2025, 2026-2035, 2036-2055) where available.

### 5. Build year assignments
Build year assumptions on developments are made using the following hierarchy:

1. Borough planner inputs, reviewed and normalized by HEIP and Capital Planning
2. DOB jobs: See HEIP phasing methodology
3: HPD Projected Closings: By 2025 unless indicated otherwise by planner
4. HPD RFPs: By 2025 unless indicated otherwise by planner or HPD
5. EDC: Build year provided by EDC
6. DCP Applications: See: https://nyco365.sharepoint.com/:w:/s/NYCPLANNING/cp/Ecv9D-Jo2_5Hl_myliKnekcBO3Gr3hmBOPVNfeqAH3oE5A?e=Yr1NcV
7. ESD Projects: Borough planner inputs
8: Neighborhood Study Affordable Housing Commitments: Individual reviews based on the project. A mix of borough planner inputs and 
Capital Planning normalization.
9. Future City-Sponsored RFPs/RFEIs: Borough planner inputs
10. Planner-Added Projects: Placed in 2036-2055 unless otherwise indicated by borough planner, normalized by Capital Planning.

### 6. Group quarters and assisted living homes are excluded

