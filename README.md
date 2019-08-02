# Known-Projects-Database

This repository contains the SQL and processing steps for creating New York City's Known Projects Database.

## Table of Contents
1. Introduction to Known Projects DB
2. Data sources
3. Prerequisites
4. Methodology
5. Process diagram

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

## Prerequisites
- Obtain DCP Housing Developments database
- Obtain HPD Projected Closings and HPD RFP data from HPD
- Obtain EDC Projected Projects data from EDC
- Download DCP project data
  * Project data
  * Project actions
  * Project BBLs
- Obtain list of polygons create to represent applicant-owned sites in DCP applications from HEIP. Available [here](https://nycplanning.carto.com/u/capitalplanning/dataset/heip_zap_polygons)
- Download **[NYC Zoning Map Amendments](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gis-zoning.page)** - This dataset contains project area polygons for all certified or approved projects seeking a zoning map amendment (ZM action)
- Obtain DCP's imPACT Visualization polygons - This dataset contains the polygons associated with all DCP projects. Because there are accuracy concerns with this dataset, nyzma was used where possible  


