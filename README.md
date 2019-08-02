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

## Data Sources
- **[DCP Housing Developments Database](https://github.com/NYCPlanning/db-developments)** - This database is created by DCP using DOB permit and certificate of occupany data
