## BIXI Data Lake and Warehouse 

### Project Scope

- Steps taken for the project
  - Download dataset from Kaggle and open data
    - https://www.kaggle.com/aubertsigouin/biximtl
    - https://montreal.bixi.com/en/open-data
    - https://api-core.bixi.com/gbfs/en/station_information.json
  - Store raw csv and json data in S3 buckets
    - OD_<YEAR>.csv for example OD_2014.csv
    - stations.json
  - Create a data model and schema design for ETL 
    - Excel spreadsheet which lists tables, columns and datatypes
    - Dimensional model showing trips, station, calendar and time tables with relationships
  - Code logic to clean data, convert to parquet and dump in S3 buckets
    - Jupyter notebook used to create POC
    - Python scripts used as tasks by airflow
  - Manual steps to create Glue Crawlers to create data catalogue
  - Create scripts for external schema in redshift to which points to Glue Data Catalog
  - Create scripts for dimension and fact tables in redshift adhering to data model and schema
  - Write ETL logic to populate fact and dimension tables on a schedule
  - Automate the process using Airflow
    - Create dags script
    - Create task scripts for data cleaning and parquet conversion
    - Create task script to execute ETL operations on redshift
    - Create airflow variables
    - Other helper functions

- Purpose of Final Data Model
  - Data model to report on trip durations sliced by month/period of day/year
  - Performance comparison of stations
  - YoY growth of trips

### Scenario

- Case where Data was increased by 100x
  - Converting data to parquet (columnar format to save storage and improve query performance)
  - Improve ETL query performance by using dist-keys and sort-keys for trips fact table
  - Use dist style as *all* for dimension tables to improve join performance
  - Boost airflow cluster memory
- Pipelines would run on a daily basis by 7 am everyday
  - Use airflow variables to add schedule variable which can be set directly from airflow web interface without needing to change the code
- The database needs to be accessed by 100 people
  - Use Redshift workload management to improve query queuing 

### Defending decisions

- Choice of tool and tech

  - Jupyter Notebook for proof of concept
  - Airflow for job monitoring and orchestration
    - Requires creation of dags with use of airflow variable for the following
      - Start Date
      - Historic/Incremental dag runs
      - Schedule 
      - Catch up
    - Python script for airflow tasks
  - S3 for data-lake with CSV/Json data transformed to parquet formats
  - Redshift as data-warehouse for reporting purpose
  - Sublime text as script editor

- Data Model

  - STAR Schema approach with the followings tables

    Fact Table

    - fact_trips (data for all the trips)

    Dimension Tables

    - dim_station (data for general station info)
    - dim_calendar (calendar data for day, month, year, holidays, week)
    - dim_time (time data for hours, minutes, period (day, night, afternoon), happy hours etc)

## Execution

### Project Code

- Clean coding standards
  - Use of pep-8 coding standards for python code
  - Use proper code syntax for SQL
- Modular
  - Create functions where ever possible for python scripts
  - Use of common table expressions and temp tables for ETL

### Quality Checks

- At least two data quality checks
  - Count of records is always greater than 0
  - Null counts

### Data Model

- ETL process result in data model outlined in write up
- Data Dictionary of Final Data Model is included
- The Data model is appropriate for identified purpose

### Dataset

- At least 2 data sources
  - Stations json (From Kaggle)
  - Trips csv
  - Stations api (From open data)
- More than a million lines of Data
  - Trips CSV include over 3.2 Million rows for an year

### Schedule

- Hourly
- Daily (default)
- Monthly

### Credentials Management

- Airflow will need S3 and Redshift credentials to access and process data
- S3 access keys and redshift dw credentials can be securely saved in Airflow
- Airflow encrypts passwords internally making it safe
- Alternate option could be using AWS secrets manager

