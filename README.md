# Insights from 2016 US Immigration data

This is the capstone project for Udacity's Data Engineering Nanodegree.

Leading up to this, following objectives using various technologies were achieved:
- Defining data models and creating ETL pipelines for PostgreSQL & Cassandra databases
- Creating data warehouses and lakes using Amazon Redshift & S3
- Processing data using Apache Spark
- Automating, monitoring and scheduling workflow jobs using Apache Airflow

**The objective of this project** is to create an ETL pipeline that includes data processing and storage in data lake using Apache Spark & AWS S3.

This project is broken into 4 steps:
1. Scope the Project and Gather Data
2. Explore and Assess the Data
3. Define the Data Model
4. Run ETL to Model the Data

---------------------------------------

**1. Scope the Project and Gather Data

Defining the datasets:
- *U.S. Immigration Data (2016)
- *U.S City Demographic Data

Benefit from Pipeline:
- Gaining insight into visitors' flow through US on state-level throughout 2016. 

End use case:
- Data prepared will used in analytics tables to assist in determining whether managing short-term rentals is worth expanding.

**2. Explore and Assess the Data

Issues to be targeted in this iteration"
    i. Remove columns empty > 50%
    ii. Remove blank records
    iii. Remove duplicate values

Steps for cleaning:
Count each column's record with filter for null values. Create a list of column names that have more than 50% null population that'll be used to drop those columns.
 
**3. Data Model

*See **Capstone_ERD** diagram*

To pipeline data, immigration dataset is used for fact, time and persons dimensions table's attributes

For states dimension table, US cities demographic data is loaded and columns are selected  

All datasets can be found in S3 bucket. 
Spark is run locally (potential for Future work - See bottom of file)
Final storage is in parquet files.

Star Schema consists of 3 dimension tables for OLAP. Note that all 3 data quality issues are resolved prior to placement into tables. 

** Why?
Immigration/arrival event and relevant details are in fact table as it serves like a business event in quantifiable metrics while contextual metrics like time, details of individuals, and city-level information about the states are placed in dimensions table. This carries potential for future work such as  adding real estate data for cities, to further enhance decision-making.

** Data model: 

Name of table: time
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & month
Distinct attribute: time_id

    - time_id  -- table-specific Unique ID
    - day of arrival -- Date of arrival
    - year -- Year of arrival
    - month -- Month of arrival
    - day -- Day of arrival

Name of table: states
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): state_code
Distinct attribute: id

    - id -- table-specific Unique ID
    - city 
    - ctate
    - state_code
    - median_age
    - male_population
    - female_population
    - total_population
    - number_of_veterans
    - foreign_born
    - average_household_size

Name of table: persons
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): i94mon
Distinct attribute: id

    - id -- table-specific unique ID
    - cicid -- ID for immigration records
    - i94bir -- Age of individual
    - admnum -- Admission number (unaware of context; research requried)
    - i94cit -- Citizen of Country
    - i94res -- Resident of Country
    - i94mode -- Mode of transportation used to arrive in US
    - i94mon -- Month of arrrival
    - visatype -- Class of admission legally admitting the non-immigrant to temporarily stay in U.S

Name of table: arrival_events (**Fact**)
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): month
Distinct attribute: cicid

    - cicid -- table-specific Unique ID
    - arrdate -- Date of arrival
    - depdate -- Date of departure
    - i94mode -- Mode of transportation used to arrive in US
    - i94addr -- US State of arrival
    - i94mon -- Month of arrrival

**4. Run ETL to Model the Data

## How to run the scripts

To run this project:

*It is assumed that IAM role have been created, and Access and Secret key are listed in s3.cfg.

Apache Spark was not used on AWS EMR. *It was used locally.

1. Run main.py in terminal using 'python main.py'

## Write up:

Background on Client: Real Estate property management company operating on national level in United States of America. 

*Client's requirement* orginates from deliberating on expanding into managing real-estate properties for short-term rentals (currently managing condominiums and housing estates).

**To assist, client wants insights about immigration frequency (i.e. visitation to US throughout year), duration (i.e. how long stays/visits are throughout year) and patterns (i.e. surges in stays/visits through various seasons)**

With current data model, following insights can be queried for:
- Duration of stay throughout year, by state
    -- Attributes: arrdate & depdate's year/month/date, state/i94addr
    
- Specific months where duration of stay is short/long (i.e. summer vs winter)
    -- Attributes: arrdate & depdate's year/month/date, state/i94addr
    
- Frequency of arrivals throughout year, by state
    -- Attributes: difference between arrdate - depdate, state/i94addr, aggregate by count of people each month
    
- Surges in visitation in region-specific states (i.e. coastal) throughout year
    -- Attributes: aggregate by count of arrdate, states (grouped by regions)

- Demographic information about various cities in states
    -- Attributes: i94addr/state
    
Pipelines loads data into AWS S3, as star schema with 3 dimension tables (time, states, and persons).
Data recorded is composed of '2016 US Visitor Arrivals' records and US cities' demographics (with population >= 65,000).

Assumptions:
- Immigration dataset is bounded to year 2016
- US Demographics information from 2015 is assumed to be similar for 2016

Although Apache Spark (Spark) is also being used locally in the project, it could be used through EMR, to use clusters and further leverage parallelization.

Apache Airflow (Airflow) can be incorporated to process batches of data (using partitions) when obtaining data from S3 for processing with Spark. Furthermore, if AWS Redshift is used, it can be used to transport data from the data lake.

Pipelines loads data into AWS S3, as star schema with 3 dimension tables.
S3 is used to reduce operational cost and effort of obtaining and maintaining storage, as well as leveraging availability. It also connect in Spark easily.
Spark is chosen for its processing ability of large datasets as well as in expectation that volume will increase.

Data, specifically immigration dataset should be updated on monthly-basis as US Visitor Arrivals data's update frequency is monthly. 

Expected issues:
- Data volume increasing 100x: Leveraging AWS EMR for processing will be necessary as well as scheduling using Airflow. This would enable parallelizing to complete DAG tasks to be completed on schedule effectively. Airflow's periodic retrival of data would also allow debugging in case pipeline breaks.

- Pipelines running periodically at specified time - Use of Airflow is encouraged however integration it with Spark needs to be explored (Airflow + Spark + Apache Livy was suggested).

- Data lake to be accessed by 100+ people: Cost budget would need to be set and monitored closely on S3. Scaling up on EC2 would also need to be explored and modified to scale computing power. Source of truth tables should also be explored as dependency on data is increased and critical. Use of Redshift may be helpful for analysis  if INSERT/UPDATE  needs to be performed to the data. Otherwise, data can be stored in NoSQL server such as Cassandra.

### Further work:
- More data quality checks for attributes
- Change of data formats (float -> int, string ->  int, etc)
- Adding data for real-estate information on city level
- Adding Airflow for scheduling of processing (use of Airflow to extract data from S3, process through Spark, and maybe put back into data lake using Airflow - check viability before pursuing)
