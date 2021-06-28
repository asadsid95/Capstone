This is the capstone project for Udacity's Data Engineering Nanodegree.

Leading up to this, following objectives using various technologies were achieved:
- Defining data models and creating ETL pipelines for PostgreSQL & Cassandra databases
- Creating data warehouses and lakes using Amazon Redshift & S3
- Processing data using Apache Spark
- Automating, monitoring and scheduling workflow jobs using Apache Airflow

**The objective of this project** is to create an ETL pipeline that includes data processing, storage & warehousing using Apache Spark, AWS S3 & Redshift

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
- *Airport Code Table

Pipeline's work benefits a real estate developer via analytics tables. 

**2. Explore and Assess the Data

Attributes of interest: 
    i. blank rows or cells
    ii. consistent value - but how about gender? Showing skew in col may be valuable 
    iii. Column empty > 50%
    iv. Changing precision in year number, 

Steps for cleaning:
- 

**3. Data Model

Star Schema consists of 4 dimension tables for OLAP. **-Why:

- Dimension
    - col1, col2,...

- Dimension
    - col1, col2,...

- Dimension
    - col1, col2,...

- Dimension
    - col1, col2,...

- **Fact**
    - col1, ...

**4. Run ETL to Model the Data

