import os
import configparser

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

import pandas as pd
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, isnull, udf, col, dayofmonth, year, month

# File for pipeline
import data_modeling

# File for data quality checks
from data_quality import diff_columns, diff_records

config = configparser.ConfigParser()
config.read('s3.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def col_cleaner(df):  
    
    '''
    Eliminates columns that have more than 50% of cells population as empty, duplicate records and empty records
    '''
    
    df_count = df.count()

    # Eliminate columns that have less than 50% population
    col_pop = df.select([(count(when(isnull(c), c)) / df_count).alias(c) for c in df.columns])
    
    col_to_elimin = [key for (key, value) in col_pop.head().asDict().items() if value > 0.5]

    if col_to_elimin:
        cleaned_df = df.drop(*col_to_elimin)
        cleaned_df = cleaned_df.dropDuplicates()
        cleaned_df = cleaned_df.dropna(how='all')
  
        return cleaned_df
    else:
        cleaned_df = df.dropDuplicates()
        cleaned_df = cleaned_df.dropna(how='all')

        return cleaned_df
    
# Cleaning the data
def transform_immigration_data(spark, cleaned_immig_data):
    
    '''
    Cleans data for dimension tables and fact table; 'time', 'person', 'arrival_event'
    Creates dimension tables and fact table and uploads to S3, partitioned by respective attributes
    '''
           
    ### Create F/D tables 
    
    # Dim Time table

    dimen_time_table = data_modeling.dimen_time_table(cleaned_immig_data)

    dimen_time_table.write.partitionBy('year','month').parquet('s3a://capstone23/time', "overwrite")

    # Dim Persons table
    
    dimen_person_table = data_modeling.dimen_person_table(cleaned_immig_data)
    
    dimen_person_table.write.partitionBy('i94mon').parquet('s3a://capstone23/person', "overwrite")
        
    fact_arrival_event_table = data_modeling.fact_table(cleaned_immig_data)   
    
    fact_arrival_event_table.write.partitionBy('i94mon').parquet('s3a://capstone23/arrival_event', "overwrite")
    
def transform_demographic_data(spark, cleaned_demo_data):
    
    '''
    Cleans data for dimension table 'state'
    Creates dimension table and uploads to S3, partitioned by attribute 'state'
    '''
        
    dimen_state_table = data_modeling.dimen_state_table(cleaned_demo_data) 
    
    dimen_state_table.write.partitionBy('state').parquet('s3a://capstone23/state', "overwrite")
        
def main():

    '''
    Creates SparkSessions
    Reads and loads datasets to be used for data modeling
    Calls function for cleaning datasets
    Calls functions for transforming datasets into dimension tables and fact table
    Calls functions for data quality checks
    
    '''
    
    # Spark session
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    
    immig_df = spark.read.load('./sas_data')
    demo_df = spark.read.csv('us-cities-demographics.csv',header=True, sep=';')
    
    cleaned_immig_data = col_cleaner(immig_df)
    cleaned_demo_data = col_cleaner(demo_df)
    
    transform_immigration_data(spark, cleaned_immig_data)
    transform_demographic_data(spark, cleaned_demo_data) 
       
    # Data quality checking for immigration datasets
    diff_columns(immig_df, cleaned_immig_data)
    diff_records(immig_df, cleaned_immig_data)
    
    # Data quality checking for demographic datasets
    diff_columns(demo_df, cleaned_demo_data)
    diff_records(demo_df, cleaned_demo_data)
    
if __name__ == "__main__":
    main()