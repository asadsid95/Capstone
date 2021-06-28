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

config = configparser.ConfigParser()
config.read('s3.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# To upload files from local directory to S3, ill need to use boto3


#could be a function

# immigration_df=spark.read.load('./sas_data')
# temp = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv', header=True)
# airport_codes = spark.read.csv('airport-codes_csv.csv',header=True, inferSchema=True)
# cities_demo = spark.read.csv('us-cities-demographics.csv', header=True,sep= ';')

def col_cleaner(df):
    
    df_count = df.count()

    # Eliminate columns that have less than 50% population
    col_pop = df.select([(count(when(isnull(c), c)) / df_count).alias(c) for c in df.columns])
    #col_pop.show(n=1,vertical=True, truncate=False)
    
    col_to_elimin = [key for (key, value) in col_pop.head().asDict().items() if value > 0.5]
    #print(col_to_elimin)

    if col_to_elimin:
        cleaned_df = df.drop(*col_to_elimin)
        #print("after drop with col listed ")
        cleaned_df = cleaned_df.dropDuplicates()
        #print(cleaned_df)
        
        cleaned_df = cleaned_df.dropna(how='all')
        #print(cleaned_df)
        return cleaned_df
    else:
        cleaned_df = df.dropDuplicates()
        #print(cleaned_df)
        
        cleaned_df = cleaned_df.dropna(how='all')
        #print(cleaned_df)
        return cleaned_df
    
# Cleaning the data
def transform_immigration_data(spark, immig_df):
    
    #immig_df=spark.read.load('./sas_data') 
    ######## See project resources for ACTUAL path for complete dataset; Format: 
    
    # returns data w/o duplicates, empty rows, and fewer columns
    cleaned_immig_data = col_cleaner(immig_df)
    
    ### Create F/D tables 
    
    # Dim Time table

    dimen_time_table = data_modeling.dimen_time_table(cleaned_immig_data)
    #dimen_time_table.show(3)
    #### Now I'll need to write that out to storage
    
    # Dim Persons table
    
    dimen_person_table = data_modeling.dimen_person_table(cleaned_immig_data)
    dimen_person_table.show(3)
    
    data_modeling.fact_table(cleaned_immig_data).show(3)    
    
    
    #dimen_visa_table = data_modeling.dimen_visa_table(cleaned_immig_data)
    #dimen_visa_table.show(10)
    
def transform_demographic_data(spark, demo_df):
   
    #demo_df = spark.read.csv('us-cities-demographics.csv',header=True, sep=';')
    
    cleaned_demo_data = col_cleaner(demo_df)
    
    # Column for state code is added to cities dataset 
    # and then joined with immig data using state code
    
    dimen_state_table = data_modeling.dimen_state_table(cleaned_demo_data) 
    #dimen_state_table.show(3)
    
    
def main():

    # Spark session
    spark = SparkSession.builder.\
        getOrCreate()
    
    immig_df=spark.read.load('./sas_data')
    demo_df = spark.read.csv('us-cities-demographics.csv',header=True, sep=';')
    
    transform_immigration_data(spark, immig_df)
    transform_demographic_data(spark, demo_df)
    
    
if __name__ == "__main__":
    main()