import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

import pandas as pd
import datetime
from pyspark.sql.functions import count, when, isnull, udf, col, dayofmonth, year, month, monotonically_increasing_id


def dimen_time_table(cleaned_immig_data):

    '''
    Creates dimension table for time
    '''
    
    get_timestamp = udf(lambda x : (datetime.date(1960, 1, 1) + datetime.timedelta(days=x)).isoformat() if x else none)
    
    formatted_arrdate = cleaned_immig_data.withColumn('timestamp', get_timestamp(cleaned_immig_data.arrdate))
    #df.show(3)
    
    dimen_time_table = formatted_arrdate.select(\
        col("timestamp").alias('day_of_arrival'),
        year("timestamp").alias('year'),
        month('timestamp').alias('month'),
        dayofmonth('timestamp').alias('day'))
    
    return dimen_time_table

def dimen_state_table(cleaned_demo_data):
    
    '''
    Creates dimension table for US States
    '''
    
    dimen_state_table = cleaned_demo_data.withColumn('id', monotonically_increasing_id())
    
    dimen_state_table = dimen_state_table.select(['id','City', 'State', 'State Code', 'Median Age', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Average Household Size'])
    
    dimen_state_table = dimen_state_table.withColumnRenamed('City', 'city') \
                        .withColumnRenamed('State','state') \
                        .withColumnRenamed('State Code', 'state_code') \
                        .withColumnRenamed('Median Age', 'median_age') \
                        .withColumnRenamed('Male Population', 'male_population') \
                        .withColumnRenamed('Female Population', 'female_population') \
                        .withColumnRenamed('Total Population', 'total_population') \
                        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
                        .withColumnRenamed('Foreign-born','number_of_foreign_born') \
                        .withColumnRenamed('Average Household Size','avg_household_size')
    
    return dimen_state_table
    
def dimen_person_table(cleaned_immig_data):
    
    '''
    Creates dimension table for persons arriving in US
    '''
    
    dimen_person_table = cleaned_immig_data.select(['cicid', 'i94bir', 'admnum', 'i94cit', 'i94res', 'i94mode', 'i94mon', 'visatype'])
    
    dimen_person_table = dimen_person_table.withColumn('id', monotonically_increasing_id())
    
    dimen_person_table = dimen_person_table.select(['id', 'cicid', 'i94bir', 'admnum', 'i94cit', 'i94res', 'i94mode', 'i94mon', 'visatype'])

    return dimen_person_table

def fact_table(cleaned_immig_data):
    
    '''
    Creates fact table for arrival events
    '''
    
    fact_table = cleaned_immig_data.select(['cicid', 'arrdate', 'depdate', 'i94mode', 'i94addr','i94mon']) # need to add column with duration of visit/residence, in days

    ### Returning the following table with added column caused 'Java Runtime error'
    #get_timestamp = udf(lambda x : (datetime.date(1960, 1, 1) + datetime.timedelta(days=x)).isoformat() if x else none)
    #formatted_arrdate = fact_table.withColumn('timestamp', get_timestamp(fact_table.arrdate))
    
    return fact_table