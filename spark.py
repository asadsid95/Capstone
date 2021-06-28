import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

import pandas as pd
from pyspark.sql import SparkSession

'''
Attributes:
    spark (type: class SparkSession): creates Spark session 
    
    immigration_df (type: Dataframe): returns DataFrame 
'''
#to be a seperate function
spark = SparkSession.builder.\
        getOrCreate()

print(type(spark))

#could be a function
immigration_df=spark.read.load('./sas_data')

def main():
    
    try:
        immigration_fact_table = immigration_df.select(["i94mon","biryear"]).show()
        print(type(immigration_fact_table))
        #immigration_fact_table.write.parquet('/',)
    
    except:
        print(3)

if __name__ == "__main__":
    main()