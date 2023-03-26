#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    small = spark.read.csv("hdfs:/user/bm106/pub/people_small.csv",header=True,schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    small.write.parquet("hdfs:/user/sy3420/people_small.parquet")
    medium = spark.read.csv("hdfs:/user/bm106/pub/people_medium.csv",header=True,schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    medium.write.parquet("hdfs:/user/sy3420/people_medium.parquet")
    large = spark.read.csv("hdfs:/user/bm106/pub/people_large.csv",header=True,schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    large.write.parquet("hdfs:/user/sy3420/people_large.parquet")




# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
