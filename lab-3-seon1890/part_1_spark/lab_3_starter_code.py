#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''
#Use getpass to obtain user netID
import getpass

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main(spark, netID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{netID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{netID}/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    #print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    #query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    #query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!
    artist_term = spark.read.csv(f'hdfs:/user/{netID}/artist_term.csv')
    artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')
    artist_term.createOrReplaceTempView('artist_term')
 
    #reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
    reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
    reserves.createOrReplaceTempView('reserves')
    
    tracks = spark.read.csv(f'hdfs:/user/{netID}/tracks.csv')
    tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')
    tracks.createOrReplaceTempView('tracks')

    print('question_1_query = sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)')
    rest = spark.sql('select sid, sname, age from sailors where age > 40')
    rest.show()
    
    print('=============================================')
    print("question_2_query = spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')")
    rest1 = reserves.filter(reserves.bid != 101).groupby(reserves.sid).count()
    rest1.show()

    print('=============================================')
    print('Using a single SQL query, how many distinct boats did each sailor reserve? The resulting DataFrame should include the sailors id, name, and the count of distinct boats. (Hint: you may need to use first(...) aggregation function on some columns.) Provide both your query and the resulting DataFrame in your response to this question.')
    
    rest3 = spark.sql('SELECT sailors.sid, first(sailors.sname), COUNT(DISTINCT reserves.bid) FROM sailors FULL JOIN reserves ON reserves.sid = sailors.sid GROUP BY sailors.sid')
    rest3.show()
    
    
    print('=============================================')
    print('question_4_query = Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).')
    rest4 = spark.sql('SELECT artist_term.term, percentile_approx(tracks.year,.5) AS median_year, MAX(tracks.duration) AS max_duration, COUNT(DISTINCT artist_term.artistID) AS artist_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term')
    rest4.show()
    
    rest4_1 = spark.sql('SELECT artist_term.term, percentile_approx(tracks.year,.5) AS median_year, AVG(tracks.duration) AS avg_duration, COUNT(DISTINCT artist_term.artistID) AS artist_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term ORDER BY avg_duration ASC LIMIT 10')
    rest4_1.show()
    
    print('=============================================')
    print('Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term. Modify this query to return only the top 10 most popular terms, and again for the bottom 10. Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response.')
    rest5 = spark.sql('SELECT artist_term.term, COUNT(DISTINCT tracks.trackID) AS distinct_track_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term ORDER BY distinct_track_count DESC LIMIT 10') 
    rest5.show()

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
