# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

rest = spark.sql('select sid, sname, age from sailors where age > 40')
rest.show()

```


Output:
```

+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

rest1 = reserves.filter(reserves.bid != 101).groupby(reserves.sid).count()
rest1.show()

```


Output:
```

+---+-----+
|sid|count|
+---+-----+
| 22|    3|
| 31|    3|
| 74|    1|
| 64|    1|
+---+-----+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

rest3 = spark.sql('SELECT sailors.sid, first(sailors.sname), COUNT(DISTINCT reserves.bid) FROM sailors FULL JOIN reserves ON reserves.sid = sailors.sid GROUP BY sailors.sid')
rest3.show()

```


Output:
```

+---+------------+-------------------+
|sid|first(sname)|count(DISTINCT bid)|
+---+------------+-------------------+
| 29|      brutus|                  0|
| 22|     dusting|                  4|
| 32|        andy|                  0|
| 31|      lubber|                  3|
| 95|         bob|                  0|
| 71|       zorba|                  0|
| 58|       rusty|                  0|
| 85|         art|                  0|
| 74|     horatio|                  1|
| 64|     horatio|                  2|
+---+------------+-------------------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

rest4 = spark.sql('SELECT artist_term.term, percentile_approx(tracks.year,.5) AS median_year, MAX(tracks.duration) AS max_duration, COUNT(DISTINCT artist_term.artistID) AS artist_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term')
rest4.show()

rest4_1 = spark.sql('SELECT artist_term.term, percentile_approx(tracks.year,.5) AS median_year, AVG(tracks.duration) AS duration_avg, COUNT(DISTINCT artist_term.artistID) as artist_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term ORDER BY avg_duration ASC LIMIT 10')
rest4_1.show()

```


Output:
```

+--------------------+-----------+------------+------------+
|                term|median_year|max_duration|artist_count|
+--------------------+-----------+------------+------------+
|  adult contemporary|       1979|   938.89264|         122|
|   singer-songwriter|       1988|   1595.0624|        1248|
|             melodic|       1999|    896.1824|         341|
|              poetry|       1979|   1270.8044|         100|
|        seattle band|       2005|    274.0763|           1|
|   traditional metal|       1986|   389.66812|          12|
|             lyrical|          0|   1904.7963|         142|
|               anime|          0|    1613.322|          55|
|     swedish hip hop|       2004|    418.9514|           5|
|        german metal|       2004|   553.89996|          21|
|          medwaybeat|          0|   286.35382|           5|
| gramusels bluesrock|       1969|    637.1522|          14|
|electronica latin...|          0|    384.3914|           2|
|         indie music|          0|    416.1824|           9|
|          indigenous|          0|   473.52118|           4|
| symphonic prog rock|          0|   419.73505|           3|
|            dub rock|       2003|   243.17342|           2|
|        haldern 2008|          0|    375.8232|           1|
| persian traditional|          0|   412.31628|           1|
|          micromusic|          0|   384.83545|           1|
+--------------------+-----------+------------+------------+


+----------------+-----------+------------------+------------+
|            term|median_year|      duration_avg|artist_count|
+----------------+-----------+------------------+------------+
|       mope rock|          0|13.661589622497559|           1|
|      murder rap|          0| 15.46403980255127|           1|
|    abstract rap|       2000| 25.91301918029785|           1|
|experimental rap|       2000| 25.91301918029785|           1|
|     ghetto rock|          0|26.461589813232422|           1|
|  brutal rapcore|          0|26.461589813232422|           1|
|     punk styles|          0| 41.29914093017578|           1|
|     turntablist|       1993| 43.32922387123108|           1|
| german hardcore|          0|45.086891174316406|           1|
|     noise grind|       2005| 47.68869247436523|           2|
+----------------+-----------+------------------+------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```python

rest5 = spark.sql('SELECT artist_term.term, COUNT(DISTINCT tracks.trackID) AS distinct_track_count FROM tracks LEFT JOIN artist_term ON artist_term.artistID = tracks.artistID GROUP BY artist_term.term ORDER BY distinct_track_count DESC LIMIT 10') 
rest5.show()


```

  
  
Output:

```
  
  +----------------+--------------------+
|            term|distinct_track_count|
+----------------+--------------------+
|            null|               26179|
|            rock|               21796|
|      electronic|               17740|
|             pop|               17129|
|alternative rock|               11402|
|         hip hop|               10926|
|            jazz|               10714|
|   united states|               10345|
|        pop rock|                9236|
|     alternative|                9209|
+----------------+--------------------+

```


## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  
  Answer:
  Answer: Comparing 2.3 and 2.4, there doesn’t seem to be a clear indication of which one is faster. Specifically, it is hard to tell which one is faster. But 2.3 might be a bit faster compared to 2.4. 
 

  - What did you try in part 2.5 to improve performance for each query?
  Answer:
  For pq_average_income, I sorted the dataframe by “zipcode”, and set the num_partition to 100.
  For pq_max_income, I sorted the dataframe by “last_name” and set the num_partition to 50
  For pq_anna, I sorted the dataframe by “first_name” and “income” and I set the num_partitions to 50.

  I also, I changed the HDFS replication factor to 1. 
  
  - What worked, and what didn't work?
  Answer:
  For pq_average_income, I sorted the dataframe by “zipcode”, and set the num_partition to 100. When I set num_partition to 50, it did not work. 
  For pq_max_income, I sorted the dataframe by “last_name” and set the num_partition to 50. When I set num_partition to 100, it did not work. 
  For pq_anna, I sorted the dataframe by “first_name” and “income” and I set the num_partitions to 50. When I set the num_partition to 100 or 150, it did not work. 


Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/

## Part2.3
Output:

```
| Files             | Size   | Minimum     | Median      | Maximum     |
|-------------------|--------|-------------|-------------|-------------|
| csv_avg_income.py | small  | 0.670438528 | 0.801469326 | 4.605517149 |
| csv_avg_income.py | medium | 1.248895407 | 1.38259387  | 5.524147034 |
| csv_avg_income.py | large  | 7.14955163  | 8.177359581 | 18.40179253 |
| csv_max_income.py | small  | 0.680200577 | 0.784301519 | 4.590170383 |
| csv_avg_income.py | medium | 1.131782055 | 1.290898085 | 5.398142099 |
| csv_avg_income.py | large  | 6.889537573 | 7.299968243 | 23.82436109 |
| csv_anna.py       | small  | 0.090932846 | 0.116205215 | 3.025209665 |
| csv_anna.py       | medium | 0.441916227 | 0.482558489 | 3.428318977 |
| csv_anna.py       | large  | 5.477737904 | 8.2380054   | 72.93558407 |
				
								
```

## Part 2.4
Output:
```
| Files            | Size   | Minimum     | Median      | Maximum     |
|------------------|--------|-------------|-------------|-------------|
| pq_avg_income.py | small  | 0.739653587 | 1.207602024 | 4.679968357 |
| pq_avg_income.py | medium | 0.486644268 | 0.543416262 | 6.220651627 |
| pq_avg_income.py | large  | 4.472256422 | 5.670502901 | 10.5002811  |
| pq_max_income.py | small  | 0.681748867 | 0.85436058  | 3.567991018 |
| pq_avg_income.py | medium | 3.789521456 | 4.572562218 | 15.98734546 |
| pq_avg_income.py | large  | 5.138335705 | 5.897226334 | 11.31412458 |
| pq_anna.py       | small  | 0.107645988 | 0.08465457  | 2.880532742 |
| pq_anna.py       | medium | 0.141239166 | 0.162191629 | 1.86583972  |
| pq_anna.py       | large  | 2.325033426 | 2.426226854 | 5.493896008 |
```

## Part 2.5: Sorting Dataframe
Output:
```
| Files            | Size   | Minimum     | Median      | Maximum     |
|------------------|--------|-------------|-------------|-------------|
| pq_avg_income.py | small  | 0.693088531 | 0.779372692 | 3.588765621 |
| pq_avg_income.py | medium | 0.901768923 | 1.440871477 | 3.975461721 |
| pq_avg_income.py | large  | 3.426688433 | 4.920966625 | 9.708969831 |
|                  |        |             |             |             |
| pq_max_income.py | small  | 0.45204668  | 0.601641464 | 2.085616112 |
| pq_avg_income.py | medium | 1.079087496 | 3.085616112 | 9.739153385 |
| pq_avg_income.py | large  | 4.239731073 | 6.02065134  | 10.63037252 |
|                  |        |             |             |             |
| pq_anna.py       | small  | 0.089650393 | 0.112985849 | 1.764534235 |
| pq_anna.py       | medium | 0.081457748 | 0.139461899 | 1.847441196 |
| pq_anna.py       | large  | 0.177967386 | 0.095187664 | 3.103428841 |
```
## Part 2.5: replication factor 1
Output:
```
| Files            | Size   | Minimum     | Median      | Maximum     |
|------------------|--------|-------------|-------------|-------------|
| pq_avg_income.py | small  | 0.54961257  | 0.717927217 | 2.099341631 |
| pq_avg_income.py | medium | 0.632803583 | 0.803679943 | 0.380200577 |
| pq_avg_income.py | large  | 2.625798702 | 3.3816607   | 7.138874769 |
| pq_max_income.py | small  | 0.307832098 | 0.49565835  | 1.033040762 |
| pq_avg_income.py | medium | 1.248895407 | 1.548335314 | 5.31272459  |
| pq_avg_income.py | large  | 3.661766291 | 7.736559629 | 8.452640533 |
| pq_anna.py       | small  | 0.090932846 | 0.094382763 | 0.146310091 |
| pq_anna.py       | medium | 0.051406217 | 0.452557325 | 1.641829491 |
| pq_anna.py       | large  | 0.252228832 | 1.279892683 | 5.245244741 |
```


