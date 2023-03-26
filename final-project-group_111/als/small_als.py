'''
$ spark-submit small_als.py bt2227 10 0.1
'''

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import sys

def main(spark, netID, rank, reg):
    rate = spark.read.parquet(f'hdfs:/user/bt2227/train_small.parquet')
    movies = spark.read.parquet(f'hdfs:/user/bt2227/small.parquet')
    rate.createOrReplaceTempView('rate')
    movies.createOrReplaceTempView('movies')
    combine = spark.sql('SELECT rate.userId, rate.rating, movies.movieId FROM rate LEFT JOIN movies ON rate.movieID = movies.movieId')
    als = ALS(rank=rank, maxIter=5, seed=3, regParam=reg, userCol='userId', itemCol='movieId', ratingCol='rating', coldStartStrategy="drop")
    model = als.fit(combine)
    path = f'hdfs:/user/{netID}'
    ptemp = "/ALS_small_rank_{}_reg_{}".format(rank, reg)
    pals = ptemp + "/als"
    als.save(path + "/models" + pals)
    pmodel = ptemp + "/als_model"
    model.save(path + "/models" + pmodel)


if __name__ == "__main__":
    memory = "8g"
    spark = SparkSession.builder.appName("ALS Small").config("spark.executor.memory", memory).config("spark.driver.memory", memory).getOrCreate()
    netID = sys.argv[1]
    rank = int(sys.argv[2])
    reg = float(sys.argv[3])
    main(spark, netID, rank, reg)
    
