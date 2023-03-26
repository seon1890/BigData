'''
$ spark-submit large_als.py bt2227 10 0.1
'''

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import sys


def main(spark, netID, rank, reg):
    ratings = spark.read.parquet(f'hdfs:/user/{netID}/train_large.parquet')
    movies = spark.read.parquet(f'hdfs:/user/{netID}/large.parquet')
    ratings.createOrReplaceTempView('ratings')
    movies.createOrReplaceTempView('movies')
    combine = spark.sql('SELECT ratings.userId, ratings.rating, movies.movieId FROM ratings LEFT JOIN movies ON ratings.movieID = movies.movieId')
    als = ALS(rank=rank, maxIter=5, seed=3, regParam=reg, userCol='userId', itemCol='movieId', ratingCol='rating', coldStartStrategy="drop")
    model = als.fit(combine)
    path = f'hdfs:/user/{netID}'
    temppath = "/ALS_rank_{}_reg_{}".format(rank, reg)
    alspath = temppath + "/als"
    als.save(path + "/models" + alspath)
    modelpath = temppath + "/als_model"
    model.save(netID + "/models" + modelpath)


if __name__ == "__main__":
    memory = "8g"
    spark = SparkSession.builder.appName("ALS Large").config("spark.executor.memory", memory).config("spark.driver.memory", memory).getOrCreate()
    netID = sys.argv[1]
    rank = int(sys.argv[2])
    reg = float(sys.argv[3])
    main(spark, netID, rank, reg)
