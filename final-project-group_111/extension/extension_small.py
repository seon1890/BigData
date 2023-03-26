'''
$ spark-submit extension_small.py bt2227 20 0.1
'''

import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALS, ALSModel
import sys
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RankingMetrics


def main(spark, rank, reg, netID):
    ptemp = "models/ALS_small_rank_{}_reg_{}".format(rank, reg)
    als_path = ptemp + "/als"
    model_path = ptemp + "/als_model"
    als = (f'hdfs:/user/{netID}'  + als_path)
    model = (f'hdfs:/user/{netID}' + model_path)
    test = spark.read.parquet(f'hdfs:/user/{netID}/test_small.parquet')
    als = ALS(rank=rank, maxIter=5, seed=42, regParam=reg, userCol='userId', itemCol='movieId', ratingCol='rating', coldStartStrategy="drop")
    test.createOrReplaceTempView("test")
    
    predictions = model.recommendForAllUsers(100)
    predictions.createOrReplaceTempView("predictions")
    
    users =  predictions.select('userId').rdd.flatMap(lambda x: x).collect()
    pred_all =  predictions.select('recommendations').rdd.flatMap(lambda x: x).collect() 
        
    movie_dict = {}
    for u in range(len(users)):
        user = users[u] 
        predict = pred_all[u] 
        ratingReal = spark.sql(f'SELECT movieId, rating FROM validation WHERE userId={user}').toPandas()
        for ratingMovie in predict:
            if ratingMovie[0] in ratingReal['movieId']:
            if ratingMovie[0] not in mov_dict.keys():
                mov_dict[ratingMovie[0]] = []
                predict = ratingMovie[1]
                real = float(ratingReal.loc[ratingReal['movieId']==ratingMovie[0]]['rating'])
                if predict>5:
                    predict = 5.0 
                mov_dict[ratingMovie[0]].append(abs(predict - real)/real)
        total = 0
        acc = 0
        for item in list(mov_dict.values()):
            total += len(item)
            err += sum(item)
        print('Error:',(1 - acc/total))
        print()
    genreMovie = spark.sql('SELECT movieId, genres FROM movies WHERE genres != "(no genres listed)"').toPandas()
    genreErr = {} 
    for movId in movie_dict.keys(): 
        genres = str(genreMovie.loc[movie_genre['movieId']==movId]['genres']).split('|')
        for g in genres:
            if g not in genreErr.keys():
                genreErr[g] = []
            genreErr[g].extend(movie_dict['movId'])         
    print('Average Error Analysis')
    for genre in genreErr.keys():
        t = sum(genreErr[genre])/len(genreErr[genre])
        genreErr[genre] = t
        print(f'The average error for {genre} is {t}')

if __name__ == "__main__":
    memory = "8g"
    spark = SparkSession.builder.appName("Extension Small").master('yarn').config("spark.executor.memory", memory).config("spark.driver.memory", memory).getOrCreate()
    path = sys.argv[1]
    rank = int(sys.argv[2])
    reg = float(sys.argv[3])
    main(spark, rank, reg, netID)
