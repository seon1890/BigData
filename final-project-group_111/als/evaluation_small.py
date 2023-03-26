'''
$ spark-submit evaluation_small.py bt2227 20 0.1
'''

import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALS, ALSModel
import sys
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RankingMetrics


def main(spark, rank, reg, netID):
    train = spark.read.parquet(f'hdfs:/user/{netID}/train_small.parquet')
    
    als = ALS(rank=rank, maxIter=5, seed=42, regParam=reg,
              userCol='userId', itemCol='movieId', ratingCol='rating',
              coldStartStrategy="drop")
    print("Training ALS model with rank {} and regularization {}...".format(rank, reg))
    model = als.fit(train)
    ptemp = "models/ALS_small_rank_{}_reg_{}".format(rank, reg)
    als_path = ptemp + "/als"
    print("Saving model...")
    #als.save(f'hdfs:/user/{netID}'  + als_path)
    model_path = ptemp + "/als_model"
    #model.save(f'hdfs:/user/{netID}' + model_path)

    # RMSE
    validation = spark.read.parquet(f'hdfs:/user/{netID}/vals_small.parquet')
    predictions = model.transform(validation)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("RSME:", rmse)
    
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

if __name__ == "__main__":
    memory = "8g"
    spark = SparkSession.builder.appName("Evaluate Small").config("spark.executor.memory", memory).config("spark.driver.memory", memory).getOrCreate()
    netID = sys.argv[1]
    rank = int(sys.argv[2])
    regP = float(sys.argv[3])
    main(spark, rank, reg, netID)
