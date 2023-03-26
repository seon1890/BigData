import getpass

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from pyspark.sql.session import SparkSession

import dask
import dask.bag as db
import dask.dataframe as dd

def main(spark, netID):

    trains = spark.read.parquet(f'hdfs:/user/{netID}/train_small.parquet').toPandas()
    vals = spark.read.parquet(f'hdfs:/user/{netID}/vals_small.parquet').toPandas()

    rtings = trains.groupby(["movieId"]).agg({'rating': ['mean', 'count']})
    rtings.columns = rtings.columns.get_level_values(1)
    total_count = np.sum(rtings["count"])
    c = np.mean(rtings["mean"])

    weighted_ratings = []
    for idx, row in rtings.iterrows():
        r = row["mean"]
        v = row["count"]
        m = total_count / 50000
        wr = (v / (v + m)) * r + (m / (v + m)) * c
        weighted_ratings.append([idx, wr])

    wr_df = pd.DataFrame(weighted_ratings, columns=["movieId", "weightedRating"])
    wr_df = wr_df.sort_values("weightedRating", ascending=False)
    wr_df = wr_df.set_index("movieId")

    # Popularity validation
    # testing our popularity model onto validation data.
    # Popularity validation
    top100 = list(wr_df[:100].index)
    top100vals = vals[vals["movieId"].isin(top100)]

    errs = []
    for index, row in top100vals.iterrows():
        mid = row["movieId"]
        mrt = row["rating"]
        wrt = wr_df.loc[mid]["weightedRating"]
        err = np.absolute((mrt - wrt) / wrt)
        errs.append(err)

    print((1 - np.mean(errs)) * 100)


if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part_1_small').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
