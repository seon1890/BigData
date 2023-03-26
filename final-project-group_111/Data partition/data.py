import numpy as np
import pandas as pd

from sklearn.model_selection import StratifiedKFold

small_p = pd.read_parquet('small.parquet')


skf = StratifiedKFold(n_splits=2)
ids = small_p["userId"]

for train, rest in skf.split(small_p, small_p["userId"]):
    trains = small_p.loc[train]
    rests = small_p.loc[rest]

for test, val in skf.split(rests, rests["userId"]):
    tests = rests.iloc[test]
    vals = rests.iloc[val]


train.to_parquet("train_small.parquet")
tests.to_parquet("test_small.parquet")
vals.to_parquet("vals_small.parquet")
