import numpy as np
import pandas as pd

from sklearn.model_selection import StratifiedKFold

large_p = pd.read_parquet('large.parquet')
skf = StratifiedKFold(n_splits=2)
ids = large_p["userId"]

for train, rest in skf.split(large_p, large_p["userId"]):
    trains = large_p.loc[train]
    rests = large_p.loc[rest]

for test, val in skf.split(rests, rests["userId"]):
    tests = rests.iloc[test]
    vals = rests.iloc[val]

train.to_parquet("train_large.parquet")
tests.to_parquet("test_large.parquet")
vals.to_parquet("vals_large.parquet")