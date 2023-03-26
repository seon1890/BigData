import pandas as pd
import numpy as np

df = pd.read_parquet('train_large.parquet').drop(columns='timestamp')
size = len(df)//100
i=0
while i != 99:
    train = df.iloc[i*size:(i+1)*size]
    train.to_parquet(f'train_large/{i}.parquet', index=False,compression=None)
    i += 1
train = df.iloc[99*size:]
train.to_parquet('train_large/99.parquet', index=False,compression=None)
