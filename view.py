import pandas as pd

df = pd.read_parquet("data/au_places.parquet")

df.to_csv("data/au_places.csv", index=False)