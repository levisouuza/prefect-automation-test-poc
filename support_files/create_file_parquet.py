import pandas as pd

df = pd.read_csv("../files/movies.csv")

df.columns = df.columns.str.lower()

df = df.astype(str)

df.to_parquet("./files/movies.parquet")
