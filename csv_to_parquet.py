import pandas as pd

pd.read_csv("users.csv").to_parquet("users.parquet")
pd.read_csv("products.csv").to_parquet("products.parquet")
pd.read_csv("purchases.csv").to_parquet("purchases.parquet")
pd.read_csv("discounts.csv").to_parquet("discounts.parquet")
