import pandas as pd

print("\nUSERS:")
print(pd.read_csv("users.csv").head())

print("\nPRODUCTS:")
print(pd.read_csv("products.csv").head())

print("\nDISCOUNTS:")
print(pd.read_csv("discounts.csv").head())

print("\nPURCHASES:")
print(pd.read_csv("purchases.csv").head())
