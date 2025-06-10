import pandas as pd
import time

start = time.perf_counter()

# Быстро читаем и чистим users
users = (
    pd.read_csv("users.csv")
    .drop_duplicates(subset="user_id")
    .assign(age=lambda df: pd.to_numeric(df["age"], errors="coerce"))
    .query("18 <= age <= 65")
    .dropna(subset=["city", "gender"])
)

print(f"Пользователей после быстрой очистки: {len(users)}")
print(users.head())

# Быстро чистим products
products = (
    pd.read_csv("products.csv")
    .drop_duplicates(subset="product_id")
)

# Быстро чистим purchases
purchases = (
    pd.read_csv("purchases.csv", parse_dates=["date"])
    .drop_duplicates(subset=["user_id", "product_id", "date"])
    .query("is_returned in [0,1] and user_id == user_id")  # убирает NaN user_id
)

# Быстро чистим discounts
discounts = (
    pd.read_csv("discounts.csv", parse_dates=["valid_from", "valid_to"])
    .drop_duplicates(subset="discount_id")
)

print(f"Products: {len(products)}, Purchases: {len(purchases)}, Discounts: {len(discounts)}")
print(f"Время быстрой очистки: {time.perf_counter() - start:.3f} сек")
