import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)
random.seed(42)

# --- USERS ---
n_users = 100_000
cities = ["Moscow", "Berlin", "London", "Paris", "Rome", "New York", "Tokyo", "Beijing", "Dubai", "Istanbul"]
genders = ["male", "female", "other"]

users = []
for i in tqdm(range(1, n_users + 1), desc="Generating users"):
    # Иногда делаем пропуски или ошибки
    age = np.random.choice(
        [np.random.randint(18, 65), None, 'twenty'], p=[0.97, 0.02, 0.01]
    )
    city = np.random.choice(cities + [None], p=[0.099]*10 + [0.01])
# 0.099 * 10 + 0.01 = 1.0 (сумма вероятностей обязательно должна быть 1)

    gender = np.random.choice(genders + [None], p=[0.48, 0.48, 0.03, 0.01])
    users.append({
        "user_id": i,
        "age": age,
        "city": city,
        "gender": gender
    })
users_df = pd.DataFrame(users)
# Добавим дубликаты
users_df = pd.concat([users_df, users_df.sample(1000, random_state=1)], ignore_index=True)
users_df.to_csv("users.csv", index=False)

# --- PRODUCTS ---
n_products = 500
categories = ["electronics", "clothing", "books", "toys", "groceries", "sports", "beauty"]
products = []
for i in tqdm(range(1001, 1001 + n_products), desc="Generating products"):
    price = np.round(np.random.uniform(5, 2000), 2)
    category = np.random.choice(categories)
    products.append({
        "product_id": i,
        "category": category,
        "price": price
    })
products_df = pd.DataFrame(products)
products_df = pd.concat([products_df, products_df.sample(50, random_state=2)], ignore_index=True)
products_df.to_csv("products.csv", index=False)

# --- DISCOUNTS ---
n_discounts = 50
discounts = []
for i in tqdm(range(1, n_discounts + 1), desc="Generating discounts"):
    percent = np.random.choice([5, 10, 15, 20, 25, 0])
    valid_from = fake.date_between(start_date='-2y', end_date='-1y')
    valid_to = fake.date_between(start_date=valid_from, end_date='+1y')
    discounts.append({
        "discount_id": i,
        "percent": percent,
        "valid_from": valid_from,
        "valid_to": valid_to
    })
discounts_df = pd.DataFrame(discounts)
discounts_df.to_csv("discounts.csv", index=False)

# --- PURCHASES ---
n_purchases = 2_000_000
purchase_rows = []
date_choices = pd.date_range("2022-01-01", "2024-06-01")
for _ in tqdm(range(n_purchases), desc="Generating purchases"):
    user_id = np.random.randint(1, n_users + 1)
    product_id = np.random.choice(products_df["product_id"])
    amount = np.random.randint(1, 5) * float(products_df[products_df["product_id"] == product_id]["price"].values[0])
    date = np.random.choice(date_choices)
    is_returned = np.random.choice([0, 1], p=[0.96, 0.04])
    discount_id = np.random.choice(list(discounts_df["discount_id"]) + [None], p=[0.018]*n_discounts + [0.1])
    purchase_rows.append({
        "user_id": user_id,
        "product_id": product_id,
        "amount": amount,
        "date": date,
        "is_returned": is_returned,
        "discount_id": discount_id
    })
purchases_df = pd.DataFrame(purchase_rows)
# Дубликаты
purchases_df = pd.concat([purchases_df, purchases_df.sample(5000, random_state=3)], ignore_index=True)
purchases_df.to_csv("purchases.csv", index=False)

print("Все данные успешно сгенерированы: users.csv, products.csv, discounts.csv, purchases.csv")
