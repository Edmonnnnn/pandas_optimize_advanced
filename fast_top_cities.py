import pandas as pd
import time

start = time.perf_counter()

# Быстрая загрузка и очистка (можешь заменить на импорт из fast_cleaning.py через pickle/hdf, если хочешь)
users = (
    pd.read_csv("users.csv")
    .drop_duplicates(subset="user_id")
    .assign(age=lambda df: pd.to_numeric(df["age"], errors="coerce"))
    .query("18 <= age <= 65")
    .dropna(subset=["city", "gender"])
)
products = pd.read_csv("products.csv").drop_duplicates(subset="product_id")
purchases = (
    pd.read_csv("purchases.csv", parse_dates=["date"])
    .drop_duplicates(subset=["user_id", "product_id", "date"])
    .query("is_returned == 0 and user_id == user_id") # только успешные, только валидные user_id
)
discounts = pd.read_csv("discounts.csv", parse_dates=["valid_from", "valid_to"]).drop_duplicates(subset="discount_id")

# Join purchases + users + products
df = purchases.merge(users[['user_id', 'city']], on='user_id', how='left')
df = df.merge(products[['product_id', 'category']], on='product_id', how='left')

# Присоединяем скидку (по discount_id и попадающей дате)
df = df.merge(discounts, on='discount_id', how='left')
# Если дата покупки попадает в интервал скидки, применяем процент, иначе 0
df['discount_percent'] = df.apply(
    lambda row: row['percent'] if pd.notna(row['discount_id']) and
                                 pd.notna(row['valid_from']) and
                                 pd.notna(row['valid_to']) and
                                 row['valid_from'] <= row['date'] <= row['valid_to']
    else 0, axis=1)
df['final_amount'] = df['amount'] * (1 - df['discount_percent']/100)

# Группировка и топ-3 города для каждой категории
top3 = (
    df.groupby(['category', 'city'])['final_amount']
      .sum()
      .reset_index()
      .sort_values(['category', 'final_amount'], ascending=[True, False])
      .groupby('category')
      .head(3)
)

print(top3)

print(f"\nВремя работы Pandas-решения: {time.perf_counter() - start:.3f} сек")
