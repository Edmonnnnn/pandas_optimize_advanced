import pandas as pd
from datetime import datetime, timedelta

# 1. Загружаем все данные (без очистки)
users = pd.read_csv("users.csv")
products = pd.read_csv("products.csv")
discounts = pd.read_csv("discounts.csv", parse_dates=["valid_from", "valid_to"])
purchases = pd.read_csv("purchases.csv", parse_dates=["date"])

# 2. Копируем DataFrame (лишнее действие, замедляет!)
users_copy = users.copy()
products_copy = products.copy()
discounts_copy = discounts.copy()
purchases_copy = purchases.copy()


# !!! ДОБАВЬ для теста
users_copy = users_copy.head(10000)
products_copy = products_copy.head(200)
discounts_copy = discounts_copy.head(20)
purchases_copy = purchases_copy.head(20000)

# 3. Убираем возвраты вручную через for (медленно!)
purchases_filtered = []
for i, row in purchases_copy.iterrows():
    if row['is_returned'] == 0:
        purchases_filtered.append(row)
purchases_filtered_df = pd.DataFrame(purchases_filtered)

# 4. Склеиваем вручную все связи: purchases → users → products → discounts (медленно через apply)
merged_rows = []
for i, row in purchases_filtered_df.iterrows():
    user_row = users_copy[users_copy['user_id'] == row['user_id']]
    product_row = products_copy[products_copy['product_id'] == row['product_id']]
    discount_row = discounts_copy[discounts_copy['discount_id'] == row['discount_id']]
    # Проверяем скидку по дате
    discount_percent = 0
    if not discount_row.empty:
        drow = discount_row.iloc[0]
        if drow['valid_from'] <= row['date'] <= drow['valid_to']:
            discount_percent = drow['percent']
    final_amount = row['amount'] * (1 - discount_percent / 100)
    merged_rows.append({
        "city": user_row.iloc[0]['city'] if not user_row.empty else None,
        "category": product_row.iloc[0]['category'] if not product_row.empty else None,
        "amount": final_amount
    })
merged_df = pd.DataFrame(merged_rows)

# 5. Для каждой категории: группируем по городам, находим топ-3
result = {}
for category in merged_df['category'].dropna().unique():
    df_cat = merged_df[merged_df['category'] == category]
    city_sums = {}
    for city in df_cat['city'].dropna().unique():
        city_sum = df_cat[df_cat['city'] == city]['amount'].sum()
        city_sums[city] = city_sum
    top_cities = sorted(city_sums.items(), key=lambda x: x[1], reverse=True)[:3]
    result[category] = top_cities

print("\nТоп-3 города по выручке в каждой категории (очень медленно):")
for category, cities in result.items():
    print(f"Категория: {category}")
    for city, amount in cities:
        print(f"  {city}: {amount:.2f}")
    print("---------")







# --- 1.2. Средний чек по возрастным группам (медленно) ---
print("\nСредний чек по возрастным группам (медленно):")

# 1. Собираем пользователей с валидным возрастом
valid_users = []
for i, row in users_copy.iterrows():
    try:
        age = int(row['age'])
        if age >= 18:
            valid_users.append({"user_id": row['user_id'], "age": age})
    except:
        continue
valid_users_df = pd.DataFrame(valid_users)

# 2. Присоединяем их к покупкам (без возвратов)
purchases_no_ret = []
for i, row in purchases_copy.iterrows():
    if row['is_returned'] == 0:
        user_info = valid_users_df[valid_users_df['user_id'] == row['user_id']]
        if not user_info.empty:
            purchases_no_ret.append({
                "user_id": row['user_id'],
                "amount": row['amount'],
                "age": user_info.iloc[0]['age']
            })
purchases_no_ret_df = pd.DataFrame(purchases_no_ret)

# 3. Группировка по возрасту вручную
groups = {
    "18-25": [],
    "26-35": [],
    "36-50": [],
    "51+": []
}
for i, row in purchases_no_ret_df.iterrows():
    age = row['age']
    if 18 <= age <= 25:
        groups["18-25"].append(row['amount'])
    elif 26 <= age <= 35:
        groups["26-35"].append(row['amount'])
    elif 36 <= age <= 50:
        groups["36-50"].append(row['amount'])
    elif age >= 51:
        groups["51+"].append(row['amount'])

# 4. Средний чек по каждой группе
for group, amounts in groups.items():
    if len(amounts) > 0:
        avg = sum(amounts) / len(amounts)
        print(f"{group}: {avg:.2f} ({len(amounts)} покупок)")
    else:
        print(f"{group}: нет данных")






print("\nКлиенты с >2 возвратами за год:")

# 1. Граница для "за последний год"
end_date = purchases_copy['date'].max()
if isinstance(end_date, pd.Timestamp):
    end_date = end_date.to_pydatetime()
start_date = end_date - timedelta(days=365)

# 2. Возвраты за год (через for)
returns = []
for i, row in purchases_copy.iterrows():
    # Проверка даты возврата
    if row['is_returned'] == 1 and start_date <= row['date'] <= end_date:
        returns.append(row['user_id'])

# 3. Считаем возвраты по пользователям
from collections import Counter
user_return_counts = Counter(returns)
users_many_returns = [user_id for user_id, count in user_return_counts.items() if count >= 1]

# 4. Получаем их города и возраст (медленно)
user_info_rows = []
for user_id in users_many_returns:
    user_rows = users_copy[users_copy['user_id'] == user_id]
    for _, user_row in user_rows.iterrows():
        try:
            age = int(user_row['age'])
            city = user_row['city']
            if pd.notna(city):
                user_info_rows.append({"age": age, "city": city})
        except:
            continue

# 5. Уникальные города
unique_cities = set([r['city'] for r in user_info_rows])
# Средний возраст
if user_info_rows:
    avg_age = sum(r['age'] for r in user_info_rows) / len(user_info_rows)
    print(f"Найдено пользователей: {len(user_info_rows)}")
    print(f"Города: {', '.join(unique_cities)}")
    print(f"Средний возраст: {avg_age:.2f}")
else:
    print("Нет пользователей с более чем 2 возвратами за год.")



print("\nRetention: процент пользователей, совершивших повторную покупку через 30+ дней (медленно)")

# 1. Собираем все покупки (без возвратов, с валидным user_id)
purchases_no_ret = []
for i, row in purchases_copy.iterrows():
    if row['is_returned'] == 0 and pd.notna(row['user_id']):
        purchases_no_ret.append((row['user_id'], row['date']))
# Для каждого пользователя — список всех дат покупок
user_dates = {}
for user_id, date in purchases_no_ret:
    user_dates.setdefault(user_id, []).append(date)
# Считаем retention
users_with_retention = 0
users_total = 0
for user_id, dates in user_dates.items():
    if len(dates) >= 2:
        users_total += 1
        dates_sorted = sorted([pd.to_datetime(d) for d in dates])
        delta_days = (dates_sorted[-1] - dates_sorted[0]).days
        if delta_days >= 30:
            users_with_retention += 1
if users_total > 0:
    retention = users_with_retention / users_total * 100
    print(f"Retention: {retention:.2f}% ({users_with_retention} из {users_total})")
else:
    print("Недостаточно пользователей с 2+ покупками.")





print("\nОчистка данных (медленно):")

# 1. Убираем дубликаты вручную (например, по user_id, по product_id)
# (это медленно — через множество циклов)

# Users
unique_users = []
user_ids = set()
for i, row in users.iterrows():
    uid = row['user_id']
    if uid not in user_ids:
        user_ids.add(uid)
        unique_users.append(row)
users_no_dupes = pd.DataFrame(unique_users)
print(f"Пользователей до: {len(users)}, после удаления дубликатов: {len(users_no_dupes)}")

# Products
unique_products = []
product_ids = set()
for i, row in products.iterrows():
    pid = row['product_id']
    if pid not in product_ids:
        product_ids.add(pid)
        unique_products.append(row)
products_no_dupes = pd.DataFrame(unique_products)
print(f"Товаров до: {len(products)}, после удаления дубликатов: {len(products_no_dupes)}")

# Purchases — дубликаты (user_id, product_id, date)
unique_purchases = []
purchase_keys = set()
for i, row in purchases.iterrows():
    key = (row['user_id'], row['product_id'], str(row['date']))
    if key not in purchase_keys:
        purchase_keys.add(key)
        unique_purchases.append(row)
purchases_no_dupes = pd.DataFrame(unique_purchases)
print(f"Покупок до: {len(purchases)}, после удаления дубликатов: {len(purchases_no_dupes)}")

# 2. Чистим пропуски и странные значения (по users)
clean_users = []
for i, row in users_no_dupes.iterrows():
    try:
        age = int(row['age'])
        city = row['city']
        gender = row['gender']
        if pd.notna(city) and pd.notna(gender) and 18 <= age <= 65:
            clean_users.append(row)
    except:
        continue
clean_users_df = pd.DataFrame(clean_users)
print(f"Пользователей после очистки от пропусков и некорректных значений: {len(clean_users_df)}")
