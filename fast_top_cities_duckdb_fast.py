import duckdb
import time

start = time.perf_counter()

con = duckdb.connect()

query = """
WITH filtered_purchases AS (
    SELECT * FROM 'purchases.parquet'
    WHERE is_returned = 0
),
users_clean AS (
    SELECT user_id, city FROM 'users.parquet'
    WHERE TRY_CAST(age AS INTEGER) BETWEEN 18 AND 65
      AND city IS NOT NULL AND gender IS NOT NULL
),
products_clean AS (
    SELECT product_id, category FROM 'products.parquet'
),
discounts_clean AS (
    SELECT * FROM 'discounts.parquet'
)
SELECT
    pr.category,
    u.city,
    SUM(p.amount * (1 - 
        COALESCE(
            CASE WHEN d.discount_id IS NOT NULL
                AND d.valid_from <= p.date
                AND d.valid_to >= p.date
            THEN d.percent/100.0
            ELSE 0 END, 0)
        )) AS final_amount
FROM filtered_purchases p
LEFT JOIN users_clean u ON p.user_id = u.user_id
LEFT JOIN products_clean pr ON p.product_id = pr.product_id
LEFT JOIN discounts_clean d ON p.discount_id = d.discount_id
GROUP BY pr.category, u.city
QUALIFY ROW_NUMBER() OVER (PARTITION BY pr.category ORDER BY SUM(p.amount * (1 - 
        COALESCE(
            CASE WHEN d.discount_id IS NOT NULL
                AND d.valid_from <= p.date
                AND d.valid_to >= p.date
            THEN d.percent/100.0
            ELSE 0 END, 0)
        )) DESC) <= 3
ORDER BY pr.category, final_amount DESC
"""

df = con.execute(query).df()
print(df)
print(f"\nВремя DuckDB: {time.perf_counter() - start:.3f} сек")
