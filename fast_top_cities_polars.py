import polars as pl
import time

start = time.perf_counter()

users = (
    pl.read_csv("users.csv", try_parse_dates=True)
    .unique(subset=["user_id"])
    .with_columns([
        pl.col("age").cast(pl.Int32, strict=False),
    ])
    .filter((pl.col("age") >= 18) & (pl.col("age") <= 65))
    .drop_nulls(["city", "gender"])
    .head(5000)
)
products = pl.read_csv("products.csv").unique(subset=["product_id"]).head(500)
purchases = (
    pl.read_csv("purchases.csv", try_parse_dates=True)
    .unique(subset=["user_id", "product_id", "date"])
    .filter((pl.col("is_returned") == 0) & (pl.col("user_id").is_not_null()))
    .head(5000)
)
discounts = pl.read_csv("discounts.csv", try_parse_dates=True).unique(subset=["discount_id"]).head(50)

# Кастуем discount_id к Int64 ВЕЗДЕ
purchases = purchases.with_columns(pl.col("discount_id").cast(pl.Int64, strict=False))
discounts = discounts.with_columns(pl.col("discount_id").cast(pl.Int64, strict=False))

df = purchases.join(users.select(["user_id", "city"]), on="user_id", how="left")
df = df.join(products.select(["product_id", "category"]), on="product_id", how="left")
df = df.join(discounts, on="discount_id", how="left")

df = df.with_columns([
    pl.when(
        (pl.col("discount_id").is_not_null()) &
        (pl.col("valid_from").is_not_null()) &
        (pl.col("valid_to").is_not_null()) &
        (pl.col("date") >= pl.col("valid_from")) &
        (pl.col("date") <= pl.col("valid_to"))
    ).then(pl.col("percent"))
     .otherwise(0)
     .alias("discount_percent")
])
df = df.with_columns([
    (pl.col("amount") * (1 - pl.col("discount_percent")/100)).alias("final_amount")
])

result = (
    df.group_by(["category", "city"])
      .agg(pl.col("final_amount").sum().alias("sum_amount"))
      .sort(["category", "sum_amount"], descending=[False, True])
      .group_by("category")
      .head(3)
)

print(result)
print(f"\nВремя работы Polars-решения: {time.perf_counter() - start:.3f} сек")
