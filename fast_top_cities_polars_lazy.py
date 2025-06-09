import polars as pl
import time

start = time.perf_counter()

users = pl.scan_parquet("users.parquet").select(["user_id", "city", "age", "gender"]) \
    .with_columns([
        pl.col("age").cast(pl.Int32, strict=False),
    ]).filter((pl.col("age") >= 18) & (pl.col("age") <= 65)) \
    .drop_nulls(["city", "gender"])
products = pl.scan_parquet("products.parquet").select(["product_id", "category"])
purchases = pl.scan_parquet("purchases.parquet") \
    .filter((pl.col("is_returned") == 0) & (pl.col("user_id").is_not_null())) \
    .with_columns([pl.col("discount_id").cast(pl.Int64, strict=False)])
discounts = pl.scan_parquet("discounts.parquet").select(["discount_id", "percent", "valid_from", "valid_to"]) \
    .with_columns([pl.col("discount_id").cast(pl.Int64, strict=False)])

df = (
    purchases
    .join(users, on="user_id", how="left")
    .join(products, on="product_id", how="left")
    .join(discounts, on="discount_id", how="left")
    .with_columns([
        pl.when(
            (pl.col("discount_id").is_not_null()) &
            (pl.col("valid_from").is_not_null()) &
            (pl.col("valid_to").is_not_null()) &
            (pl.col("date") >= pl.col("valid_from")) &
            (pl.col("date") <= pl.col("valid_to"))
        ).then(pl.col("percent"))
         .otherwise(0)
         .alias("discount_percent"),
        (pl.col("amount") * (1 - pl.col("percent").fill_null(0) / 100)).alias("final_amount")
    ])
    .group_by(["category", "city"])
    .agg(pl.col("final_amount").sum().alias("sum_amount"))
    .sort(["category", "sum_amount"], descending=[False, True])
    .group_by("category")
    .head(3)
)

result = df.collect()
print(result)
print(f"\nВремя Polars Lazy: {time.perf_counter() - start:.3f} сек")
