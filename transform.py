import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# 1. Dynamically calculate the target filename to match the Cloud Function's output
yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
dynamic_bucket_path = f"gs://paypal_pipeline/raw_data/paypal_transactions_{yesterday_str}.json"

# Initialize Spark Session with allocated memory
spark = SparkSession.builder \
    .appName("PayPal-Data-Transformer") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memoryOverhead", "512m") \
    .getOrCreate()

print(f"Reading JSON dataset from: {dynamic_bucket_path}")
raw_df = spark.read.json(dynamic_bucket_path)

# Explode the nested array and flatten the transaction structure
flattened_df = raw_df.select(
    col("account_number"),
    explode(col("transaction_details")).alias("detail")
).select(
    col("account_number"),
    col("detail.transaction_info.transaction_id").alias("transaction_id"),
    col("detail.transaction_info.transaction_event_code").alias("event_code"),
    col("detail.transaction_info.transaction_status").alias("status"),
    col("detail.transaction_info.transaction_amount.currency_code").alias("currency"),
    col("detail.transaction_info.transaction_amount.value").cast("double").alias("amount"),
    col("detail.transaction_info.ending_balance.value").cast("double").alias("ending_balance")
)

print("\n--- Transformed Schema ---")
flattened_df.printSchema()

print("\n--- Sample Processed Records ---")
flattened_df.show(5, truncate=False)

print(f"\nTotal records successfully transformed: {flattened_df.count()}")

# Save the cleaned output as Parquet
flattened_df.write.mode("overwrite").parquet("processed_transactions_parquet")
print("\nParquet file written successfully!")

spark.stop()