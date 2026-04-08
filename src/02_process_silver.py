# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Process Silver Data
# MAGIC 
# MAGIC This notebook parses the raw JSON payload from the Bronze table, flattens the columns, casts correct datatypes, deduplicates runs, and writes directly to the Silver delta table.

# COMMAND ----------
from pyspark.sql.functions import col, from_json, to_timestamp, dropDuplicates
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

BRONZE_TABLE_NAME = "runalyze_bronze"
SILVER_TABLE_NAME = "runalyze_silver"

# COMMAND ----------
# 1. Define the Expected Schema of the JSON payload
# Note: For production, you may want to parse a more extensive schema depending on what Runalyze returns.
runalyze_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("sportid", IntegerType(), True),         # Might be used to filter for runs
    StructField("distance", DoubleType(), True),         # Usually returned in km
    StructField("duration", DoubleType(), True),         # Usually returned in seconds
    StructField("time", StringType(), True),             # UNIX timestamp or ISO string
    StructField("heart_rate_avg", IntegerType(), True),
    StructField("heart_rate_max", IntegerType(), True),
    StructField("title", StringType(), True)
])

# COMMAND ----------
# 2. Read Bronze Data
df_bronze = spark.table(BRONZE_TABLE_NAME)

# COMMAND ----------
# 3. Clean and Flatten
# We use from_json() against our schema and then unnest the resulting object
df_silver = df_bronze.withColumn("parsed", from_json(col("raw_json"), runalyze_schema)) \
                     .select("parsed.*", "ingested_at")

# Cast numerical data and format dates
# Assuming Runalyze "time" is a unix UTC timestamp, we cast it:
df_silver = df_silver.withColumn("activity_timestamp", to_timestamp(col("time"))) \
                     .drop("time")

# Only keep Running activities (often sportid == 1, check your Runalyze 'sports' endpoint for exact ID)
df_silver = df_silver.filter(col("sportid") == 1)

# Handle Deduplication (since Bronze just blindly APPENDS during execution)
df_silver = df_silver.dropDuplicates(["id"])

# COMMAND ----------
# 4. Write to Silver via MERGE
# Instead of overwriting, we use Delta's native MERGE INTO syntax.
# For simplicity with dataframe API, you can do an overwrite, but MERGE handles updates perfectly.

from delta.tables import DeltaTable

if spark.catalog.tableExists(SILVER_TABLE_NAME):
    silver_table = DeltaTable.forName(spark, SILVER_TABLE_NAME)
    
    # Upsert logic based on the Activity 'id'
    silver_table.alias("target").merge(
        df_silver.alias("source"),
        "target.id = source.id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    print("Merged successfully into the existing Silver table.")

else:
    # Initial load: Just save as a new table
    df_silver.write \
             .format("delta") \
             .mode("overwrite") \
             .saveAsTable(SILVER_TABLE_NAME)
    print("Created Silver table for the first time.")
