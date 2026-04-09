# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Ingest Runalyze Data (Bronze Layer)
# MAGIC 
# MAGIC This notebook fetches activity data from the Runalyze REST API and stores the raw JSON responses into a Bronze Delta table.

# COMMAND ----------
import requests
import json
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp

# 1. Configuration & Secrets
# Ensure you configure 'runalyze-scope' in your Databricks secrets and store your generated PAT.
try:
    API_TOKEN = dbutils.secrets.get(scope="runalyze-scope", key="api-token")
except Exception as e:
    print("WARNING: Secret not found. You will need to configure `dbutils.secrets`.")
    API_TOKEN = "<fallback-or-empty-if-testing>"

BASE_URL = "https://runalyze.com/api/v1/activity"
BRONZE_TABLE_NAME = "runalyze_bronze"

# COMMAND ----------
# 2. Determine incremental sync strategy
# Check if the table already exists to get the latest activity date we have.
latest_time = None
try:
    if spark.catalog.tableExists(BRONZE_TABLE_NAME):
        # Assuming we eventually parse a timestamp column, but for raw JSON we might 
        # need to query the 'time' object. For initial load, we fetch all.
        print("Bronze table exists. In an enterprise pipeline, we would fetch the MAX() timestamp here.")
        pass
except Exception:
    pass

# COMMAND ----------
# 3. Call the API
headers = {
    "Token": API_TOKEN,
    "Content-Type": "application/json"
}

# The Runalyze API supports pagination or timespan filtering. 
# For demonstration, we simply call the main endpoint.
response = requests.get(BASE_URL, headers=headers)
response.raise_for_status()
activities = response.json()

if not activities:
    dbutils.notebook.exit("No new activities found.")

# COMMAND ----------
# 4. Save to Bronze
# We want to store the Raw JSON exactly as it is, along with an ingestion timestamp.
# This prevents data loss and allows us to replay the Silver process if we find a bug later.

# Convert list of dicts to a list of strings (each activity is a JSON string)
raw_json_records = [json.dumps(a) for a in activities]

# Create a DataFrame
df_raw = spark.createDataFrame([(r,) for r in raw_json_records], ["raw_json"])
df_raw = df_raw.withColumn("ingested_at", current_timestamp())

# Write to Delta
# We use APPEND so we just add the newly arrived records based on API response.
df_raw.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_TABLE_NAME)

print(f"Successfully appended {len(activities)} activities into {BRONZE_TABLE_NAME}.")
