# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Process Gold Data
# MAGIC 
# MAGIC This notebook calculates aggregate metrics ready for Dashboarding.

# COMMAND ----------
from pyspark.sql.functions import col, sum, avg, count, when, date_trunc, floor, format_string

SILVER_TABLE_NAME = "runalyze_silver"

# COMMAND ----------
# Read Silver Data
df_silver = spark.table(SILVER_TABLE_NAME)

# Extract week from the timestamp to group properly
df_with_week = df_silver.withColumn("run_week", date_trunc("week", col("activity_timestamp")))

# COMMAND ----------
# 1. Weekly Running Mileage
# ---------------------------
# Summing the distance column aggregated by week
df_weekly_mileage = df_with_week.groupBy("run_week") \
                                .agg(sum("distance").alias("total_distance_km"),
                                     count("id").alias("run_count")) \
                                .orderBy("run_week")

df_weekly_mileage.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_weekly_mileage")

# COMMAND ----------
# 2. Time in HR Zones (Estimates using averages)
# ---------------------------
# Since we only receive the `hr_avg` per run directly on the root API endpoint 
# (unless using extended arrays endpoint), we categorize the entire run into a dominant zone.
# E.g.
# Zone 1: < 135
# Zone 2: 135 - 150
# Zone 3: 151 - 165
# Zone 4: 166 - 175
# Zone 5: > 175

df_hr_zones = df_silver.withColumn(
    "dominant_hr_zone",
    when(col("hr_avg") < 135, "Zone 1 (Easy)")
    .when(col("hr_avg") <= 150, "Zone 2 (Aerobic)")
    .when(col("hr_avg") <= 165, "Zone 3 (Tempo)")
    .when(col("hr_avg") <= 175, "Zone 4 (Threshold)")
    .otherwise("Zone 5 (Anaerobic)")
)

df_hr_summary = df_hr_zones.groupBy("dominant_hr_zone") \
                           .count() \
                           .withColumnRenamed("count", "total_runs_in_zone")

df_hr_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_hr_zones")

# COMMAND ----------
# 3. Average Weekly Pace
# ---------------------------
# Duration is in seconds, distance is in KM.
# Pace = (Duration / 60) / Distance
df_weekly_pace = df_with_week.groupBy("run_week") \
                             .agg(sum("duration").alias("total_duration_sec"),
                                  sum("distance").alias("total_distance_km"))

# Calculate Weighted average pace per week in minutes/km and total seconds/km
df_weekly_pace = df_weekly_pace.withColumn(
    "avg_pace_minutes_per_km", 
    (col("total_duration_sec") / 60) / col("total_distance_km")
).withColumn(
    "pace_seconds_per_km",
    col("total_duration_sec") / col("total_distance_km")
).withColumn(
    "friendly_pace",
    format_string("%d:%02d min/km", 
                  floor(col("pace_seconds_per_km") / 60).cast("int"), 
                  floor(col("pace_seconds_per_km") % 60).cast("int"))
).drop("pace_seconds_per_km") \
 .select("run_week", "avg_pace_minutes_per_km", "friendly_pace") \
 .orderBy("run_week")

df_weekly_pace.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_weekly_pace")

print("Successfully refreshed all Gold Datamarts!")
