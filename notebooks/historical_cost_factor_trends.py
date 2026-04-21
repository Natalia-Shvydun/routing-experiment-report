# Databricks notebook source
# MAGIC %md
# MAGIC # Historical Cost Factor Trends
# MAGIC
# MAGIC Weekly cost factor (cost/GMV) for every combination of the 5 factor
# MAGIC dimensions across the full data period. Use this to check:
# MAGIC - Are factors stable over time or drifting?
# MAGIC - Are there seasonal patterns?
# MAGIC - Which segments have volatile factors that may need smoothing?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Users/natalia.shvydun@getyourguide.com/.bundle/Cursor/dev/files/scripts")

from pyspark.sql import functions as F
from pyspark.sql import Window

from payment_costs_forecasting.config import ForecastingConfig
from payment_costs_forecasting.bucketing import apply_bucketing

config = ForecastingConfig()

FACTOR_COLS = config.factor_columns
print(f"Factor columns: {FACTOR_COLS}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

raw = spark.table(config.snapshot_table)
df = apply_bucketing(
    raw.filter(F.col("gmv") > 0).filter(F.col("total_payment_costs") != 0),
    config,
)
df = df.withColumn("week", F.date_trunc("week", F.col("date_of_checkout")))

print(f"Total carts: {df.count():,}")
print(f"Date range: {df.agg(F.min('date_of_checkout'), F.max('date_of_checkout')).collect()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Full 5-factor weekly table
# MAGIC
# MAGIC One row per week × factor combination.

# COMMAND ----------

weekly_factors = (
    df
    .groupBy(["week"] + FACTOR_COLS)
    .agg(
        F.count("*").alias("carts"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor", F.col("total_cost") / F.col("total_gmv"))
    .orderBy(["week"] + FACTOR_COLS)
)

weekly_factors.cache()
print(f"Rows: {weekly_factors.count():,}")
display(weekly_factors)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Overall weekly trend (all segments combined)

# COMMAND ----------

display(
    df
    .groupBy("week")
    .agg(
        F.count("*").alias("carts"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor", F.round(F.col("total_cost") / F.col("total_gmv"), 6))
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .orderBy("week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Weekly trend by each factor dimension
# MAGIC
# MAGIC One chart per dimension — aggregated across the other 4 dimensions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 By rnpl_segment

# COMMAND ----------

display(
    weekly_factors
    .groupBy("week", "rnpl_segment")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .orderBy("rnpl_segment", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 By payment_method_detail

# COMMAND ----------

top_methods = [r[0] for r in
    weekly_factors.groupBy("payment_method_detail")
    .agg(F.sum("carts").alias("n")).orderBy(F.desc("n")).limit(10).collect()]

display(
    weekly_factors
    .filter(F.col("payment_method_detail").isin(top_methods))
    .groupBy("week", "payment_method_detail")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .orderBy("payment_method_detail", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 By currency

# COMMAND ----------

top_currencies = [r[0] for r in
    weekly_factors.groupBy("currency")
    .agg(F.sum("carts").alias("n")).orderBy(F.desc("n")).limit(10).collect()]

display(
    weekly_factors
    .filter(F.col("currency").isin(top_currencies))
    .groupBy("week", "currency")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .orderBy("currency", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 By payment_processor

# COMMAND ----------

display(
    weekly_factors
    .groupBy("week", "payment_processor")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .filter(F.col("carts") >= 100)
    .orderBy("payment_processor", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 By country_bucket

# COMMAND ----------

top_countries = [r[0] for r in
    weekly_factors.groupBy("country_bucket")
    .agg(F.sum("carts").alias("n")).orderBy(F.desc("n")).limit(15).collect()]

display(
    weekly_factors
    .filter(F.col("country_bucket").isin(top_countries))
    .groupBy("week", "country_bucket")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .orderBy("country_bucket", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Factor stability metrics
# MAGIC
# MAGIC For each 5-factor segment, compute the coefficient of variation (CV)
# MAGIC of the weekly cost factor. High CV = unstable factor.

# COMMAND ----------

stability = (
    weekly_factors
    .filter(F.col("carts") >= config.min_volume_for_factor)
    .groupBy(FACTOR_COLS)
    .agg(
        F.count("*").alias("weeks_present"),
        F.sum("carts").alias("total_carts"),
        F.round(F.avg("cost_factor"), 6).alias("avg_factor"),
        F.round(F.stddev("cost_factor"), 6).alias("std_factor"),
        F.round(F.min("cost_factor"), 6).alias("min_factor"),
        F.round(F.max("cost_factor"), 6).alias("max_factor"),
    )
    .withColumn("cv", F.round(
        F.abs(F.col("std_factor") / F.col("avg_factor")), 4))
    .withColumn("range_pct", F.round(
        (F.col("max_factor") - F.col("min_factor")) / F.abs(F.col("avg_factor")) * 100, 2))
)

display(stability.orderBy(F.desc("cv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Stability summary by dimension

# COMMAND ----------

for dim in FACTOR_COLS:
    dim_stability = (
        stability
        .groupBy(dim)
        .agg(
            F.count("*").alias("segments"),
            F.round(F.avg("cv"), 4).alias("avg_cv"),
            F.round(F.max("cv"), 4).alias("max_cv"),
            F.round(F.avg("total_carts"), 0).alias("avg_carts"),
            F.sum(F.when(F.col("cv") > 0.20, 1).otherwise(0)).alias("volatile_segments"),
        )
        .orderBy(F.desc("avg_cv"))
    )
    print(f"\n{'='*50}")
    print(f"  Factor stability by {dim}")
    print(f"  (CV > 0.20 = volatile)")
    print(f"{'='*50}")
    display(dim_stability)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Top volatile segments
# MAGIC
# MAGIC Segments with high CV that might need special treatment
# MAGIC (longer smoothing window, fallback to broader group, etc.)

# COMMAND ----------

volatile = (
    stability
    .filter(F.col("cv") > 0.15)
    .filter(F.col("weeks_present") >= 10)
    .orderBy(F.desc("cv"))
)

print(f"Segments with CV > 0.15 (present ≥10 weeks): {volatile.count()}")
display(volatile.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Two-factor cross trends
# MAGIC
# MAGIC Weekly cost factor for key 2-factor combinations to spot interaction
# MAGIC effects and non-stationarity.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 rnpl_segment × payment_method_detail

# COMMAND ----------

display(
    weekly_factors
    .filter(F.col("payment_method_detail").isin(top_methods))
    .groupBy("week", "rnpl_segment", "payment_method_detail")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .withColumn("segment", F.concat(F.col("rnpl_segment"), F.lit(" | "), F.col("payment_method_detail")))
    .orderBy("segment", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 payment_processor × currency

# COMMAND ----------

display(
    weekly_factors
    .filter(F.col("currency").isin(top_currencies[:5]))
    .groupBy("week", "payment_processor", "currency")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .withColumn("segment", F.concat(F.col("payment_processor"), F.lit(" | "), F.col("currency")))
    .filter(F.col("carts") >= 100)
    .orderBy("segment", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 payment_method_detail × currency

# COMMAND ----------

display(
    weekly_factors
    .filter(F.col("payment_method_detail").isin("VISA", "MASTERCARD", "paypal"))
    .filter(F.col("currency").isin(top_currencies[:5]))
    .groupBy("week", "payment_method_detail", "currency")
    .agg(
        F.sum("carts").alias("carts"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("total_gmv").alias("total_gmv"),
    )
    .withColumn("cost_factor_pct", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
    .withColumn("segment", F.concat(F.col("payment_method_detail"), F.lit(" | "), F.col("currency")))
    .filter(F.col("carts") >= 50)
    .orderBy("segment", "week")
)
