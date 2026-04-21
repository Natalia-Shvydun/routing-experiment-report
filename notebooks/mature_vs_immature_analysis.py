# Databricks notebook source
# MAGIC %md
# MAGIC # Mature vs Immature Carts — Composition & Cost Trends
# MAGIC
# MAGIC Weekly analysis of how the split between mature (actual costs known)
# MAGIC and immature (forecasted) carts evolves over time, plus a combined
# MAGIC "evaluated" cost view (actual for mature + forecast for immature).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Users/natalia.shvydun@getyourguide.com/.bundle/Cursor/dev/files/scripts")

from pyspark.sql import functions as F
from pyspark.sql import Window

from payment_costs_forecasting.config import ForecastingConfig
from payment_costs_forecasting.bucketing import apply_bucketing
from payment_costs_forecasting.factors import compute_factors
from payment_costs_forecasting.forecast import get_immature_carts, apply_factors_with_fallback
from payment_costs_forecasting.validation import _filter_mature_actuals

config = ForecastingConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

dbutils.widgets.text("backtest_start", "2025-08-18", "Start date (Monday)")
dbutils.widgets.text("backtest_end", "2026-02-16", "End date (Monday)")
dbutils.widgets.text("snapshot_date", date.today().isoformat(), "Snapshot date")

backtest_start = date.fromisoformat(dbutils.widgets.get("backtest_start"))
backtest_end = date.fromisoformat(dbutils.widgets.get("backtest_end"))
snapshot_date = date.fromisoformat(dbutils.widgets.get("snapshot_date"))

safe_maturity = max(
    config.non_rnpl_maturity_days,
    config.rnpl_maturity_days,
    config.rnpl_cancelled_maturity_days,
)
latest_safe = snapshot_date - timedelta(days=safe_maturity + 14)
if backtest_end > latest_safe:
    print(f"WARNING: capping backtest_end from {backtest_end} to {latest_safe}")
    backtest_end = latest_safe

evaluation_dates = []
d = backtest_start
while d <= backtest_end:
    evaluation_dates.append(d)
    d += timedelta(weeks=1)

print(f"Period: {evaluation_dates[0]} → {evaluation_dates[-1]}")
print(f"Evaluation dates: {len(evaluation_dates)} weeks")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

raw = spark.table(config.snapshot_table)
bucketed = apply_bucketing(raw.filter(F.col("gmv") > 0), config)
bucketed.cache()
print(f"Total carts: {bucketed.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Compute weekly mature/immature split + costs

# COMMAND ----------

RESULT_TABLE = "testing.analytics._mature_immature_weekly"

for i, eval_date in enumerate(evaluation_dates):
    factors = compute_factors(bucketed, eval_date, config)
    immature = get_immature_carts(bucketed, eval_date, config)
    forecast_df = apply_factors_with_fallback(immature, factors, config)

    immature_ids = immature.select("shopping_cart_id").distinct()

    # Mature carts: everything NOT immature, with checkout <= eval_date
    mature_carts = (
        bucketed
        .filter(F.col("date_of_checkout") <= F.lit(eval_date))
        .join(immature_ids, "shopping_cart_id", "left_anti")
    )

    mature_summary = mature_carts.agg(
        F.count("*").alias("mature_carts"),
        F.sum("gmv").alias("mature_gmv"),
        F.sum("total_payment_costs").alias("mature_actual_cost"),
    )

    immature_summary = immature.agg(
        F.count("*").alias("immature_carts"),
        F.sum("gmv").alias("immature_gmv"),
        F.sum("total_payment_costs").alias("immature_actual_cost"),
    )

    forecast_summary = forecast_df.agg(
        F.sum("amount").alias("immature_forecast_cost"),
    )

    row = (
        mature_summary.crossJoin(immature_summary).crossJoin(forecast_summary)
        .withColumn("eval_date", F.lit(eval_date))
    )

    mode = "overwrite" if i == 0 else "append"
    row.write.mode(mode).saveAsTable(RESULT_TABLE)

    if (i + 1) % 5 == 0 or (i + 1) == len(evaluation_dates):
        print(f"  {i + 1}/{len(evaluation_dates)} dates written")

print("Done")

# COMMAND ----------

weekly = spark.table(RESULT_TABLE)

weekly = (
    weekly
    .withColumn("total_carts", F.col("mature_carts") + F.col("immature_carts"))
    .withColumn("immature_pct", F.round(F.col("immature_carts") / F.col("total_carts") * 100, 2))
    .withColumn("mature_pct", F.round(F.col("mature_carts") / F.col("total_carts") * 100, 2))
    .withColumn("total_gmv", F.col("mature_gmv") + F.col("immature_gmv"))
    .withColumn("immature_gmv_pct", F.round(F.col("immature_gmv") / F.col("total_gmv") * 100, 2))
    .withColumn("total_evaluated_cost",
        F.col("mature_actual_cost") + F.col("immature_forecast_cost"))
    .withColumn("total_actual_cost",
        F.col("mature_actual_cost") + F.col("immature_actual_cost"))
    .orderBy("eval_date")
)
weekly.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Cart composition over time

# COMMAND ----------

display(
    weekly.select(
        "eval_date",
        "mature_carts", "immature_carts", "total_carts",
        "mature_pct", "immature_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. GMV composition over time

# COMMAND ----------

display(
    weekly.select(
        "eval_date",
        F.round("mature_gmv", 2).alias("mature_gmv"),
        F.round("immature_gmv", 2).alias("immature_gmv"),
        F.round("total_gmv", 2).alias("total_gmv"),
        "immature_gmv_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Cost breakdown: actual vs forecast vs total evaluated
# MAGIC
# MAGIC - **Mature actual**: real settled costs for mature carts
# MAGIC - **Immature actual**: real costs that have arrived so far for immature carts (partial)
# MAGIC - **Immature forecast**: predicted total costs for immature carts (V1 model)
# MAGIC - **Total evaluated**: mature actual + immature forecast (what we'd report)
# MAGIC - **Total actual**: mature actual + immature actual (what really happened — visible only in hindsight)

# COMMAND ----------

display(
    weekly.select(
        "eval_date",
        F.round("mature_actual_cost", 2).alias("mature_actual"),
        F.round("immature_actual_cost", 2).alias("immature_actual"),
        F.round("immature_forecast_cost", 2).alias("immature_forecast"),
        F.round("total_evaluated_cost", 2).alias("total_evaluated"),
        F.round("total_actual_cost", 2).alias("total_actual"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Forecast accuracy for immature carts
# MAGIC
# MAGIC How close is the forecast to the actual for the immature portion?

# COMMAND ----------

display(
    weekly
    .withColumn("immature_dev_pct",
        F.round(
            (F.col("immature_forecast_cost") - F.col("immature_actual_cost"))
            / F.abs(F.col("immature_actual_cost")) * 100, 3))
    .withColumn("total_dev_pct",
        F.round(
            (F.col("total_evaluated_cost") - F.col("total_actual_cost"))
            / F.abs(F.col("total_actual_cost")) * 100, 3))
    .select(
        "eval_date",
        F.round("immature_actual_cost", 2).alias("immature_actual"),
        F.round("immature_forecast_cost", 2).alias("immature_forecast"),
        "immature_dev_pct",
        F.round("total_actual_cost", 2).alias("total_actual"),
        F.round("total_evaluated_cost", 2).alias("total_evaluated"),
        "total_dev_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Cost rate comparison (cost / GMV %)
# MAGIC
# MAGIC Cost rate for mature vs immature segments.

# COMMAND ----------

display(
    weekly
    .withColumn("mature_rate_pct",
        F.round(F.col("mature_actual_cost") / F.col("mature_gmv") * 100, 4))
    .withColumn("immature_actual_rate_pct",
        F.round(F.col("immature_actual_cost") / F.col("immature_gmv") * 100, 4))
    .withColumn("immature_forecast_rate_pct",
        F.round(F.col("immature_forecast_cost") / F.col("immature_gmv") * 100, 4))
    .withColumn("total_evaluated_rate_pct",
        F.round(F.col("total_evaluated_cost") / F.col("total_gmv") * 100, 4))
    .withColumn("total_actual_rate_pct",
        F.round(F.col("total_actual_cost") / F.col("total_gmv") * 100, 4))
    .select(
        "eval_date",
        "mature_rate_pct",
        "immature_actual_rate_pct",
        "immature_forecast_rate_pct",
        "total_evaluated_rate_pct",
        "total_actual_rate_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Summary statistics

# COMMAND ----------

stats = weekly.agg(
    F.round(F.avg("immature_pct"), 2).alias("avg_immature_cart_pct"),
    F.round(F.min("immature_pct"), 2).alias("min_immature_cart_pct"),
    F.round(F.max("immature_pct"), 2).alias("max_immature_cart_pct"),
    F.round(F.avg("immature_gmv_pct"), 2).alias("avg_immature_gmv_pct"),
).collect()[0]

print(f"Immature cart share:  avg {stats['avg_immature_cart_pct']}%  "
      f"(range {stats['min_immature_cart_pct']}% – {stats['max_immature_cart_pct']}%)")
print(f"Immature GMV share:   avg {stats['avg_immature_gmv_pct']}%")

dev_stats = (
    weekly
    .withColumn("total_dev_pct",
        F.abs(F.col("total_evaluated_cost") - F.col("total_actual_cost"))
        / F.abs(F.col("total_actual_cost")) * 100)
    .agg(
        F.round(F.avg("total_dev_pct"), 3).alias("avg_total_dev"),
        F.round(F.max("total_dev_pct"), 3).alias("max_total_dev"),
    ).collect()[0]
)

print(f"Total evaluated vs actual deviation: avg {dev_stats['avg_total_dev']}%  "
      f"max {dev_stats['max_total_dev']}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Immature share by rnpl_segment over time

# COMMAND ----------

SEGMENT_TABLE = "testing.analytics._mature_immature_by_segment"

for i, eval_date in enumerate(evaluation_dates):
    immature = get_immature_carts(bucketed, eval_date, config)
    immature_tagged = immature.select("shopping_cart_id").distinct().withColumn("_imm", F.lit(True))

    all_carts = bucketed.filter(F.col("date_of_checkout") <= F.lit(eval_date))

    segment_summary = (
        all_carts
        .join(immature_tagged, "shopping_cart_id", "left")
        .withColumn("maturity",
            F.when(F.col("_imm") == True, F.lit("immature"))
            .otherwise(F.lit("mature"))
        )
        .groupBy("rnpl_segment", "maturity")
        .agg(
            F.count("*").alias("carts"),
            F.sum("gmv").alias("gmv"),
            F.sum("total_payment_costs").alias("actual_cost"),
        )
        .withColumn("eval_date", F.lit(eval_date))
        .drop("_imm")
    )

    mode = "overwrite" if i == 0 else "append"
    segment_summary.write.mode(mode).saveAsTable(SEGMENT_TABLE)

    if (i + 1) % 5 == 0 or (i + 1) == len(evaluation_dates):
        print(f"  {i + 1}/{len(evaluation_dates)} dates written")

print("Done")

# COMMAND ----------

seg_results = spark.table(SEGMENT_TABLE)

display(
    seg_results
    .groupBy("eval_date", "rnpl_segment")
    .agg(
        F.sum(F.when(F.col("maturity") == "immature", F.col("carts")).otherwise(0)).alias("immature_carts"),
        F.sum(F.when(F.col("maturity") == "mature", F.col("carts")).otherwise(0)).alias("mature_carts"),
        F.sum("carts").alias("total_carts"),
    )
    .withColumn("immature_pct",
        F.round(F.col("immature_carts") / F.col("total_carts") * 100, 2))
    .orderBy("rnpl_segment", "eval_date")
)
