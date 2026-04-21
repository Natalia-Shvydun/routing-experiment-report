# Databricks notebook source
# MAGIC %md
# MAGIC # Forecast Deviation Analysis — Full Period (V4)
# MAGIC
# MAGIC Same structure as V1 but re-run with the improved modules:
# MAGIC - GMV-weighted fallback (instead of simple average)
# MAGIC - `_UNKNOWN` processor → NULL (falls back to average across processors)
# MAGIC - Hardened factor filtering (exclude NULL values from factor computation)
# MAGIC
# MAGIC Results are stored in a separate table so V1 results are preserved for comparison.

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
dbutils.widgets.text("snapshot_date", date.today().isoformat(), "Snapshot date (when costs were last observed)")

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
    print(f"WARNING: capping backtest_end from {backtest_end} to {latest_safe} (costs may not be mature)")
    backtest_end = latest_safe

evaluation_dates = []
d = backtest_start
while d <= backtest_end:
    evaluation_dates.append(d)
    d += timedelta(weeks=1)

print(f"Period: {evaluation_dates[0]} → {evaluation_dates[-1]}")
print(f"Evaluation dates: {len(evaluation_dates)} weeks")
print(f"Snapshot date: {snapshot_date}")
print(f"Factor columns: {config.factor_columns}")

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
# MAGIC # 1. Run full-period backtest (cart-level)

# COMMAND ----------

DIMS = [
    "rnpl_segment", "payment_processor", "payment_method_detail",
    "currency", "country_bucket", "country_group",
]

RESULT_TABLE = "testing.analytics._forecast_deviation_cart_level_v4"
BATCH_SIZE = 5

for i, eval_date in enumerate(evaluation_dates):
    factors = compute_factors(bucketed, eval_date, config)
    immature = get_immature_carts(bucketed, eval_date, config)
    forecast_df = apply_factors_with_fallback(immature, factors, config)

    immature_ids = immature.select("shopping_cart_id").distinct()
    actuals_df = (
        bucketed
        .join(immature_ids, "shopping_cart_id", "inner")
        .filter(F.col("total_payment_costs") != 0)
    )
    actuals_df = _filter_mature_actuals(actuals_df, snapshot_date, config)

    mature_ids = actuals_df.select("shopping_cart_id")
    forecast_df = forecast_df.join(mature_ids, "shopping_cart_id", "inner")

    cart_level = (
        forecast_df.select(
            F.col("shopping_cart_id"),
            F.col("amount").alias("forecast_cost"),
            F.col("gmv"),
            F.col("source").alias("fallback_level"),
            *[F.col(d) for d in DIMS if d in forecast_df.columns]
        )
        .join(
            actuals_df.select("shopping_cart_id", F.col("total_payment_costs").alias("actual_cost")),
            "shopping_cart_id", "inner",
        )
        .withColumn("eval_date", F.lit(eval_date))
    )

    mode = "overwrite" if i == 0 else "append"
    cart_level.write.mode(mode).saveAsTable(RESULT_TABLE)

    if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(evaluation_dates):
        print(f"  Written {i + 1}/{len(evaluation_dates)} dates to {RESULT_TABLE}")

print(f"Done — {len(evaluation_dates)} dates written")

# COMMAND ----------

results = spark.table(RESULT_TABLE)
results.cache()
print(f"Total cart-level rows: {results.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Overall deviation trend
# MAGIC
# MAGIC Weekly signed deviation over the full period.
# MAGIC Positive = over-prediction (forecast more negative than actual).

# COMMAND ----------

overall = (
    results
    .groupBy("eval_date")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("forecast_rate_pct", F.round(F.col("forecast") / F.col("gmv") * 100, 4))
    .withColumn("actual_rate_pct", F.round(F.col("actual") / F.col("gmv") * 100, 4))
    .orderBy("eval_date")
)

display(overall)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary statistics

# COMMAND ----------

stats = overall.agg(
    F.round(F.avg("signed_dev_pct"), 3).alias("avg_signed_dev"),
    F.round(F.avg("abs_dev_pct"), 3).alias("avg_abs_dev"),
    F.round(F.max("abs_dev_pct"), 3).alias("max_abs_dev"),
    F.round(F.min("signed_dev_pct"), 3).alias("min_signed_dev"),
    F.round(F.max("signed_dev_pct"), 3).alias("max_signed_dev"),
    F.round(F.stddev("signed_dev_pct"), 3).alias("std_signed_dev"),
    F.sum(F.when(F.col("signed_dev_pct") > 0, 1).otherwise(0)).alias("weeks_over"),
    F.sum(F.when(F.col("signed_dev_pct") < 0, 1).otherwise(0)).alias("weeks_under"),
).collect()[0]

print(f"Avg signed deviation: {stats['avg_signed_dev']}%")
print(f"Avg absolute deviation: {stats['avg_abs_dev']}%")
print(f"Max absolute deviation: {stats['max_abs_dev']}%")
print(f"Signed range: {stats['min_signed_dev']}% to {stats['max_signed_dev']}%")
print(f"Std of signed deviation: {stats['std_signed_dev']}%")
print(f"Weeks over-predicting: {stats['weeks_over']} / {stats['weeks_over'] + stats['weeks_under']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Deviation by rnpl_segment

# COMMAND ----------

display(
    results
    .groupBy("eval_date", "rnpl_segment")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("rnpl_segment", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Deviation by payment_processor

# COMMAND ----------

display(
    results
    .groupBy("eval_date", "payment_processor")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .filter(F.col("carts") >= 500)
    .orderBy("payment_processor", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Deviation by payment_method_detail

# COMMAND ----------

top_methods = [r[0] for r in
    results.groupBy("payment_method_detail")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(20).collect()]

display(
    results
    .filter(F.col("payment_method_detail").isin(top_methods))
    .groupBy("eval_date", "payment_method_detail")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("payment_method_detail", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Deviation by currency

# COMMAND ----------

top_currencies = [r[0] for r in
    results.groupBy("currency")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(20).collect()]

display(
    results
    .filter(F.col("currency").isin(top_currencies))
    .groupBy("eval_date", "currency")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("currency", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Deviation by country_bucket

# COMMAND ----------

top_countries = [r[0] for r in
    results.groupBy("country_bucket")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(20).collect()]

display(
    results
    .filter(F.col("country_bucket").isin(top_countries))
    .groupBy("eval_date", "country_bucket")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("country_bucket", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Deviation by country_group

# COMMAND ----------

display(
    results
    .groupBy("eval_date", "country_group")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .filter(F.col("carts") >= 500)
    .orderBy("country_group", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 9. Bias summary — persistent over/under-prediction
# MAGIC
# MAGIC Which dimension values consistently over- or under-predict across the
# MAGIC full period?

# COMMAND ----------

def bias_for_dim(dim_col):
    return (
        results
        .groupBy("eval_date", dim_col)
        .agg(
            F.sum("forecast_cost").alias("forecast"),
            F.sum("actual_cost").alias("actual"),
            F.count("*").alias("carts"),
        )
        .withColumn("signed_dev_pct",
            (F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100)
        .filter(F.col("carts") >= config.min_carts_for_validation)
        .groupBy(dim_col)
        .agg(
            F.count("*").alias("weeks"),
            F.round(F.avg("signed_dev_pct"), 3).alias("avg_signed_dev"),
            F.round(F.avg(F.abs(F.col("signed_dev_pct"))), 3).alias("avg_abs_dev"),
            F.round(F.max(F.abs(F.col("signed_dev_pct"))), 3).alias("max_abs_dev"),
            F.round(F.stddev("signed_dev_pct"), 3).alias("std_dev"),
            F.round(F.avg("carts"), 0).alias("avg_carts"),
            F.sum(F.when(F.col("signed_dev_pct") > 0, 1).otherwise(0)).alias("weeks_over"),
            F.sum(F.when(F.col("signed_dev_pct") < 0, 1).otherwise(0)).alias("weeks_under"),
        )
        .withColumn("bias",
            F.when(F.col("weeks_over") == F.col("weeks"), F.lit("ALWAYS OVER"))
            .when(F.col("weeks_under") == F.col("weeks"), F.lit("ALWAYS UNDER"))
            .when(F.col("weeks_over") > F.col("weeks") * 0.8, F.lit("mostly over"))
            .when(F.col("weeks_under") > F.col("weeks") * 0.8, F.lit("mostly under"))
            .otherwise(F.lit("mixed"))
        )
        .withColumn("dimension", F.lit(dim_col))
        .withColumnRenamed(dim_col, "value")
        .orderBy(F.desc("avg_abs_dev"))
    )

# COMMAND ----------

all_bias = None
for dim in DIMS:
    if dim not in results.columns:
        continue
    b = bias_for_dim(dim)
    if all_bias is None:
        all_bias = b
    else:
        all_bias = all_bias.unionByName(b, allowMissingColumns=True)

display(
    all_bias
    .filter(F.col("avg_abs_dev") > 1.0)
    .orderBy(F.desc("avg_abs_dev"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Bias direction distribution

# COMMAND ----------

display(
    all_bias
    .groupBy("dimension", "bias")
    .agg(F.count("*").alias("segments"))
    .orderBy("dimension", "bias")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 10. Cost rate trend over time
# MAGIC
# MAGIC Is the actual cost rate changing over time? If rates are trending
# MAGIC downward, the historical factor systematically over-predicts.

# COMMAND ----------

rate_trend = (
    results
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .withColumn("forecast_rate", F.col("forecast_cost") / F.col("gmv"))
    .groupBy("eval_date")
    .agg(
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.round(F.avg("forecast_rate") * 100, 4).alias("avg_forecast_rate_pct"),
        F.round(F.percentile_approx("actual_rate", 0.5) * 100, 4).alias("median_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .withColumn("rate_gap_pct",
        F.round(F.col("avg_forecast_rate_pct") - F.col("avg_actual_rate_pct"), 4))
    .orderBy("eval_date")
)

display(rate_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Rate trend by rnpl_segment

# COMMAND ----------

display(
    results
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date", "rnpl_segment")
    .agg(
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.round(F.percentile_approx("actual_rate", 0.5) * 100, 4).alias("median_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .orderBy("rnpl_segment", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Rate trend by payment_processor

# COMMAND ----------

display(
    results
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date", "payment_processor")
    .agg(
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .filter(F.col("carts") >= 500)
    .orderBy("payment_processor", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Rate trend by currency (top 5)

# COMMAND ----------

top5_curr = top_currencies[:5] if len(top_currencies) >= 5 else top_currencies

display(
    results
    .filter(F.col("currency").isin(top5_curr))
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date", "currency")
    .agg(
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .orderBy("currency", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 11. Fallback level analysis

# COMMAND ----------

display(
    results
    .groupBy("eval_date", "fallback_level")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("fallback_level", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Fallback usage over time

# COMMAND ----------

display(
    results
    .groupBy("eval_date", "fallback_level")
    .agg(F.count("*").alias("carts"))
    .withColumn("total", F.sum("carts").over(Window.partitionBy("eval_date")))
    .withColumn("pct", F.round(F.col("carts") / F.col("total") * 100, 1))
    .orderBy("eval_date", "fallback_level")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 12. Monthly aggregated view
# MAGIC
# MAGIC Smoothed monthly view for easier trend detection.

# COMMAND ----------

display(
    results
    .withColumn("month", F.date_trunc("month", F.col("eval_date")))
    .groupBy("month")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
        F.countDistinct("eval_date").alias("weeks"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 Monthly by rnpl_segment

# COMMAND ----------

display(
    results
    .withColumn("month", F.date_trunc("month", F.col("eval_date")))
    .groupBy("month", "rnpl_segment")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("rnpl_segment", "month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Monthly by payment_processor

# COMMAND ----------

display(
    results
    .withColumn("month", F.date_trunc("month", F.col("eval_date")))
    .groupBy("month", "payment_processor")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .filter(F.col("carts") >= 1000)
    .orderBy("payment_processor", "month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 13. V4 vs V1 comparison
# MAGIC
# MAGIC Side-by-side weekly deviation: V1 (old modules) vs V4 (improved modules).

# COMMAND ----------

V1_TABLE = "testing.analytics._forecast_deviation_cart_level"

try:
    v1_results = spark.table(V1_TABLE)

    v1_overall = (
        v1_results
        .groupBy("eval_date")
        .agg(
            F.sum("forecast_cost").alias("v1_forecast"),
            F.sum("actual_cost").alias("v1_actual"),
            F.count("*").alias("v1_carts"),
        )
        .withColumn("v1_signed_dev_pct",
            F.round((F.col("v1_forecast") - F.col("v1_actual")) / F.abs(F.col("v1_actual")) * 100, 3))
    )

    v4_overall = (
        results
        .groupBy("eval_date")
        .agg(
            F.sum("forecast_cost").alias("v4_forecast"),
            F.sum("actual_cost").alias("v4_actual"),
            F.count("*").alias("v4_carts"),
        )
        .withColumn("v4_signed_dev_pct",
            F.round((F.col("v4_forecast") - F.col("v4_actual")) / F.abs(F.col("v4_actual")) * 100, 3))
    )

    comparison = (
        v1_overall.join(v4_overall, "eval_date", "outer")
        .withColumn("improvement_pp",
            F.round(F.abs(F.col("v1_signed_dev_pct")) - F.abs(F.col("v4_signed_dev_pct")), 3))
        .orderBy("eval_date")
    )

    display(comparison)
except Exception as e:
    print(f"V1 table not available for comparison: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.1 V4 vs V1 summary

# COMMAND ----------

try:
    comp_stats = comparison.agg(
        F.round(F.avg("v1_signed_dev_pct"), 3).alias("v1_avg_signed_dev"),
        F.round(F.avg(F.abs(F.col("v1_signed_dev_pct"))), 3).alias("v1_avg_abs_dev"),
        F.round(F.avg("v4_signed_dev_pct"), 3).alias("v4_avg_signed_dev"),
        F.round(F.avg(F.abs(F.col("v4_signed_dev_pct"))), 3).alias("v4_avg_abs_dev"),
        F.round(F.avg("improvement_pp"), 3).alias("avg_improvement_pp"),
        F.sum(F.when(F.col("improvement_pp") > 0, 1).otherwise(0)).alias("weeks_v4_better"),
        F.sum(F.when(F.col("improvement_pp") < 0, 1).otherwise(0)).alias("weeks_v1_better"),
    ).collect()[0]

    print(f"V1 avg absolute deviation: {comp_stats['v1_avg_abs_dev']}%")
    print(f"V4 avg absolute deviation: {comp_stats['v4_avg_abs_dev']}%")
    print(f"Avg improvement (positive = V4 better): {comp_stats['avg_improvement_pp']} pp")
    print(f"Weeks V4 better: {comp_stats['weeks_v4_better']} / {comp_stats['weeks_v4_better'] + comp_stats['weeks_v1_better']}")
except Exception:
    print("V1 comparison not available")
