# Databricks notebook source
# MAGIC %md
# MAGIC # Forecast Deviation Diagnostics
# MAGIC
# MAGIC Quick investigation of forecast vs actual deviation trends across key
# MAGIC dimensions. Helps identify which segments drive systematic bias and
# MAGIC whether the bias is stable or time-dependent.

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

dbutils.widgets.text("backtest_start", "2025-12-01", "Start date")
dbutils.widgets.text("backtest_end", "2026-02-16", "End date")
dbutils.widgets.text("snapshot_date", date.today().isoformat(), "Snapshot date")

backtest_start = date.fromisoformat(dbutils.widgets.get("backtest_start"))
backtest_end = date.fromisoformat(dbutils.widgets.get("backtest_end"))
snapshot_date = date.fromisoformat(dbutils.widgets.get("snapshot_date"))

evaluation_dates = []
d = backtest_start
while d <= backtest_end:
    evaluation_dates.append(d)
    d += timedelta(weeks=1)

print(f"Evaluating {len(evaluation_dates)} dates: {evaluation_dates[0]} → {evaluation_dates[-1]}")
print(f"Snapshot date: {snapshot_date}")
print(f"Factor columns: {config.factor_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load and prepare data

# COMMAND ----------

raw = spark.table(config.snapshot_table)
bucketed = apply_bucketing(raw.filter(F.col("gmv") > 0), config)
bucketed.cache()
print(f"Bucketed rows: {bucketed.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Run backtest and collect cart-level results
# MAGIC
# MAGIC Instead of aggregate metrics, collect per-cart forecast vs actual for
# MAGIC full flexibility in slicing.

# COMMAND ----------

DIAGNOSTIC_DIMS = [
    "rnpl_segment", "payment_processor", "payment_method_detail",
    "currency", "country_bucket", "country_group",
]

all_cart_results = []

for eval_date in evaluation_dates:
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

    keep_cols = ["shopping_cart_id", "amount", "source"]
    for dim in DIAGNOSTIC_DIMS:
        if dim in forecast_df.columns and dim not in keep_cols:
            keep_cols.append(dim)

    cart_level = (
        forecast_df.select(
            F.col("shopping_cart_id"),
            F.col("amount").alias("forecast_cost"),
            F.col("gmv"),
            F.col("source").alias("fallback_level"),
            *[F.col(d) for d in DIAGNOSTIC_DIMS if d in forecast_df.columns]
        )
        .join(
            actuals_df.select("shopping_cart_id", F.col("total_payment_costs").alias("actual_cost")),
            "shopping_cart_id", "inner",
        )
        .withColumn("eval_date", F.lit(eval_date))
    )

    all_cart_results.append(cart_level)

print(f"Collected cart-level results for {len(all_cart_results)} dates")

# COMMAND ----------

cart_results = all_cart_results[0]
for df in all_cart_results[1:]:
    cart_results = cart_results.unionByName(df, allowMissingColumns=True)

cart_results.cache()
print(f"Total cart-level rows: {cart_results.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Overall deviation trend
# MAGIC
# MAGIC Signed deviation (forecast - actual) / |actual| — positive means
# MAGIC over-prediction (forecast more negative than actual).

# COMMAND ----------

overall_trend = (
    cart_results
    .groupBy("eval_date")
    .agg(
        F.sum("forecast_cost").alias("forecast_total"),
        F.sum("actual_cost").alias("actual_total"),
        F.count("*").alias("carts"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("abs_deviation_pct",
        F.round(F.abs(F.col("forecast_total") - F.col("actual_total"))
        / F.abs(F.col("actual_total")) * 100, 3))
    .withColumn("signed_deviation_pct",
        F.round((F.col("forecast_total") - F.col("actual_total"))
        / F.abs(F.col("actual_total")) * 100, 3))
    .withColumn("forecast_rate", F.round(F.col("forecast_total") / F.col("total_gmv") * 100, 4))
    .withColumn("actual_rate", F.round(F.col("actual_total") / F.col("total_gmv") * 100, 4))
    .orderBy("eval_date")
)

display(overall_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Deviation by dimension over time
# MAGIC
# MAGIC For each key dimension, track signed deviation per eval_date.
# MAGIC Positive = over-prediction, negative = under-prediction.

# COMMAND ----------

def compute_dim_deviation(dim_col):
    return (
        cart_results
        .groupBy("eval_date", dim_col)
        .agg(
            F.sum("forecast_cost").alias("forecast_total"),
            F.sum("actual_cost").alias("actual_total"),
            F.count("*").alias("carts"),
            F.sum("gmv").alias("total_gmv"),
        )
        .withColumn("signed_dev_pct",
            F.round((F.col("forecast_total") - F.col("actual_total"))
            / F.abs(F.col("actual_total")) * 100, 3))
        .withColumn("abs_dev_pct",
            F.round(F.abs(F.col("forecast_total") - F.col("actual_total"))
            / F.abs(F.col("actual_total")) * 100, 3))
        .withColumn("forecast_rate",
            F.round(F.col("forecast_total") / F.col("total_gmv") * 100, 4))
        .withColumn("actual_rate",
            F.round(F.col("actual_total") / F.col("total_gmv") * 100, 4))
        .withColumn("dimension", F.lit(dim_col))
        .withColumnRenamed(dim_col, "dimension_value")
        .filter(F.col("carts") >= config.min_carts_for_validation)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 By rnpl_segment

# COMMAND ----------

display(
    compute_dim_deviation("rnpl_segment")
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 By payment_processor

# COMMAND ----------

display(
    compute_dim_deviation("payment_processor")
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 By payment_method_detail (top methods only)

# COMMAND ----------

top_methods = [r[0] for r in
    cart_results.groupBy("payment_method_detail")
    .agg(F.count("*").alias("n"))
    .orderBy(F.desc("n"))
    .limit(10)
    .collect()
]

display(
    compute_dim_deviation("payment_method_detail")
    .filter(F.col("dimension_value").isin(top_methods))
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 By currency (top currencies only)

# COMMAND ----------

top_currencies = [r[0] for r in
    cart_results.groupBy("currency")
    .agg(F.count("*").alias("n"))
    .orderBy(F.desc("n"))
    .limit(10)
    .collect()
]

display(
    compute_dim_deviation("currency")
    .filter(F.col("dimension_value").isin(top_currencies))
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 By country_bucket (top countries only)

# COMMAND ----------

top_countries = [r[0] for r in
    cart_results.groupBy("country_bucket")
    .agg(F.count("*").alias("n"))
    .orderBy(F.desc("n"))
    .limit(15)
    .collect()
]

display(
    compute_dim_deviation("country_bucket")
    .filter(F.col("dimension_value").isin(top_countries))
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 By country_group

# COMMAND ----------

display(
    compute_dim_deviation("country_group")
    .orderBy("dimension_value", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Bias direction summary
# MAGIC
# MAGIC Across all eval dates, which dimension values consistently over- or
# MAGIC under-predict? A consistently positive signed deviation = systematic
# MAGIC over-prediction.

# COMMAND ----------

all_dim_devs = None
for dim in DIAGNOSTIC_DIMS:
    if dim not in cart_results.columns:
        continue
    dev = compute_dim_deviation(dim)
    if all_dim_devs is None:
        all_dim_devs = dev
    else:
        all_dim_devs = all_dim_devs.unionByName(dev, allowMissingColumns=True)

bias_summary = (
    all_dim_devs
    .groupBy("dimension", "dimension_value")
    .agg(
        F.count("*").alias("eval_dates"),
        F.round(F.avg("signed_dev_pct"), 3).alias("avg_signed_dev_pct"),
        F.round(F.avg("abs_dev_pct"), 3).alias("avg_abs_dev_pct"),
        F.round(F.stddev("signed_dev_pct"), 3).alias("std_signed_dev_pct"),
        F.round(F.avg("carts"), 0).alias("avg_carts"),
        F.sum(F.when(F.col("signed_dev_pct") > 0, 1).otherwise(0)).alias("dates_over"),
        F.sum(F.when(F.col("signed_dev_pct") < 0, 1).otherwise(0)).alias("dates_under"),
    )
    .withColumn("bias_direction",
        F.when(F.col("dates_over") == F.col("eval_dates"), F.lit("ALWAYS OVER"))
        .when(F.col("dates_under") == F.col("eval_dates"), F.lit("ALWAYS UNDER"))
        .when(F.col("dates_over") > F.col("dates_under"), F.lit("mostly over"))
        .when(F.col("dates_under") > F.col("dates_over"), F.lit("mostly under"))
        .otherwise(F.lit("mixed"))
    )
    .orderBy(F.desc("avg_abs_dev_pct"))
)

display(bias_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Factor stability over time
# MAGIC
# MAGIC Are cost factors changing across eval dates? Unstable factors
# MAGIC contribute to forecast deviation.

# COMMAND ----------

factor_snapshots = []
for eval_date in evaluation_dates:
    f = compute_factors(bucketed, eval_date, config)
    f = f.withColumn("eval_date", F.lit(eval_date))
    factor_snapshots.append(f)

all_factors = factor_snapshots[0]
for f in factor_snapshots[1:]:
    all_factors = all_factors.unionByName(f, allowMissingColumns=True)

all_factors.cache()
print(f"Factor snapshots: {all_factors.count():,} rows across {len(evaluation_dates)} dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Factor drift by rnpl_segment
# MAGIC
# MAGIC How much does the average cost factor change across eval dates?

# COMMAND ----------

display(
    all_factors
    .groupBy("eval_date", "rnpl_segment")
    .agg(
        F.round(F.avg("cost_factor"), 6).alias("avg_factor"),
        F.round(F.min("cost_factor"), 6).alias("min_factor"),
        F.round(F.max("cost_factor"), 6).alias("max_factor"),
        F.count("*").alias("segments"),
        F.sum("segment_volume").alias("total_carts"),
    )
    .orderBy("rnpl_segment", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Factor drift by payment_processor
# MAGIC
# MAGIC Weighted average factor per processor over time.

# COMMAND ----------

display(
    all_factors
    .groupBy("eval_date", "payment_processor")
    .agg(
        F.round(
            F.sum(F.col("cost_factor") * F.col("segment_volume"))
            / F.sum("segment_volume"), 6
        ).alias("weighted_avg_factor"),
        F.sum("segment_volume").alias("total_carts"),
    )
    .filter(F.col("total_carts") >= 1000)
    .orderBy("payment_processor", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Factor drift by payment_method_detail (top methods)

# COMMAND ----------

display(
    all_factors
    .filter(F.col("payment_method_detail").isin(top_methods))
    .groupBy("eval_date", "payment_method_detail")
    .agg(
        F.round(
            F.sum(F.col("cost_factor") * F.col("segment_volume"))
            / F.sum("segment_volume"), 6
        ).alias("weighted_avg_factor"),
        F.sum("segment_volume").alias("total_carts"),
    )
    .orderBy("payment_method_detail", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Factor drift by currency (top currencies)

# COMMAND ----------

display(
    all_factors
    .filter(F.col("currency").isin(top_currencies))
    .groupBy("eval_date", "currency")
    .agg(
        F.round(
            F.sum(F.col("cost_factor") * F.col("segment_volume"))
            / F.sum("segment_volume"), 6
        ).alias("weighted_avg_factor"),
        F.sum("segment_volume").alias("total_carts"),
    )
    .orderBy("currency", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Fallback level analysis
# MAGIC
# MAGIC Do carts that use fallback (non-exact match) have higher deviation?

# COMMAND ----------

fallback_dev = (
    cart_results
    .groupBy("eval_date", "fallback_level")
    .agg(
        F.sum("forecast_cost").alias("forecast_total"),
        F.sum("actual_cost").alias("actual_total"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast_total") - F.col("actual_total"))
        / F.abs(F.col("actual_total")) * 100, 3))
    .orderBy("fallback_level", "eval_date")
)

display(fallback_dev)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Fallback level distribution

# COMMAND ----------

display(
    cart_results
    .groupBy("fallback_level")
    .agg(
        F.count("*").alias("total_carts"),
        F.round(F.avg(
            (F.col("forecast_cost") - F.col("actual_cost"))
            / F.abs(F.col("actual_cost"))
        ) * 100, 3).alias("avg_signed_dev_pct"),
        F.round(F.avg(
            F.abs(F.col("forecast_cost") - F.col("actual_cost"))
            / F.abs(F.col("actual_cost"))
        ) * 100, 3).alias("avg_abs_dev_pct"),
    )
    .orderBy("fallback_level")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Actual vs factor rate comparison
# MAGIC
# MAGIC Is the factor systematically higher than the actual rate of immature
# MAGIC carts? This would confirm a time-trend / non-stationarity hypothesis.

# COMMAND ----------

rate_comparison = (
    cart_results
    .withColumn("forecast_rate", F.col("forecast_cost") / F.col("gmv"))
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date")
    .agg(
        F.round(F.avg("forecast_rate") * 100, 4).alias("avg_forecast_rate_pct"),
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.round(F.percentile_approx("forecast_rate", 0.5) * 100, 4).alias("median_forecast_rate_pct"),
        F.round(F.percentile_approx("actual_rate", 0.5) * 100, 4).alias("median_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .withColumn("rate_gap_pct",
        F.round(F.col("avg_forecast_rate_pct") - F.col("avg_actual_rate_pct"), 4))
    .orderBy("eval_date")
)

print("If rate_gap_pct is consistently positive → factor over-estimates cost rate")
print("If rate_gap_pct decreases over time → factor becomes more accurate for recent data")
print()
display(rate_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Rate gap by rnpl_segment

# COMMAND ----------

display(
    cart_results
    .withColumn("forecast_rate", F.col("forecast_cost") / F.col("gmv"))
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date", "rnpl_segment")
    .agg(
        F.round(F.avg("forecast_rate") * 100, 4).alias("avg_forecast_rate_pct"),
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .withColumn("rate_gap_pct",
        F.round(F.col("avg_forecast_rate_pct") - F.col("avg_actual_rate_pct"), 4))
    .orderBy("rnpl_segment", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Rate gap by payment_processor

# COMMAND ----------

display(
    cart_results
    .withColumn("forecast_rate", F.col("forecast_cost") / F.col("gmv"))
    .withColumn("actual_rate", F.col("actual_cost") / F.col("gmv"))
    .groupBy("eval_date", "payment_processor")
    .agg(
        F.round(F.avg("forecast_rate") * 100, 4).alias("avg_forecast_rate_pct"),
        F.round(F.avg("actual_rate") * 100, 4).alias("avg_actual_rate_pct"),
        F.count("*").alias("carts"),
    )
    .withColumn("rate_gap_pct",
        F.round(F.col("avg_forecast_rate_pct") - F.col("avg_actual_rate_pct"), 4))
    .filter(F.col("carts") >= 1000)
    .orderBy("payment_processor", "eval_date")
)
