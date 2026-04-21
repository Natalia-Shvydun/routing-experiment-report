# Databricks notebook source
# MAGIC %md
# MAGIC # Forecast Deviation Analysis — Full Period (V3: min_volume = 1000)
# MAGIC
# MAGIC Same backtest as V1 but with `min_volume_for_factor = 1000` (vs 100).
# MAGIC Hypothesis: raising the minimum cart threshold for factor segments
# MAGIC forces more carts into broader fallback groups, reducing noise from
# MAGIC small volatile segments (e.g. adyen_twint during testing).

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
config.min_volume_for_factor = 1000

print(f"min_volume_for_factor: {config.min_volume_for_factor}  (V1 default: 100)")

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

RESULT_TABLE = "testing.analytics._forecast_deviation_cart_level_v3"
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
# MAGIC # 2. Overall deviation trend (V3 vs V1 comparison)

# COMMAND ----------

overall_v3 = (
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

display(overall_v3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 V3 vs V1 side-by-side

# COMMAND ----------

V1_TABLE = "testing.analytics._forecast_deviation_cart_level"

try:
    v1_exists = spark.catalog.tableExists(V1_TABLE)
except Exception:
    v1_exists = False

if v1_exists:
    results_v1 = spark.table(V1_TABLE)

    overall_v1 = (
        results_v1
        .groupBy("eval_date")
        .agg(
            F.sum("forecast_cost").alias("forecast_v1"),
            F.sum("actual_cost").alias("actual_v1"),
            F.count("*").alias("carts_v1"),
        )
        .withColumn("signed_dev_pct_v1",
            F.round((F.col("forecast_v1") - F.col("actual_v1")) / F.abs(F.col("actual_v1")) * 100, 3))
    )

    comparison = (
        overall_v3.select("eval_date", "carts",
            F.col("signed_dev_pct").alias("signed_dev_pct_v3"),
            F.col("abs_dev_pct").alias("abs_dev_pct_v3"))
        .join(
            overall_v1.select("eval_date", "signed_dev_pct_v1"),
            "eval_date", "left",
        )
        .withColumn("improvement",
            F.round(F.abs(F.col("signed_dev_pct_v1")) - F.abs(F.col("signed_dev_pct_v3")), 3))
        .orderBy("eval_date")
    )

    display(comparison)

    imp = comparison.agg(
        F.round(F.avg("signed_dev_pct_v1"), 3).alias("avg_dev_v1"),
        F.round(F.avg("signed_dev_pct_v3"), 3).alias("avg_dev_v3"),
        F.round(F.avg(F.abs(F.col("signed_dev_pct_v1"))), 3).alias("avg_abs_v1"),
        F.round(F.avg(F.abs(F.col("signed_dev_pct_v3"))), 3).alias("avg_abs_v3"),
        F.round(F.avg("improvement"), 3).alias("avg_improvement"),
    ).collect()[0]

    print(f"V1 avg signed deviation: {imp['avg_dev_v1']}%  (min_volume=100)")
    print(f"V3 avg signed deviation: {imp['avg_dev_v3']}%  (min_volume=1000)")
    print(f"V1 avg abs deviation:    {imp['avg_abs_v1']}%")
    print(f"V3 avg abs deviation:    {imp['avg_abs_v3']}%")
    print(f"Avg improvement (positive = V3 better): {imp['avg_improvement']}%")
else:
    print(f"V1 table {V1_TABLE} not found — run forecast_deviation_full_period first to enable comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Summary statistics (V3)

# COMMAND ----------

stats = overall_v3.agg(
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
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("forecast_rate", F.round(F.col("forecast") / F.col("total_gmv") * 100, 4))
    .withColumn("actual_rate", F.round(F.col("actual") / F.col("total_gmv") * 100, 4))
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
# MAGIC # 9. Bias summary

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
# MAGIC # 10. Cost rate trend

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
# MAGIC ---
# MAGIC # 11. Fallback level analysis
# MAGIC
# MAGIC With min_volume=1000, more carts should fall through to broader levels.

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
