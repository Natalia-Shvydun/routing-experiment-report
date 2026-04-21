# Databricks notebook source
# MAGIC %md
# MAGIC # Estimated Costs — Analysis & Separate Forecasting
# MAGIC
# MAGIC **Context**: "Estimated" acquirer fees are aggregate costs (invoice deductions,
# MAGIC tiered pricing adjustments, network token fees) that processors distribute
# MAGIC across transactions. They don't arrive with each transaction but in batches:
# MAGIC
# MAGIC | Processor | Estimated cost type | Frequency | Granularity |
# MAGIC |-----------|-------------------|-----------|-------------|
# MAGIC | **ADYEN** | Transaction fees | Daily | merchant_account |
# MAGIC | **ADYEN** | Invoice deductions | **Monthly** | merchant_account |
# MAGIC | **CHECKOUT** | Network token fees | Daily | merchant_account × payment_method |
# MAGIC | **CHECKOUT** | Adjustments | Daily | merchant_account × payment_method |
# MAGIC | **CHECKOUT** | Tiered pricing adj | **Monthly** | merchant_account |
# MAGIC | **JPMC** | Card network fees | **Monthly** | merchant_account × payment_method |
# MAGIC | **JPMC** | Other fees | Daily | merchant_account |
# MAGIC
# MAGIC Because monthly items only appear once invoiced, recent carts may have
# MAGIC zero estimated costs even though they'll eventually receive them.
# MAGIC
# MAGIC **Goal**: Build a separate forecasting layer for estimated costs that can
# MAGIC be added on top of V2 (precise-only) results.

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

config = ForecastingConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

dbutils.widgets.text("snapshot_date", date.today().isoformat(), "Snapshot date")
snapshot_date = date.fromisoformat(dbutils.widgets.get("snapshot_date"))
print(f"Snapshot date: {snapshot_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

raw = spark.table(config.snapshot_table)
df = apply_bucketing(raw.filter(F.col("gmv") > 0), config)

df = (
    df
    .withColumn("estimated_cost",
        F.col("total_payment_costs") - F.col("total_payment_costs_wo_estimated"))
    .withColumn("precise_cost", F.col("total_payment_costs_wo_estimated"))
    .withColumn("week", F.date_trunc("week", F.col("date_of_checkout")))
)
df.cache()

total = df.count()
print(f"Total carts: {total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Overall estimated cost profile

# COMMAND ----------

totals = df.agg(
    F.sum("total_payment_costs").alias("total_costs"),
    F.sum("precise_cost").alias("precise_costs"),
    F.sum("estimated_cost").alias("estimated_costs"),
    F.sum("gmv").alias("total_gmv"),
    F.count("*").alias("carts"),
    F.sum(F.when(F.col("estimated_cost") != 0, 1).otherwise(0)).alias("carts_with_estimated"),
).collect()[0]

print(f"Total costs:           {float(totals['total_costs']):>18,.2f} EUR")
print(f"  Precise costs:       {float(totals['precise_costs']):>18,.2f} EUR")
print(f"  Estimated costs:     {float(totals['estimated_costs']):>18,.2f} EUR")
print(f"  Estimated share:     {abs(float(totals['estimated_costs']) / float(totals['total_costs'])) * 100:.2f}%")
print(f"")
print(f"Total GMV:             {float(totals['total_gmv']):>18,.2f} EUR")
print(f"Estimated cost rate:   {float(totals['estimated_costs']) / float(totals['total_gmv']) * 100:.4f}% of GMV")
print(f"")
print(f"Carts with estimated:  {int(totals['carts_with_estimated']):,} / {int(totals['carts']):,} "
      f"({int(totals['carts_with_estimated']) / int(totals['carts']) * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Estimated costs by processor
# MAGIC
# MAGIC Which processors contribute estimated costs?

# COMMAND ----------

display(
    df
    .groupBy("payment_processor")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("precise_cost").alias("precise_cost"),
        F.sum("estimated_cost").alias("estimated_cost"),
    )
    .withColumn("estimated_rate_bps",
        F.round(F.col("estimated_cost") / F.col("gmv") * 10000, 2))
    .withColumn("estimated_share_pct",
        F.round(F.col("estimated_cost") / F.col("total_cost") * 100, 2))
    .withColumn("precise_rate_bps",
        F.round(F.col("precise_cost") / F.col("gmv") * 10000, 2))
    .orderBy(F.asc("estimated_cost"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Estimated cost rate over time (weekly)
# MAGIC
# MAGIC Is the estimated cost rate stable or does it fluctuate?

# COMMAND ----------

display(
    df
    .groupBy("week")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum("estimated_cost").alias("estimated_cost"),
        F.sum("precise_cost").alias("precise_cost"),
        F.sum("total_payment_costs").alias("total_cost"),
    )
    .withColumn("estimated_rate_bps",
        F.round(F.col("estimated_cost") / F.col("gmv") * 10000, 2))
    .withColumn("precise_rate_bps",
        F.round(F.col("precise_cost") / F.col("gmv") * 10000, 2))
    .withColumn("total_rate_bps",
        F.round(F.col("total_cost") / F.col("gmv") * 10000, 2))
    .orderBy("week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Estimated cost rate by processor over time

# COMMAND ----------

display(
    df
    .groupBy("week", "payment_processor")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum("estimated_cost").alias("estimated_cost"),
    )
    .withColumn("estimated_rate_bps",
        F.round(F.col("estimated_cost") / F.col("gmv") * 10000, 2))
    .filter(F.col("carts") >= 100)
    .orderBy("payment_processor", "week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Monthly invoice pattern analysis
# MAGIC
# MAGIC For monthly-invoiced items (Adyen invoice deductions, CKO tiered pricing,
# MAGIC JPMC network fees), costs arrive retroactively. Carts from the current
# MAGIC month may not yet have them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Estimated costs by checkout month — do recent months have lower estimated costs?

# COMMAND ----------

display(
    df
    .withColumn("checkout_month", F.date_trunc("month", F.col("date_of_checkout")))
    .groupBy("checkout_month", "payment_processor")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum("estimated_cost").alias("estimated_cost"),
    )
    .withColumn("estimated_rate_bps",
        F.round(F.col("estimated_cost") / F.col("gmv") * 10000, 2))
    .filter(F.col("carts") >= 100)
    .orderBy("payment_processor", "checkout_month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Share of carts with zero estimated cost by checkout recency
# MAGIC
# MAGIC If monthly invoices haven't landed, recent carts should show higher
# MAGIC zero-estimated-cost rates.

# COMMAND ----------

display(
    df
    .withColumn("days_since_checkout",
        F.datediff(F.lit(snapshot_date), F.col("date_of_checkout")))
    .withColumn("recency_bucket",
        F.when(F.col("days_since_checkout") <= 7, "0-7d")
        .when(F.col("days_since_checkout") <= 14, "8-14d")
        .when(F.col("days_since_checkout") <= 28, "15-28d")
        .when(F.col("days_since_checkout") <= 45, "29-45d")
        .when(F.col("days_since_checkout") <= 60, "46-60d")
        .otherwise("60d+")
    )
    .groupBy("recency_bucket", "payment_processor")
    .agg(
        F.count("*").alias("carts"),
        F.sum(F.when(F.col("estimated_cost") == 0, 1).otherwise(0)).alias("zero_estimated"),
        F.sum("gmv").alias("gmv"),
        F.sum("estimated_cost").alias("estimated_cost"),
    )
    .withColumn("zero_pct", F.round(F.col("zero_estimated") / F.col("carts") * 100, 1))
    .withColumn("estimated_rate_bps",
        F.round(F.col("estimated_cost") / F.col("gmv") * 10000, 2))
    .filter(F.col("carts") >= 50)
    .orderBy("payment_processor", "recency_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Estimated cost maturation curve
# MAGIC
# MAGIC After how many days post-checkout do estimated costs reach their
# MAGIC final value? Use older carts (60d+) as the "mature" baseline.

# COMMAND ----------

MATURE_CUTOFF_DAYS = 60

mature_rate_by_processor = (
    df
    .filter(
        F.datediff(F.lit(snapshot_date), F.col("date_of_checkout")) >= MATURE_CUTOFF_DAYS
    )
    .groupBy("payment_processor")
    .agg(
        F.sum("estimated_cost").alias("mature_estimated"),
        F.sum("gmv").alias("mature_gmv"),
    )
    .withColumn("mature_rate",
        F.col("mature_estimated") / F.col("mature_gmv"))
    .select("payment_processor", "mature_rate")
)

display(mature_rate_by_processor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Estimated cost accumulation by days since checkout

# COMMAND ----------

display(
    df
    .withColumn("days_since_checkout",
        F.datediff(F.lit(snapshot_date), F.col("date_of_checkout")))
    .withColumn("day_bucket",
        F.when(F.col("days_since_checkout") <= 60,
            (F.col("days_since_checkout") / 7).cast("int") * 7)
        .otherwise(F.lit(60))
    )
    .groupBy("day_bucket", "payment_processor")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum("estimated_cost").alias("estimated_cost"),
    )
    .join(mature_rate_by_processor, "payment_processor", "left")
    .withColumn("current_rate",
        F.col("estimated_cost") / F.col("gmv"))
    .withColumn("maturation_pct",
        F.when(F.col("mature_rate") != 0,
            F.round(F.col("current_rate") / F.col("mature_rate") * 100, 1))
        .otherwise(F.lit(None))
    )
    .filter(F.col("carts") >= 50)
    .orderBy("payment_processor", "day_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Estimated cost factor model
# MAGIC
# MAGIC **Approach**: For each processor, compute the estimated cost rate
# MAGIC (estimated_cost / GMV) from mature carts (60d+ since checkout).
# MAGIC Then apply this rate to all carts — the difference between applied
# MAGIC and actual is the "estimated cost forecast".
# MAGIC
# MAGIC Unlike the main model, estimated costs are primarily driven by
# MAGIC **processor** (each has different fee structures), and less by
# MAGIC payment method/currency/country.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Factor by processor only

# COMMAND ----------

est_factors_processor = (
    df
    .filter(
        F.datediff(F.lit(snapshot_date), F.col("date_of_checkout")) >= MATURE_CUTOFF_DAYS
    )
    .groupBy("payment_processor")
    .agg(
        F.sum("estimated_cost").alias("total_est"),
        F.sum("gmv").alias("total_gmv"),
        F.count("*").alias("carts"),
    )
    .withColumn("est_factor", F.col("total_est") / F.col("total_gmv"))
    .filter(F.col("carts") >= 100)
)

display(est_factors_processor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Factor by processor × payment_method_detail
# MAGIC
# MAGIC CKO and JPMC distribute some fees by payment_method. Check if
# MAGIC the rate varies significantly within a processor.

# COMMAND ----------

est_factors_proc_method = (
    df
    .filter(
        F.datediff(F.lit(snapshot_date), F.col("date_of_checkout")) >= MATURE_CUTOFF_DAYS
    )
    .groupBy("payment_processor", "payment_method_detail")
    .agg(
        F.sum("estimated_cost").alias("total_est"),
        F.sum("gmv").alias("total_gmv"),
        F.count("*").alias("carts"),
    )
    .withColumn("est_factor", F.col("total_est") / F.col("total_gmv"))
    .filter(F.col("carts") >= 100)
)

display(est_factors_proc_method.orderBy("payment_processor", "payment_method_detail"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Within-processor variance — is payment_method_detail needed?

# COMMAND ----------

variance_check = (
    est_factors_proc_method
    .groupBy("payment_processor")
    .agg(
        F.count("*").alias("method_segments"),
        F.round(F.avg("est_factor") * 10000, 2).alias("avg_rate_bps"),
        F.round(F.stddev("est_factor") * 10000, 2).alias("std_rate_bps"),
        F.round(F.min("est_factor") * 10000, 2).alias("min_rate_bps"),
        F.round(F.max("est_factor") * 10000, 2).alias("max_rate_bps"),
    )
    .withColumn("cv", F.round(F.abs(F.col("std_rate_bps") / F.col("avg_rate_bps")), 3))
    .orderBy("payment_processor")
)

display(variance_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Backtest — estimated cost forecasting
# MAGIC
# MAGIC For each evaluation date, identify carts that may not yet have their
# MAGIC estimated costs (checkout within last N days). Apply the estimated
# MAGIC cost factor and compare to actual.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Define which carts are "estimated-immature"
# MAGIC
# MAGIC Monthly invoices typically arrive by the 10th-15th of the following
# MAGIC month. So a cart from the current calendar month (and potentially
# MAGIC the prior month if invoice hasn't arrived) may be missing estimated
# MAGIC costs. We test different recency windows.

# COMMAND ----------

dbutils.widgets.text("backtest_start", "2025-10-01", "Backtest start")
dbutils.widgets.text("backtest_end", "2026-02-16", "Backtest end")

bt_start = date.fromisoformat(dbutils.widgets.get("backtest_start"))
bt_end = date.fromisoformat(dbutils.widgets.get("backtest_end"))

latest_safe = snapshot_date - timedelta(days=MATURE_CUTOFF_DAYS)
if bt_end > latest_safe:
    print(f"WARNING: capping backtest_end from {bt_end} to {latest_safe}")
    bt_end = latest_safe

eval_dates = []
d = bt_start
while d <= bt_end:
    eval_dates.append(d)
    d += timedelta(weeks=1)

print(f"Backtest: {eval_dates[0]} → {eval_dates[-1]} ({len(eval_dates)} weeks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Run backtest with different immaturity windows
# MAGIC
# MAGIC Test windows: 14d, 28d, 35d, 45d — how far back do we need to go
# MAGIC to catch carts without estimated costs?

# COMMAND ----------

WINDOWS = [14, 28, 35, 45]
FACTOR_WINDOW = 28
BT_TABLE = "testing.analytics._estimated_cost_backtest"

first_write = True

for window_days in WINDOWS:
    for i, eval_date in enumerate(eval_dates):
        # Mature carts: compute estimated cost rate by processor
        factor_start = eval_date - timedelta(days=MATURE_CUTOFF_DAYS + FACTOR_WINDOW)
        factor_end = eval_date - timedelta(days=MATURE_CUTOFF_DAYS)

        factors = (
            df
            .filter(F.col("date_of_checkout").between(F.lit(factor_start), F.lit(factor_end)))
            .groupBy("payment_processor")
            .agg(
                F.sum("estimated_cost").alias("hist_est"),
                F.sum("gmv").alias("hist_gmv"),
                F.count("*").alias("hist_carts"),
            )
            .withColumn("est_factor", F.col("hist_est") / F.col("hist_gmv"))
            .filter(F.col("hist_carts") >= 50)
            .select("payment_processor", "est_factor")
        )

        # Immature carts: checkout within last window_days
        cutoff = eval_date - timedelta(days=window_days)
        immature = (
            df
            .filter(F.col("date_of_checkout") > F.lit(cutoff))
            .filter(F.col("date_of_checkout") <= F.lit(eval_date))
        )

        # Apply factor
        forecasted = (
            immature
            .join(factors, "payment_processor", "left")
            .withColumn("forecast_estimated",
                F.col("gmv") * F.coalesce(F.col("est_factor"), F.lit(0.0)))
        )

        summary = forecasted.agg(
            F.count("*").alias("carts"),
            F.sum("gmv").alias("gmv"),
            F.sum("estimated_cost").alias("actual_estimated"),
            F.sum("forecast_estimated").alias("forecast_estimated"),
            F.sum("precise_cost").alias("precise_cost"),
            F.sum("total_payment_costs").alias("total_actual"),
        ).withColumn("eval_date", F.lit(eval_date)
        ).withColumn("window_days", F.lit(window_days))

        mode = "overwrite" if first_write else "append"
        summary.write.mode(mode).saveAsTable(BT_TABLE)
        first_write = False

    print(f"  Window {window_days}d: {len(eval_dates)} dates written")

print("Done")

# COMMAND ----------

bt = spark.table(BT_TABLE)
bt = (
    bt
    .withColumn("actual_est_rate_bps",
        F.round(F.col("actual_estimated") / F.col("gmv") * 10000, 2))
    .withColumn("forecast_est_rate_bps",
        F.round(F.col("forecast_estimated") / F.col("gmv") * 10000, 2))
    .withColumn("est_deviation_pct",
        F.round(
            (F.col("forecast_estimated") - F.col("actual_estimated"))
            / F.abs(F.col("actual_estimated")) * 100, 3))
    .withColumn("total_v2_plus_est",
        F.col("precise_cost") + F.col("forecast_estimated"))
    .withColumn("total_deviation_pct",
        F.round(
            (F.col("total_v2_plus_est") - F.col("total_actual"))
            / F.abs(F.col("total_actual")) * 100, 3))
    .orderBy("window_days", "eval_date")
)
bt.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Results by window size

# COMMAND ----------

display(
    bt.select(
        "window_days", "eval_date", "carts", "gmv",
        "actual_estimated", "forecast_estimated",
        "actual_est_rate_bps", "forecast_est_rate_bps",
        "est_deviation_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Summary by window — which window works best?

# COMMAND ----------

display(
    bt
    .groupBy("window_days")
    .agg(
        F.round(F.avg("est_deviation_pct"), 3).alias("avg_est_dev_pct"),
        F.round(F.avg(F.abs(F.col("est_deviation_pct"))), 3).alias("avg_abs_est_dev_pct"),
        F.round(F.max(F.abs(F.col("est_deviation_pct"))), 3).alias("max_abs_est_dev_pct"),
        F.round(F.stddev("est_deviation_pct"), 3).alias("std_est_dev_pct"),
        F.round(F.avg("total_deviation_pct"), 3).alias("avg_total_dev_pct"),
        F.round(F.avg(F.abs(F.col("total_deviation_pct"))), 3).alias("avg_abs_total_dev_pct"),
        F.round(F.avg("carts"), 0).alias("avg_immature_carts"),
    )
    .orderBy("window_days")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 Total cost accuracy: V2 + estimated forecast vs full actual
# MAGIC
# MAGIC The combined model = V2 precise costs (actual for immature carts)
# MAGIC + estimated cost forecast. Compare this to total actual costs.

# COMMAND ----------

display(
    bt
    .select(
        "window_days", "eval_date",
        F.round("total_actual", 2).alias("total_actual"),
        F.round("total_v2_plus_est", 2).alias("v2_plus_est_forecast"),
        "total_deviation_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Processor-level backtest detail
# MAGIC
# MAGIC How does the estimated cost forecast perform per processor?

# COMMAND ----------

BEST_WINDOW = 28  # adjust after reviewing section 7.4

BT_PROC_TABLE = "testing.analytics._estimated_cost_backtest_by_processor"

first_write = True

for i, eval_date in enumerate(eval_dates):
    factor_start = eval_date - timedelta(days=MATURE_CUTOFF_DAYS + FACTOR_WINDOW)
    factor_end = eval_date - timedelta(days=MATURE_CUTOFF_DAYS)

    factors = (
        df
        .filter(F.col("date_of_checkout").between(F.lit(factor_start), F.lit(factor_end)))
        .groupBy("payment_processor")
        .agg(
            F.sum("estimated_cost").alias("hist_est"),
            F.sum("gmv").alias("hist_gmv"),
        )
        .withColumn("est_factor", F.col("hist_est") / F.col("hist_gmv"))
        .select("payment_processor", "est_factor")
    )

    cutoff = eval_date - timedelta(days=BEST_WINDOW)
    immature = (
        df
        .filter(F.col("date_of_checkout") > F.lit(cutoff))
        .filter(F.col("date_of_checkout") <= F.lit(eval_date))
    )

    by_proc = (
        immature
        .join(factors, "payment_processor", "left")
        .withColumn("forecast_estimated",
            F.col("gmv") * F.coalesce(F.col("est_factor"), F.lit(0.0)))
        .groupBy("payment_processor")
        .agg(
            F.count("*").alias("carts"),
            F.sum("gmv").alias("gmv"),
            F.sum("estimated_cost").alias("actual_estimated"),
            F.sum("forecast_estimated").alias("forecast_estimated"),
        )
        .withColumn("eval_date", F.lit(eval_date))
    )

    mode = "overwrite" if first_write else "append"
    by_proc.write.mode(mode).saveAsTable(BT_PROC_TABLE)
    first_write = False

    if (i + 1) % 5 == 0 or (i + 1) == len(eval_dates):
        print(f"  {i + 1}/{len(eval_dates)} dates written")

print("Done")

# COMMAND ----------

bt_proc = (
    spark.table(BT_PROC_TABLE)
    .withColumn("deviation_pct",
        F.round(
            (F.col("forecast_estimated") - F.col("actual_estimated"))
            / F.abs(F.col("actual_estimated")) * 100, 3))
    .withColumn("actual_rate_bps",
        F.round(F.col("actual_estimated") / F.col("gmv") * 10000, 2))
    .withColumn("forecast_rate_bps",
        F.round(F.col("forecast_estimated") / F.col("gmv") * 10000, 2))
)

display(
    bt_proc
    .filter(F.col("carts") >= 100)
    .orderBy("payment_processor", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Summary by processor

# COMMAND ----------

display(
    bt_proc
    .filter(F.col("carts") >= 100)
    .groupBy("payment_processor")
    .agg(
        F.round(F.avg("deviation_pct"), 3).alias("avg_dev_pct"),
        F.round(F.avg(F.abs(F.col("deviation_pct"))), 3).alias("avg_abs_dev_pct"),
        F.round(F.max(F.abs(F.col("deviation_pct"))), 3).alias("max_abs_dev_pct"),
        F.round(F.avg("actual_rate_bps"), 2).alias("avg_actual_rate_bps"),
        F.round(F.avg("forecast_rate_bps"), 2).alias("avg_forecast_rate_bps"),
        F.round(F.avg("carts"), 0).alias("avg_carts"),
    )
    .orderBy("payment_processor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 9. Recommendations
# MAGIC
# MAGIC Based on the analysis above, summarize:
# MAGIC 1. Which processors have material estimated costs
# MAGIC 2. What immaturity window to use for estimated costs
# MAGIC 3. Whether processor-only factor is sufficient or
# MAGIC    processor × method is needed
# MAGIC 4. How this layer combines with V2 for a total forecast

# COMMAND ----------

print("="*60)
print("  Review sections 2, 6.3, 7.4, and 8.1 to determine:")
print("="*60)
print()
print("  1. ESTIMATED COST SHARE by processor (section 2)")
print("     → Which processors need this layer?")
print()
print("  2. FACTOR GRANULARITY (section 6.3)")
print("     → If CV < 0.2 for a processor, processor-only is enough.")
print("     → If CV > 0.2, add payment_method_detail as a factor.")
print()
print("  3. OPTIMAL WINDOW (section 7.4)")
print("     → Pick the window with lowest avg_abs_est_dev_pct.")
print("     → Typically 28-35d captures the monthly invoice cycle.")
print()
print("  4. COMBINED MODEL = V2 (precise-only) + estimated forecast")
print("     → Check total_deviation_pct in section 7.5 to validate.")
