# Databricks notebook source
# MAGIC %md
# MAGIC # Cancellation Cost Maturation Analysis
# MAGIC
# MAGIC Verify the 7-day maturity window for cancelled bookings (both RNPL and
# MAGIC non-RNPL). Should it be shorter?
# MAGIC
# MAGIC **Approach:** join cart-level data with daily processing costs and measure
# MAGIC how quickly costs settle relative to the cancellation date.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data preparation — cancelled bookings + daily costs
# MAGIC 2. RNPL cancelled — maturation curve (days since cancellation)
# MAGIC 3. Non-RNPL cancelled — maturation curve (days since cancellation)
# MAGIC 4. Comparison: cancelled vs non-cancelled maturation
# MAGIC 5. Window sensitivity analysis — forecast deviation at different cutoffs

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

config = ForecastingConfig()

SNAPSHOT_TABLE = config.snapshot_table
COSTS_DETAIL_TABLE = config.costs_detail_table

ANALYSIS_START = "2025-09-01"
ANALYSIS_END = "2026-02-28"

print(f"Snapshot table: {SNAPSHOT_TABLE}")
print(f"Costs detail: {COSTS_DETAIL_TABLE}")
print(f"Analysis period: {ANALYSIS_START} to {ANALYSIS_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Load snapshot data — cancelled bookings only

# COMMAND ----------

raw = spark.table(SNAPSHOT_TABLE)

cancelled = (
    raw
    .filter(F.col("last_date_of_cancelation").isNotNull())
    .filter(F.col("date_of_checkout").between(ANALYSIS_START, ANALYSIS_END))
    .filter(F.col("total_payment_costs") != 0)
    .filter(F.col("gmv") > 0)
)

rnpl_cancelled = cancelled.filter(F.col("is_rnpl") == True)
non_rnpl_cancelled = cancelled.filter(F.col("is_rnpl") == False)

total_cancelled = cancelled.count()
total_rnpl = rnpl_cancelled.count()
total_non_rnpl = non_rnpl_cancelled.count()

print(f"Total cancelled carts:     {total_cancelled:,}")
print(f"  RNPL cancelled:          {total_rnpl:,}")
print(f"  Non-RNPL cancelled:      {total_non_rnpl:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Load daily processing costs

# COMMAND ----------

costs_daily = spark.sql(f"""
SELECT
    regexp_extract(pc.payment_reference, '^SC([0-9]+)', 1) AS shopping_cart_id,
    processing_timestamp::date AS processing_date,
    SUM(CASE WHEN amount_type_l0='FEE' THEN payout_amount_value / COALESCE(ex.exchange_rate, 1) ELSE 0 END) AS daily_fee_eur
FROM {COSTS_DETAIL_TABLE} pc
LEFT JOIN production.dwh.dim_currency c ON c.iso_code = pc.payout_currency
LEFT JOIN production.dwh.dim_exchange_rate_eur_daily ex ON ex.currency_id = c.currency_id
    AND pc.processing_timestamp::date = ex.update_timestamp
GROUP BY 1, 2
""")

costs_daily = costs_daily.filter(F.col("shopping_cart_id").isNotNull())
print(f"Daily cost rows loaded: {costs_daily.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Overview: time between checkout, travel, cancellation

# COMMAND ----------

display(
    cancelled
    .withColumn("days_checkout_to_cancel",
        F.datediff(F.col("last_date_of_cancelation"), F.col("date_of_checkout")))
    .withColumn("days_travel_to_cancel",
        F.when(F.col("last_date_of_travel").isNotNull(),
            F.datediff(F.col("last_date_of_cancelation"), F.col("last_date_of_travel"))))
    .groupBy("is_rnpl")
    .agg(
        F.count("*").alias("carts"),
        F.round(F.avg("days_checkout_to_cancel"), 1).alias("avg_days_checkout_to_cancel"),
        F.round(F.percentile_approx("days_checkout_to_cancel", 0.5), 1).alias("median_days_checkout_to_cancel"),
        F.round(F.avg("days_travel_to_cancel"), 1).alias("avg_days_travel_to_cancel"),
        F.round(F.percentile_approx("days_travel_to_cancel", 0.5), 1).alias("median_days_travel_to_cancel"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. RNPL Cancelled — Maturation Curve
# MAGIC
# MAGIC How quickly do costs settle after the cancellation date?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Days since cancellation

# COMMAND ----------

rnpl_cancel_daily = (
    rnpl_cancelled
    .select("shopping_cart_id", "last_date_of_cancelation", "total_payment_costs")
    .join(costs_daily, "shopping_cart_id", "inner")
    .withColumn("days_since_cancel",
        F.datediff(F.col("processing_date"), F.col("last_date_of_cancelation")))
)

rnpl_cancel_curve = (
    rnpl_cancel_daily
    .groupBy("days_since_cancel")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee_rnpl = float(rnpl_cancel_curve.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

w_cum = Window.orderBy("days_since_cancel").rowsBetween(Window.unboundedPreceding, 0)
rnpl_cancel_maturation = (
    rnpl_cancel_curve
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(w_cum))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_fee_rnpl) * 100, 2))
    .orderBy("days_since_cancel")
)

print(f"RNPL cancelled — maturation curve (anchored to cancellation date):")
print(f"Total absolute fees: {total_fee_rnpl:,.2f} EUR")
display(rnpl_cancel_maturation.filter(F.col("days_since_cancel").between(-30, 30)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 RNPL cancelled — key percentiles

# COMMAND ----------

print("RNPL cancelled — % of costs settled by day N after cancellation:")
for pct in [80, 90, 95, 99, 99.5, 99.9]:
    row = rnpl_cancel_maturation.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct:>5.1f}% settled by day {row['days_since_cancel']}")
    else:
        print(f"  {pct:>5.1f}% — not reached in data range")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 RNPL cancelled — also anchor on checkout (for comparison)

# COMMAND ----------

rnpl_cancel_checkout = (
    rnpl_cancelled
    .select("shopping_cart_id", "date_of_checkout", "total_payment_costs")
    .join(costs_daily, "shopping_cart_id", "inner")
    .withColumn("days_since_checkout",
        F.datediff(F.col("processing_date"), F.col("date_of_checkout")))
    .groupBy("days_since_checkout")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee_rnpl_co = float(rnpl_cancel_checkout.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

rnpl_cancel_checkout_curve = (
    rnpl_cancel_checkout
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(
        Window.orderBy("days_since_checkout").rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_fee_rnpl_co) * 100, 2))
    .orderBy("days_since_checkout")
)

print("RNPL cancelled — anchored to date_of_checkout (for comparison):")
for pct in [90, 95, 99, 99.5]:
    row = rnpl_cancel_checkout_curve.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct}% settled by day {row['days_since_checkout']} after checkout")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Non-RNPL Cancelled — Maturation Curve
# MAGIC
# MAGIC Non-RNPL bookings can also be cancelled. Do their costs settle faster
# MAGIC than the standard 14-day non-RNPL window? If so, using
# MAGIC cancellation date as anchor could tighten the maturity window.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Days since cancellation

# COMMAND ----------

non_rnpl_cancel_daily = (
    non_rnpl_cancelled
    .select("shopping_cart_id", "last_date_of_cancelation", "date_of_checkout", "total_payment_costs")
    .join(costs_daily, "shopping_cart_id", "inner")
    .withColumn("days_since_cancel",
        F.datediff(F.col("processing_date"), F.col("last_date_of_cancelation")))
)

non_rnpl_cancel_curve_raw = (
    non_rnpl_cancel_daily
    .groupBy("days_since_cancel")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee_non_rnpl = float(non_rnpl_cancel_curve_raw.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

non_rnpl_cancel_maturation = (
    non_rnpl_cancel_curve_raw
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(w_cum))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_fee_non_rnpl) * 100, 2))
    .orderBy("days_since_cancel")
)

print(f"Non-RNPL cancelled — maturation curve (anchored to cancellation date):")
print(f"Total absolute fees: {total_fee_non_rnpl:,.2f} EUR")
display(non_rnpl_cancel_maturation.filter(F.col("days_since_cancel").between(-30, 30)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Non-RNPL cancelled — key percentiles

# COMMAND ----------

print("Non-RNPL cancelled — % of costs settled by day N after cancellation:")
for pct in [80, 90, 95, 99, 99.5, 99.9]:
    row = non_rnpl_cancel_maturation.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct:>5.1f}% settled by day {row['days_since_cancel']}")
    else:
        print(f"  {pct:>5.1f}% — not reached in data range")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Non-RNPL cancelled — anchored to checkout (for comparison)
# MAGIC
# MAGIC Since non-RNPL currently uses checkout as anchor with 14-day maturity,
# MAGIC this shows the standard curve for comparison.

# COMMAND ----------

non_rnpl_cancel_checkout = (
    non_rnpl_cancel_daily
    .withColumn("days_since_checkout",
        F.datediff(F.col("processing_date"), F.col("date_of_checkout")))
    .groupBy("days_since_checkout")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee_non_rnpl_co = float(non_rnpl_cancel_checkout.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

non_rnpl_cancel_checkout_curve = (
    non_rnpl_cancel_checkout
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(
        Window.orderBy("days_since_checkout").rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_fee_non_rnpl_co) * 100, 2))
    .orderBy("days_since_checkout")
)

print("Non-RNPL cancelled — anchored to date_of_checkout:")
for pct in [90, 95, 99, 99.5]:
    row = non_rnpl_cancel_checkout_curve.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct}% settled by day {row['days_since_checkout']} after checkout")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Comparison: Cancelled vs Non-Cancelled Maturation
# MAGIC
# MAGIC Do cancelled bookings settle faster than non-cancelled ones?
# MAGIC This helps decide if a shorter maturity window is appropriate.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 RNPL: cancelled vs active — side by side

# COMMAND ----------

rnpl_active = (
    raw
    .filter(F.col("is_rnpl") == True)
    .filter(F.col("last_date_of_cancelation").isNull())
    .filter(F.col("date_of_checkout").between(ANALYSIS_START, ANALYSIS_END))
    .filter(F.col("total_payment_costs") != 0)
    .filter(F.col("gmv") > 0)
    .filter(F.col("last_date_of_travel").isNotNull())
)

rnpl_active_daily = (
    rnpl_active
    .select("shopping_cart_id", "last_date_of_travel")
    .join(costs_daily, "shopping_cart_id", "inner")
    .withColumn("days_since_anchor",
        F.datediff(F.col("processing_date"), F.col("last_date_of_travel")))
    .groupBy("days_since_anchor")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_active = float(rnpl_active_daily.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

rnpl_active_curve = (
    rnpl_active_daily
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(
        Window.orderBy("days_since_anchor").rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_active) * 100, 2))
    .withColumn("segment", F.lit("rnpl_active (anchor=travel)"))
    .select("days_since_anchor", "pct_settled", "segment")
)

rnpl_cancel_compare = (
    rnpl_cancel_maturation
    .withColumnRenamed("days_since_cancel", "days_since_anchor")
    .withColumn("segment", F.lit("rnpl_cancelled (anchor=cancel)"))
    .select("days_since_anchor", "pct_settled", "segment")
)

comparison_rnpl = rnpl_active_curve.unionByName(rnpl_cancel_compare)

display(
    comparison_rnpl
    .filter(F.col("days_since_anchor").between(-5, 21))
    .orderBy("segment", "days_since_anchor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Non-RNPL: cancelled (anchor=cancel) vs all (anchor=checkout)

# COMMAND ----------

non_rnpl_all = (
    raw
    .filter(F.col("is_rnpl") == False)
    .filter(F.col("date_of_checkout").between(ANALYSIS_START, ANALYSIS_END))
    .filter(F.col("total_payment_costs") != 0)
    .filter(F.col("gmv") > 0)
)

non_rnpl_all_daily = (
    non_rnpl_all
    .select("shopping_cart_id", "date_of_checkout")
    .join(costs_daily, "shopping_cart_id", "inner")
    .withColumn("days_since_anchor",
        F.datediff(F.col("processing_date"), F.col("date_of_checkout")))
    .groupBy("days_since_anchor")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_all = float(non_rnpl_all_daily.agg(F.sum("fee_in_bucket")).collect()[0][0] or 0)

non_rnpl_all_curve = (
    non_rnpl_all_daily
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(
        Window.orderBy("days_since_anchor").rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / F.lit(total_all) * 100, 2))
    .withColumn("segment", F.lit("non_rnpl_all (anchor=checkout)"))
    .select("days_since_anchor", "pct_settled", "segment")
)

non_rnpl_cancel_compare = (
    non_rnpl_cancel_maturation
    .withColumnRenamed("days_since_cancel", "days_since_anchor")
    .withColumn("segment", F.lit("non_rnpl_cancelled (anchor=cancel)"))
    .select("days_since_anchor", "pct_settled", "segment")
)

comparison_non_rnpl = non_rnpl_all_curve.unionByName(non_rnpl_cancel_compare)

display(
    comparison_non_rnpl
    .filter(F.col("days_since_anchor").between(-5, 21))
    .orderBy("segment", "days_since_anchor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Window Sensitivity — Forecast Deviation at Different Cutoffs
# MAGIC
# MAGIC For each candidate maturity window (1–14 days after cancellation),
# MAGIC simulate the forecast and measure deviation from actuals.
# MAGIC
# MAGIC If 3 or 5 days performs as well as 7, we can tighten the window.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 RNPL cancelled — window sensitivity

# COMMAND ----------

from payment_costs_forecasting.bucketing import apply_bucketing

eval_date = date(2026, 1, 15)
bucketed = apply_bucketing(
    raw.filter(F.col("gmv") > 0).filter(F.col("total_payment_costs") != 0),
    config,
)

rnpl_bookings = bucketed.filter(F.col("rnpl_segment") == "rnpl")

rnpl_cancel_test = (
    rnpl_bookings
    .filter(F.col("last_date_of_cancelation").isNotNull())
    .filter(F.col("last_date_of_cancelation").cast("date") <= F.lit(eval_date))
)

mature_rnpl_cancel = (
    rnpl_cancel_test
    .filter(
        F.col("last_date_of_cancelation").cast("date")
        <= F.lit(eval_date - timedelta(days=30))
    )
)
mature_factor = mature_rnpl_cancel.agg(
    F.sum("total_payment_costs").alias("cost"),
    F.sum("gmv").alias("gmv"),
).collect()[0]
baseline_factor = float(mature_factor["cost"]) / float(mature_factor["gmv"])

print(f"Eval date: {eval_date}")
print(f"RNPL cancelled baseline factor (30d+ mature): {baseline_factor:.6f}")
print(f"Mature carts for factor: {mature_rnpl_cancel.count():,}")
print()

results = []
for window in range(1, 15):
    cutoff = eval_date - timedelta(days=window)

    immature = rnpl_cancel_test.filter(
        F.col("last_date_of_cancelation").cast("date") > F.lit(cutoff)
    )
    n_immature = immature.count()
    if n_immature == 0:
        continue

    forecast_total = float(immature.agg(F.sum("gmv")).collect()[0][0] or 0) * baseline_factor
    actual_total = float(immature.agg(F.sum("total_payment_costs")).collect()[0][0] or 0)

    if abs(actual_total) > 0:
        dev = abs(forecast_total - actual_total) / abs(actual_total)
    else:
        dev = None

    results.append({
        "maturity_days": window,
        "immature_carts": n_immature,
        "forecast_total": round(forecast_total, 2),
        "actual_total": round(actual_total, 2),
        "deviation_pct": round(dev * 100, 3) if dev is not None else None,
    })

results_df = spark.createDataFrame(results)
display(results_df.orderBy("maturity_days"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Non-RNPL cancelled — window sensitivity
# MAGIC
# MAGIC For non-RNPL, cancellation anchor is currently not used (checkout anchor
# MAGIC with 14 days). This shows what would happen if we used cancellation date
# MAGIC as anchor for non-RNPL cancelled bookings too.

# COMMAND ----------

non_rnpl_bookings = bucketed.filter(F.col("rnpl_segment") == "non_rnpl")

non_rnpl_cancel_test = (
    non_rnpl_bookings
    .filter(F.col("last_date_of_cancelation").isNotNull())
    .filter(F.col("last_date_of_cancelation").cast("date") <= F.lit(eval_date))
)

mature_non_rnpl_cancel = (
    non_rnpl_cancel_test
    .filter(
        F.col("last_date_of_cancelation").cast("date")
        <= F.lit(eval_date - timedelta(days=30))
    )
)
mature_nr_factor = mature_non_rnpl_cancel.agg(
    F.sum("total_payment_costs").alias("cost"),
    F.sum("gmv").alias("gmv"),
).collect()[0]
nr_baseline = float(mature_nr_factor["cost"]) / float(mature_nr_factor["gmv"])

print(f"Non-RNPL cancelled baseline factor (30d+ mature): {nr_baseline:.6f}")
print(f"Mature carts: {mature_non_rnpl_cancel.count():,}")
print(f"Total non-RNPL cancelled carts in scope: {non_rnpl_cancel_test.count():,}")
print()

results_nr = []
for window in range(1, 15):
    cutoff = eval_date - timedelta(days=window)

    immature = non_rnpl_cancel_test.filter(
        F.col("last_date_of_cancelation").cast("date") > F.lit(cutoff)
    )
    n_immature = immature.count()
    if n_immature == 0:
        continue

    forecast_total = float(immature.agg(F.sum("gmv")).collect()[0][0] or 0) * nr_baseline
    actual_total = float(immature.agg(F.sum("total_payment_costs")).collect()[0][0] or 0)

    if abs(actual_total) > 0:
        dev = abs(forecast_total - actual_total) / abs(actual_total)
    else:
        dev = None

    results_nr.append({
        "maturity_days": window,
        "immature_carts": n_immature,
        "forecast_total": round(forecast_total, 2),
        "actual_total": round(actual_total, 2),
        "deviation_pct": round(dev * 100, 3) if dev is not None else None,
    })

results_nr_df = spark.createDataFrame(results_nr)
display(results_nr_df.orderBy("maturity_days"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Recommendations
# MAGIC
# MAGIC Review the curves and sensitivity tables above:
# MAGIC
# MAGIC 1. **RNPL cancelled**: if 99%+ settled by day 3-4, consider reducing
# MAGIC    `rnpl_cancelled_maturity_days` from 7 to 3 or 4.
# MAGIC
# MAGIC 2. **Non-RNPL cancelled**: if the cancellation-anchored curve settles
# MAGIC    much faster than the checkout-anchored curve, consider adding
# MAGIC    cancellation date as an alternative anchor for non-RNPL too.
# MAGIC
# MAGIC 3. **Window sensitivity**: look for the smallest window where deviation
# MAGIC    is still < 3%. That's the optimal maturity cutoff.
# MAGIC
# MAGIC 4. **Trade-off**: shorter windows mean fewer carts are "immature"
# MAGIC    (needing forecast), which reduces the overall forecast footprint
# MAGIC    and potential error.
