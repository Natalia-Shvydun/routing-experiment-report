# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Production Forecast & Validation
# MAGIC
# MAGIC Run the full forecasting pipeline and validate quality.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Compute cost factors from the checkout-day snapshot
# MAGIC 2. Apply factors to immature carts (forecast)
# MAGIC 3. Single-date validation against all downstream dimensions
# MAGIC 4. Multi-date backtest for comprehensive quality evaluation
# MAGIC 5. Head-to-head comparison vs current production model (`fact_payment_cost`)
# MAGIC
# MAGIC **Prerequisites:** update `config.py` with findings from Phase 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Users/natalia.shvydun@getyourguide.com/.bundle/Cursor/dev/files/scripts")

from pyspark.sql import functions as F

from payment_costs_forecasting.config import ForecastingConfig
from payment_costs_forecasting.bucketing import apply_bucketing
from payment_costs_forecasting.factors import compute_factors
from payment_costs_forecasting.forecast import forecast, get_immature_carts, apply_factors_with_fallback
from payment_costs_forecasting.validation import evaluate_quality, backtest

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

dbutils.widgets.text("as_of_date", date.today().isoformat(), "As of date (YYYY-MM-DD)")
dbutils.widgets.text("snapshot_table", "testing.analytics.payment_costs_data", "Snapshot table")
dbutils.widgets.text("factors_table", "testing.analytics.payment_cost_factors", "Output factors table")
dbutils.widgets.text("forecasted_costs_table", "testing.analytics.forecasted_costs", "Output forecast table")
dbutils.widgets.text("quality_metrics_table", "testing.analytics.quality_metrics", "Output quality table")
dbutils.widgets.text("backtest_start", "2025-12-01", "Backtest: start date (Monday)")
dbutils.widgets.text("backtest_end", "2026-02-23", "Backtest: end date (Monday)")
dbutils.widgets.text("snapshot_date", date.today().isoformat(), "Date when snapshot costs were last observed")

# COMMAND ----------

as_of_date = date.fromisoformat(dbutils.widgets.get("as_of_date"))

config = ForecastingConfig(
    snapshot_table=dbutils.widgets.get("snapshot_table"),
    factors_table=dbutils.widgets.get("factors_table"),
    forecasted_costs_table=dbutils.widgets.get("forecasted_costs_table"),
    quality_metrics_table=dbutils.widgets.get("quality_metrics_table"),
)

backtest_start = date.fromisoformat(dbutils.widgets.get("backtest_start"))
backtest_end = date.fromisoformat(dbutils.widgets.get("backtest_end"))
snapshot_date = date.fromisoformat(dbutils.widgets.get("snapshot_date"))

print(f"As of date: {as_of_date}")
print(f"Snapshot date: {snapshot_date}")
print(f"Factor columns: {config.factor_columns}")
print(f"Non-RNPL: anchor={config.non_rnpl_anchor}, maturity={config.non_rnpl_maturity_days}d")
print(f"RNPL (active): anchor={config.rnpl_anchor}, maturity={config.rnpl_maturity_days}d")
print(f"RNPL (cancelled): anchor={config.rnpl_cancelled_anchor}, maturity={config.rnpl_cancelled_maturity_days}d")
print(f"RNPL factor: single blended factor covering both active+cancelled")
print(f"Window: {config.window_days}d")
print(f"Validation dimensions: {config.validation_dimensions}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

snapshot = spark.table(config.snapshot_table)
print(f"Snapshot rows: {snapshot.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # STEP 1: Factor Computation

# COMMAND ----------

factors = compute_factors(
    snapshot=snapshot,
    as_of_date=as_of_date,
    config=config,
)

print(f"Computed {factors.count()} factor rows for {as_of_date}")

# COMMAND ----------

display(factors.orderBy(F.desc("segment_volume")))

# COMMAND ----------

display(
    factors.groupBy("rnpl_segment")
    .agg(
        F.count("*").alias("segments"),
        F.sum("segment_volume").alias("total_carts"),
        F.mean("cost_factor").alias("avg_factor"),
        F.min("cost_factor").alias("min_factor"),
        F.max("cost_factor").alias("max_factor"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save factors

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {config.factors_table}")
factors.write.saveAsTable(config.factors_table)
print(f"Factors written to {config.factors_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # STEP 2: Forecast

# COMMAND ----------

result = forecast(
    snapshot=snapshot,
    as_of_date=as_of_date,
    config=config,
    factors=factors,
)

# COMMAND ----------

display(
    result.groupBy("source")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("total_gmv"),
        F.sum("amount").alias("total_cost"),
        (F.sum("amount") / F.sum("gmv")).alias("cost_rate"),
    )
)

# COMMAND ----------

print(f"Total output rows: {result.count():,}")
display(result.orderBy(F.desc("gmv")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save forecast

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {config.forecasted_costs_table}")
result.write.saveAsTable(config.forecasted_costs_table)
print(f"Forecast written to {config.forecasted_costs_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # STEP 3: Single-Date Validation
# MAGIC
# MAGIC Compare forecast vs actuals for the given as_of_date across all
# MAGIC validation dimensions. This requires as_of_date to be in the past
# MAGIC (so that actuals have matured).

# COMMAND ----------

from payment_costs_forecasting.validation import _filter_mature_actuals

bucketed = apply_bucketing(snapshot, config)

immature = get_immature_carts(bucketed, as_of_date, config)
forecast_df = apply_factors_with_fallback(immature, factors, config)

actuals_df = (
    bucketed
    .join(immature.select("shopping_cart_id"), "shopping_cart_id", "inner")
    .filter(F.col("total_payment_costs") != 0)
)
actuals_df = _filter_mature_actuals(actuals_df, snapshot_date, config)

mature_ids = actuals_df.select("shopping_cart_id")
forecast_df = forecast_df.join(mature_ids, "shopping_cart_id", "inner")

print(f"Immature carts forecasted: {forecast_df.count():,}")
print(f"Actuals with mature costs: {actuals_df.count():,}")

# COMMAND ----------

quality_metrics, all_passed = evaluate_quality(
    spark=spark,
    forecast_df=forecast_df,
    actuals_df=actuals_df,
    evaluation_date=as_of_date,
    config=config,
)

# COMMAND ----------

display(quality_metrics.orderBy("dimension", F.desc("deviation_pct")))

# COMMAND ----------

if all_passed:
    print(f"PASS: All dimensions within {config.deviation_threshold*100}% on {as_of_date}")
else:
    failed = quality_metrics.filter(
        (F.abs(F.col("actual_total")) > 0) & (F.col("pass_flag") == False)
    )
    print(f"FAIL: {failed.count()} checks exceed threshold on {as_of_date}")
    display(failed.orderBy(F.desc("deviation_pct")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # STEP 4: Multi-Date Backtest
# MAGIC
# MAGIC Run the forecast for multiple historical "as of" dates and check
# MAGIC deviation from actuals. This gives confidence that the model
# MAGIC performs consistently over time.

# COMMAND ----------

safe_maturity = max(
    config.non_rnpl_maturity_days,
    config.rnpl_maturity_days,
    config.rnpl_cancelled_maturity_days,
)
latest_safe_date = date.today() - timedelta(days=safe_maturity + 14)

if backtest_end > latest_safe_date:
    print(f"WARNING: backtest_end {backtest_end} is too recent — costs may not be mature. Capping at {latest_safe_date}")
    backtest_end = latest_safe_date

evaluation_dates = []
d = backtest_start
while d <= backtest_end:
    evaluation_dates.append(d)
    d += timedelta(weeks=1)

print(f"Evaluating {len(evaluation_dates)} consecutive weeks: {evaluation_dates[0]} → {evaluation_dates[-1]}")
for d in evaluation_dates:
    print(f"  {d}")

# COMMAND ----------

bt_metrics = backtest(
    spark=spark,
    snapshot=snapshot,
    evaluation_dates=evaluation_dates,
    config=config,
    snapshot_date=snapshot_date,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save backtest results

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {config.quality_metrics_table}")
bt_metrics.write.saveAsTable(config.quality_metrics_table)
print(f"Backtest results written to {config.quality_metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall deviation per evaluation date

# COMMAND ----------

display(
    bt_metrics
    .filter(F.col("dimension") == "_overall")
    .orderBy("evaluation_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actual vs Predicted — weekly time series
# MAGIC
# MAGIC Each evaluation date shows total actual cost vs total forecasted cost
# MAGIC for the immature carts of that week. Use this chart to visually confirm
# MAGIC the model tracks actuals over the consecutive backtest period.

# COMMAND ----------

weekly_comparison = (
    bt_metrics
    .filter(F.col("dimension") == "_overall")
    .select(
        F.col("evaluation_date"),
        F.round(F.col("actual_total"), 2).alias("actual"),
        F.round(F.col("forecast_total"), 2).alias("forecast"),
        F.round(F.col("deviation_pct") * 100, 2).alias("deviation_%"),
        F.col("cart_count"),
    )
    .orderBy("evaluation_date")
)
display(weekly_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actual vs Predicted — by RNPL segment over time

# COMMAND ----------

rnpl_seg_comparison = (
    bt_metrics
    .filter(F.col("dimension") == "rnpl_segment")
    .select(
        F.col("evaluation_date"),
        F.col("dimension_value").alias("segment"),
        F.round(F.col("actual_total"), 2).alias("actual"),
        F.round(F.col("forecast_total"), 2).alias("forecast"),
        F.round(F.col("deviation_pct") * 100, 2).alias("deviation_%"),
        F.col("cart_count"),
    )
    .orderBy("evaluation_date", "segment")
)
display(rnpl_seg_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Max deviation per dimension (significant segments only)
# MAGIC
# MAGIC Only includes segments with >= min_carts_for_validation carts.

# COMMAND ----------

significant = bt_metrics.filter(F.col("cart_count") >= config.min_carts_for_validation)

display(
    significant
    .filter(F.abs(F.col("actual_total")) > 0)
    .groupBy("dimension")
    .agg(
        F.max("deviation_pct").alias("max_deviation"),
        F.avg("deviation_pct").alias("avg_deviation"),
        F.sum(F.when(F.col("pass_flag") == False, 1).otherwise(0)).alias("fail_count"),
        F.count("*").alias("total_checks"),
        F.avg("cart_count").alias("avg_segment_carts"),
    )
    .orderBy(F.desc("max_deviation"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill-down: which dimension values fail?

# COMMAND ----------

display(
    bt_metrics
    .filter(F.col("pass_flag") == False)
    .select("dimension", "dimension_value", "evaluation_date",
            "cart_count", "actual_total", "forecast_total", "deviation_pct")
    .orderBy(F.desc("deviation_pct"))
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Noisy small segments (excluded from pass/fail)
# MAGIC
# MAGIC These are the segments below the min volume threshold — shown for visibility.

# COMMAND ----------

small_segments = bt_metrics.filter(
    (F.col("cart_count") < config.min_carts_for_validation)
    & (F.abs(F.col("actual_total")) > 0)
)

print(f"Segments below {config.min_carts_for_validation} carts: {small_segments.count()}")
if small_segments.count() > 0:
    display(
        small_segments
        .groupBy("dimension")
        .agg(
            F.count("*").alias("num_small_segments"),
            F.max("deviation_pct").alias("max_deviation"),
            F.avg("deviation_pct").alias("avg_deviation"),
            F.sum("cart_count").alias("total_carts_in_small_segs"),
        )
        .orderBy(F.desc("max_deviation"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deviation trend over time (significant segments only)

# COMMAND ----------

display(
    significant
    .filter(F.col("dimension") != "_overall")
    .filter(F.abs(F.col("actual_total")) > 0)
    .groupBy("dimension", "evaluation_date")
    .agg(
        F.max("deviation_pct").alias("max_deviation"),
        F.avg("deviation_pct").alias("avg_deviation"),
    )
    .orderBy("dimension", "evaluation_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnostic: worst-performing dimension values
# MAGIC
# MAGIC Drill into the worst segments to understand WHY they deviate.

# COMMAND ----------

worst = (
    bt_metrics
    .filter(F.col("pass_flag") == False)
    .filter(F.col("cart_count") >= config.min_carts_for_validation)
    .groupBy("dimension", "dimension_value")
    .agg(
        F.count("*").alias("dates_failed"),
        F.avg("deviation_pct").alias("avg_deviation"),
        F.max("deviation_pct").alias("max_deviation"),
        F.avg("cart_count").alias("avg_carts"),
        F.avg("actual_total").alias("avg_actual"),
        F.avg("forecast_total").alias("avg_forecast"),
    )
    .withColumn("forecast_vs_actual_ratio",
                F.round(F.col("avg_forecast") / F.col("avg_actual"), 3))
    .orderBy(F.desc("avg_deviation"))
)
print("Persistent failures (significant segments that fail across dates):")
display(worst.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Factor coverage check
# MAGIC
# MAGIC Do all immature cart segments have matching factors?
# MAGIC Missing factors → global fallback → potentially large deviation.

# COMMAND ----------

sample_date = evaluation_dates[len(evaluation_dates) // 2]
sample_factors = compute_factors(bucketed, sample_date, config)
sample_immature = get_immature_carts(bucketed, sample_date, config)

for dim in config.factor_columns:
    if dim not in sample_immature.columns or dim not in sample_factors.columns:
        continue

    immature_vals = set(
        r[dim] for r in
        sample_immature.select(dim).distinct().collect()
        if r[dim] is not None
    )
    factor_vals = set(
        r[dim] for r in
        sample_factors.select(dim).distinct().collect()
        if r[dim] is not None
    )
    missing = immature_vals - factor_vals
    if missing:
        print(f"  {dim}: {len(missing)} values in immature carts but NOT in factors: {missing}")
    else:
        print(f"  {dim}: all {len(immature_vals)} values covered by factors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final assertion

# COMMAND ----------

latest_date = max(evaluation_dates)
latest_metrics = bt_metrics.filter(
    F.col("evaluation_date") == F.lit(latest_date)
)
failed = latest_metrics.filter(
    (F.abs(F.col("actual_total")) > 0) & (F.col("pass_flag") == False)
)
fail_count = failed.count()

if fail_count > 0:
    print(f"FAIL: {fail_count} dimension values exceed {config.deviation_threshold*100}% deviation on {latest_date}")
    display(failed.orderBy(F.desc("deviation_pct")))
else:
    print(f"PASS: All dimensions within {config.deviation_threshold*100}% deviation on {latest_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # STEP 5: Comparison with Current Production Model
# MAGIC
# MAGIC Compare the **new model** forecast vs the **current production forecast**
# MAGIC (`production.dwh.fact_payment_cost WHERE source='Forecasted'`) vs **actuals**.
# MAGIC
# MAGIC This proves the new model is an improvement over the existing one.

# COMMAND ----------

CURRENT_MODEL_TABLE = "production.dwh.fact_payment_cost"

current_forecasts = (
    spark.table(CURRENT_MODEL_TABLE)
    .filter(F.col("source") == "Forecasted")
)

print(f"Current model forecast rows: {current_forecasts.count():,}")
print("Columns available:")
for c in sorted(current_forecasts.columns):
    print(f"  {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Overview: current model structure

# COMMAND ----------

display(current_forecasts.limit(10))

# COMMAND ----------

display(
    current_forecasts.agg(
        F.count("*").alias("rows"),
        F.countDistinct("shopping_cart_id").alias("distinct_carts"),
        F.sum("amount").alias("total_forecasted_cost"),
        F.min("date_of_checkout").alias("min_date"),
        F.max("date_of_checkout").alias("max_date"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Head-to-head: new model vs current model vs actuals
# MAGIC
# MAGIC For each backtest evaluation date, compute:
# MAGIC - Which carts were immature (forecasted by both models)
# MAGIC - Actual matured costs for those carts
# MAGIC - New model forecast vs current model forecast vs actuals
# MAGIC - Deviation of each model from actuals

# COMMAND ----------

comparison_results = []
bucketed = apply_bucketing(snapshot, config)

for eval_date in evaluation_dates:
    factors_eval = compute_factors(bucketed, eval_date, config)
    immature = get_immature_carts(bucketed, eval_date, config)
    new_forecast = apply_factors_with_fallback(immature, factors_eval, config)

    immature_ids = immature.select("shopping_cart_id")

    actuals_raw = (
        bucketed
        .join(immature_ids, "shopping_cart_id", "inner")
        .filter(F.col("total_payment_costs") != 0)
    )
    actuals_raw = _filter_mature_actuals(actuals_raw, snapshot_date, config)
    actuals = actuals_raw.select("shopping_cart_id", F.col("total_payment_costs").alias("actual_cost"))

    new_forecast = new_forecast.join(actuals.select("shopping_cart_id"), "shopping_cart_id", "inner")

    current_fc = (
        current_forecasts
        .join(immature_ids, "shopping_cart_id", "inner")
        .groupBy("shopping_cart_id")
        .agg(F.sum("amount").alias("current_model_cost"))
    )

    new_fc = (
        new_forecast
        .select("shopping_cart_id", F.col("amount").alias("new_model_cost"))
    )

    joined = (
        actuals
        .join(new_fc, "shopping_cart_id", "left")
        .join(current_fc, "shopping_cart_id", "left")
    )

    totals = joined.agg(
        F.sum("actual_cost").alias("actual"),
        F.sum("new_model_cost").alias("new_model"),
        F.sum("current_model_cost").alias("current_model"),
        F.count("*").alias("carts_with_actuals"),
        F.sum(F.when(F.col("current_model_cost").isNotNull(), 1).otherwise(0)).alias("carts_with_current"),
    ).collect()[0]

    actual_abs = abs(totals["actual"] or 0)
    if actual_abs > 0:
        new_dev = abs((totals["new_model"] or 0) - (totals["actual"] or 0)) / actual_abs
        current_dev = abs((totals["current_model"] or 0) - (totals["actual"] or 0)) / actual_abs if totals["current_model"] else None
    else:
        new_dev = None
        current_dev = None

    comparison_results.append({
        "evaluation_date": eval_date,
        "actual_total": float(totals["actual"] or 0),
        "new_model_total": float(totals["new_model"] or 0),
        "current_model_total": float(totals["current_model"] or 0) if totals["current_model"] else None,
        "new_model_deviation_pct": float(new_dev) if new_dev is not None else None,
        "current_model_deviation_pct": float(current_dev) if current_dev is not None else None,
        "carts_with_actuals": int(totals["carts_with_actuals"]),
        "carts_with_current_forecast": int(totals["carts_with_current"]),
    })

comparison_df = spark.createDataFrame(comparison_results)
display(comparison_df.orderBy("evaluation_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Summary: new model vs current model

# COMMAND ----------

summary = comparison_df.filter(F.col("current_model_deviation_pct").isNotNull())

if summary.count() == 0:
    print("No overlapping dates found between backtest and current production forecasts.")
    print("This may happen if the current model forecasts don't cover the backtest date range.")
else:
    stats = summary.agg(
        F.avg("new_model_deviation_pct").alias("new_avg_dev"),
        F.avg("current_model_deviation_pct").alias("current_avg_dev"),
        F.max("new_model_deviation_pct").alias("new_max_dev"),
        F.max("current_model_deviation_pct").alias("current_max_dev"),
        F.count("*").alias("dates_compared"),
    ).collect()[0]

    print(f"Dates compared: {stats['dates_compared']}")
    print()
    print(f"{'Metric':<30} {'New Model':>15} {'Current Model':>15} {'Winner':>10}")
    print("-" * 75)
    print(f"{'Avg deviation %':<30} {stats['new_avg_dev']*100:>14.2f}% {stats['current_avg_dev']*100:>14.2f}% "
          f"{'NEW' if stats['new_avg_dev'] < stats['current_avg_dev'] else 'CURRENT':>10}")
    print(f"{'Max deviation %':<30} {stats['new_max_dev']*100:>14.2f}% {stats['current_max_dev']*100:>14.2f}% "
          f"{'NEW' if stats['new_max_dev'] < stats['current_max_dev'] else 'CURRENT':>10}")

    wins = summary.filter(
        F.col("new_model_deviation_pct") < F.col("current_model_deviation_pct")
    ).count()
    total = summary.count()
    print(f"\nNew model wins {wins}/{total} dates ({wins/total*100:.0f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Comparison by dimension
# MAGIC
# MAGIC For each validation dimension, compare deviation of new vs current model.

# COMMAND ----------

COMPARISON_DIMS = ["country_group", "rnpl_segment", "payment_processor", "weekly"]

eval_date_for_dim = latest_date

factors_cmp = compute_factors(bucketed, eval_date_for_dim, config)
immature_cmp = get_immature_carts(bucketed, eval_date_for_dim, config)
new_fc_cmp = apply_factors_with_fallback(immature_cmp, factors_cmp, config)
immature_ids_cmp = immature_cmp.select("shopping_cart_id")

actuals_cmp = (
    bucketed
    .join(immature_ids_cmp, "shopping_cart_id", "inner")
    .filter(F.col("total_payment_costs") != 0)
)
actuals_cmp = _filter_mature_actuals(actuals_cmp, snapshot_date, config)
new_fc_cmp = new_fc_cmp.join(actuals_cmp.select("shopping_cart_id"), "shopping_cart_id", "inner")

current_fc_cmp = (
    current_forecasts
    .join(immature_ids_cmp, "shopping_cart_id", "inner")
    .groupBy("shopping_cart_id")
    .agg(F.sum("amount").alias("current_model_cost"))
)

for dim in COMPARISON_DIMS:
    if dim == "weekly":
        new_agg = (
            new_fc_cmp
            .withColumn("_dim", F.date_trunc("week", F.col("date_of_checkout")).cast("string"))
            .groupBy("_dim").agg(F.sum("amount").alias("new_total"))
        )
        actual_agg = (
            actuals_cmp
            .withColumn("_dim", F.date_trunc("week", F.col("date_of_checkout")).cast("string"))
            .groupBy("_dim").agg(F.sum("total_payment_costs").alias("actual_total"))
        )
        current_agg = (
            current_forecasts
            .join(immature_ids_cmp, "shopping_cart_id", "inner")
            .withColumn("_dim", F.date_trunc("week", F.col("date_of_checkout")).cast("string"))
            .groupBy("_dim").agg(F.sum("amount").alias("current_total"))
        )
    else:
        if dim not in actuals_cmp.columns:
            print(f"\n--- {dim}: column not found, skipping ---")
            continue

        new_agg = new_fc_cmp.groupBy(F.col(dim).cast("string").alias("_dim")).agg(F.sum("amount").alias("new_total"))
        actual_agg = actuals_cmp.groupBy(F.col(dim).cast("string").alias("_dim")).agg(F.sum("total_payment_costs").alias("actual_total"))

        if dim in current_forecasts.columns:
            current_agg = (
                current_forecasts
                .join(immature_ids_cmp, "shopping_cart_id", "inner")
                .groupBy(F.col(dim).cast("string").alias("_dim"))
                .agg(F.sum("amount").alias("current_total"))
            )
        else:
            current_agg = None

    dim_compare = (
        actual_agg
        .join(new_agg, "_dim", "left")
    )
    if current_agg is not None:
        dim_compare = dim_compare.join(current_agg, "_dim", "left")
    else:
        dim_compare = dim_compare.withColumn("current_total", F.lit(None).cast("double"))

    dim_compare = (
        dim_compare
        .withColumn("new_dev_pct", F.when(
            F.abs(F.col("actual_total")) > 0,
            F.abs(F.col("new_total") - F.col("actual_total")) / F.abs(F.col("actual_total")) * 100,
        ))
        .withColumn("current_dev_pct", F.when(
            (F.abs(F.col("actual_total")) > 0) & F.col("current_total").isNotNull(),
            F.abs(F.col("current_total") - F.col("actual_total")) / F.abs(F.col("actual_total")) * 100,
        ))
        .withColumn("improvement_pp", F.col("current_dev_pct") - F.col("new_dev_pct"))
        .orderBy("_dim")
    )

    print(f"\n--- {dim} (eval date: {eval_date_for_dim}) ---")
    display(dim_compare)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Overall verdict

# COMMAND ----------

print("=" * 60)
print("MODEL COMPARISON SUMMARY")
print("=" * 60)
print()
print(f"Current model factors: country_group, is_rnpl")
print(f"New model factors:     {', '.join(config.factor_columns)}")
print()
print("Review the tables above to confirm:")
print("  1. New model has lower avg/max deviation than current")
print("  2. New model wins on most evaluation dates")
print("  3. New model is closer to actuals for all key dimensions")
print("  4. No dimension has regression (new model worse than current)")
