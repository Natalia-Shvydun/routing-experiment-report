# Databricks notebook source
# MAGIC %md
# MAGIC # Fallback Strategy Comparison
# MAGIC
# MAGIC Short-period experiment (Jan 2026, 4 weeks) comparing **which factor to
# MAGIC drop first** when falling back from the exact 5-factor match.
# MAGIC
# MAGIC **Strategies tested** (each drops a different factor from the exact match):
# MAGIC
# MAGIC | Strategy | Dropped factor | Remaining 4 factors |
# MAGIC |----------|---------------|---------------------|
# MAGIC | drop_country | country_bucket | rnpl + method + currency + processor |
# MAGIC | drop_processor | payment_processor | rnpl + method + currency + country |
# MAGIC | drop_currency | currency | rnpl + method + processor + country |
# MAGIC | drop_method | payment_method_detail | rnpl + currency + processor + country |
# MAGIC | drop_rnpl | rnpl_segment | method + currency + processor + country |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

import sys
from datetime import date, timedelta
from typing import List, Tuple

sys.path.insert(0, "/Workspace/Users/natalia.shvydun@getyourguide.com/.bundle/Cursor/dev/files/scripts")

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from payment_costs_forecasting.config import ForecastingConfig
from payment_costs_forecasting.bucketing import apply_bucketing
from payment_costs_forecasting.factors import compute_factors
from payment_costs_forecasting.forecast import get_immature_carts
from payment_costs_forecasting.validation import _filter_mature_actuals

config = ForecastingConfig()

ALL_FACTORS = ["rnpl_segment", "payment_method_detail", "currency", "payment_processor", "country_bucket"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

dbutils.widgets.text("backtest_start", "2026-01-05", "Start date (Monday)")
dbutils.widgets.text("backtest_end", "2026-01-26", "End date (Monday)")
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

print(f"Period: {evaluation_dates[0]} → {evaluation_dates[-1]}  ({len(evaluation_dates)} weeks)")

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
# MAGIC # 1. Define fallback strategies

# COMMAND ----------

def build_linear_fallback(factor_columns: List[str]) -> List[Tuple[List[str], str]]:
    """Build a linear fallback chain by dropping one factor at a time from the end."""
    levels = []
    for i in range(len(factor_columns), -1, -1):
        dims = factor_columns[:i]
        if len(dims) == len(factor_columns):
            label = "exact_match"
        elif len(dims) == 0:
            label = "global"
        else:
            label = f"fb_{len(dims)}"
        levels.append((dims, label))
    return levels


STRATEGIES = {}

for drop_col in ALL_FACTORS:
    remaining = [c for c in ALL_FACTORS if c != drop_col]
    name = f"drop_{drop_col.replace('payment_', '').replace('_bucket', '').replace('_segment', '').replace('_detail', '')}"
    levels = [
        (ALL_FACTORS, "exact_match"),
        (remaining, f"fb4_no_{drop_col.split('_')[-1]}"),
    ]
    for i in range(len(remaining) - 1, 0, -1):
        levels.append((remaining[:i], f"fb_{i}"))
    levels.append(([], "global"))
    STRATEGIES[name] = levels

STRATEGIES["v4_baseline"] = build_linear_fallback(ALL_FACTORS)

for name, levels in STRATEGIES.items():
    print(f"\n{name}:")
    for dims, label in levels:
        print(f"  {label:25s}  {dims or '(global)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Apply factors with custom fallback

# COMMAND ----------

def apply_factors_custom(immature, factors, levels):
    result = immature.withColumn(
        "_factor", F.lit(None).cast("double")
    ).withColumn(
        "_source", F.lit(None).cast("string")
    )

    for level_dims, label in levels:
        if not level_dims:
            gf_row = factors.agg(
                (F.sum("total_cost") / F.sum("total_gmv")).alias("gf")
            ).collect()
            if gf_row and gf_row[0]["gf"] is not None:
                gf = float(gf_row[0]["gf"])
                result = result.withColumn(
                    "_factor", F.coalesce(F.col("_factor"), F.lit(gf))
                ).withColumn(
                    "_source", F.coalesce(F.col("_source"), F.lit(label))
                )
            continue

        level_factors = (
            factors.groupBy(level_dims)
            .agg((F.sum("total_cost") / F.sum("total_gmv")).alias("_lf"))
            .withColumn("_ls", F.lit(label))
        )

        result = (
            result.join(level_factors, level_dims, "left")
            .withColumn("_factor", F.coalesce(F.col("_factor"), F.col("_lf")))
            .withColumn("_source", F.coalesce(F.col("_source"), F.col("_ls")))
            .drop("_lf", "_ls")
        )

    result = (
        result
        .withColumn("amount", F.col("gmv") * F.coalesce(F.col("_factor"), F.lit(0.0)))
        .withColumn("source", F.col("_source"))
        .drop("_factor", "_source")
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Run backtest for all strategies

# COMMAND ----------

DIMS = [
    "rnpl_segment", "payment_processor", "payment_method_detail",
    "currency", "country_bucket", "country_group",
]

all_results = None

for strategy_name, levels in STRATEGIES.items():
    print(f"\n=== Strategy: {strategy_name} ===")

    for i, eval_date in enumerate(evaluation_dates):
        factors = compute_factors(bucketed, eval_date, config)
        immature = get_immature_carts(bucketed, eval_date, config)
        forecast_df = apply_factors_custom(immature, factors, levels)

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
            .withColumn("strategy", F.lit(strategy_name))
        )

        if all_results is None:
            all_results = cart_level
        else:
            all_results = all_results.unionByName(cart_level)

        print(f"  {eval_date} done")

    print(f"  {strategy_name} complete")

all_results.cache()
print(f"\nTotal rows: {all_results.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Overall deviation by strategy
# MAGIC
# MAGIC Which strategy has the lowest aggregate deviation?

# COMMAND ----------

overall_by_strategy = (
    all_results
    .groupBy("strategy")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 4))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 4))
    .orderBy("abs_dev_pct")
)

display(overall_by_strategy)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Weekly deviation by strategy

# COMMAND ----------

display(
    all_results
    .groupBy("strategy", "eval_date")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("strategy", "eval_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Deviation by payment_method_detail × strategy

# COMMAND ----------

top_methods = [r[0] for r in
    all_results.filter(F.col("strategy") == "v4_baseline")
    .groupBy("payment_method_detail")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(20).collect()]

method_dev = (
    all_results
    .filter(F.col("payment_method_detail").isin(top_methods))
    .groupBy("strategy", "payment_method_detail")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

display(
    method_dev
    .orderBy("payment_method_detail", "abs_dev_pct")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Best strategy per payment method

# COMMAND ----------

w = Window.partitionBy("payment_method_detail").orderBy("abs_dev_pct")

display(
    method_dev
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") == 1)
    .select("payment_method_detail", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy(F.desc("carts"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Deviation by currency × strategy

# COMMAND ----------

top_currencies = [r[0] for r in
    all_results.filter(F.col("strategy") == "v4_baseline")
    .groupBy("currency")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(15).collect()]

curr_dev = (
    all_results
    .filter(F.col("currency").isin(top_currencies))
    .groupBy("strategy", "currency")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

display(curr_dev.orderBy("currency", "abs_dev_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Best strategy per currency

# COMMAND ----------

w_curr = Window.partitionBy("currency").orderBy("abs_dev_pct")

display(
    curr_dev
    .withColumn("rank", F.row_number().over(w_curr))
    .filter(F.col("rank") == 1)
    .select("currency", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy(F.desc("carts"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Deviation by country_bucket × strategy

# COMMAND ----------

top_countries = [r[0] for r in
    all_results.filter(F.col("strategy") == "v4_baseline")
    .groupBy("country_bucket")
    .agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(15).collect()]

country_dev = (
    all_results
    .filter(F.col("country_bucket").isin(top_countries))
    .groupBy("strategy", "country_bucket")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

display(country_dev.orderBy("country_bucket", "abs_dev_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Best strategy per country

# COMMAND ----------

w_ctry = Window.partitionBy("country_bucket").orderBy("abs_dev_pct")

display(
    country_dev
    .withColumn("rank", F.row_number().over(w_ctry))
    .filter(F.col("rank") == 1)
    .select("country_bucket", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy(F.desc("carts"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Deviation by payment_processor × strategy

# COMMAND ----------

proc_dev = (
    all_results
    .groupBy("strategy", "payment_processor")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .filter(F.col("carts") >= 500)
)

display(proc_dev.orderBy("payment_processor", "abs_dev_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 9. Deviation by rnpl_segment × strategy

# COMMAND ----------

display(
    all_results
    .groupBy("strategy", "rnpl_segment")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .orderBy("rnpl_segment", "abs_dev_pct")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 10. Combined dimension deviation: method × country
# MAGIC
# MAGIC For the top 10 methods × top 10 countries, which strategy wins?

# COMMAND ----------

top10_methods = top_methods[:10]
top10_countries = top_countries[:10]

combo_method_country = (
    all_results
    .filter(
        F.col("payment_method_detail").isin(top10_methods)
        & F.col("country_bucket").isin(top10_countries)
    )
    .groupBy("strategy", "payment_method_detail", "country_bucket")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .filter(F.col("carts") >= 100)
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

w_combo = Window.partitionBy("payment_method_detail", "country_bucket").orderBy("abs_dev_pct")

display(
    combo_method_country
    .withColumn("rank", F.row_number().over(w_combo))
    .filter(F.col("rank") == 1)
    .select("payment_method_detail", "country_bucket", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy("payment_method_detail", "country_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Strategy win count across method × country combos

# COMMAND ----------

combo_winners = (
    combo_method_country
    .withColumn("rank", F.row_number().over(w_combo))
    .filter(F.col("rank") == 1)
)

display(
    combo_winners
    .groupBy("strategy")
    .agg(
        F.count("*").alias("wins"),
        F.round(F.avg("abs_dev_pct"), 3).alias("avg_winning_dev"),
    )
    .orderBy(F.desc("wins"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 11. Combined dimension deviation: method × currency

# COMMAND ----------

top10_curr = top_currencies[:10]

combo_method_curr = (
    all_results
    .filter(
        F.col("payment_method_detail").isin(top10_methods)
        & F.col("currency").isin(top10_curr)
    )
    .groupBy("strategy", "payment_method_detail", "currency")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .filter(F.col("carts") >= 100)
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

w_mc = Window.partitionBy("payment_method_detail", "currency").orderBy("abs_dev_pct")

display(
    combo_method_curr
    .withColumn("rank", F.row_number().over(w_mc))
    .filter(F.col("rank") == 1)
    .select("payment_method_detail", "currency", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy("payment_method_detail", "currency")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Strategy win count across method × currency combos

# COMMAND ----------

combo_mc_winners = (
    combo_method_curr
    .withColumn("rank", F.row_number().over(w_mc))
    .filter(F.col("rank") == 1)
)

display(
    combo_mc_winners
    .groupBy("strategy")
    .agg(
        F.count("*").alias("wins"),
        F.round(F.avg("abs_dev_pct"), 3).alias("avg_winning_dev"),
    )
    .orderBy(F.desc("wins"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 12. Combined dimension deviation: country × currency

# COMMAND ----------

combo_country_curr = (
    all_results
    .filter(
        F.col("country_bucket").isin(top10_countries)
        & F.col("currency").isin(top10_curr)
    )
    .groupBy("strategy", "country_bucket", "currency")
    .agg(
        F.sum("forecast_cost").alias("forecast"),
        F.sum("actual_cost").alias("actual"),
        F.count("*").alias("carts"),
    )
    .filter(F.col("carts") >= 100)
    .withColumn("signed_dev_pct",
        F.round((F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
    .withColumn("abs_dev_pct",
        F.round(F.abs(F.col("forecast") - F.col("actual")) / F.abs(F.col("actual")) * 100, 3))
)

w_cc = Window.partitionBy("country_bucket", "currency").orderBy("abs_dev_pct")

display(
    combo_country_curr
    .withColumn("rank", F.row_number().over(w_cc))
    .filter(F.col("rank") == 1)
    .select("country_bucket", "currency", "strategy", "abs_dev_pct", "signed_dev_pct", "carts")
    .orderBy("country_bucket", "currency")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 Strategy win count across country × currency combos

# COMMAND ----------

combo_cc_winners = (
    combo_country_curr
    .withColumn("rank", F.row_number().over(w_cc))
    .filter(F.col("rank") == 1)
)

display(
    combo_cc_winners
    .groupBy("strategy")
    .agg(
        F.count("*").alias("wins"),
        F.round(F.avg("abs_dev_pct"), 3).alias("avg_winning_dev"),
    )
    .orderBy(F.desc("wins"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 13. Grand summary — strategy scorecard
# MAGIC
# MAGIC Aggregate across all single-dimension and combination analyses.

# COMMAND ----------

scorecard_rows = []

overall_data = overall_by_strategy.collect()
for row in overall_data:
    scorecard_rows.append({
        "strategy": row["strategy"],
        "metric": "overall_abs_dev_pct",
        "value": float(row["abs_dev_pct"]),
    })

for label, combo_df, w_fn in [
    ("method", method_dev, Window.partitionBy("payment_method_detail").orderBy("abs_dev_pct")),
    ("currency", curr_dev, Window.partitionBy("currency").orderBy("abs_dev_pct")),
    ("country", country_dev, Window.partitionBy("country_bucket").orderBy("abs_dev_pct")),
    ("method×country", combo_method_country, w_combo),
    ("method×currency", combo_method_curr, w_mc),
    ("country×currency", combo_country_curr, w_cc),
]:
    wins = (
        combo_df
        .withColumn("rank", F.row_number().over(w_fn))
        .filter(F.col("rank") == 1)
        .groupBy("strategy")
        .agg(F.count("*").alias("wins"))
        .collect()
    )
    for row in wins:
        scorecard_rows.append({
            "strategy": row["strategy"],
            "metric": f"wins_{label}",
            "value": float(row["wins"]),
        })

scorecard_df = spark.createDataFrame(scorecard_rows)

display(
    scorecard_df
    .groupBy("strategy")
    .pivot("metric")
    .agg(F.first("value"))
    .orderBy("overall_abs_dev_pct")
)
