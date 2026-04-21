# Databricks notebook source
# MAGIC %md
# MAGIC # Factor Grouping & Fallback Analysis
# MAGIC
# MAGIC Understand dimension distributions, identify sparse segments, and design
# MAGIC optimal fallback rules for payment cost forecasting.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Single-dimension profiles
# MAGIC 2. Country: `country_bucket` vs `country_group`
# MAGIC 3. Processor x country sparsity
# MAGIC 4. Payment method detail: long-tail
# MAGIC 5. Full cross-product segment analysis
# MAGIC 6. Fallback strategy simulation
# MAGIC 7. Recommendations

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
from payment_costs_forecasting.bucketing import (
    apply_bucketing,
    CARD_METHODS,
    PROCESSOR_ALIASES,
    VARIANT_ALIASES,
    _normalize_column,
    compute_top_n,
)

config = ForecastingConfig()

ANALYSIS_START = "2025-12-01"
ANALYSIS_END = "2026-02-28"
MIN_VOL = config.min_volume_for_factor

print(f"Analysis period: {ANALYSIS_START} to {ANALYSIS_END}")
print(f"Factor columns: {config.factor_columns}")
print(f"Min volume for factor: {MIN_VOL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and prepare data

# COMMAND ----------

raw = spark.table(config.snapshot_table)

df = (
    raw
    .filter(F.col("date_of_checkout").between(ANALYSIS_START, ANALYSIS_END))
    .filter(F.col("gmv") > 0)
    .filter(F.col("total_payment_costs") != 0)
)

df = apply_bucketing(df, config)

df = df.withColumn("cost_rate", F.col("total_payment_costs") / F.col("gmv"))

print(f"Carts in analysis period: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Single-Dimension Profiles
# MAGIC
# MAGIC For each factor dimension, show volume, cost distribution, and identify
# MAGIC long-tail values that may be too small for stable factors.

# COMMAND ----------

PROFILE_DIMS = [
    "rnpl_segment",
    "payment_processor",
    "payment_method_detail",
    "currency",
    "country_group",
    "country_bucket",
]

def profile_dimension(data, dim_col):
    return (
        data
        .groupBy(dim_col)
        .agg(
            F.count("*").alias("cart_count"),
            F.sum("gmv").alias("total_gmv"),
            F.sum("total_payment_costs").alias("total_cost"),
            F.avg("cost_rate").alias("avg_cost_rate"),
            F.percentile_approx("cost_rate", 0.5).alias("median_cost_rate"),
            F.stddev("cost_rate").alias("std_cost_rate"),
        )
        .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
        .withColumn("dimension", F.lit(dim_col))
        .withColumnRenamed(dim_col, "dimension_value")
        .select(
            "dimension", "dimension_value", "cart_count", "total_gmv",
            "total_cost", "avg_cost_rate", "median_cost_rate",
            "std_cost_rate", "agg_cost_rate",
        )
        .orderBy(F.desc("cart_count"))
    )

# COMMAND ----------

for dim in PROFILE_DIMS:
    if dim not in df.columns:
        print(f"Skipping {dim} — not in DataFrame")
        continue
    print(f"\n{'='*60}")
    print(f"  {dim}")
    print(f"{'='*60}")
    prof = profile_dimension(df, dim)
    display(prof)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment count summary per dimension

# COMMAND ----------

for dim in PROFILE_DIMS:
    if dim not in df.columns:
        continue
    total_vals = df.select(dim).distinct().count()
    small = (
        df.groupBy(dim)
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") < MIN_VOL)
        .count()
    )
    print(f"{dim:30s}  total values: {total_vals:5d}  below {MIN_VOL} carts: {small:5d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Country — `country_bucket` vs `country_group`
# MAGIC
# MAGIC Is keeping individual country codes (top 20) worth it, or does
# MAGIC `country_group` provide sufficient granularity?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Side-by-side profiles

# COMMAND ----------

cg_prof = profile_dimension(df, "country_group")
cb_prof = profile_dimension(df, "country_bucket")

print("country_group profile:")
display(cg_prof)

print("country_bucket profile:")
display(cb_prof)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Individual country rate vs its country_group rate
# MAGIC
# MAGIC For each top-N country code, how much does its cost rate differ from
# MAGIC its group average? Large deviations justify keeping the country separate.

# COMMAND ----------

top_countries = compute_top_n(df, "country_code", config.country_top_n)

country_rates = (
    df
    .filter(F.col("country_code").isin(top_countries))
    .groupBy("country_code", "country_group")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("country_rate", F.col("total_cost") / F.col("total_gmv"))
)

group_rates = (
    df
    .groupBy("country_group")
    .agg(
        F.sum("total_payment_costs").alias("grp_cost"),
        F.sum("gmv").alias("grp_gmv"),
    )
    .withColumn("group_rate", F.col("grp_cost") / F.col("grp_gmv"))
    .select("country_group", "group_rate")
)

comparison = (
    country_rates.join(group_rates, "country_group")
    .withColumn(
        "deviation_from_group",
        F.abs(F.col("country_rate") - F.col("group_rate"))
        / F.abs(F.col("group_rate")),
    )
    .withColumn("rate_diff_pct",
        F.round((F.col("country_rate") - F.col("group_rate")) * 100, 4))
    .orderBy(F.desc("deviation_from_group"))
)

display(comparison.select(
    "country_code", "country_group", "cart_count",
    F.round("country_rate", 6).alias("country_rate"),
    F.round("group_rate", 6).alias("group_rate"),
    "rate_diff_pct",
    F.round("deviation_from_group", 4).alias("deviation_from_group"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2b Country vs group rate **by payment method detail**
# MAGIC
# MAGIC The overall rate comparison above mixes payment method effects.
# MAGIC Countries like PT (mbway) or PL (blik) have local methods that shift the
# MAGIC overall rate, but for the same card network (VISA, MASTERCARD) they might
# MAGIC align closely with their country group.
# MAGIC
# MAGIC This section isolates the question: *for the same payment method, does the
# MAGIC country still deviate from its group?*

# COMMAND ----------

KEY_METHODS = ["VISA", "MASTERCARD", "AMEX", "paypal", "apple_pay", "google_pay"]

country_method_rates = (
    df
    .filter(F.col("country_code").isin(top_countries))
    .filter(F.col("payment_method_detail").isin(KEY_METHODS))
    .groupBy("country_code", "country_group", "payment_method_detail")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("country_rate", F.col("total_cost") / F.col("total_gmv"))
)

group_method_rates = (
    df
    .filter(F.col("payment_method_detail").isin(KEY_METHODS))
    .groupBy("country_group", "payment_method_detail")
    .agg(
        F.sum("total_payment_costs").alias("grp_cost"),
        F.sum("gmv").alias("grp_gmv"),
    )
    .withColumn("group_rate", F.col("grp_cost") / F.col("grp_gmv"))
    .select("country_group", "payment_method_detail", "group_rate")
)

method_comparison = (
    country_method_rates
    .join(group_method_rates, ["country_group", "payment_method_detail"])
    .withColumn(
        "deviation_from_group",
        F.abs(F.col("country_rate") - F.col("group_rate"))
        / F.abs(F.col("group_rate")),
    )
    .withColumn("rate_diff_pp",
        F.round((F.col("country_rate") - F.col("group_rate")) * 100, 4))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Per-method deviation: pivot by country

# COMMAND ----------

display(
    method_comparison
    .select(
        "country_code", "country_group", "payment_method_detail", "cart_count",
        F.round("country_rate", 6).alias("country_rate"),
        F.round("group_rate", 6).alias("group_rate"),
        "rate_diff_pp",
        F.round("deviation_from_group", 4).alias("deviation_from_group"),
    )
    .orderBy("payment_method_detail", F.desc("deviation_from_group"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary: average deviation by payment method
# MAGIC
# MAGIC If card networks (VISA, MASTERCARD) show low deviation while the overall
# MAGIC comparison showed high deviation, the gap is driven by **payment mix**,
# MAGIC not intrinsic country-level rate differences. In that case `country_group`
# MAGIC is sufficient when combined with `payment_method_detail`.

# COMMAND ----------

method_dev_summary = (
    method_comparison
    .filter(F.col("cart_count") >= MIN_VOL)
    .groupBy("payment_method_detail")
    .agg(
        F.count("*").alias("country_count"),
        F.avg("deviation_from_group").alias("avg_deviation"),
        F.max("deviation_from_group").alias("max_deviation"),
        F.sum(F.when(F.col("deviation_from_group") > 0.10, 1).otherwise(0)).alias("countries_above_10pct"),
    )
    .orderBy(F.desc("avg_deviation"))
)
display(method_dev_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Side-by-side: overall deviation vs card-only deviation per country
# MAGIC
# MAGIC Shows whether a country's deviation from group shrinks when we control
# MAGIC for payment method.

# COMMAND ----------

overall_dev = (
    comparison
    .select(
        F.col("country_code"),
        F.round("deviation_from_group", 4).alias("overall_deviation"),
    )
)

card_only_dev = (
    method_comparison
    .filter(F.col("payment_method_detail").isin("VISA", "MASTERCARD"))
    .filter(F.col("cart_count") >= MIN_VOL)
    .groupBy("country_code")
    .agg(
        F.round(F.avg("deviation_from_group"), 4).alias("card_avg_deviation"),
        F.round(F.max("deviation_from_group"), 4).alias("card_max_deviation"),
    )
)

side_by_side = (
    overall_dev.join(card_only_dev, "country_code", "left")
    .withColumn("mix_effect",
        F.round(F.col("overall_deviation") - F.coalesce(F.col("card_avg_deviation"), F.lit(0)), 4))
    .orderBy(F.desc("mix_effect"))
)

print("mix_effect = overall_deviation − card_avg_deviation")
print("High mix_effect → country differs from group mainly because of payment method mix,")
print("not because the same card network costs differently.\n")
display(side_by_side)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Verdict: does country_bucket add value over country_group?
# MAGIC
# MAGIC If most top-20 countries have deviation < 10% from their group, then
# MAGIC `country_group` is sufficient and reduces segment fragmentation.
# MAGIC
# MAGIC **Important**: also consider the per-method analysis above. If the deviation
# MAGIC shrinks substantially when controlling for payment method, the gap is a
# MAGIC payment-mix effect already captured by `payment_method_detail` — in which
# MAGIC case `country_group` is sufficient.

# COMMAND ----------

significant_countries = comparison.filter(F.col("deviation_from_group") > 0.10).count()
total_top = comparison.count()
print(f"Top-{config.country_top_n} countries with >10% deviation from group rate: {significant_countries} / {total_top}")
print()
if significant_countries > total_top * 0.5:
    print("RECOMMENDATION: Keep country_bucket — many countries differ meaningfully from group.")
else:
    print("RECOMMENDATION: Consider using country_group only — most countries align with group rate.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Processor x Country Sparsity
# MAGIC
# MAGIC Some processors have very low traffic in certain countries/groups.
# MAGIC These sparse cells create unreliable factors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Cross-tab: processor x country_group

# COMMAND ----------

cross = (
    df
    .groupBy("payment_processor", "country_group")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .withColumn("is_sparse", F.col("cart_count") < MIN_VOL)
    .orderBy("payment_processor", "country_group")
)

display(cross)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Sparsity summary

# COMMAND ----------

total_cells = cross.count()
sparse_cells = cross.filter(F.col("is_sparse")).count()
sparse_carts = cross.filter(F.col("is_sparse")).agg(F.sum("cart_count")).collect()[0][0] or 0
total_carts = cross.agg(F.sum("cart_count")).collect()[0][0]

print(f"Total processor x country_group cells: {total_cells}")
print(f"Sparse cells (< {MIN_VOL} carts):      {sparse_cells} ({sparse_cells/total_cells*100:.1f}%)")
print(f"Carts in sparse cells:                 {sparse_carts:,} ({sparse_carts/total_carts*100:.2f}% of total)")
print()
print("Sparse cells by processor:")

display(
    cross
    .filter(F.col("is_sparse"))
    .groupBy("payment_processor")
    .agg(
        F.count("*").alias("sparse_cells"),
        F.sum("cart_count").alias("carts_in_sparse"),
    )
    .orderBy(F.desc("sparse_cells"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 For sparse processor+country, how does the processor-only rate compare?
# MAGIC
# MAGIC If processor-only rate is close to the sparse cell rate, dropping country
# MAGIC from the fallback is better than keeping an unreliable cell.

# COMMAND ----------

proc_rates = (
    df
    .groupBy("payment_processor")
    .agg(
        F.sum("total_payment_costs").alias("proc_cost"),
        F.sum("gmv").alias("proc_gmv"),
    )
    .withColumn("proc_only_rate", F.col("proc_cost") / F.col("proc_gmv"))
    .select("payment_processor", "proc_only_rate")
)

sparse_analysis = (
    cross
    .filter(F.col("is_sparse"))
    .join(proc_rates, "payment_processor")
    .withColumn("rate_diff_from_proc",
        F.abs(F.col("agg_cost_rate") - F.col("proc_only_rate"))
        / F.abs(F.col("proc_only_rate"))
    )
    .select(
        "payment_processor", "country_group", "cart_count",
        F.round("agg_cost_rate", 6).alias("cell_rate"),
        F.round("proc_only_rate", 6).alias("proc_rate"),
        F.round("rate_diff_from_proc", 4).alias("rate_diff"),
    )
    .orderBy(F.desc("rate_diff"))
)

display(sparse_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Payment Method Detail — Long Tail
# MAGIC
# MAGIC Are there `payment_method_detail` values with so few carts that they
# MAGIC should be grouped into an `OTHER` bucket?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Full profile ordered by volume

# COMMAND ----------

method_prof = profile_dimension(df, "payment_method_detail")
display(method_prof)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Long-tail grouping analysis
# MAGIC
# MAGIC Show cumulative volume if we keep only variants above a threshold
# MAGIC and group the rest into `OTHER_CARD`.

# COMMAND ----------

method_counts = (
    df
    .groupBy("payment_method_detail")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .orderBy(F.desc("cart_count"))
)

total_method_carts = float(method_counts.agg(F.sum("cart_count")).collect()[0][0] or 0)

for threshold in [100, 500, 1000, 5000]:
    above = method_counts.filter(F.col("cart_count") >= threshold)
    below = method_counts.filter(F.col("cart_count") < threshold)
    n_above = above.count()
    n_below = below.count()
    carts_above = float(above.agg(F.sum("cart_count")).collect()[0][0] or 0)
    carts_below = float(below.agg(F.sum("cart_count")).collect()[0][0] or 0)
    below_rate_row = below.agg(
        F.sum("total_cost").alias("c"), F.sum("total_gmv").alias("g")
    ).collect()[0]
    c_val = float(below_rate_row["c"] or 0)
    g_val = float(below_rate_row["g"] or 0)
    below_rate = c_val / g_val if g_val else 0

    print(f"Threshold {threshold:>5d} carts:")
    print(f"  Keep {n_above} variants ({carts_above:,.0f} carts = {carts_above/total_method_carts*100:.1f}%)")
    print(f"  Group {n_below} variants into OTHER ({carts_below:,.0f} carts = {carts_below/total_method_carts*100:.1f}%)")
    print(f"  OTHER aggregate cost rate: {below_rate:.6f}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Variants that would be grouped at threshold=1000

# COMMAND ----------

display(
    method_counts
    .filter(F.col("cart_count") < 1000)
    .select(
        "payment_method_detail", "cart_count",
        F.round("agg_cost_rate", 6).alias("agg_cost_rate"),
    )
    .orderBy(F.desc("cart_count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4b: Currency vs Country Redundancy Analysis
# MAGIC
# MAGIC How much independent information does `country_bucket` add beyond
# MAGIC `currency`? If most country-level variation is already captured by
# MAGIC currency, then country should be the **first dimension dropped** in
# MAGIC the fallback chain.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.1 Country rate vs its dominant currency rate
# MAGIC
# MAGIC For countries with a single dominant currency (US→USD, BR→BRL, etc.),
# MAGIC the rates should be nearly identical. For EUR-zone countries sharing
# MAGIC EUR, meaningful differences would justify keeping `country_bucket`.

# COMMAND ----------

country_stats = (
    df
    .groupBy("country_bucket")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("country_rate", F.col("total_cost") / F.col("total_gmv"))
    .filter(F.col("cart_count") >= MIN_VOL)
)

dominant_currency = (
    df
    .groupBy("country_bucket", "currency")
    .agg(F.count("*").alias("n"))
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("country_bucket").orderBy(F.desc("n"))))
    .filter(F.col("rank") == 1)
    .select("country_bucket", F.col("currency").alias("dominant_currency"))
)

currency_stats = (
    df
    .groupBy("currency")
    .agg(
        F.sum("total_payment_costs").alias("curr_cost"),
        F.sum("gmv").alias("curr_gmv"),
    )
    .withColumn("currency_rate", F.col("curr_cost") / F.col("curr_gmv"))
    .select("currency", "currency_rate")
)

redundancy = (
    country_stats
    .join(dominant_currency, "country_bucket")
    .join(currency_stats, F.col("dominant_currency") == F.col("currency"))
    .withColumn("rate_diff_pp",
        F.round((F.col("country_rate") - F.col("currency_rate")) * 100, 4))
    .withColumn("abs_diff_pp",
        F.round(F.abs(F.col("country_rate") - F.col("currency_rate")) * 100, 4))
    .withColumn("is_eur", F.col("dominant_currency") == "EUR")
    .select(
        "country_bucket", "cart_count", "dominant_currency",
        F.round("country_rate", 6).alias("country_rate"),
        F.round("currency_rate", 6).alias("currency_rate"),
        "rate_diff_pp", "abs_diff_pp", "is_eur",
    )
    .orderBy(F.desc("abs_diff_pp"))
)

display(redundancy)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.2 Redundancy summary: EUR vs non-EUR countries
# MAGIC
# MAGIC For non-EUR countries, currency should capture nearly all the variation.
# MAGIC For EUR countries, country adds independent information.

# COMMAND ----------

eur_summary = (
    redundancy
    .groupBy("is_eur")
    .agg(
        F.count("*").alias("countries"),
        F.round(F.avg("abs_diff_pp"), 4).alias("avg_abs_diff_pp"),
        F.round(F.max("abs_diff_pp"), 4).alias("max_abs_diff_pp"),
        F.sum("cart_count").alias("total_carts"),
    )
)

print("Redundancy between country_bucket and currency:")
print("(abs_diff_pp = how much country rate differs from its currency rate)\n")
display(eur_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.3 Chart: country rate vs currency rate
# MAGIC
# MAGIC Points close to the diagonal = currency already explains the country rate.
# MAGIC EUR-zone countries (sharing the same currency_rate) show as a vertical
# MAGIC cluster — their spread is the independent value of country.

# COMMAND ----------

display(
    redundancy
    .select(
        "country_bucket", "dominant_currency",
        F.col("country_rate").cast("double").alias("country_rate"),
        F.col("currency_rate").cast("double").alias("currency_rate"),
        F.col("is_eur").cast("string").alias("is_eur"),
        F.col("cart_count").cast("double").alias("cart_count"),
    )
    .orderBy("is_eur", "country_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.4 EUR-zone deep dive: within-EUR variation
# MAGIC
# MAGIC Since EUR countries share the same currency, any rate differences must
# MAGIC be driven by country-specific factors (local methods, acquirer routing,
# MAGIC regulatory costs).

# COMMAND ----------

eur_countries = (
    redundancy
    .filter(F.col("is_eur") == True)
    .select(
        "country_bucket", "cart_count",
        F.col("country_rate").cast("double").alias("country_rate"),
        F.col("currency_rate").cast("double").alias("eur_avg_rate"),
        "rate_diff_pp",
    )
    .orderBy("country_rate")
)

display(eur_countries)

# COMMAND ----------

eur_rates = [float(r["country_rate"]) for r in eur_countries.collect()]
if len(eur_rates) >= 2:
    eur_min, eur_max = min(eur_rates), max(eur_rates)
    print(f"EUR-zone country rate range: {eur_min*100:.3f}% to {eur_max*100:.3f}%")
    print(f"Spread: {(eur_max - eur_min)*100:.3f} pp")
    if eur_min != 0:
        print(f"Ratio: {eur_max/eur_min:.1f}x")
    print()
    print("If this spread is small relative to the cross-currency spread,")
    print("then country_bucket adds limited value and should be dropped first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.5 Processor rate vs its dominant currency mix
# MAGIC
# MAGIC How much of the processor rate variation is driven by the currencies
# MAGIC it processes? If most of the variation is due to currency mix,
# MAGIC processor adds limited independent value above currency.

# COMMAND ----------

proc_currency_mix = (
    df
    .groupBy("payment_processor", "currency")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .join(currency_stats, "currency")
    .withColumn("rate_diff_from_currency",
        F.round((F.col("agg_cost_rate") - F.col("currency_rate")) * 100, 4))
    .filter(F.col("cart_count") >= MIN_VOL)
)

proc_indep = (
    proc_currency_mix
    .groupBy("payment_processor")
    .agg(
        F.count("*").alias("currencies_served"),
        F.sum("cart_count").alias("total_carts"),
        F.round(F.avg(F.abs(F.col("rate_diff_from_currency") / 100)), 6).alias("avg_abs_diff_from_currency"),
        F.round(F.max(F.abs(F.col("rate_diff_from_currency") / 100)), 6).alias("max_abs_diff_from_currency"),
    )
    .orderBy(F.desc("total_carts"))
)

print("Processor independence from currency:")
print("(How much does processor rate differ from its currency rate, ")
print(" for the same currency? Higher = more independent value)\n")
display(proc_indep)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.6 Processor x currency detail
# MAGIC
# MAGIC For each processor+currency combination, compare the actual rate vs
# MAGIC the pure currency rate. Large differences mean the processor matters.

# COMMAND ----------

display(
    proc_currency_mix
    .select(
        "payment_processor", "currency", "cart_count",
        F.round("agg_cost_rate", 6).alias("proc_curr_rate"),
        F.round("currency_rate", 6).alias("pure_currency_rate"),
        "rate_diff_from_currency",
    )
    .orderBy("payment_processor", F.desc(F.abs(F.col("rate_diff_from_currency"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b.7 Proposed fallback order
# MAGIC
# MAGIC Based on the independent information each dimension adds:
# MAGIC
# MAGIC | Priority | Dimension | Rationale |
# MAGIC |---|---|---|
# MAGIC | 1 (keep longest) | `rnpl_segment` | Fundamental behavioral split |
# MAGIC | 2 | `payment_method_detail` | Widest rate spread (~10x), card network distinction |
# MAGIC | 3 | `currency` | Strong independent differentiator (3x EUR→USD) |
# MAGIC | 4 | `payment_processor` | Moderate independent value above currency |
# MAGIC | 5 (drop first) | `country_bucket` | Most redundant with currency |
# MAGIC
# MAGIC **Key insight:** For ~70% of carts (non-EUR), currency captures nearly
# MAGIC all country-level variation (< 0.1pp difference). Country only adds
# MAGIC meaningful value for EUR-zone countries (~0.3pp spread within EUR).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 5: Full Cross-Product Segment Analysis
# MAGIC
# MAGIC Compute all 5-factor segments (the actual factor table) and analyze
# MAGIC the distribution of segment sizes.

# COMMAND ----------

FACTOR_COLS = config.factor_columns

segments = (
    df
    .groupBy(FACTOR_COLS)
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("total_payment_costs").alias("total_cost"),
        F.sum("gmv").alias("total_gmv"),
        F.stddev("cost_rate").alias("std_cost_rate"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
)

total_segments = segments.count()
total_seg_carts = segments.agg(F.sum("cart_count")).collect()[0][0]

print(f"Total 5-factor segments: {total_segments:,}")
print(f"Total carts:             {total_seg_carts:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Segment size distribution

# COMMAND ----------

from pyspark.sql.types import DoubleType

stats = segments.select("cart_count").summary("min", "5%", "10%", "25%", "50%", "75%", "90%", "95%", "max", "mean")
display(stats)

# COMMAND ----------

below_min = segments.filter(F.col("cart_count") < MIN_VOL)
n_below = below_min.count()
carts_below = below_min.agg(F.sum("cart_count")).collect()[0][0] or 0

print(f"Segments below {MIN_VOL} carts: {n_below} / {total_segments} ({n_below/total_segments*100:.1f}%)")
print(f"Carts in those segments:      {carts_below:,} / {total_seg_carts:,} ({carts_below/total_seg_carts*100:.2f}%)")
print()
print("These carts will use a FALLBACK factor instead of an exact match.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Sparse segments breakdown — which dimensions cause sparsity?

# COMMAND ----------

for dim in FACTOR_COLS:
    sparse_in_dim = (
        below_min
        .groupBy(dim)
        .agg(
            F.count("*").alias("sparse_segments"),
            F.sum("cart_count").alias("carts"),
        )
        .orderBy(F.desc("sparse_segments"))
    )
    print(f"\nSparse segments by {dim}:")
    display(sparse_in_dim)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Alternative: segments with `country_group` instead of `country_bucket`
# MAGIC
# MAGIC Compare segment counts and sparsity if we use `country_group`.

# COMMAND ----------

alt_factors = [c if c != "country_bucket" else "country_group" for c in FACTOR_COLS]

alt_segments = (
    df
    .groupBy(alt_factors)
    .agg(F.count("*").alias("cart_count"))
)

alt_total = alt_segments.count()
alt_below = alt_segments.filter(F.col("cart_count") < MIN_VOL).count()
alt_carts_below = (
    alt_segments.filter(F.col("cart_count") < MIN_VOL)
    .agg(F.sum("cart_count")).collect()[0][0] or 0
)

print(f"With country_bucket: {total_segments} segments, {n_below} sparse ({n_below/total_segments*100:.1f}%)")
print(f"With country_group:  {alt_total} segments, {alt_below} sparse ({alt_below/alt_total*100:.1f}%)")
print()
print(f"Carts hitting fallback: country_bucket={carts_below:,}  country_group={alt_carts_below:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 6: Fallback Strategy Simulation
# MAGIC
# MAGIC Simulate different fallback strategies on the analysis period.
# MAGIC For each, measure how many carts use each fallback level and the
# MAGIC aggregate forecast deviation vs actuals.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.0 Prepare: compute factors and identify immature carts for a sample date

# COMMAND ----------

from payment_costs_forecasting.factors import compute_factors
from payment_costs_forecasting.forecast import get_immature_carts

eval_date = date(2026, 2, 1)
bucketed = apply_bucketing(
    raw.filter(F.col("gmv") > 0).filter(F.col("total_payment_costs") != 0),
    config,
)
bucketed = bucketed.withColumn("cost_rate", F.col("total_payment_costs") / F.col("gmv"))

factors_df = compute_factors(bucketed, eval_date, config)
immature = get_immature_carts(bucketed, eval_date, config)

actuals = (
    bucketed
    .join(immature.select("shopping_cart_id"), "shopping_cart_id", "inner")
)

print(f"Eval date: {eval_date}")
print(f"Factor rows: {factors_df.count():,}")
print(f"Immature carts: {immature.count():,}")
print(f"Actuals available: {actuals.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Fallback simulation helper

# COMMAND ----------

def simulate_fallback(immature_df, factors_df, fallback_chain, factor_cols_for_agg):
    """
    Simulate a fallback strategy and return per-cart forecasts with the
    fallback level that was used.

    fallback_chain: list of lists, e.g. [["a","b","c"], ["a","b"], ["a"], []]
    factor_cols_for_agg: all factor columns to aggregate factors by
    """
    result = immature_df.withColumn("_factor", F.lit(None).cast("double"))
    result = result.withColumn("_level", F.lit(None).cast("int"))

    for i, level_dims in enumerate(fallback_chain):
        if not level_dims:
            gf_row = factors_df.agg(F.avg("cost_factor").alias("gf")).collect()
            if gf_row and gf_row[0]["gf"] is not None:
                gf = float(gf_row[0]["gf"])
                result = result.withColumn(
                    "_factor", F.coalesce(F.col("_factor"), F.lit(gf))
                ).withColumn(
                    "_level", F.coalesce(F.col("_level"), F.lit(i))
                )
            continue

        valid_dims = [d for d in level_dims if d in factors_df.columns and d in result.columns]
        if len(valid_dims) != len(level_dims):
            continue

        lf = factors_df.groupBy(valid_dims).agg(F.avg("cost_factor").alias("_lf"))

        result = (
            result.join(lf, valid_dims, "left")
            .withColumn("_factor", F.coalesce(F.col("_factor"), F.col("_lf")))
            .withColumn("_level", F.coalesce(F.col("_level"), F.when(F.col("_lf").isNotNull(), F.lit(i))))
            .drop("_lf")
        )

    result = result.withColumn("forecast_cost", F.col("gmv") * F.coalesce(F.col("_factor"), F.lit(0.0)))
    return result

# COMMAND ----------

def evaluate_strategy(name, immature_df, actuals_df, factors_df, fallback_chain, factor_cols):
    """Run fallback simulation and print summary."""
    forecasted = simulate_fallback(immature_df, factors_df, fallback_chain, factor_cols)

    level_summary = (
        forecasted
        .groupBy("_level")
        .agg(
            F.count("*").alias("carts"),
            F.sum("gmv").alias("gmv"),
        )
        .orderBy("_level")
    )

    joined = (
        forecasted.alias("f")
        .join(actuals_df.alias("a"), F.col("f.shopping_cart_id") == F.col("a.shopping_cart_id"), "inner")
        .select(
            F.col("f.shopping_cart_id"),
            F.col("f.forecast_cost"),
            F.col("a.total_payment_costs").alias("actual_cost"),
            F.col("f._level"),
        )
    )

    overall = joined.agg(
        F.sum("forecast_cost").alias("total_forecast"),
        F.sum("actual_cost").alias("total_actual"),
        F.count("*").alias("n"),
    ).collect()[0]

    total_f = overall["total_forecast"]
    total_a = overall["total_actual"]
    dev = abs(total_f - total_a) / abs(total_a) if total_a else 0

    print(f"\n{'='*60}")
    print(f"  Strategy: {name}")
    print(f"{'='*60}")
    print(f"  Total forecast: {total_f:,.2f}")
    print(f"  Total actual:   {total_a:,.2f}")
    print(f"  Overall deviation: {dev*100:.3f}%")
    print(f"  Carts matched:  {overall['n']:,}")
    print()
    print("  Carts per fallback level:")
    display(level_summary)

    per_level_dev = (
        joined
        .groupBy("_level")
        .agg(
            F.count("*").alias("carts"),
            F.sum("forecast_cost").alias("level_forecast"),
            F.sum("actual_cost").alias("level_actual"),
        )
        .withColumn("deviation",
            F.abs(F.col("level_forecast") - F.col("level_actual"))
            / F.abs(F.col("level_actual"))
        )
        .orderBy("_level")
    )
    print("  Deviation per fallback level:")
    display(per_level_dev)

    return dev

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Strategy A — Current: drop from right
# MAGIC
# MAGIC `[rnpl_segment, processor, method, currency, country_bucket]`
# MAGIC Fallback drops rightmost dimension first.

# COMMAND ----------

chain_a = [FACTOR_COLS[:i] for i in range(len(FACTOR_COLS), -1, -1)]
print("Fallback chain A:")
for i, lvl in enumerate(chain_a):
    print(f"  Level {i}: {lvl if lvl else '[global]'}")

dev_a = evaluate_strategy("A: Current (drop-right)", immature, actuals, factors_df, chain_a, FACTOR_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Strategy B — Proposed order (from Section 4b analysis)
# MAGIC
# MAGIC Reorder factors by independent information value:
# MAGIC `rnpl_segment → payment_method_detail → currency → payment_processor → country_bucket`
# MAGIC
# MAGIC Rationale:
# MAGIC - `payment_method_detail` has widest rate spread (~10x), kept before processor
# MAGIC - `currency` captures most geographic cost variation, kept before processor
# MAGIC - `payment_processor` has moderate independent value above currency
# MAGIC - `country_bucket` is most redundant with currency, dropped first

# COMMAND ----------

PROPOSED_COLS = ["rnpl_segment", "payment_method_detail", "currency", "payment_processor", "country_bucket"]

chain_b = [PROPOSED_COLS[:i] for i in range(len(PROPOSED_COLS), -1, -1)]
print("Fallback chain B (proposed order):")
for i, lvl in enumerate(chain_b):
    print(f"  Level {i}: {lvl if lvl else '[global]'}")

dev_b = evaluate_strategy("B: Proposed order (method→currency→processor→country)", immature, actuals, factors_df, chain_b, PROPOSED_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Summary comparison

# COMMAND ----------

print(f"{'Strategy':<60s}  {'Deviation':>10s}")
print("-" * 73)
print(f"{'A: Current order (rnpl→proc→method→curr→country)':<60s}  {dev_a*100:>9.3f}%")
print(f"{'B: Proposed order (rnpl→method→curr→proc→country)':<60s}  {dev_b*100:>9.3f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 7: Recommendations
# MAGIC
# MAGIC ### Proposed factor column order (for `config.py`):
# MAGIC
# MAGIC ```
# MAGIC factor_columns = [
# MAGIC     "rnpl_segment",           # 1 — fundamental behavioral split
# MAGIC     "payment_method_detail",   # 2 — widest rate spread (~10x)
# MAGIC     "currency",               # 3 — strong independent differentiator
# MAGIC     "payment_processor",       # 4 — moderate independent value
# MAGIC     "country_bucket",          # 5 — most redundant with currency
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC ### Key findings:
# MAGIC 1. **Country vs currency redundancy** (Section 4b): for ~70% of carts,
# MAGIC    currency captures nearly all country-level variation (< 0.1pp diff).
# MAGIC    Country only adds meaningful value for EUR-zone countries.
# MAGIC 2. **Payment method before processor**: method has wider rate spread and
# MAGIC    is less redundant. Dropping processor while keeping method preserves
# MAGIC    the critical card network distinction (VISA vs AMEX = 2x rate diff).
# MAGIC 3. **Strategy comparison** (Section 6): compare deviation of current
# MAGIC    vs proposed order to confirm the reorder improves or maintains accuracy.
# MAGIC
# MAGIC ### Other `config.py` updates:
# MAGIC - **`country_top_n`**: keep at 20 — EUR-zone countries benefit from it
# MAGIC - **`payment_method_detail`**: if Section 4 shows grouping rare variants
# MAGIC   doesn't hurt, consider adding a threshold in `bucketing.py`
