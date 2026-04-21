# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Discovery Analysis
# MAGIC
# MAGIC Exploratory analysis to determine optimal parameters for the payment costs
# MAGIC forecasting model. Run this once (or whenever the data landscape changes)
# MAGIC and use the findings to update `config.py`.
# MAGIC
# MAGIC **Table of contents:**
# MAGIC 1. Data Profiling — understand the shape of the data
# MAGIC 2. Factor Selection — which attributes explain cost rate variation?
# MAGIC 3. Non-RNPL Maturation Curves — optimal anchor, cutoff, window
# MAGIC 4. RNPL Analysis — anchor comparison, cancellation recovery, window

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 1: Data Profiling
# MAGIC
# MAGIC Understand the shape of `testing.analytics.payment_costs_data`.
# MAGIC
# MAGIC **Key questions:**
# MAGIC - Cart distribution: attempts per cart, bookings per cart
# MAGIC - Segment sizes for each candidate factor (long tail?)
# MAGIC - Cost rate distributions overall and by dimension
# MAGIC - Cost type decomposition: which components drive variation?
# MAGIC - Table 2 coverage: how much cost data has daily granularity?
# MAGIC - Estimated vs precise acquirer fees

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Setup

# COMMAND ----------

SNAPSHOT_TABLE = "testing.analytics.payment_costs_data"

CANDIDATE_FACTORS = [
    "is_rnpl",
    "payment_processor",
    "payment_method",
    "payment_method_variant",
    "currency",
    "country_code",
    "country_group",
]

COST_TYPE_COLS = [
    "acquirer_fee_amount_eur",
    "interchange_fee_amount_eur",
    "scheme_fee_amount_eur",
]

CURRENT_MODEL_FACTORS = ["country_group", "is_rnpl"]

MIN_SEGMENT_SIZE = 5

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from itertools import combinations
import datetime

df = spark.table(SNAPSHOT_TABLE)
total_rows = df.count()
print(f"Total rows (shopping carts): {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Basic statistics

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Cart composition: attempts and bookings

# COMMAND ----------

display(
    df.groupBy("cnt_payment_attempts")
    .agg(
        F.count("*").alias("num_carts"),
        F.round(F.count("*") / total_rows * 100, 2).alias("pct"),
    )
    .orderBy("cnt_payment_attempts")
)

# COMMAND ----------

single_attempt_pct = (
    df.filter(F.col("cnt_payment_attempts") == 1).count() / total_rows * 100
)
print(f"Carts with exactly 1 payment attempt: {single_attempt_pct:.1f}%")

# COMMAND ----------

display(
    df.groupBy("cnt_bookings")
    .agg(
        F.count("*").alias("num_carts"),
        F.round(F.count("*") / total_rows * 100, 2).alias("pct"),
    )
    .orderBy("cnt_bookings")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Null / missing value analysis

# COMMAND ----------

null_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]
null_counts = df.select(null_exprs).toPandas().T
null_counts.columns = ["null_count"]
null_counts["pct"] = (null_counts["null_count"] / total_rows * 100).round(2)
display(spark.createDataFrame(
    null_counts.reset_index().rename(columns={"index": "column"})
).orderBy(F.desc("pct")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Segment size analysis per candidate factor
# MAGIC
# MAGIC For each factor, how many distinct values? How many segments < 30 carts?

# COMMAND ----------

for col_name in CANDIDATE_FACTORS:
    if col_name not in df.columns:
        print(f"Skipping {col_name} — not in table")
        continue

    seg = (
        df.groupBy(col_name)
        .agg(
            F.count("*").alias("cart_count"),
            F.sum("gmv").alias("total_gmv"),
        )
        .orderBy(F.desc("cart_count"))
    )
    n_segments = seg.count()
    small_segments = seg.filter(F.col("cart_count") < MIN_SEGMENT_SIZE).count()

    print(f"\n{'='*60}")
    print(f"  {col_name}: {n_segments} distinct values, {small_segments} below {MIN_SEGMENT_SIZE} carts")
    print(f"{'='*60}")
    display(seg.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Long tail analysis: country_code, payment_method, payment_processor
# MAGIC
# MAGIC Cumulative volume share — how many top values capture 90%/95%/99%?

# COMMAND ----------

for col_name in ["country_code", "payment_method", "payment_processor"]:
    if col_name not in df.columns:
        continue

    seg = (
        df.groupBy(col_name)
        .agg(F.count("*").alias("cart_count"))
        .orderBy(F.desc("cart_count"))
    )
    total = seg.agg(F.sum("cart_count")).collect()[0][0]

    w = Window.orderBy(F.desc("cart_count")).rowsBetween(Window.unboundedPreceding, 0)
    cumulative = (
        seg.withColumn("cumulative_count", F.sum("cart_count").over(w))
        .withColumn("cumulative_pct", F.round(F.col("cumulative_count") / total * 100, 2))
        .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("cart_count"))))
    )

    print(f"\n--- {col_name}: cumulative volume share ---")
    display(cumulative)

    for threshold in [90, 95, 99]:
        n = cumulative.filter(F.col("cumulative_pct") >= threshold).agg(
            F.min("rank")
        ).collect()[0][0]
        print(f"  Top {n} values of {col_name} capture {threshold}% of volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Cost rate distribution — overall

# COMMAND ----------

cost_df = df.filter(F.col("gmv") > 0).filter(F.col("total_payment_costs") != 0).withColumn(
    "cost_rate", F.abs(F.col("total_payment_costs")) / F.col("gmv")
)

display(
    cost_df.select(
        F.count("*").alias("n"),
        F.mean("cost_rate").alias("mean"),
        F.expr("percentile_approx(cost_rate, 0.25)").alias("p25"),
        F.expr("percentile_approx(cost_rate, 0.5)").alias("median"),
        F.expr("percentile_approx(cost_rate, 0.75)").alias("p75"),
        F.expr("percentile_approx(cost_rate, 0.95)").alias("p95"),
        F.stddev("cost_rate").alias("std_dev"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Cost rate by each candidate factor

# COMMAND ----------

for col_name in CANDIDATE_FACTORS:
    if col_name not in cost_df.columns:
        continue

    print(f"\n--- Cost rate by {col_name} ---")
    agg = (
        cost_df.groupBy(col_name)
        .agg(
            F.count("*").alias("cart_count"),
            F.sum("gmv").alias("total_gmv"),
            F.sum("total_payment_costs").alias("total_cost"),
            F.mean("cost_rate").alias("avg_cost_rate"),
            F.expr("percentile_approx(cost_rate, 0.5)").alias("median_cost_rate"),
            F.stddev("cost_rate").alias("std_cost_rate"),
        )
        .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
        .orderBy(F.desc("total_gmv"))
    )
    display(agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Cost type decomposition by factor
# MAGIC
# MAGIC Which cost components (acquirer, interchange, scheme) drive variation?

# COMMAND ----------

for factor in ["payment_processor", "country_group", "payment_method", "is_rnpl"]:
    if factor not in df.columns:
        continue

    print(f"\n--- Cost type breakdown by {factor} ---")
    agg = (
        df.filter(F.col("gmv") > 0)
        .groupBy(factor)
        .agg(
            F.count("*").alias("cart_count"),
            F.sum("gmv").alias("total_gmv"),
            F.sum(F.abs("total_payment_costs")).alias("total_cost"),
            *[F.sum(F.abs(c)).alias(c) for c in COST_TYPE_COLS],
        )
        .withColumn("cost_pct_of_gmv", F.round(F.col("total_cost") / F.col("total_gmv") * 100, 4))
        .withColumn("acquirer_pct", F.round(F.col("acquirer_fee_amount_eur") / F.col("total_cost") * 100, 1))
        .withColumn("interchange_pct", F.round(F.col("interchange_fee_amount_eur") / F.col("total_cost") * 100, 1))
        .withColumn("scheme_pct", F.round(F.col("scheme_fee_amount_eur") / F.col("total_cost") * 100, 1))
        .orderBy(F.desc("total_gmv"))
    )
    display(agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Estimated vs precise acquirer fees

# COMMAND ----------

totals = df.agg(
    F.sum(F.abs("precise_acquirer_fee_amount_eur")).alias("precise"),
    F.sum(F.abs("estimated_acquirer_fee_amount_eur")).alias("estimated"),
    F.sum(F.abs("acquirer_fee_amount_eur")).alias("total_acquirer"),
    F.sum(F.abs("total_payment_costs")).alias("total_costs"),
).collect()[0]

print(f"Precise acquirer fees:   {totals['precise']:,.2f} EUR")
print(f"Estimated acquirer fees: {totals['estimated']:,.2f} EUR")
print(f"Total acquirer fees:     {totals['total_acquirer']:,.2f} EUR")
print(f"Estimated share of acquirer: {totals['estimated'] / max(totals['total_acquirer'], 1) * 100:.1f}%")
print(f"Estimated share of total costs: {totals['estimated'] / max(totals['total_costs'], 1) * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.10 Table 2 coverage check
# MAGIC
# MAGIC What fraction of Table 1 costs is covered by daily rows in Table 2?

# COMMAND ----------

costs_daily = spark.sql("""
SELECT
    regexp_extract(pc.payment_reference, '^SC([0-9]+)', 1) AS shopping_cart_id,
    SUM(CASE WHEN amount_type_l0='FEE' THEN payout_amount_value / COALESCE(ex.exchange_rate, 1) ELSE 0 END) AS daily_fee_eur
FROM production.payments.int_all_psp_transactions_payment_costs pc
LEFT JOIN production.dwh.dim_currency c ON c.iso_code = pc.payout_currency
LEFT JOIN production.dwh.dim_exchange_rate_eur_daily ex ON ex.currency_id = c.currency_id
    AND pc.processing_timestamp::date = ex.update_timestamp
GROUP BY 1
""")

coverage = (
    df.select("shopping_cart_id", "total_payment_costs")
    .join(costs_daily, "shopping_cart_id", "left")
    .agg(
        F.sum("total_payment_costs").alias("table1_total"),
        F.sum("daily_fee_eur").alias("table2_total"),
    )
    .collect()[0]
)

t1 = abs(coverage["table1_total"] or 0)
t2 = abs(coverage["table2_total"] or 0)
print(f"Table 1 total costs: {t1:,.2f} EUR (abs)")
print(f"Table 2 daily costs: {t2:,.2f} EUR (abs)")
print(f"Coverage: {t2 / max(t1, 1) * 100:.1f}%")
print(f"Gap: {(t1 - t2):,.2f} EUR ({(t1 - t2) / max(t1, 1) * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 2: Factor Selection
# MAGIC
# MAGIC Validate the required factor set and confirm that it improves on the current model.
# MAGIC
# MAGIC **Required factors** (domain-driven):
# MAGIC - `is_rnpl`
# MAGIC - `payment_processor`
# MAGIC - `payment_method_detail` (variant for cards/wallets, method for rest)
# MAGIC - `currency`
# MAGIC - `country_bucket` (top N country_codes + country_group for rest)
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Create derived columns (`payment_method_detail`, `country_bucket`)
# MAGIC 2. Measure R² for each single factor and for the full required set
# MAGIC 3. Compare against current 2-factor baseline (country_group + is_rnpl)
# MAGIC 4. Test whether optional extras (cnt_payment_attempts, cnt_bookings) add value
# MAGIC 5. Tune country_bucket top-N threshold
# MAGIC
# MAGIC **Interpreting R²:** R² here measures cart-level variance explained.
# MAGIC Low absolute values (10-15%) are expected — individual carts have high
# MAGIC natural variance from fixed fee components, interchange tiers, etc.
# MAGIC What matters is **relative R²** (which set explains more?) and **aggregate
# MAGIC forecast accuracy** (tested in Parts 3 & 4).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Setup — derive columns

# COMMAND ----------

CARD_METHODS = ("payment_card", "apple_pay", "google_pay")
COUNTRY_TOP_N = 10  # UPDATE after reviewing section 2.5

data = (
    df.filter(F.col("gmv") > 0)
    .filter(F.col("total_payment_costs") != 0)
    .withColumn("cost_rate", F.abs(F.col("total_payment_costs")) / F.col("gmv"))
    .withColumn(
        "payment_method_detail",
        F.when(
            F.col("payment_method").isin(*CARD_METHODS)
            & F.col("payment_method_variant").isNotNull(),
            F.col("payment_method_variant"),
        ).otherwise(F.col("payment_method")),
    )
)

total_count = data.count()
global_stats = data.agg(
    F.mean("cost_rate").alias("global_mean"),
    F.variance("cost_rate").alias("global_var"),
).collect()[0]

global_mean = global_stats["global_mean"]
global_var = global_stats["global_var"]

print(f"Carts with valid cost_rate: {total_count:,}")
print(f"Global mean cost rate: {global_mean:.6f}")
print(f"Global variance: {global_var:.10f}")

# COMMAND ----------

def compute_r_squared(df, factor_cols, global_mean, global_var, total_n):
    """Between-group variance ratio (R-squared analog)."""
    grouped = df.groupBy(factor_cols).agg(
        F.count("*").alias("n"),
        F.mean("cost_rate").alias("group_mean"),
    )
    stats = grouped.agg(
        F.count("*").alias("num_segments"),
        F.min("n").alias("min_size"),
        F.expr("percentile_approx(n, 0.5)").alias("median_size"),
        F.max("n").alias("max_size"),
        F.sum(F.col("n") * F.pow(F.col("group_mean") - global_mean, 2)).alias("weighted_ss"),
    ).collect()[0]

    between_var = stats["weighted_ss"] / total_n
    r2 = between_var / global_var if global_var > 0 else 0.0

    return {
        "factors": ", ".join(factor_cols) if isinstance(factor_cols, list) else factor_cols,
        "r_squared": float(r2),
        "num_segments": int(stats["num_segments"]),
        "min_size": int(stats["min_size"]),
        "median_size": int(stats["median_size"]),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify payment_method_detail distribution

# COMMAND ----------

pmd_dist = (
    data.groupBy("payment_method_detail")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("gmv").alias("total_gmv"),
        F.sum(F.abs("total_payment_costs")).alias("total_cost"),
    )
    .withColumn("cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .orderBy(F.desc("cart_count"))
)
display(pmd_dist)
print(f"\nDistinct payment_method_detail values: {pmd_dist.count()}")

# Show how it maps from payment_method + variant
display(
    data.groupBy("payment_method", "payment_method_variant", "payment_method_detail")
    .agg(F.count("*").alias("n"))
    .orderBy(F.desc("n"))
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Card network vs entry point: should we group Visa card + Visa Apple Pay?
# MAGIC
# MAGIC `payment_method_detail` groups all Visa transactions together regardless of
# MAGIC entry point: `payment_card+visa`, `applepay+visa`, `googlepay+visa` → all become `"visa"`.
# MAGIC
# MAGIC **Question:** Is this valid? Do these entry points have similar cost rates
# MAGIC when the card network is the same?
# MAGIC
# MAGIC We test by comparing cost rates for:
# MAGIC - `visa` via payment_card vs `visa` via applepay vs `visa` via googlepay
# MAGIC - Same for mastercard, amex, etc.
# MAGIC
# MAGIC If within-network spread is small, grouping is justified.

# COMMAND ----------

cards_only = data.filter(F.col("payment_method").isin(*CARD_METHODS))

print("--- Cost rate by payment_method (entry point) for cards/wallets ---")
print("    Shows: payment_card vs applepay vs googlepay (ignoring network)")
print()
display(
    cards_only.groupBy("payment_method")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("gmv").alias("total_gmv"),
        F.sum(F.abs("total_payment_costs")).alias("total_cost"),
        F.mean("cost_rate").alias("avg_cost_rate"),
        F.expr("percentile_approx(cost_rate, 0.5)").alias("median_cost_rate"),
        F.stddev("cost_rate").alias("std_cost_rate"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .orderBy(F.desc("cart_count"))
)

# COMMAND ----------

print("--- Cost rate by card network (payment_method_variant) ---")
print("    Shows: visa vs mastercard vs amex (ignoring entry point)")
print()
display(
    cards_only.groupBy("payment_method_variant")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("gmv").alias("total_gmv"),
        F.sum(F.abs("total_payment_costs")).alias("total_cost"),
        F.mean("cost_rate").alias("avg_cost_rate"),
        F.expr("percentile_approx(cost_rate, 0.5)").alias("median_cost_rate"),
        F.stddev("cost_rate").alias("std_cost_rate"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .orderBy(F.desc("cart_count"))
)

# COMMAND ----------

print("--- Cost rate by network × entry point (full cross-tab) ---")
print("    KEY TABLE: Compare rows with same network but different payment_method.")
print("    E.g., is visa+payment_card ≈ visa+applepay ≈ visa+googlepay?")
print()
cross = (
    cards_only.groupBy("payment_method_variant", "payment_method")
    .agg(
        F.count("*").alias("cart_count"),
        F.sum("gmv").alias("total_gmv"),
        F.sum(F.abs("total_payment_costs")).alias("total_cost"),
        F.mean("cost_rate").alias("avg_cost_rate"),
    )
    .withColumn("agg_cost_rate", F.col("total_cost") / F.col("total_gmv"))
    .orderBy("payment_method_variant", "payment_method")
)
display(cross)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quantify: does entry point matter within the same network?
# MAGIC
# MAGIC For each card network, compute the within-network cost rate variance
# MAGIC between entry points. If low → safe to group by network alone.

# COMMAND ----------

within_network = (
    cards_only
    .filter(F.col("payment_method_variant").isNotNull())
    .groupBy("payment_method_variant", "payment_method")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.abs("total_payment_costs")).alias("cost"),
        F.sum("gmv").alias("gmv"),
    )
    .withColumn("rate", F.col("cost") / F.col("gmv"))
)

w = Window.partitionBy("payment_method_variant")
network_spread = (
    within_network
    .withColumn("network_avg_rate", F.sum("cost").over(w) / F.sum("gmv").over(w))
    .withColumn("rate_diff_pct", F.round((F.col("rate") - F.col("network_avg_rate")) / F.col("network_avg_rate") * 100, 2))
    .orderBy("payment_method_variant", "payment_method")
)

print("Rate difference from network average (%) — small values mean safe to group:")
print("  E.g., if visa+payment_card = 1.8% and visa+applepay = 1.9%, diff is ~5%")
print("  which is small → safe to group all visa together.")
print()
display(network_spread.select(
    "payment_method_variant", "payment_method", "n", "rate",
    "network_avg_rate", "rate_diff_pct",
))

max_spread = network_spread.agg(F.max(F.abs("rate_diff_pct"))).collect()[0][0]
avg_spread = network_spread.agg(F.avg(F.abs("rate_diff_pct"))).collect()[0][0]
print(f"\nMax within-network spread: {max_spread:.1f}%")
print(f"Avg within-network spread: {avg_spread:.1f}%")
if max_spread < 10:
    print("→ All entry points within 10% of their network average. Grouping by network is safe.")
elif max_spread < 20:
    print("→ Some spread, but grouping is likely still acceptable for forecast stability.")
else:
    print("→ Significant spread — consider keeping entry point separate for high-spread networks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### R² comparison: three grouping strategies
# MAGIC
# MAGIC | # | Strategy | Example values | What it tests |
# MAGIC |---|----------|---------------|---------------|
# MAGIC | 1 | `payment_method` | payment_card, applepay, paypal | Entry point only (ignores network) |
# MAGIC | 2 | `payment_method_detail` | visa, mastercard, amex, paypal | **Groups all visa together** regardless of entry point |
# MAGIC | 3 | `method × variant` cross | payment_card_visa, applepay_visa, ... | Keeps entry point separate within network |
# MAGIC
# MAGIC If R²(2) ≈ R²(3), then grouping by network is fine (entry point doesn't matter).
# MAGIC If R²(2) >> R²(1), then the network is what drives cost differences, not entry point.

# COMMAND ----------

data_with_cross = data.withColumn(
    "method_x_variant",
    F.when(
        F.col("payment_method").isin(*CARD_METHODS)
        & F.col("payment_method_variant").isNotNull(),
        F.concat_ws("_", F.col("payment_method"), F.col("payment_method_variant")),
    ).otherwise(F.col("payment_method")),
)

r2_method = compute_r_squared(data, ["payment_method"], global_mean, global_var, total_count)
r2_detail = compute_r_squared(data, ["payment_method_detail"], global_mean, global_var, total_count)
r2_cross = compute_r_squared(data_with_cross, ["method_x_variant"], global_mean, global_var, total_count)

print(f"1. payment_method (entry point only):          R²={r2_method['r_squared']:.4f}  segs={r2_method['num_segments']}")
print(f"2. payment_method_detail (network for cards):  R²={r2_detail['r_squared']:.4f}  segs={r2_detail['num_segments']}")
print(f"3. method × variant cross (most granular):     R²={r2_cross['r_squared']:.4f}  segs={r2_cross['num_segments']}")
print()

detail_vs_method = r2_detail['r_squared'] - r2_method['r_squared']
cross_vs_detail = r2_cross['r_squared'] - r2_detail['r_squared']

print(f"Network matters (2 vs 1): +{detail_vs_method:.4f} R² — "
      f"{'YES, network adds value' if detail_vs_method > 0.002 else 'no, similar'}")
print(f"Entry point matters (3 vs 2): +{cross_vs_detail:.4f} R² — "
      f"{'YES, entry point adds value' if cross_vs_detail > 0.002 else 'NO, safe to group by network'}")
print()
if cross_vs_detail < 0.002:
    print("CONCLUSION: Visa via card ≈ Visa via Apple Pay ≈ Visa via Google Pay.")
    print("  → Use payment_method_detail (group by network for cards/wallets).")
else:
    print(f"CONCLUSION: Entry point matters (adds {cross_vs_detail:.4f} R²).")
    print("  → Consider using method_x_variant for higher precision.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Single-factor R² — all candidates

# COMMAND ----------

ALL_CANDIDATES = [
    "is_rnpl",
    "payment_processor",
    "payment_method",
    "payment_method_variant",
    "payment_method_detail",
    "currency",
    "country_code",
    "country_group",
]

single_results = []
for factor in ALL_CANDIDATES:
    if factor not in data.columns:
        continue
    result = compute_r_squared(data, [factor], global_mean, global_var, total_count)
    single_results.append(result)

single_df = spark.createDataFrame(single_results)
display(single_df.orderBy(F.desc("r_squared")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Current model baseline: country_group + is_rnpl

# COMMAND ----------

baseline = compute_r_squared(data, CURRENT_MODEL_FACTORS, global_mean, global_var, total_count)
print(f"Current model (country_group + is_rnpl):")
print(f"  R² = {baseline['r_squared']:.4f}")
print(f"  Segments: {baseline['num_segments']}, min size: {baseline['min_size']}, median: {baseline['median_size']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Required factor set — cumulative R²
# MAGIC
# MAGIC Add required factors one by one to see each one's marginal contribution.

# COMMAND ----------

required_factors = [
    "is_rnpl",
    "payment_processor",
    "payment_method_detail",
    "currency",
    "country_code",
]

cumulative = []
current_r2 = 0.0
for i, factor in enumerate(required_factors):
    trial = required_factors[:i+1]
    result = compute_r_squared(data, trial, global_mean, global_var, total_count)
    marginal = result["r_squared"] - current_r2
    cumulative.append({
        "step": i + 1,
        "added_factor": factor,
        "cumulative_factors": ", ".join(trial),
        "r_squared": round(result["r_squared"], 6),
        "marginal_gain": round(marginal, 6),
        "num_segments": result["num_segments"],
        "median_size": result["median_size"],
        "min_size": result["min_size"],
    })
    current_r2 = result["r_squared"]
    print(f"Step {i+1}: +{factor:25s}  R²={result['r_squared']:.4f}  marginal={marginal:.4f}  "
          f"segs={result['num_segments']}  median={result['median_size']}")

display(spark.createDataFrame(cumulative))

full_r2 = current_r2
print(f"\nFull required set R²: {full_r2:.4f}")
print(f"Current model R²:     {baseline['r_squared']:.4f}")
print(f"Improvement:          +{full_r2 - baseline['r_squared']:.4f} "
      f"({(full_r2 - baseline['r_squared']) / max(baseline['r_squared'], 0.0001) * 100:.0f}% relative)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Optional factors test
# MAGIC
# MAGIC Do additional columns (cnt_payment_attempts, cnt_bookings) explain
# MAGIC meaningful additional variance beyond the required factors?

# COMMAND ----------

OPTIONAL_CANDIDATES = ["cnt_payment_attempts", "cnt_bookings"]

for opt in OPTIONAL_CANDIDATES:
    if opt not in data.columns:
        print(f"  {opt}: column not found, skipping")
        continue

    trial = required_factors + [opt]
    result = compute_r_squared(data, trial, global_mean, global_var, total_count)
    marginal = result["r_squared"] - full_r2
    print(f"  +{opt:25s}  R²={result['r_squared']:.4f}  marginal={marginal:.4f}  "
          f"segs={result['num_segments']}  median={result['median_size']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Country bucketing: top N + country_group for rest
# MAGIC
# MAGIC Test whether using `country_bucket` (top N individual codes + country_group
# MAGIC for rest) is better than raw country_code (too many segments) or
# MAGIC country_group alone (too coarse).

# COMMAND ----------

country_seg = (
    data.groupBy("country_code")
    .agg(F.count("*").alias("cart_count"))
    .orderBy(F.desc("cart_count"))
)

raw_r2 = compute_r_squared(data, ["country_code"], global_mean, global_var, total_count)
group_r2 = compute_r_squared(data, ["country_group"], global_mean, global_var, total_count)
print(f"country_code (raw):  R²={raw_r2['r_squared']:.4f}  segs={raw_r2['num_segments']}")
print(f"country_group:       R²={group_r2['r_squared']:.4f}  segs={group_r2['num_segments']}")
print()

for top_n in [5, 10, 15, 20, 30]:
    top_countries = [r["country_code"] for r in country_seg.limit(top_n).collect()]

    bucketed = data.withColumn(
        "country_bucket",
        F.when(F.col("country_code").isin(top_countries), F.col("country_code"))
        .otherwise(F.col("country_group")),
    )

    hybrid = compute_r_squared(bucketed, ["country_bucket"], global_mean, global_var, total_count)
    print(f"Top {top_n:>2} + country_group: R²={hybrid['r_squared']:.4f}  "
          f"segs={hybrid['num_segments']}  median={hybrid['median_size']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Full factor set with country_bucket

# COMMAND ----------

top_countries = [r["country_code"] for r in country_seg.limit(COUNTRY_TOP_N).collect()]
bucketed_data = data.withColumn(
    "country_bucket",
    F.when(F.col("country_code").isin(top_countries), F.col("country_code"))
    .otherwise(F.col("country_group")),
)

FINAL_FACTORS = [
    "is_rnpl",
    "payment_processor",
    "payment_method_detail",
    "currency",
    "country_bucket",
]

final_result = compute_r_squared(bucketed_data, FINAL_FACTORS, global_mean, global_var, total_count)

print("=" * 60)
print("CONFIRMED FACTOR SET")
print("=" * 60)
print(f"Factors: {FINAL_FACTORS}")
print(f"R²: {final_result['r_squared']:.4f}")
print(f"Segments: {final_result['num_segments']}, min: {final_result['min_size']}, median: {final_result['median_size']}")
print()
print(f"Current model (country_group + is_rnpl): R²={baseline['r_squared']:.4f}")
improvement = final_result['r_squared'] - baseline['r_squared']
print(f"Improvement: +{improvement:.4f} ({improvement / max(baseline['r_squared'], 0.0001) * 100:.0f}% relative)")
print()
print(f"Country bucket: top {COUNTRY_TOP_N} codes + country_group for rest")
print(f"Payment method: variant for cards/wallets, method for rest")
print()
print("These factors are already set in config.py.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 3: Maturation Curves (Non-RNPL)
# MAGIC
# MAGIC How quickly do non-RNPL costs settle? What maturity cutoff and window
# MAGIC width are optimal? Should factors use `date_of_checkout` or `last_date_of_travel`?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.0 Setup

# COMMAND ----------

snapshot = spark.table(SNAPSHOT_TABLE)

non_rnpl = snapshot.filter(
    (F.col("is_rnpl") == False)
    & (F.col("gmv") > 0)
    & (F.col("total_payment_costs") != 0)
    & (F.datediff(F.current_date(), F.col("date_of_checkout")) > 60)
)

print(f"Non-RNPL carts with >60 days since checkout: {non_rnpl.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Table 2 coverage check (non-RNPL)

# COMMAND ----------

costs_daily_raw = spark.sql("""
SELECT
    regexp_extract(pc.payment_reference, '^SC([0-9]+)', 1) AS shopping_cart_id,
    processing_timestamp::date AS processing_date,
    SUM(CASE WHEN amount_type_l0='FEE' THEN payout_amount_value / COALESCE(ex.exchange_rate, 1) ELSE 0 END) AS daily_fee_eur
FROM production.payments.int_all_psp_transactions_payment_costs pc
LEFT JOIN production.dwh.dim_currency c ON c.iso_code = pc.payout_currency
LEFT JOIN production.dwh.dim_exchange_rate_eur_daily ex ON ex.currency_id = c.currency_id
    AND pc.processing_timestamp::date = ex.update_timestamp
GROUP BY 1, 2
""")

costs_daily_agg = costs_daily_raw.groupBy("shopping_cart_id").agg(
    F.sum("daily_fee_eur").alias("table2_total")
)

coverage = (
    non_rnpl.select("shopping_cart_id", "total_payment_costs")
    .join(costs_daily_agg, "shopping_cart_id", "left")
    .agg(
        F.count("*").alias("carts"),
        F.sum(F.when(F.col("table2_total").isNotNull(), 1).otherwise(0)).alias("carts_in_table2"),
        F.sum("total_payment_costs").alias("t1_total"),
        F.sum("table2_total").alias("t2_total"),
    )
    .collect()[0]
)

t1_abs = abs(coverage['t1_total'] or 0)
t2_abs = abs(coverage['t2_total'] or 0)
print(f"Non-RNPL carts: {coverage['carts']:,}")
print(f"  with Table 2 data: {coverage['carts_in_table2']:,} ({coverage['carts_in_table2']/coverage['carts']*100:.1f}%)")
print(f"  Table 1 costs: {t1_abs:,.2f} EUR (abs)")
print(f"  Table 2 costs: {t2_abs:,.2f} EUR (abs)")
print(f"  Coverage: {t2_abs / max(t1_abs, 1) * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Non-RNPL maturation curve — days since checkout

# COMMAND ----------

non_rnpl_with_daily = (
    non_rnpl.select("shopping_cart_id", "date_of_checkout", "total_payment_costs")
    .join(costs_daily_raw, "shopping_cart_id", "inner")
    .withColumn("days_since_checkout", F.datediff(F.col("processing_date"), F.col("date_of_checkout")))
)

maturation_checkout = (
    non_rnpl_with_daily
    .groupBy("days_since_checkout")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee = maturation_checkout.agg(F.sum("fee_in_bucket")).collect()[0][0]

w_cum = Window.orderBy("days_since_checkout").rowsBetween(Window.unboundedPreceding, 0)
maturation_curve_checkout = (
    maturation_checkout
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(w_cum))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / total_fee * 100, 2))
    .orderBy("days_since_checkout")
)

print("Non-RNPL maturation curve — anchored to date_of_checkout:")
display(maturation_curve_checkout.filter(F.col("days_since_checkout").between(-5, 60)))

# COMMAND ----------

for pct in [90, 95, 99, 99.9]:
    row = maturation_curve_checkout.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct}% settled by day {row['days_since_checkout']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Non-RNPL maturation curve — days since last_date_of_travel

# COMMAND ----------

non_rnpl_travel = (
    non_rnpl.select("shopping_cart_id", "last_date_of_travel", "total_payment_costs")
    .filter(F.col("last_date_of_travel").isNotNull())
    .join(costs_daily_raw, "shopping_cart_id", "inner")
    .withColumn("days_since_travel", F.datediff(F.col("processing_date"), F.col("last_date_of_travel")))
)

maturation_travel = (
    non_rnpl_travel
    .groupBy("days_since_travel")
    .agg(F.sum(F.abs("daily_fee_eur")).alias("fee_in_bucket"))
)

total_fee_travel = maturation_travel.agg(F.sum("fee_in_bucket")).collect()[0][0]

maturation_curve_travel = (
    maturation_travel
    .withColumn("cumulative_fee", F.sum("fee_in_bucket").over(
        Window.orderBy("days_since_travel").rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("pct_settled", F.round(F.col("cumulative_fee") / total_fee_travel * 100, 2))
    .orderBy("days_since_travel")
)

print("Non-RNPL maturation curve — anchored to last_date_of_travel:")
display(maturation_curve_travel.filter(F.col("days_since_travel").between(-30, 60)))

# COMMAND ----------

for pct in [90, 95, 99, 99.9]:
    row = maturation_curve_travel.filter(F.col("pct_settled") >= pct).first()
    if row:
        print(f"  {pct}% settled by day {row['days_since_travel']} (travel anchor)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Anchor comparison: checkout vs travel

# COMMAND ----------

compare_data = (
    maturation_curve_checkout.select(
        F.col("days_since_checkout").alias("day_offset"),
        F.col("pct_settled").alias("checkout_settled"),
    )
    .join(
        maturation_curve_travel.select(
            F.col("days_since_travel").alias("day_offset"),
            F.col("pct_settled").alias("travel_settled"),
        ),
        "day_offset",
        "outer",
    )
    .orderBy("day_offset")
    .filter(F.col("day_offset").between(0, 45))
)

display(compare_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Maturity cutoff test

# COMMAND ----------

def compute_weekly_factors(df, anchor_col, maturity_days, window_days, factor_cols):
    """Compute weekly factor values for stability analysis."""
    return (
        df.withColumn("anchor", F.col(anchor_col))
        .filter(F.col("anchor").isNotNull())
        .withColumn("week", F.date_trunc("week", F.col("anchor")))
        .groupBy(["week"] + factor_cols)
        .agg(
            F.sum(F.abs("total_payment_costs")).alias("total_cost"),
            F.sum("gmv").alias("total_gmv"),
            F.count("*").alias("cart_count"),
        )
        .withColumn("factor", F.col("total_cost") / F.col("total_gmv"))
        .filter(F.col("total_gmv") > 0)
    )

# COMMAND ----------

anchor = "date_of_checkout"
factor_cols = ["country_group", "is_rnpl"]

for maturity_days in [7, 10, 14, 21, 28]:
    mature = non_rnpl.filter(
        F.datediff(F.current_date(), F.col(anchor)) > maturity_days
    )

    weekly = compute_weekly_factors(mature, anchor, maturity_days, 14, factor_cols)

    stability = (
        weekly.groupBy(factor_cols)
        .agg(
            F.count("*").alias("num_weeks"),
            F.mean("factor").alias("mean_factor"),
            F.stddev("factor").alias("std_factor"),
        )
        .withColumn("cv", F.col("std_factor") / F.col("mean_factor"))
        .agg(
            F.mean("cv").alias("avg_cv"),
            F.expr("percentile_approx(cv, 0.5)").alias("median_cv"),
            F.max("cv").alias("max_cv"),
        )
        .collect()[0]
    )

    print(f"Cutoff T-{maturity_days:>2}: avg_CV={stability['avg_cv']:.4f}  "
          f"median_CV={stability['median_cv']:.4f}  max_CV={stability['max_cv']:.4f}  "
          f"carts={mature.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Window width test

# COMMAND ----------

as_of_dates = [
    datetime.date(2025, 12, 1) - datetime.timedelta(weeks=w)
    for w in range(12)
]

maturity = 14

for window_days in [7, 14, 21, 28]:
    factors_over_time = []

    for as_of in as_of_dates:
        window_start = as_of - datetime.timedelta(days=maturity + window_days)
        window_end = as_of - datetime.timedelta(days=maturity)

        window_data = non_rnpl.filter(
            (F.col("date_of_checkout") >= window_start)
            & (F.col("date_of_checkout") < window_end)
        )

        factor_val = window_data.agg(
            (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("factor")
        ).collect()[0]["factor"]

        if factor_val is not None:
            factors_over_time.append({
                "as_of": str(as_of),
                "window_days": window_days,
                "factor": float(factor_val),
            })

    if factors_over_time:
        factors_df = spark.createDataFrame(factors_over_time)
        stats = factors_df.agg(
            F.mean("factor").alias("mean"),
            F.stddev("factor").alias("std"),
        ).collect()[0]
        cv = stats["std"] / stats["mean"] if stats["mean"] > 0 else None
        print(f"Window {window_days:>2} days: mean_factor={stats['mean']:.6f}  "
              f"std={stats['std']:.6f}  CV={cv:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 End-to-end factor comparison (single date)
# MAGIC
# MAGIC Travel anchor vs checkout anchor for non-RNPL factor computation.

# COMMAND ----------

as_of = datetime.date(2025, 11, 1)
maturity = 14
window_days = 14

mature_window_start = as_of - datetime.timedelta(days=maturity + window_days)
mature_window_end = as_of - datetime.timedelta(days=maturity)

factor_cols = ["country_group"]

mature_travel = (
    non_rnpl.filter(F.col("last_date_of_travel").isNotNull())
    .filter(
        (F.col("last_date_of_travel") >= mature_window_start)
        & (F.col("last_date_of_travel") < mature_window_end)
    )
)

factors_travel = (
    mature_travel.groupBy(factor_cols)
    .agg(
        (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("factor_travel"),
        F.count("*").alias("n_travel"),
    )
)

mature_checkout = non_rnpl.filter(
    (F.col("date_of_checkout") >= mature_window_start)
    & (F.col("date_of_checkout") < mature_window_end)
)

factors_checkout = (
    mature_checkout.groupBy(factor_cols)
    .agg(
        (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("factor_checkout"),
        F.count("*").alias("n_checkout"),
    )
)

immature = non_rnpl.filter(
    (F.col("date_of_checkout") >= (as_of - datetime.timedelta(days=maturity)))
    & (F.col("date_of_checkout") < as_of)
)

comparison = (
    immature.join(factors_travel, factor_cols, "left")
    .join(factors_checkout, factor_cols, "left")
    .withColumn("forecast_travel", F.col("gmv") * F.col("factor_travel"))
    .withColumn("forecast_checkout", F.col("gmv") * F.col("factor_checkout"))
    .agg(
        F.count("*").alias("immature_carts"),
        F.sum(F.abs("total_payment_costs")).alias("actual"),
        F.sum("forecast_travel").alias("fc_travel"),
        F.sum("forecast_checkout").alias("fc_checkout"),
    )
    .collect()[0]
)

actual = comparison["actual"]
print(f"Immature non-RNPL carts: {comparison['immature_carts']:,}")
print(f"Actual costs: {actual:,.2f}")
print(f"Forecast (travel anchor): {comparison['fc_travel']:,.2f}  "
      f"deviation: {abs(comparison['fc_travel'] - actual) / max(actual, 1) * 100:.2f}%")
print(f"Forecast (checkout anchor): {comparison['fc_checkout']:,.2f}  "
      f"deviation: {abs(comparison['fc_checkout'] - actual) / max(actual, 1) * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 End-to-end over multiple as_of dates

# COMMAND ----------

results_e2e = []
for weeks_back in range(4, 20):
    as_of = datetime.date(2025, 12, 1) - datetime.timedelta(weeks=weeks_back)

    mature_start = as_of - datetime.timedelta(days=maturity + window_days)
    mature_end = as_of - datetime.timedelta(days=maturity)

    ft = (
        non_rnpl.filter(F.col("last_date_of_travel").isNotNull())
        .filter((F.col("last_date_of_travel") >= mature_start) & (F.col("last_date_of_travel") < mature_end))
        .groupBy(factor_cols)
        .agg((F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("f_travel"))
    )
    fc = (
        non_rnpl.filter((F.col("date_of_checkout") >= mature_start) & (F.col("date_of_checkout") < mature_end))
        .groupBy(factor_cols)
        .agg((F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("f_checkout"))
    )

    immature = non_rnpl.filter(
        (F.col("date_of_checkout") >= (as_of - datetime.timedelta(days=maturity)))
        & (F.col("date_of_checkout") < as_of)
    )
    if immature.count() == 0:
        continue

    row = (
        immature.join(ft, factor_cols, "left").join(fc, factor_cols, "left")
        .withColumn("fc_t", F.col("gmv") * F.col("f_travel"))
        .withColumn("fc_c", F.col("gmv") * F.col("f_checkout"))
        .agg(
            F.sum(F.abs("total_payment_costs")).alias("actual"),
            F.sum("fc_t").alias("forecast_travel"),
            F.sum("fc_c").alias("forecast_checkout"),
        )
        .collect()[0]
    )

    if row["actual"] and row["actual"] > 0:
        results_e2e.append({
            "as_of": str(as_of),
            "actual": float(row["actual"]),
            "dev_travel_pct": abs(row["forecast_travel"] - row["actual"]) / row["actual"] * 100
                if row["forecast_travel"] else None,
            "dev_checkout_pct": abs(row["forecast_checkout"] - row["actual"]) / row["actual"] * 100
                if row["forecast_checkout"] else None,
        })

display(spark.createDataFrame(results_e2e))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 4: RNPL-Specific Analysis
# MAGIC
# MAGIC Determine optimal anchor, maturity cutoff, window width, and
# MAGIC cancellation recovery factor for RNPL carts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.0 Setup

# COMMAND ----------

rnpl = snapshot.filter(
    (F.col("is_rnpl") == True)
    & (F.col("gmv") > 0)
    & (F.col("total_payment_costs") != 0)
)

rnpl_old = rnpl.filter(F.datediff(F.current_date(), F.col("date_of_checkout")) > 90)

print(f"Total RNPL carts: {rnpl.count():,}")
print(f"RNPL carts >90 days old: {rnpl_old.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 RNPL overview

# COMMAND ----------

display(
    rnpl.agg(
        F.count("*").alias("total_carts"),
        F.sum("gmv").alias("total_gmv"),
        F.sum(F.abs("total_payment_costs")).alias("total_costs_abs"),
        (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("overall_cost_rate"),
        F.sum(F.when(F.col("last_date_of_cancelation").isNotNull(), 1).otherwise(0)).alias("cancelled_carts"),
        F.mean(F.datediff(F.col("last_date_of_travel"), F.col("date_of_checkout"))).alias("avg_days_checkout_to_travel"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cancellation rate

# COMMAND ----------

cancel_stats = (
    rnpl.withColumn(
        "is_cancelled",
        F.when(F.col("last_date_of_cancelation").isNotNull(), True).otherwise(False),
    )
    .groupBy("is_cancelled")
    .agg(
        F.count("*").alias("carts"),
        F.sum("gmv").alias("gmv"),
        F.sum(F.abs("total_payment_costs")).alias("costs"),
        (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("cost_rate"),
    )
)
display(cancel_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cancellation rate by segment

# COMMAND ----------

for dim in ["country_group", "payment_processor"]:
    if dim not in rnpl.columns:
        continue

    print(f"\n--- Cancellation rate by {dim} ---")
    display(
        rnpl.withColumn(
            "is_cancelled",
            F.when(F.col("last_date_of_cancelation").isNotNull(), 1).otherwise(0),
        )
        .groupBy(dim)
        .agg(
            F.count("*").alias("carts"),
            F.sum("is_cancelled").alias("cancelled"),
            F.round(F.mean("is_cancelled") * 100, 1).alias("cancel_rate_pct"),
            F.sum("gmv").alias("gmv"),
            F.sum(F.abs("total_payment_costs")).alias("costs"),
            (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("cost_rate"),
        )
        .orderBy(F.desc("carts"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Cancellation recovery factor (k)

# COMMAND ----------

rnpl_with_cancel = rnpl_old.withColumn(
    "is_cancelled",
    F.when(F.col("last_date_of_cancelation").isNotNull(), True).otherwise(False),
)

recovery = (
    rnpl_with_cancel.groupBy("is_cancelled")
    .agg(
        F.count("*").alias("carts"),
        F.sum(F.abs("total_payment_costs")).alias("costs"),
        F.sum("gmv").alias("gmv"),
    )
    .withColumn("cost_rate", F.col("costs") / F.col("gmv"))
    .collect()
)

rates = {r["is_cancelled"]: r["cost_rate"] for r in recovery}
completed_rate = rates.get(False, 0)
cancelled_rate = rates.get(True, 0)

if completed_rate > 0:
    recovery_k = 1.0 - (cancelled_rate / completed_rate)
    print(f"Completed RNPL cost rate: {completed_rate:.6f}")
    print(f"Cancelled RNPL cost rate: {cancelled_rate:.6f}")
    print(f"Recovery ratio (k): {recovery_k:.2f}")
    print(f"  Cancellation recovers ~{recovery_k*100:.0f}% of costs")
else:
    print("No completed RNPL carts found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery by segment

# COMMAND ----------

for dim in ["country_group", "payment_processor"]:
    if dim not in rnpl_old.columns:
        continue

    print(f"\n--- Recovery ratio by {dim} ---")
    seg = (
        rnpl_with_cancel.groupBy(dim, "is_cancelled")
        .agg(
            F.sum(F.abs("total_payment_costs")).alias("costs"),
            F.sum("gmv").alias("gmv"),
        )
        .withColumn("cost_rate", F.col("costs") / F.col("gmv"))
    )

    completed = seg.filter(~F.col("is_cancelled")).select(
        dim, F.col("cost_rate").alias("rate_completed")
    )
    cancelled = seg.filter(F.col("is_cancelled")).select(
        dim, F.col("cost_rate").alias("rate_cancelled")
    )

    display(
        completed.join(cancelled, dim, "outer")
        .withColumn("recovery_k", 1.0 - F.col("rate_cancelled") / F.col("rate_completed"))
        .orderBy(dim)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 RNPL maturation curves — three anchor comparison
# MAGIC
# MAGIC Anchored to: `last_date_of_travel`, `rnpl_scheduled_payment_date`, `date_of_checkout`.

# COMMAND ----------

rnpl_with_daily = (
    rnpl_old.select(
        "shopping_cart_id", "date_of_checkout", "last_date_of_travel",
        "rnpl_scheduled_payment_date", "total_payment_costs",
    )
    .join(costs_daily_raw, "shopping_cart_id", "inner")
)

print(f"RNPL carts with daily cost data: {rnpl_with_daily.select('shopping_cart_id').distinct().count():,}")

# COMMAND ----------

anchors = {
    "checkout": "date_of_checkout",
    "travel": "last_date_of_travel",
    "rnpl_payment": "rnpl_scheduled_payment_date",
}

curves = {}
for label, anchor_col in anchors.items():
    filtered = rnpl_with_daily.filter(F.col(anchor_col).isNotNull())

    by_day = (
        filtered.withColumn(
            "day_offset", F.datediff(F.col("processing_date"), F.col(anchor_col))
        )
        .groupBy("day_offset")
        .agg(F.sum(F.abs("daily_fee_eur")).alias("fee"))
    )

    total = by_day.agg(F.sum("fee")).collect()[0][0]
    if total is None or total == 0:
        print(f"Skipping {label}: no fee data")
        continue

    w_c = Window.orderBy("day_offset").rowsBetween(Window.unboundedPreceding, 0)
    curve = (
        by_day.withColumn("cumulative", F.sum("fee").over(w_c))
        .withColumn("pct_settled", F.round(F.col("cumulative") / total * 100, 2))
        .withColumn("anchor", F.lit(label))
    )
    curves[label] = curve

    print(f"\n--- RNPL maturation: {label} ({anchor_col}) ---")
    for pct in [90, 95, 99, 99.9]:
        row = curve.filter(F.col("pct_settled") >= pct).first()
        if row:
            print(f"  {pct}% settled by day {row['day_offset']}")

# COMMAND ----------

if curves:
    all_curves = None
    for label, curve in curves.items():
        subset = curve.select("day_offset", "pct_settled", "anchor").filter(
            F.col("day_offset").between(-10, 90)
        )
        all_curves = subset if all_curves is None else all_curves.unionByName(subset)

    display(all_curves.orderBy("anchor", "day_offset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 RNPL maturity cutoff test

# COMMAND ----------

best_anchor = "last_date_of_travel"  # UPDATE after reviewing section 4.3

factor_cols_rnpl = ["country_group"]

for maturity_days in [14, 21, 30, 45, 60]:
    mature = rnpl.filter(
        F.col(best_anchor).isNotNull()
    ).filter(
        F.datediff(F.current_date(), F.col(best_anchor)) > maturity_days
    )

    if mature.count() < 100:
        print(f"Cutoff T-{maturity_days}: too few carts ({mature.count()}), skipping")
        continue

    weekly = (
        mature.withColumn("week", F.date_trunc("week", F.col(best_anchor)))
        .groupBy(["week"] + factor_cols_rnpl)
        .agg(
            (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("factor"),
            F.count("*").alias("n"),
        )
        .filter(F.col("n") >= 5)
    )

    stability = (
        weekly.groupBy(factor_cols_rnpl)
        .agg(
            F.mean("factor").alias("mean_f"),
            F.stddev("factor").alias("std_f"),
        )
        .withColumn("cv", F.col("std_f") / F.col("mean_f"))
        .agg(F.mean("cv").alias("avg_cv"), F.max("cv").alias("max_cv"))
        .collect()[0]
    )

    print(f"RNPL cutoff T-{maturity_days:>2}: avg_CV={stability['avg_cv']:.4f}  "
          f"max_CV={stability['max_cv']:.4f}  carts={mature.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Window width test for RNPL

# COMMAND ----------

rnpl_maturity = 45  # UPDATE after section 4.4

for window_days in [14, 21, 28, 42]:
    factors_over_time = []

    for weeks_back in range(4, 16):
        as_of = datetime.date(2025, 12, 1) - datetime.timedelta(weeks=weeks_back)
        w_start = as_of - datetime.timedelta(days=rnpl_maturity + window_days)
        w_end = as_of - datetime.timedelta(days=rnpl_maturity)

        window_data = rnpl.filter(
            F.col(best_anchor).isNotNull()
        ).filter(
            (F.col(best_anchor) >= w_start) & (F.col(best_anchor) < w_end)
        )

        factor_val = window_data.agg(
            (F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("factor")
        ).collect()[0]["factor"]

        if factor_val is not None:
            factors_over_time.append(float(factor_val))

    if len(factors_over_time) >= 3:
        import numpy as np
        arr = np.array(factors_over_time)
        cv = arr.std() / arr.mean() if arr.mean() > 0 else None
        print(f"RNPL window {window_days:>2} days: mean={arr.mean():.6f}  std={arr.std():.6f}  CV={cv:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 End-to-end RNPL factor comparison (travel vs checkout anchor)

# COMMAND ----------

results_rnpl = []

for weeks_back in range(8, 20):
    as_of = datetime.date(2025, 12, 1) - datetime.timedelta(weeks=weeks_back)
    rnpl_maturity = 45
    window_days = 28

    w_start = as_of - datetime.timedelta(days=rnpl_maturity + window_days)
    w_end = as_of - datetime.timedelta(days=rnpl_maturity)

    factor_cols_rnpl = ["country_group"]

    ft = (
        rnpl.filter(F.col("last_date_of_travel").isNotNull())
        .filter((F.col("last_date_of_travel") >= w_start) & (F.col("last_date_of_travel") < w_end))
        .groupBy(factor_cols_rnpl)
        .agg((F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("f_travel"))
    )

    fc = (
        rnpl.filter((F.col("date_of_checkout") >= w_start) & (F.col("date_of_checkout") < w_end))
        .groupBy(factor_cols_rnpl)
        .agg((F.sum(F.abs("total_payment_costs")) / F.sum("gmv")).alias("f_checkout"))
    )

    immature = rnpl.filter(
        F.col("last_date_of_travel").isNotNull()
    ).filter(
        (F.col("last_date_of_travel") >= (as_of - datetime.timedelta(days=rnpl_maturity)))
        & (F.col("last_date_of_travel") < as_of)
    )

    if immature.count() == 0:
        continue

    row = (
        immature.join(ft, factor_cols_rnpl, "left").join(fc, factor_cols_rnpl, "left")
        .withColumn("fc_t", F.col("gmv") * F.col("f_travel"))
        .withColumn("fc_c", F.col("gmv") * F.col("f_checkout"))
        .agg(
            F.sum(F.abs("total_payment_costs")).alias("actual"),
            F.sum("fc_t").alias("forecast_travel"),
            F.sum("fc_c").alias("forecast_checkout"),
        )
        .collect()[0]
    )

    if row["actual"] and row["actual"] > 0:
        results_rnpl.append({
            "as_of": str(as_of),
            "actual": float(row["actual"]),
            "dev_travel_pct": abs(row["forecast_travel"] - row["actual"]) / row["actual"] * 100
                if row["forecast_travel"] else None,
            "dev_checkout_pct": abs(row["forecast_checkout"] - row["actual"]) / row["actual"] * 100
                if row["forecast_checkout"] else None,
        })

if results_rnpl:
    display(spark.createDataFrame(results_rnpl))
    import numpy as np
    dev_t = [r["dev_travel_pct"] for r in results_rnpl if r["dev_travel_pct"] is not None]
    dev_c = [r["dev_checkout_pct"] for r in results_rnpl if r["dev_checkout_pct"] is not None]
    print(f"Avg deviation (travel anchor): {np.mean(dev_t):.2f}%")
    print(f"Avg deviation (checkout anchor): {np.mean(dev_c):.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Should cancelled RNPL be a separate segment?

# COMMAND ----------

rnpl_labelled = rnpl.withColumn(
    "rnpl_status",
    F.when(F.col("last_date_of_cancelation").isNotNull(), F.lit("cancelled"))
    .otherwise(F.lit("completed")),
)

pooled = (
    rnpl_labelled.groupBy("country_group")
    .agg((F.sum("total_payment_costs") / F.sum("gmv")).alias("factor_pooled"))
)

split = (
    rnpl_labelled.groupBy("country_group", "rnpl_status")
    .agg((F.sum("total_payment_costs") / F.sum("gmv")).alias("factor_split"))
)

comparison = (
    rnpl_labelled.join(pooled, "country_group", "left")
    .join(split, ["country_group", "rnpl_status"], "left")
    .withColumn("forecast_pooled", F.col("gmv") * F.col("factor_pooled"))
    .withColumn("forecast_split", F.col("gmv") * F.col("factor_split"))
    .groupBy("rnpl_status")
    .agg(
        F.count("*").alias("carts"),
        F.sum("total_payment_costs").alias("actual"),
        F.sum("forecast_pooled").alias("fc_pooled"),
        F.sum("forecast_split").alias("fc_split"),
    )
    .withColumn("dev_pooled_pct", F.abs(F.col("fc_pooled") - F.col("actual")) / F.col("actual") * 100)
    .withColumn("dev_split_pct", F.abs(F.col("fc_split") - F.col("actual")) / F.col("actual") * 100)
)

display(comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary — Findings to update in config.py
# MAGIC
# MAGIC **Fill in after running all parts:**
# MAGIC
# MAGIC ### Data profiling
# MAGIC - Single-attempt carts: ___%
# MAGIC - Table 2 coverage: ___%
# MAGIC - Estimated share of total costs: ___%
# MAGIC
# MAGIC ### Factor selection
# MAGIC - Recommended factors: ___
# MAGIC - R² improvement over baseline: +___
# MAGIC - Payment method: top ___ + 'Other'
# MAGIC - Country: top ___ individually + country_group for rest (or just country_group)
# MAGIC
# MAGIC ### Non-RNPL maturation
# MAGIC - Costs 95% settled by day ___ (checkout anchor)
# MAGIC - Better anchor: checkout / travel
# MAGIC - Recommended maturity cutoff: ___ days
# MAGIC - Recommended window width: ___ days
# MAGIC - Average deviation (checkout anchor): ___% vs travel anchor: ___%
# MAGIC
# MAGIC ### RNPL
# MAGIC - Best RNPL anchor: ___
# MAGIC - Recommended RNPL maturity cutoff: ___ days
# MAGIC - Recommended RNPL window width: ___ days
# MAGIC - Cancellation recovery k: ___
# MAGIC - Treat cancelled RNPL separately: yes / no
