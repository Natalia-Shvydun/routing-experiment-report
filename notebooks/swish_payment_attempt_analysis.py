# Databricks notebook source
# MAGIC %md
# MAGIC # Swish Payment Method — Impact on Payment Attempts & Success Rate
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-swish`
# MAGIC
# MAGIC **Question:** Why does enabling Swish (a local APM in Sweden) lead to an increase
# MAGIC in the number of payment attempts and a decline in payment attempt success rate?
# MAGIC
# MAGIC **Hypotheses under investigation:**
# MAGIC
# MAGIC | # | Hypothesis | Mechanism |
# MAGIC |---|-----------|-----------|
# MAGIC | H1 | **Method-switching / fallback** | Customers try Swish first, fail or abandon, then retry with a different payment method (e.g. credit card). This inflates attempts and dilutes the success rate. |
# MAGIC | H2 | **Swish inherent retry rate** | Swish itself has a lower first-attempt success rate or requires multiple attempts (e.g. app-switch timeouts, BankID issues), inflating attempts per visitor. |
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction — assignments, payment options loaded, payment attempts
# MAGIC 2. Top-level experiment metrics — attempts, success rates, conversion
# MAGIC 3. Payment method mix — what methods are used in each group
# MAGIC 4. H1 investigation — method-switching & fallback patterns
# MAGIC 5. H2 investigation — Swish-specific attempt & success analysis
# MAGIC 6. Counterfactual — metrics after excluding Swish attempts
# MAGIC 7. Summary & conclusions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from scipy.stats import norm

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]
A_COLOR, B_COLOR = COLORS[0], COLORS[1]

EXPERIMENT_ID = "pay-payment-orchestration-swish"
START_DATE = "2026-02-19"

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test. Returns (z, p_value, delta_pp, ci_lo_pp, ci_hi_pp)."""
    p1, p2 = s1 / n1, s2 / n2
    delta = p1 - p2
    p_pool = (s1 + s2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return 0.0, 1.0, 0.0, 0.0, 0.0
    z = delta / se
    p_val = 2 * (1 - norm.cdf(abs(z)))
    ci_half = 1.96 * se
    return z, p_val, delta * 100, (delta - ci_half) * 100, (delta + ci_half) * 100

def sig_label(p):
    if p < 0.001: return "***"
    if p < 0.01:  return "**"
    if p < 0.05:  return "*"
    return ""

def print_z_test(label, n_a, s_a, n_b, s_b, group_a="control", group_b="test"):
    z, p, d, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
    print(f"  {label}:  {group_a}={s_a/n_a:.2%}  {group_b}={s_b/n_b:.2%}  "
          f"Δ={d:+.2f}pp  95%CI=[{ci_lo:+.2f}, {ci_hi:+.2f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Assignment & payment-options-loaded (visitor-level base)
# MAGIC
# MAGIC Uses the first query provided — one row per visitor with their first
# MAGIC payment-options-loaded event after assignment.

# COMMAND ----------

df_base = spark.sql(f"""
WITH filtered_customers AS (
  SELECT visitor_id
  FROM production.dwh.dim_customer c
  JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
  WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
),
assignment AS (
  SELECT
    e.group_name,
    e.user_id AS visitor_id,
    COALESCE(
      e.user_dimensions:custom.visitorPlatform,
      e.user_dimensions:visitorPlatform,
      e.user_dimensions:platform
    ) AS platform,
    e.user_dimensions:custom.countryCode AS user_country_code,
    e.user_dimensions:custom.currency AS user_currency,
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{START_DATE}'
    AND experiment_id = '{EXPERIMENT_ID}'
),
payment_methods_loaded AS (
  SELECT
    user.visitor_id AS visitor_id,
    group_name,
    assigned_at,
    a.platform,
    a.user_country_code,
    a.user_currency,
    from_json(json_event:payment_methods_list, 'ARRAY<STRING>') AS payment_methods_list,
    json_event:payment_terms_preselected AS payment_terms_preselected,
    json_event:payment_method_preselected AS payment_method_preselected,
    array_size(from_json(json_event:payment_methods_list, 'ARRAY<STRING>')) AS cnt_payment_methods,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'applepay') THEN 1 ELSE 0 END AS if_applepay,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'paypal') THEN 1 ELSE 0 END AS if_paypal,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'klarna') THEN 1 ELSE 0 END AS if_klarna,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'creditcard') THEN 1 ELSE 0 END AS if_creditcard,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'googlepay') THEN 1 ELSE 0 END AS if_googlepay,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'swish') THEN 1 ELSE 0 END AS if_swish,
    event_properties.timestamp AS timestamp,
    json_event:cart_hash,
    shopping_cart_id
  FROM assignment a
  JOIN production.events.events e
    ON a.visitor_id = user.visitor_id
    AND event_properties.timestamp > a.assigned_at - INTERVAL 120 SECONDS
  LEFT JOIN production.db_mirror_dbz.gyg__shopping_cart sc
    ON json_event:cart_hash = sc.hash_code
    AND sc.date_of_creation::date >= '{START_DATE}'
    AND user.visitor_id = sc.visitor_id
  WHERE event_name IN ('TravelerWebPaymentOptionsLoaded', 'MobileAppPaymentOptionsLoaded')
    AND e.date >= '{START_DATE}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp) = 1
)
SELECT * FROM payment_methods_loaded
""")
df_base.cache()

base_counts = df_base.groupBy("group_name").count().toPandas()
print("Visitors with payment options loaded (deduplicated):")
for _, row in base_counts.iterrows():
    print(f"  {row['group_name']}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Payment attempt data (attempt-level)
# MAGIC
# MAGIC Granular attempt data joined back to assignments — one row per payment attempt.

# COMMAND ----------

df_base.createOrReplaceTempView("payment_methods_loaded")

df_attempts = spark.sql(f"""
WITH filtered_customers AS (
  SELECT visitor_id
  FROM production.dwh.dim_customer c
  JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
  WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
),
assignment AS (
  SELECT
    e.group_name,
    e.user_id AS visitor_id,
    COALESCE(
      e.user_dimensions:custom.visitorPlatform,
      e.user_dimensions:visitorPlatform,
      e.user_dimensions:platform
    ) AS platform,
    e.user_dimensions:custom.countryCode AS user_country_code,
    e.user_dimensions:custom.currency AS user_currency,
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{START_DATE}'
    AND experiment_id = '{EXPERIMENT_ID}'
),
payment_attempt_data AS (
  SELECT
    a.group_name,
    a.platform,
    a.visitor_id,
    a.assigned_at,
    COALESCE(p.payment_attempt_id, p.payment_attempt_id_primer) AS attempt_id,
    p.payment_method,
    p.payment_processor,
    p.payment_attempt_status,
    p.if_payment_attempt_initiated,
    p.if_shopping_cart_finally_settled,
    p.payment_attempt_creation_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY a.visitor_id
      ORDER BY p.payment_attempt_creation_timestamp
    ) AS attempt_seq
  FROM assignment a
  JOIN testing.analytics.payment_attempts_data_model p
    ON a.visitor_id = p.visitor_id
  WHERE p.payment_attempt_creation_timestamp::date >= '{START_DATE}'
)
SELECT * FROM payment_attempt_data
""")
df_attempts.cache()

attempt_counts = df_attempts.groupBy("group_name").count().toPandas()
print("Total payment attempts:")
for _, row in attempt_counts.iterrows():
    print(f"  {row['group_name']}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Top-Level Experiment Metrics
# MAGIC
# MAGIC High-level comparison: visitors, attempts, success rates, booking rates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Visitor-level summary

# COMMAND ----------

visitor_metrics = (
    df_attempts
    .groupBy("group_name", "visitor_id")
    .agg(
        F.count("attempt_id").alias("total_attempts"),
        F.countDistinct("attempt_id").alias("distinct_attempts"),
        F.sum(F.when(F.col("if_payment_attempt_initiated") == 1, 1).otherwise(0)).alias("initiated_attempts"),
        F.sum(F.when(F.lower("payment_attempt_status").isin("settled", "authorized"), 1).otherwise(0)).alias("succeeded_attempts"),
        F.max("if_shopping_cart_finally_settled").alias("cart_settled"),
        F.countDistinct("payment_method").alias("distinct_methods_used"),
        F.max(F.when(F.lower("payment_method").contains("swish"), 1).otherwise(0)).alias("used_swish"),
        F.collect_set("payment_method").alias("methods_used_set"),
    )
    .withColumn("is_converted", (F.col("succeeded_attempts") > 0).cast("int"))
    .withColumn("has_multiple_methods", (F.col("distinct_methods_used") > 1).cast("int"))
)
visitor_metrics.cache()

visitor_summary = (
    visitor_metrics
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("total_attempts").alias("total_attempts"),
        F.mean("total_attempts").alias("avg_attempts_per_visitor"),
        F.expr("percentile_approx(total_attempts, 0.5)").alias("median_attempts"),
        F.sum("succeeded_attempts").alias("total_succeeded"),
        F.sum("is_converted").alias("converted_visitors"),
        F.sum("used_swish").alias("visitors_used_swish"),
        F.sum("has_multiple_methods").alias("visitors_multi_method"),
        F.sum("distinct_methods_used").alias("total_method_switches"),
    )
    .withColumn("conversion_rate", F.col("converted_visitors") / F.col("visitors"))
    .withColumn("attempt_success_rate", F.col("total_succeeded") / F.col("total_attempts"))
    .withColumn("pct_used_swish", F.col("visitors_used_swish") / F.col("visitors"))
    .withColumn("pct_multi_method", F.col("visitors_multi_method") / F.col("visitors"))
    .toPandas()
)

print("=" * 90)
print("TOP-LEVEL VISITOR SUMMARY")
print("=" * 90)
for _, r in visitor_summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Unique visitors              : {r['visitors']:,.0f}")
    print(f"    Total payment attempts       : {r['total_attempts']:,.0f}")
    print(f"    Avg attempts / visitor       : {r['avg_attempts_per_visitor']:.3f}")
    print(f"    Median attempts / visitor    : {r['median_attempts']:.0f}")
    print(f"    Total succeeded attempts     : {r['total_succeeded']:,.0f}")
    print(f"    Attempt success rate         : {r['attempt_success_rate']:.2%}")
    print(f"    Converted visitors           : {r['converted_visitors']:,.0f}  ({r['conversion_rate']:.2%})")
    print(f"    Visitors who used Swish      : {r['visitors_used_swish']:,.0f}  ({r['pct_used_swish']:.2%})")
    print(f"    Visitors with >1 method      : {r['visitors_multi_method']:,.0f}  ({r['pct_multi_method']:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Statistical tests — key metrics

# COMMAND ----------

sorted_groups = sorted(visitor_summary["group_name"].unique())
if len(sorted_groups) == 2:
    ra = visitor_summary[visitor_summary["group_name"] == sorted_groups[0]].iloc[0]
    rb = visitor_summary[visitor_summary["group_name"] == sorted_groups[1]].iloc[0]

    print("Statistical significance tests (two-proportion z-test):\n")
    print_z_test("Visitor conversion rate",
                 ra["visitors"], ra["converted_visitors"],
                 rb["visitors"], rb["converted_visitors"],
                 sorted_groups[0], sorted_groups[1])
    print_z_test("Attempt success rate",
                 ra["total_attempts"], ra["total_succeeded"],
                 rb["total_attempts"], rb["total_succeeded"],
                 sorted_groups[0], sorted_groups[1])
    print_z_test("Multi-method visitor rate",
                 ra["visitors"], ra["visitors_multi_method"],
                 rb["visitors"], rb["visitors_multi_method"],
                 sorted_groups[0], sorted_groups[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Visual comparison of key metrics

# COMMAND ----------

fig, axes = plt.subplots(1, 4, figsize=(22, 5))
metrics = [
    ("avg_attempts_per_visitor", "Avg Attempts / Visitor", False),
    ("attempt_success_rate", "Attempt Success Rate", True),
    ("conversion_rate", "Visitor Conversion Rate", True),
    ("pct_multi_method", "% Visitors with >1 Method", True),
]

for i, (col, title, is_pct) in enumerate(metrics):
    vals = visitor_summary.sort_values("group_name")
    colors = [A_COLOR if idx == 0 else B_COLOR for idx in range(len(vals))]
    bars = axes[i].bar(vals["group_name"], vals[col], color=colors, width=0.5)
    axes[i].set_title(title, fontweight="bold", fontsize=11)
    if is_pct:
        axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, vals[col]):
        fmt = f"{val:.2%}" if is_pct else f"{val:.3f}"
        axes[i].text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + bar.get_height() * 0.01,
                     fmt, ha="center", va="bottom", fontsize=11, fontweight="bold")

fig.suptitle("Swish Experiment — Top-Level Metrics", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Payment Method Mix
# MAGIC
# MAGIC What payment methods are being attempted in each group?
# MAGIC Swish should appear only/mostly in the treatment group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Attempts by payment method × group

# COMMAND ----------

method_mix = (
    df_attempts
    .groupBy("group_name", "payment_method")
    .agg(
        F.count("attempt_id").alias("attempts"),
        F.countDistinct("visitor_id").alias("visitors"),
        F.sum(F.when(F.lower("payment_attempt_status").isin("settled", "authorized"), 1).otherwise(0)).alias("succeeded"),
    )
    .withColumn("success_rate", F.col("succeeded") / F.col("attempts"))
    .toPandas()
)

for g in sorted(method_mix["group_name"].unique()):
    subset = method_mix[method_mix["group_name"] == g].sort_values("attempts", ascending=False)
    total = subset["attempts"].sum()
    print(f"\n{'='*70}")
    print(f"  {g} — Payment Method Distribution")
    print(f"{'='*70}")
    print(f"  {'Method':<30} {'Attempts':>10} {'Share':>8} {'Visitors':>10} {'Success':>10}")
    print(f"  {'-'*68}")
    for _, r in subset.iterrows():
        pct = r["attempts"] / total
        print(f"  {r['payment_method']:<30} {r['attempts']:>10,.0f} {pct:>8.1%} {r['visitors']:>10,.0f} {r['success_rate']:>10.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Swish share visualization

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

for idx, g in enumerate(sorted(method_mix["group_name"].unique())):
    subset = method_mix[method_mix["group_name"] == g].sort_values("attempts", ascending=True)
    bars = axes[idx].barh(subset["payment_method"], subset["attempts"], color=COLORS[idx])
    axes[idx].set_title(f"{g}", fontweight="bold", fontsize=12)
    axes[idx].set_xlabel("Number of Attempts")
    for bar, val in zip(bars, subset["attempts"]):
        axes[idx].text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                       f"{val:,.0f}", va="center", fontsize=9)

fig.suptitle("Payment Attempts by Method", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Success rate by payment method × group

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 6))
groups = sorted(method_mix["group_name"].unique())
methods = sorted(method_mix["payment_method"].unique())
x = np.arange(len(methods))
width = 0.35

for idx, g in enumerate(groups):
    subset = method_mix[method_mix["group_name"] == g].set_index("payment_method")
    rates = [subset.loc[m, "success_rate"] if m in subset.index else 0 for m in methods]
    offset = -width / 2 + idx * width
    bars = ax.bar(x + offset, rates, width, label=g, color=COLORS[idx])
    for bar, val in zip(bars, rates):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005,
                    f"{val:.1%}", ha="center", va="bottom", fontsize=8)

ax.set_xticks(x)
ax.set_xticklabels(methods, rotation=45, ha="right")
ax.set_ylabel("Success Rate")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
ax.set_title("Attempt Success Rate by Payment Method", fontweight="bold", fontsize=13)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — H1: Method-Switching / Fallback Behavior
# MAGIC
# MAGIC **Hypothesis:** Customers in the treatment group try Swish first, fail, and then
# MAGIC fall back to another method (e.g. credit card). This creates *additional* failed
# MAGIC attempts that inflate the total attempt count and drag down the success rate.
# MAGIC
# MAGIC **Analyses:**
# MAGIC - 4.1 Multi-method visitor rates by group
# MAGIC - 4.2 Swish → fallback sequences
# MAGIC - 4.3 Conversion rate of Swish-then-fallback visitors vs others

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Multi-method visitor rates

# COMMAND ----------

multi_method_summary = (
    visitor_metrics
    .groupBy("group_name")
    .agg(
        F.count("*").alias("total_visitors"),
        F.sum("has_multiple_methods").alias("multi_method_visitors"),
        F.mean("distinct_methods_used").alias("avg_methods_per_visitor"),
    )
    .withColumn("multi_method_rate", F.col("multi_method_visitors") / F.col("total_visitors"))
    .toPandas()
)

print("Multi-method visitor rates:")
for _, r in multi_method_summary.iterrows():
    print(f"  {r['group_name']}:  {r['multi_method_visitors']:,.0f} / {r['total_visitors']:,.0f}"
          f"  = {r['multi_method_rate']:.2%}   (avg {r['avg_methods_per_visitor']:.2f} methods/visitor)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Swish → fallback sequences (treatment only)
# MAGIC
# MAGIC For visitors who used Swish, identify the sequence pattern:
# MAGIC - Swish only (no fallback)
# MAGIC - Swish first → then another method
# MAGIC - Another method first → then Swish
# MAGIC - Mixed / interleaved

# COMMAND ----------

swish_sequences = (
    df_attempts
    .withColumn("is_swish", F.when(F.lower("payment_method").contains("swish"), 1).otherwise(0))
    .withColumn("swish_succeeded",
                F.when(
                    (F.lower("payment_method").contains("swish")) &
                    (F.lower("payment_attempt_status").isin("settled", "authorized")),
                    1
                ).otherwise(0))
    .groupBy("group_name", "visitor_id")
    .agg(
        F.count("attempt_id").alias("total_attempts"),
        F.sum("is_swish").alias("swish_attempts"),
        F.sum(F.when(F.col("is_swish") == 0, 1).otherwise(0)).alias("non_swish_attempts"),
        F.max("is_swish").alias("ever_used_swish"),
        F.sum("swish_succeeded").alias("swish_successes"),
        F.sum(F.when(
            (F.col("is_swish") == 0) &
            (F.lower("payment_attempt_status").isin("settled", "authorized")),
            1
        ).otherwise(0)).alias("non_swish_successes"),
        F.min(F.when(F.col("is_swish") == 1, F.col("attempt_seq"))).alias("first_swish_seq"),
        F.min(F.when(F.col("is_swish") == 0, F.col("attempt_seq"))).alias("first_non_swish_seq"),
        F.max("if_shopping_cart_finally_settled").alias("cart_settled"),
    )
    .withColumn("visitor_pattern", F.when(
        F.col("ever_used_swish") == 0, "no_swish"
    ).when(
        (F.col("swish_attempts") > 0) & (F.col("non_swish_attempts") == 0), "swish_only"
    ).when(
        F.col("first_swish_seq") < F.col("first_non_swish_seq"), "swish_first_then_fallback"
    ).when(
        F.col("first_swish_seq") > F.col("first_non_swish_seq"), "other_first_then_swish"
    ).otherwise("mixed"))
)
swish_sequences.cache()

pattern_summary = (
    swish_sequences
    .groupBy("group_name", "visitor_pattern")
    .agg(
        F.count("*").alias("visitors"),
        F.mean("total_attempts").alias("avg_attempts"),
        F.mean("swish_attempts").alias("avg_swish_attempts"),
        F.sum((F.col("cart_settled") == 1).cast("int")).alias("converted"),
    )
    .withColumn("conversion_rate", F.col("converted") / F.col("visitors"))
    .toPandas()
)

print("=" * 100)
print("VISITOR PATTERNS — Swish usage sequences")
print("=" * 100)
for g in sorted(pattern_summary["group_name"].unique()):
    subset = pattern_summary[pattern_summary["group_name"] == g].sort_values("visitors", ascending=False)
    g_total = subset["visitors"].sum()
    print(f"\n  {g} (total: {g_total:,})")
    print(f"  {'Pattern':<30} {'Visitors':>10} {'Share':>8} {'Avg Att.':>10} {'Avg Swish':>10} {'Conv. Rate':>12}")
    print(f"  {'-'*80}")
    for _, r in subset.iterrows():
        pct = r["visitors"] / g_total
        print(f"  {r['visitor_pattern']:<30} {r['visitors']:>10,.0f} {pct:>8.1%}"
              f" {r['avg_attempts']:>10.2f} {r['avg_swish_attempts']:>10.2f}"
              f" {r['conversion_rate']:>12.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Visual — visitor pattern distribution

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

for idx, g in enumerate(sorted(pattern_summary["group_name"].unique())):
    subset = pattern_summary[pattern_summary["group_name"] == g].sort_values("visitors", ascending=True)
    g_total = subset["visitors"].sum()
    bars = axes[idx].barh(
        subset["visitor_pattern"],
        subset["visitors"] / g_total * 100,
        color=COLORS[idx]
    )
    axes[idx].set_title(f"{g}", fontweight="bold")
    axes[idx].set_xlabel("% of Visitors")
    for bar, (_, r) in zip(bars, subset.iterrows()):
        pct = r["visitors"] / g_total * 100
        axes[idx].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                       f"{pct:.1f}% ({r['visitors']:,.0f})", va="center", fontsize=9)

fig.suptitle("Visitor Patterns — Swish Usage Sequences", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Fallback method analysis
# MAGIC
# MAGIC For visitors who tried Swish first then fell back — what did they fall back to?
# MAGIC Did the fallback attempt succeed?

# COMMAND ----------

fallback_visitors = swish_sequences.filter(F.col("visitor_pattern") == "swish_first_then_fallback")

fallback_details = (
    df_attempts
    .join(
        fallback_visitors.select("visitor_id"),
        on="visitor_id",
        how="inner"
    )
    .filter(~F.lower("payment_method").contains("swish"))
    .groupBy("group_name", "payment_method")
    .agg(
        F.countDistinct("visitor_id").alias("visitors"),
        F.count("attempt_id").alias("attempts"),
        F.sum(F.when(F.lower("payment_attempt_status").isin("settled", "authorized"), 1).otherwise(0)).alias("succeeded"),
    )
    .withColumn("success_rate", F.col("succeeded") / F.col("attempts"))
    .toPandas()
)

if not fallback_details.empty:
    print("Fallback methods used after Swish failed:")
    for g in sorted(fallback_details["group_name"].unique()):
        subset = fallback_details[fallback_details["group_name"] == g].sort_values("visitors", ascending=False)
        print(f"\n  {g}:")
        print(f"  {'Fallback Method':<30} {'Visitors':>10} {'Attempts':>10} {'Succeeded':>10} {'Success %':>10}")
        print(f"  {'-'*70}")
        for _, r in subset.iterrows():
            print(f"  {r['payment_method']:<30} {r['visitors']:>10,.0f} {r['attempts']:>10,.0f}"
                  f" {r['succeeded']:>10,.0f} {r['success_rate']:>10.2%}")
else:
    print("No Swish→fallback visitors found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Key H1 metric — extra attempts from method-switching
# MAGIC
# MAGIC Quantify how many *additional* attempts come from the Swish→fallback pattern
# MAGIC compared to the control group.

# COMMAND ----------

extra_attempts = (
    swish_sequences
    .groupBy("group_name")
    .agg(
        F.sum("total_attempts").alias("total_attempts"),
        F.sum("swish_attempts").alias("swish_attempts"),
        F.sum("non_swish_attempts").alias("non_swish_attempts"),
        F.sum(F.when(F.col("visitor_pattern") == "swish_first_then_fallback",
                     F.col("total_attempts")).otherwise(0)).alias("attempts_from_switchers"),
        F.sum(F.when(F.col("visitor_pattern") == "swish_first_then_fallback",
                     F.col("swish_attempts")).otherwise(0)).alias("failed_swish_from_switchers"),
        F.count("*").alias("total_visitors"),
        F.sum(F.when(F.col("visitor_pattern") == "swish_first_then_fallback", 1).otherwise(0)).alias("switcher_visitors"),
    )
    .toPandas()
)

print("=" * 90)
print("ATTEMPT DECOMPOSITION — Method-Switching Impact")
print("=" * 90)
for _, r in extra_attempts.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Total attempts              : {r['total_attempts']:,.0f}")
    print(f"    ├─ Swish attempts           : {r['swish_attempts']:,.0f} ({r['swish_attempts']/r['total_attempts']:.1%})")
    print(f"    └─ Non-Swish attempts       : {r['non_swish_attempts']:,.0f} ({r['non_swish_attempts']/r['total_attempts']:.1%})")
    print(f"    Switcher visitors           : {r['switcher_visitors']:,.0f} ({r['switcher_visitors']/r['total_visitors']:.1%})")
    print(f"    Attempts from switchers     : {r['attempts_from_switchers']:,.0f} ({r['attempts_from_switchers']/r['total_attempts']:.1%})")
    print(f"    ├─ Failed Swish (wasted)    : {r['failed_swish_from_switchers']:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — H2: Swish Inherent Retry / Failure Rate
# MAGIC
# MAGIC **Hypothesis:** Swish has a lower first-attempt success rate or requires more
# MAGIC retries per visitor than other methods (e.g. due to BankID timeouts, app-switch
# MAGIC failures, network issues).
# MAGIC
# MAGIC **Analyses:**
# MAGIC - 5.1 Swish attempt-level success rate vs other methods
# MAGIC - 5.2 Attempts per visitor for Swish-only visitors vs non-Swish visitors
# MAGIC - 5.3 Swish failure reasons / statuses

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Swish success rate vs other methods (attempt-level)

# COMMAND ----------

method_success = (
    df_attempts
    .withColumn("method_group", F.when(
        F.lower("payment_method").contains("swish"), "swish"
    ).otherwise("other"))
    .groupBy("group_name", "method_group")
    .agg(
        F.count("attempt_id").alias("attempts"),
        F.countDistinct("visitor_id").alias("visitors"),
        F.sum(F.when(F.col("if_payment_attempt_initiated") == 1, 1).otherwise(0)).alias("initiated"),
        F.sum(F.when(F.lower("payment_attempt_status").isin("settled", "authorized"), 1).otherwise(0)).alias("succeeded"),
    )
    .withColumn("success_rate", F.col("succeeded") / F.col("attempts"))
    .withColumn("initiation_rate", F.col("initiated") / F.col("attempts"))
    .toPandas()
)

print("Attempt success rate — Swish vs Other methods:")
print(f"  {'Group':<20} {'Method':<10} {'Attempts':>10} {'Succeeded':>10} {'Success %':>10} {'Init. Rate':>10}")
print(f"  {'-'*70}")
for _, r in method_success.sort_values(["group_name", "method_group"]).iterrows():
    print(f"  {r['group_name']:<20} {r['method_group']:<10} {r['attempts']:>10,.0f}"
          f" {r['succeeded']:>10,.0f} {r['success_rate']:>10.2%} {r['initiation_rate']:>10.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Attempts per visitor — Swish-only visitors vs non-Swish visitors

# COMMAND ----------

attempts_by_pattern = (
    swish_sequences
    .groupBy("group_name", "visitor_pattern")
    .agg(
        F.count("*").alias("visitors"),
        F.mean("total_attempts").alias("avg_total_attempts"),
        F.mean("swish_attempts").alias("avg_swish_attempts"),
        F.mean("non_swish_attempts").alias("avg_non_swish_attempts"),
        F.expr("percentile_approx(total_attempts, 0.5)").alias("median_attempts"),
        F.expr("percentile_approx(total_attempts, 0.9)").alias("p90_attempts"),
    )
    .toPandas()
)

print("Attempts per visitor by usage pattern:")
print(f"  {'Group':<20} {'Pattern':<30} {'N':>8} {'Avg':>8} {'Median':>8} {'P90':>8}")
print(f"  {'-'*82}")
for _, r in attempts_by_pattern.sort_values(["group_name", "visitor_pattern"]).iterrows():
    print(f"  {r['group_name']:<20} {r['visitor_pattern']:<30} {r['visitors']:>8,.0f}"
          f" {r['avg_total_attempts']:>8.2f} {r['median_attempts']:>8.0f} {r['p90_attempts']:>8.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Distribution of attempts per visitor (histogram)

# COMMAND ----------

hist_data = visitor_metrics.select("group_name", "total_attempts").toPandas()

fig, ax = plt.subplots(figsize=(12, 5))
groups = sorted(hist_data["group_name"].unique())
max_att = min(int(hist_data["total_attempts"].quantile(0.99)), 15)
bins = np.arange(0.5, max_att + 1.5, 1)

for idx, g in enumerate(groups):
    vals = hist_data[hist_data["group_name"] == g]["total_attempts"].clip(upper=max_att)
    ax.hist(vals, bins=bins, alpha=0.6, label=g, color=COLORS[idx], density=True, edgecolor="white")

ax.set_xlabel("Number of Payment Attempts")
ax.set_ylabel("Density")
ax.set_title("Distribution of Attempts per Visitor", fontweight="bold", fontsize=13)
ax.legend()
ax.set_xticks(range(1, max_att + 1))
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Swish failure reasons

# COMMAND ----------

swish_failures = (
    df_attempts
    .filter(F.lower("payment_method").contains("swish"))
    .filter(~F.lower("payment_attempt_status").isin("settled", "authorized"))
    .groupBy("payment_attempt_status")
    .agg(
        F.count("attempt_id").alias("attempts"),
        F.countDistinct("visitor_id").alias("visitors"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

if not swish_failures.empty:
    total_fail = swish_failures["attempts"].sum()
    print("Swish failure breakdown by status:")
    print(f"  {'Status':<40} {'Attempts':>10} {'Share':>8} {'Visitors':>10}")
    print(f"  {'-'*68}")
    for _, r in swish_failures.iterrows():
        print(f"  {r['payment_attempt_status']:<40} {r['attempts']:>10,.0f}"
              f" {r['attempts']/total_fail:>8.1%} {r['visitors']:>10,.0f}")
else:
    print("No Swish failures found (or no Swish attempts at all).")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Counterfactual: Metrics Excluding Swish Attempts
# MAGIC
# MAGIC If we remove all Swish attempts from the treatment group, do attempt counts
# MAGIC and success rates converge with the control group?
# MAGIC
# MAGIC **If they converge:** The impact is entirely driven by Swish attempts themselves
# MAGIC (supports H1 or H2 depending on whether the extra attempts are from switching
# MAGIC or Swish-internal retries).
# MAGIC
# MAGIC **If they don't converge:** Swish is also indirectly affecting non-Swish attempts
# MAGIC (e.g. changed pre-selection, user fatigue).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Non-Swish-only metrics

# COMMAND ----------

non_swish_attempts = df_attempts.filter(~F.lower("payment_method").contains("swish"))

non_swish_visitor = (
    non_swish_attempts
    .groupBy("group_name", "visitor_id")
    .agg(
        F.count("attempt_id").alias("total_attempts"),
        F.sum(F.when(F.lower("payment_attempt_status").isin("settled", "authorized"), 1).otherwise(0)).alias("succeeded"),
    )
    .withColumn("is_converted", (F.col("succeeded") > 0).cast("int"))
)

non_swish_summary = (
    non_swish_visitor
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("total_attempts").alias("total_attempts"),
        F.mean("total_attempts").alias("avg_attempts"),
        F.sum("succeeded").alias("total_succeeded"),
        F.sum("is_converted").alias("converted"),
    )
    .withColumn("success_rate", F.col("total_succeeded") / F.col("total_attempts"))
    .withColumn("conversion_rate", F.col("converted") / F.col("visitors"))
    .toPandas()
)

print("=" * 90)
print("COUNTERFACTUAL — Non-Swish attempts only")
print("=" * 90)
for _, r in non_swish_summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Visitors (w/ non-Swish att.) : {r['visitors']:,.0f}")
    print(f"    Total non-Swish attempts     : {r['total_attempts']:,.0f}")
    print(f"    Avg attempts / visitor       : {r['avg_attempts']:.3f}")
    print(f"    Success rate (attempt-level) : {r['success_rate']:.2%}")
    print(f"    Conversion rate (visitor)    : {r['conversion_rate']:.2%}")

if len(non_swish_summary) == 2:
    ra = non_swish_summary.sort_values("group_name").iloc[0]
    rb = non_swish_summary.sort_values("group_name").iloc[1]
    groups_sorted = sorted(non_swish_summary["group_name"].unique())
    print("\n  Statistical tests (non-Swish only):")
    print_z_test("Attempt success rate (excl. Swish)",
                 ra["total_attempts"], ra["total_succeeded"],
                 rb["total_attempts"], rb["total_succeeded"],
                 groups_sorted[0], groups_sorted[1])
    print_z_test("Conversion rate (excl. Swish)",
                 ra["visitors"], ra["converted"],
                 rb["visitors"], rb["converted"],
                 groups_sorted[0], groups_sorted[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Side-by-side comparison: all attempts vs non-Swish only

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

comparison_data = []
for _, r in visitor_summary.iterrows():
    comparison_data.append({
        "group": r["group_name"], "scope": "All attempts",
        "avg_attempts": r["avg_attempts_per_visitor"],
        "success_rate": r["attempt_success_rate"],
        "conversion_rate": r["conversion_rate"],
    })
for _, r in non_swish_summary.iterrows():
    comparison_data.append({
        "group": r["group_name"], "scope": "Excl. Swish",
        "avg_attempts": r["avg_attempts"],
        "success_rate": r["success_rate"],
        "conversion_rate": r["conversion_rate"],
    })
comp_df = pd.DataFrame(comparison_data)

for i, (col, title, is_pct) in enumerate([
    ("avg_attempts", "Avg Attempts / Visitor", False),
    ("success_rate", "Attempt Success Rate", True),
    ("conversion_rate", "Visitor Conversion Rate", True),
]):
    x = np.arange(len(comp_df["scope"].unique()))
    width = 0.3
    groups = sorted(comp_df["group"].unique())
    scopes = sorted(comp_df["scope"].unique())

    for g_idx, g in enumerate(groups):
        vals = [comp_df[(comp_df["group"] == g) & (comp_df["scope"] == s)][col].values[0] for s in scopes]
        offset = -width / 2 + g_idx * width
        bars = axes[i].bar(x + offset, vals, width, label=g, color=COLORS[g_idx])
        for bar, val in zip(bars, vals):
            fmt = f"{val:.2%}" if is_pct else f"{val:.3f}"
            axes[i].text(bar.get_x() + bar.get_width() / 2,
                         bar.get_height() + bar.get_height() * 0.005,
                         fmt, ha="center", va="bottom", fontsize=9, fontweight="bold")

    axes[i].set_xticks(x)
    axes[i].set_xticklabels(scopes)
    axes[i].set_title(title, fontweight="bold", fontsize=11)
    if is_pct:
        axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[i].legend(fontsize=8)

fig.suptitle("All Attempts vs Excluding Swish", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Summary & Conclusions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthesis of findings
# MAGIC
# MAGIC This section prints a diagnostic summary based on the numbers computed above.

# COMMAND ----------

print("=" * 90)
print("DIAGNOSTIC SUMMARY")
print("=" * 90)

if len(sorted_groups) == 2:
    ctrl = visitor_summary[visitor_summary["group_name"] == sorted_groups[0]].iloc[0]
    test = visitor_summary[visitor_summary["group_name"] == sorted_groups[1]].iloc[0]
    ctrl_ns = non_swish_summary.sort_values("group_name").iloc[0]
    test_ns = non_swish_summary.sort_values("group_name").iloc[1]

    att_delta = test["avg_attempts_per_visitor"] - ctrl["avg_attempts_per_visitor"]
    sr_delta = test["attempt_success_rate"] - ctrl["attempt_success_rate"]
    att_ns_delta = test_ns["avg_attempts"] - ctrl_ns["avg_attempts"]
    sr_ns_delta = test_ns["success_rate"] - ctrl_ns["success_rate"]

    print(f"""
  1. OVERALL IMPACT
     ─────────────────────────────────────────────────
     Avg attempts/visitor:  {sorted_groups[0]}={ctrl['avg_attempts_per_visitor']:.3f}  {sorted_groups[1]}={test['avg_attempts_per_visitor']:.3f}  Δ={att_delta:+.3f}
     Attempt success rate:  {sorted_groups[0]}={ctrl['attempt_success_rate']:.2%}  {sorted_groups[1]}={test['attempt_success_rate']:.2%}  Δ={sr_delta*100:+.2f}pp
     Visitor conversion:    {sorted_groups[0]}={ctrl['conversion_rate']:.2%}  {sorted_groups[1]}={test['conversion_rate']:.2%}

  2. H1 — METHOD-SWITCHING
     ─────────────────────────────────────────────────
     Visitors with >1 method:  {sorted_groups[0]}={ctrl['pct_multi_method']:.2%}  {sorted_groups[1]}={test['pct_multi_method']:.2%}
     → If test has significantly more multi-method visitors, H1 is supported.
     → Check Section 4.2 for the Swish→fallback pattern size.

  3. H2 — SWISH INHERENT FAILURES
     ─────────────────────────────────────────────────
     → Check Section 5.1 for Swish attempt success rate vs other methods.
     → Check Section 5.4 for Swish failure reasons.
     → If Swish success rate is much lower than other methods, H2 is supported.

  4. COUNTERFACTUAL (excl. Swish)
     ─────────────────────────────────────────────────
     Avg attempts (excl. Swish):  {sorted_groups[0]}={ctrl_ns['avg_attempts']:.3f}  {sorted_groups[1]}={test_ns['avg_attempts']:.3f}  Δ={att_ns_delta:+.3f}
     Success rate (excl. Swish):  {sorted_groups[0]}={ctrl_ns['success_rate']:.2%}  {sorted_groups[1]}={test_ns['success_rate']:.2%}  Δ={sr_ns_delta*100:+.2f}pp
     → If the gap closes after excluding Swish, the impact is explained by Swish
       attempts (either switching or inherent failures).
     → If the gap remains, there's an indirect effect on non-Swish attempts.

  5. DIAGNOSIS GUIDE
     ─────────────────────────────────────────────────
     The root cause is likely a combination of:
       a) Swish adds a NEW payment method that some customers try but fail with,
          generating additional failed attempts (inflating attempt count).
       b) Swish→fallback customers contribute TWO sets of attempts
          (one failed Swish + one for the fallback method).
       c) Swish's own success rate may be lower than alternatives,
          dragging down the blended success rate.

     → To confirm, compare the sizes of patterns from Section 4.2.
     → The Swish failure reasons (Section 5.4) reveal if it's systemic
       (e.g. BankID timeouts) or user-driven (abandonment → retry elsewhere).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution — how much of the attempt increase is from H1 vs H2?

# COMMAND ----------

if len(sorted_groups) == 2:
    ctrl_extra = extra_attempts[extra_attempts["group_name"] == sorted_groups[0]].iloc[0]
    test_extra = extra_attempts[extra_attempts["group_name"] == sorted_groups[1]].iloc[0]

    total_attempt_delta = test_extra["total_attempts"] - ctrl_extra["total_attempts"]
    swish_attempt_contribution = test_extra["swish_attempts"] - ctrl_extra["swish_attempts"]

    test_pattern = pattern_summary[
        (pattern_summary["group_name"] == sorted_groups[1]) &
        (pattern_summary["visitor_pattern"] == "swish_first_then_fallback")
    ]
    switcher_count = test_pattern["visitors"].values[0] if len(test_pattern) > 0 else 0
    switcher_avg = test_pattern["avg_attempts"].values[0] if len(test_pattern) > 0 else 0

    swish_only_pattern = pattern_summary[
        (pattern_summary["group_name"] == sorted_groups[1]) &
        (pattern_summary["visitor_pattern"] == "swish_only")
    ]
    swish_only_count = swish_only_pattern["visitors"].values[0] if len(swish_only_pattern) > 0 else 0
    swish_only_avg = swish_only_pattern["avg_attempts"].values[0] if len(swish_only_pattern) > 0 else 0

    print(f"""
  ATTEMPT INCREASE ATTRIBUTION
  ═══════════════════════════════════════════════════
  Total attempt delta (test - control): {total_attempt_delta:+,.0f}
  Swish attempts in test:               {test_extra['swish_attempts']:,.0f}
  Swish attempts in control:            {ctrl_extra['swish_attempts']:,.0f}

  H1 — Method-switching contribution:
    Switcher visitors (Swish→fallback):  {switcher_count:,.0f}
    Avg attempts per switcher:           {switcher_avg:.2f}
    These visitors generate ~{switcher_avg:.1f} attempts each
    (at least 1 failed Swish + 1+ fallback attempt)

  H2 — Swish-only / inherent retry contribution:
    Swish-only visitors:                 {swish_only_count:,.0f}
    Avg Swish attempts per visitor:      {swish_only_avg:.2f}
    If avg > 1, Swish itself causes retries.

  → Swish accounts for {swish_attempt_contribution:,.0f} of the {total_attempt_delta:+,.0f} extra attempts.
""")
