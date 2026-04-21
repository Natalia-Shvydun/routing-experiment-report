# Databricks notebook source
# MAGIC %md
# MAGIC # Payment Orchestration Routing Experiment Analysis
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house`
# MAGIC
# MAGIC **Goal:** Validate that the in-house routing orchestration (variant B) performs
# MAGIC at parity with the external solution (variant A). Investigate any regressions,
# MAGIC with a focus on **3DS handling** and **country-level patterns**.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction (fixed assignment deduplication)
# MAGIC 2. Visitor-level analysis — sample sizes, conversion rates
# MAGIC 3. Attempt-level performance — success rates, errors, processors
# MAGIC 4. 3DS deep dive — challenge rates, challenge success, flow differences
# MAGIC 5. Country patterns — country × variant × 3DS interactions

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
from scipy.stats import norm, chi2_contingency

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]
A_COLOR, B_COLOR = COLORS[0], COLORS[1]

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-03-16"
PAYMENT_START = "2026-03-18"

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test. Returns (z_stat, p_value, delta_pp, ci_lower_pp, ci_upper_pp)."""
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
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Data Extraction
# MAGIC
# MAGIC Key fixes vs the original query:
# MAGIC - **Deduplicated assignments** — one row per visitor (first exposure)
# MAGIC - Removed duplicate `is_successful` column
# MAGIC - `is_successful` is used only as a metric, not as a GROUP BY dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Assignment data (deduplicated)

# COMMAND ----------

df_assignment = spark.sql(f"""
WITH filtered_customers AS (
  SELECT visitor_id
  FROM production.dwh.dim_customer c
  JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
  WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
)
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
WHERE DATE(timestamp) >= '{ASSIGNMENT_START}'
  AND experiment_id = '{EXPERIMENT_ID}'
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
""")
df_assignment.cache()

assignment_counts = df_assignment.groupBy("group_name").count().toPandas()
print("Assignment counts (deduplicated):")
for _, row in assignment_counts.iterrows():
    print(f"  {row['group_name']}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Payment attempt data joined with assignments

# COMMAND ----------

df_assignment.createOrReplaceTempView("assignment")

df_attempts = spark.sql(f"""
SELECT
  a.group_name,
  a.platform,
  a.visitor_id,
  p.payment_provider_reference,
  p.customer_system_attempt_reference,
  p.payment_flow,
  p.payment_method_variant,
  p.currency,
  p.payment_processor,
  p.payment_attempt_status,
  p.challenge_issued,
  p.response_code,
  p.fraud_pre_auth_result,
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.attempt_type,
  p.is_customer_attempt_successful,
  p.is_shopping_cart_successful,
  p.is_successful,
  p.error_code,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
""")
df_attempts.cache()

total_attempts = df_attempts.count()
print(f"Total matched payment attempts: {total_attempts:,}")

attempt_counts = df_attempts.groupBy("group_name").count().toPandas()
for _, row in attempt_counts.iterrows():
    print(f"  {row['group_name']}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Visitor-Level Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Unique visitors and high-level conversion

# COMMAND ----------

visitor_metrics = (
    df_attempts
    .groupBy("group_name", "visitor_id")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.max("is_shopping_cart_successful").alias("cart_success"),
        F.max("is_customer_attempt_successful").alias("cust_attempt_success"),
        F.max(F.col("challenge_issued").cast("boolean").cast("int")).alias("had_challenge"),
    )
    .withColumn("is_converted", (F.col("successes") > 0).cast("int"))
)
visitor_metrics.cache()

visitor_summary = (
    visitor_metrics
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("attempts").alias("total_attempts"),
        F.mean("attempts").alias("avg_attempts_per_visitor"),
        F.sum("is_converted").alias("converted_visitors"),
        F.sum("had_challenge").alias("visitors_with_challenge"),
    )
    .withColumn("conversion_rate", F.col("converted_visitors") / F.col("visitors"))
    .withColumn("challenge_rate", F.col("visitors_with_challenge") / F.col("visitors"))
    .toPandas()
)

print("=" * 80)
print("VISITOR-LEVEL SUMMARY")
print("=" * 80)
for _, r in visitor_summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Unique visitors          : {r['visitors']:,.0f}")
    print(f"    Total attempts           : {r['total_attempts']:,.0f}")
    print(f"    Avg attempts/visitor     : {r['avg_attempts_per_visitor']:.2f}")
    print(f"    Converted visitors       : {r['converted_visitors']:,.0f}  ({r['conversion_rate']:.2%})")
    print(f"    Visitors with 3DS chall. : {r['visitors_with_challenge']:,.0f}  ({r['challenge_rate']:.2%})")

sorted_groups = sorted(visitor_summary["group_name"].unique())
if len(sorted_groups) == 2:
    ra = visitor_summary[visitor_summary["group_name"] == sorted_groups[0]].iloc[0]
    rb = visitor_summary[visitor_summary["group_name"] == sorted_groups[1]].iloc[0]
    for metric_name, n_col, s_col in [
        ("Conversion rate", "visitors", "converted_visitors"),
        ("3DS challenge visitor rate", "visitors", "visitors_with_challenge"),
    ]:
        z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
            ra[n_col], ra[s_col], rb[n_col], rb[s_col],
        )
        print(f"\n  {metric_name}:  {sorted_groups[0]}={ra[s_col]/ra[n_col]:.2%}  "
              f"{sorted_groups[1]}={rb[s_col]/rb[n_col]:.2%}  "
              f"Δ={delta_pp:+.2f}pp  95%CI=[{ci_lo:+.2f}, {ci_hi:+.2f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Visitor conversion rate comparison

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

for i, (metric, label) in enumerate([
    ("conversion_rate", "Visitor Conversion Rate"),
    ("challenge_rate", "Visitor 3DS Challenge Rate"),
    ("avg_attempts_per_visitor", "Avg Attempts per Visitor"),
]):
    vals = visitor_summary.sort_values("group_name")
    colors = [A_COLOR if idx == 0 else B_COLOR for idx in range(len(vals))]
    bars = axes[i].bar(vals["group_name"], vals[metric], color=colors)
    axes[i].set_title(label, fontweight="bold", fontsize=12)
    if "rate" in metric.lower():
        axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, vals[metric]):
        fmt = f"{val:.2%}" if "rate" in metric.lower() else f"{val:.2f}"
        axes[i].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + bar.get_height() * 0.01,
                     fmt, ha="center", va="bottom", fontsize=11, fontweight="bold")

fig.suptitle("Visitor-Level Metrics by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Visitor-level conversion by platform

# COMMAND ----------

visitor_by_platform = (
    visitor_metrics
    .join(df_assignment.select("visitor_id", "platform").dropDuplicates(["visitor_id"]),
          on="visitor_id", how="inner")
    .groupBy("group_name", "platform")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("is_converted").alias("converted"),
    )
    .withColumn("conversion_rate", F.col("converted") / F.col("visitors"))
    .toPandas()
)

platforms = visitor_by_platform.groupby("platform")["visitors"].sum().nlargest(5).index
plot_data = visitor_by_platform[visitor_by_platform["platform"].isin(platforms)]

fig, ax = plt.subplots(figsize=(14, 5))
groups = plot_data["group_name"].unique()
x = np.arange(len(platforms))
w = 0.8 / len(groups)
for i, grp in enumerate(sorted(groups)):
    grp_data = plot_data[plot_data["group_name"] == grp].set_index("platform").reindex(platforms)
    ax.bar(x + i * w, grp_data["conversion_rate"].fillna(0), w,
           label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (rate, vol) in enumerate(zip(grp_data["conversion_rate"].fillna(0), grp_data["visitors"].fillna(0))):
        ax.text(x[j] + i * w, rate + 0.003, f"{rate:.1%}\nn={vol:,.0f}",
                ha="center", va="bottom", fontsize=7)
ax.set_xticks(x + w / 2)
ax.set_xticklabels(platforms)
ax.set_title("Visitor Conversion Rate by Platform", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Attempt-Level Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Overall attempt success rate by variant

# COMMAND ----------

attempt_summary = (
    df_attempts
    .groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum("is_customer_attempt_successful").alias("cust_attempt_successes"),
        F.sum("is_shopping_cart_successful").alias("cart_successes"),
        F.sum(F.col("challenge_issued").cast("boolean").cast("int")).alias("challenges_issued"),
    )
    .withColumn("attempt_sr", F.col("successes") / F.col("attempts"))
    .withColumn("cust_attempt_sr", F.col("cust_attempt_successes") / F.col("attempts"))
    .withColumn("cart_sr", F.col("cart_successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
    .toPandas()
)

print("=" * 80)
print("ATTEMPT-LEVEL SUMMARY")
print("=" * 80)
for _, r in attempt_summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Attempts           : {r['attempts']:,.0f}")
    print(f"    Attempt SR         : {r['attempt_sr']:.4%}")
    print(f"    Cust Attempt SR    : {r['cust_attempt_sr']:.4%}")
    print(f"    Cart SR            : {r['cart_sr']:.4%}")
    print(f"    3DS Challenge Rate : {r['challenge_rate']:.4%}")

att_rows = attempt_summary.sort_values("group_name")
att_groups = att_rows["group_name"].tolist()
if len(att_rows) == 2:
    ra, rb = att_rows.iloc[0], att_rows.iloc[1]
    for metric_name, s_col in [
        ("Attempt SR", "successes"),
        ("Cust Attempt SR", "cust_attempt_successes"),
        ("Cart SR", "cart_successes"),
        ("3DS Challenge Rate", "challenges_issued"),
    ]:
        z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
            ra["attempts"], ra[s_col], rb["attempts"], rb[s_col],
        )
        print(f"\n  {metric_name}:  {att_groups[0]}={ra[s_col]/ra['attempts']:.4%}  "
              f"{att_groups[1]}={rb[s_col]/rb['attempts']:.4%}  "
              f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f}, {ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Success rate comparison chart

# COMMAND ----------

metrics_to_plot = ["attempt_sr", "cust_attempt_sr", "cart_sr", "challenge_rate"]
metric_labels = ["Attempt SR", "Cust Attempt SR", "Cart SR", "3DS Challenge Rate"]

fig, axes = plt.subplots(1, 4, figsize=(20, 5))
for i, (col, label) in enumerate(zip(metrics_to_plot, metric_labels)):
    vals = attempt_summary.sort_values("group_name")
    colors = [A_COLOR, B_COLOR] if len(vals) == 2 else [A_COLOR]
    bars = axes[i].bar(vals["group_name"], vals[col], color=colors[:len(vals)])
    axes[i].set_title(label, fontweight="bold", fontsize=11)
    axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, vals[col]):
        axes[i].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + bar.get_height() * 0.005,
                     f"{val:.2%}", ha="center", va="bottom", fontsize=10, fontweight="bold")

fig.suptitle("Attempt-Level Metrics by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Daily attempt success rate trend by variant

# COMMAND ----------

daily_sr = (
    df_attempts
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(16, 5))
for i, (grp, grp_data) in enumerate(daily_sr.groupby("group_name")):
    color = A_COLOR if i == 0 else B_COLOR
    ax.plot(grp_data["attempt_date"], grp_data["sr"], color=color, linewidth=1.5,
            label=grp, marker=".", markersize=4)
ax.set_title("Daily Attempt Success Rate by Variant", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
ax.set_xlabel("")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Success rate by customer attempt rank and variant

# COMMAND ----------

sr_by_rank = (
    df_attempts
    .filter(F.col("customer_attempt_rank") <= 5)
    .groupBy("group_name", "customer_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("customer_attempt_rank")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 5))
groups = sorted(sr_by_rank["group_name"].unique())
ranks = sorted(sr_by_rank["customer_attempt_rank"].unique())
x = np.arange(len(ranks))
w = 0.35

for i, grp in enumerate(groups):
    grp_data = sr_by_rank[sr_by_rank["group_name"] == grp].set_index("customer_attempt_rank").reindex(ranks)
    bars = ax.bar(x + i * w, grp_data["sr"].fillna(0), w,
                  label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (rate, vol) in enumerate(zip(grp_data["sr"].fillna(0), grp_data["attempts"].fillna(0))):
        ax.text(x[j] + i * w, rate + 0.005, f"{rate:.1%}\nn={vol:,.0f}",
                ha="center", va="bottom", fontsize=7)

ax.set_xticks(x + w / 2)
ax.set_xticklabels([str(r) for r in ranks])
ax.set_xlabel("Customer Attempt Rank")
ax.set_title("Attempt SR by Customer Attempt Rank", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Error code distribution — A vs B (top 15 error codes)

# COMMAND ----------

errors_by_variant = (
    df_attempts
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_errors = errors_by_variant.groupby("group_name")["cnt"].sum()
top_errors = errors_by_variant.groupby("error_code")["cnt"].sum().nlargest(15).index

plot_errors = errors_by_variant[errors_by_variant["error_code"].isin(top_errors)].copy()
plot_errors["pct"] = plot_errors.apply(
    lambda r: r["cnt"] / total_errors.get(r["group_name"], 1), axis=1,
)

fig, ax = plt.subplots(figsize=(14, 7))
groups = sorted(plot_errors["group_name"].unique())
error_list = (plot_errors.groupby("error_code")["cnt"].sum()
              .sort_values(ascending=True).index.tolist())
y = np.arange(len(error_list))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = plot_errors[plot_errors["group_name"] == grp].set_index("error_code").reindex(error_list)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)

ax.set_yticks(y + h / 2)
ax.set_yticklabels(error_list, fontsize=9)
ax.set_title("Top 15 Error Codes — Share of Failed Attempts", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Success rate by payment processor and variant

# COMMAND ----------

proc_variant = (
    df_attempts
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

top_procs = proc_variant.groupby("payment_processor")["attempts"].sum().nlargest(8).index
plot_proc = proc_variant[proc_variant["payment_processor"].isin(top_procs)]

fig, ax = plt.subplots(figsize=(14, 6))
groups = sorted(plot_proc["group_name"].unique())
proc_list = (plot_proc.groupby("payment_processor")["attempts"].sum()
             .sort_values(ascending=True).index.tolist())
y = np.arange(len(proc_list))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = plot_proc[plot_proc["group_name"] == grp].set_index("payment_processor").reindex(proc_list)
    bars = ax.barh(y + i * h, grp_data["sr"].fillna(0), h,
                   label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (rate, vol) in enumerate(zip(grp_data["sr"].fillna(0), grp_data["attempts"].fillna(0))):
        if vol > 0:
            ax.text(rate + 0.003, y[j] + i * h, f"{rate:.1%} (n={vol:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels(proc_list, fontsize=9)
ax.set_title("Attempt SR by Processor × Variant", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Response code distribution — A vs B (top 15)

# COMMAND ----------

resp_by_variant = (
    df_attempts
    .groupBy("group_name", "response_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_group = resp_by_variant.groupby("group_name")["cnt"].sum()
top_resp = resp_by_variant.groupby("response_code")["cnt"].sum().nlargest(15).index
plot_resp = resp_by_variant[resp_by_variant["response_code"].isin(top_resp)].copy()
plot_resp["pct"] = plot_resp.apply(
    lambda r: r["cnt"] / total_by_group.get(r["group_name"], 1), axis=1,
)

fig, ax = plt.subplots(figsize=(14, 7))
groups = sorted(plot_resp["group_name"].unique())
resp_list = (plot_resp.groupby("response_code")["cnt"].sum()
             .sort_values(ascending=True).index.tolist())
y = np.arange(len(resp_list))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = plot_resp[plot_resp["group_name"] == grp].set_index("response_code").reindex(resp_list)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)

ax.set_yticks(y + h / 2)
ax.set_yticklabels([str(r) for r in resp_list], fontsize=9)
ax.set_title("Top 15 Response Codes — Share of All Attempts", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — 3DS Deep Dive
# MAGIC
# MAGIC The main hypothesis is that the in-house routing orchestration handles 3DS
# MAGIC differently (challenge issuance rate, challenge completion success, frictionless
# MAGIC pass-through). This section isolates the 3DS path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 3DS challenge rate by variant

# COMMAND ----------

challenge_stats = (
    df_attempts
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(
            F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)
        ).alias("challenge_successes"),
        F.sum(
            F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)
        ).alias("no_challenge_successes"),
        F.sum(
            F.when(~F.col("_challenged"), 1).otherwise(0)
        ).alias("no_challenge_attempts"),
    )
    .withColumn("challenge_rate", F.col("challenges") / F.col("total_attempts"))
    .withColumn("challenge_sr", F.col("challenge_successes") / F.col("challenges"))
    .withColumn("no_challenge_sr", F.col("no_challenge_successes") / F.col("no_challenge_attempts"))
    .toPandas()
)

print("=" * 80)
print("3DS CHALLENGE ANALYSIS")
print("=" * 80)
for _, r in challenge_stats.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Total attempts              : {r['total_attempts']:,.0f}")
    print(f"    Challenges issued           : {r['challenges']:,.0f}  ({r['challenge_rate']:.2%})")
    print(f"    SR when challenge issued     : {r['challenge_sr']:.2%}  (n={r['challenges']:,.0f})")
    print(f"    SR when NO challenge         : {r['no_challenge_sr']:.2%}  (n={r['no_challenge_attempts']:,.0f})")

ch_rows = challenge_stats.sort_values("group_name")
ch_groups = ch_rows["group_name"].tolist()
if len(ch_rows) == 2:
    ra, rb = ch_rows.iloc[0], ch_rows.iloc[1]
    for metric_name, n_a, s_a, n_b, s_b in [
        ("Challenge Rate",
         ra["total_attempts"], ra["challenges"],
         rb["total_attempts"], rb["challenges"]),
        ("SR when challenged",
         ra["challenges"], ra["challenge_successes"],
         rb["challenges"], rb["challenge_successes"]),
        ("SR when NOT challenged",
         ra["no_challenge_attempts"], ra["no_challenge_successes"],
         rb["no_challenge_attempts"], rb["no_challenge_successes"]),
    ]:
        if n_a > 0 and n_b > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
            print(f"\n  {metric_name}:  {ch_groups[0]}={s_a/n_a:.2%}  {ch_groups[1]}={s_b/n_b:.2%}  "
                  f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f}, {ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 3DS challenge rate & success — visual comparison

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

vals = challenge_stats.sort_values("group_name")
colors = [A_COLOR, B_COLOR][:len(vals)]

for i, (col, label) in enumerate([
    ("challenge_rate", "3DS Challenge Rate"),
    ("challenge_sr", "SR When Challenge Issued"),
    ("no_challenge_sr", "SR When No Challenge"),
]):
    bars = axes[i].bar(vals["group_name"], vals[col], color=colors)
    axes[i].set_title(label, fontweight="bold", fontsize=12)
    axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, vals[col]):
        axes[i].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + bar.get_height() * 0.005,
                     f"{val:.2%}", ha="center", va="bottom", fontsize=11, fontweight="bold")

fig.suptitle("3DS Challenge — Rate & Success by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Daily 3DS challenge rate trend by variant

# COMMAND ----------

daily_challenge = (
    df_attempts
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("attempt_date", "group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(
            F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)
        ).alias("challenge_successes"),
    )
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .withColumn("challenge_sr", F.col("challenge_successes") / F.col("challenges"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(18, 5))

for i, (col, label) in enumerate([
    ("challenge_rate", "Daily 3DS Challenge Rate"),
    ("challenge_sr", "Daily SR When Challenge Issued"),
]):
    for j, (grp, grp_data) in enumerate(daily_challenge.groupby("group_name")):
        color = A_COLOR if j == 0 else B_COLOR
        axes[i].plot(grp_data["attempt_date"], grp_data[col], color=color,
                     linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[i].set_title(label, fontsize=13, fontweight="bold")
    axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[i].legend(fontsize=10)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 3DS challenge rate by payment method variant

# COMMAND ----------

challenge_by_pmv = (
    df_attempts
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "payment_method_variant")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum("is_successful").alias("successes"),
        F.sum(
            F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)
        ).alias("challenge_successes"),
    )
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_sr",
                F.when(F.col("challenges") > 0, F.col("challenge_successes") / F.col("challenges")))
    .toPandas()
)

top_pmv = challenge_by_pmv.groupby("payment_method_variant")["attempts"].sum().nlargest(6).index
plot_pmv = challenge_by_pmv[challenge_by_pmv["payment_method_variant"].isin(top_pmv)]

fig, axes = plt.subplots(1, 3, figsize=(20, 6))
pmv_list = sorted(top_pmv)
y = np.arange(len(pmv_list))
h = 0.35

for ax_idx, (col, title) in enumerate([
    ("challenge_rate", "3DS Challenge Rate"),
    ("sr", "Attempt SR"),
    ("challenge_sr", "SR When Challenged"),
]):
    groups = sorted(plot_pmv["group_name"].unique())
    for i, grp in enumerate(groups):
        grp_data = plot_pmv[plot_pmv["group_name"] == grp].set_index("payment_method_variant").reindex(pmv_list)
        bars = axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                                 label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (rate, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(rate + 0.003, y[j] + i * h, f"{rate:.1%}",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(pmv_list, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=9)
fig.suptitle("3DS & SR by Payment Method Variant × Experiment Group", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Fraud pre-auth result distribution by variant

# COMMAND ----------

fraud_dist = (
    df_attempts
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_grp = fraud_dist.groupby("group_name")["cnt"].sum()
fraud_dist["pct"] = fraud_dist.apply(
    lambda r: r["cnt"] / total_by_grp.get(r["group_name"], 1), axis=1,
)

fig, ax = plt.subplots(figsize=(14, 6))
groups = sorted(fraud_dist["group_name"].unique())
fraud_results = (fraud_dist.groupby("fraud_pre_auth_result")["cnt"].sum()
                 .sort_values(ascending=True).index.tolist())
y = np.arange(len(fraud_results))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = fraud_dist[fraud_dist["group_name"] == grp].set_index("fraud_pre_auth_result").reindex(fraud_results)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.003, y[j] + i * h, f"{pct:.1%} (n={cnt:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels([str(f) for f in fraud_results], fontsize=9)
ax.set_title("Fraud Pre-Auth Result Distribution by Variant", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 3DS challenge outcome decomposition
# MAGIC
# MAGIC For attempts where a challenge was issued, what fraction succeed vs fail,
# MAGIC and what are the dominant error codes for 3DS failures?

# COMMAND ----------

challenge_outcomes = (
    df_attempts
    .filter(F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "payment_attempt_status", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_challenged = challenge_outcomes.groupby("group_name")["cnt"].sum()
challenge_outcomes["pct"] = challenge_outcomes.apply(
    lambda r: r["cnt"] / total_challenged.get(r["group_name"], 1), axis=1,
)

pivot_status = (
    challenge_outcomes
    .groupby(["group_name", "payment_attempt_status"])["pct"].sum()
    .unstack(fill_value=0)
)

print("3DS Challenge Outcome Distribution (share of challenged attempts):")
print(pivot_status.to_string())

top_3ds_errors = (
    challenge_outcomes[challenge_outcomes["payment_attempt_status"] != "authorized"]
    .groupby("error_code")["cnt"].sum()
    .nlargest(10)
    .index
)
print("\nTop 10 error codes when 3DS challenge fails:")
for ec in top_3ds_errors:
    subset = challenge_outcomes[challenge_outcomes["error_code"] == ec]
    for _, r in subset.iterrows():
        print(f"  {r['group_name']}  error={ec}  n={r['cnt']:,}  ({r['pct']:.2%} of challenged)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Success rate stratified by 3DS path (waterfall view)

# COMMAND ----------

three_ds_paths = (
    df_attempts
    .withColumn("three_ds_path",
        F.when(F.col("challenge_issued").cast("boolean"), "3DS Challenge Issued")
         .otherwise("No 3DS Challenge")
    )
    .groupBy("group_name", "three_ds_path")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, 5))
paths = ["No 3DS Challenge", "3DS Challenge Issued"]
groups = sorted(three_ds_paths["group_name"].unique())
x = np.arange(len(paths))
w = 0.35

for i, grp in enumerate(groups):
    grp_data = three_ds_paths[three_ds_paths["group_name"] == grp].set_index("three_ds_path").reindex(paths)
    bars = ax.bar(x + i * w, grp_data["sr"].fillna(0), w,
                  label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (rate, vol) in enumerate(zip(grp_data["sr"].fillna(0), grp_data["attempts"].fillna(0))):
        ax.text(x[j] + i * w, rate + 0.008,
                f"{rate:.1%}\nn={vol:,.0f}", ha="center", va="bottom", fontsize=9)

ax.set_xticks(x + w / 2)
ax.set_xticklabels(paths)
ax.set_title("Attempt SR by 3DS Path × Variant", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Country-Level Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Success rate by issuer country × variant (top 15 countries by volume)

# COMMAND ----------

country_variant = (
    df_attempts
    .groupBy("group_name", "bin_issuer_country_code")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.col("challenge_issued").cast("boolean").cast("int")).alias("challenges"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .toPandas()
)

top_countries = country_variant.groupby("bin_issuer_country_code")["attempts"].sum().nlargest(15).index
plot_country = country_variant[country_variant["bin_issuer_country_code"].isin(top_countries)]

fig, axes = plt.subplots(1, 2, figsize=(20, 8))
country_order = (plot_country.groupby("bin_issuer_country_code")["attempts"].sum()
                 .sort_values(ascending=True).index.tolist())
y = np.arange(len(country_order))
h = 0.35
groups = sorted(plot_country["group_name"].unique())

for ax_idx, (col, title) in enumerate([("sr", "Attempt SR"), ("challenge_rate", "3DS Challenge Rate")]):
    for i, grp in enumerate(groups):
        grp_data = (plot_country[plot_country["group_name"] == grp]
                    .set_index("bin_issuer_country_code").reindex(country_order))
        bars = axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                                 label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (rate, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(rate + 0.003, y[j] + i * h, f"{rate:.1%}",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(country_order, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=13)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("Top 15 Issuer Countries — SR & 3DS Challenge Rate by Variant",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Country-level delta — which countries drive the A-B gap?

# COMMAND ----------

MIN_COUNTRY_ATTEMPTS = 100

country_groups = sorted(plot_country["group_name"].unique())
valid = pd.DataFrame()
g_a, g_b = None, None

if len(country_groups) == 2:
    g_a, g_b = country_groups[0], country_groups[1]

    country_pivot = plot_country.pivot_table(
        index="bin_issuer_country_code",
        columns="group_name",
        values=["sr", "challenge_rate", "attempts", "successes", "challenges"],
        aggfunc="first",
    )

    delta_df = pd.DataFrame({
        "country": country_pivot.index,
        "attempts_A": country_pivot[("attempts", g_a)].values,
        "attempts_B": country_pivot[("attempts", g_b)].values,
        "sr_A": country_pivot[("sr", g_a)].values,
        "sr_B": country_pivot[("sr", g_b)].values,
        "challenge_rate_A": country_pivot[("challenge_rate", g_a)].values,
        "challenge_rate_B": country_pivot[("challenge_rate", g_b)].values,
    })
    delta_df["sr_delta_pp"] = (delta_df["sr_A"] - delta_df["sr_B"]) * 100
    delta_df["challenge_delta_pp"] = (delta_df["challenge_rate_A"] - delta_df["challenge_rate_B"]) * 100
    delta_df["total_attempts"] = delta_df["attempts_A"] + delta_df["attempts_B"]

    valid = delta_df[
        (delta_df["attempts_A"] >= MIN_COUNTRY_ATTEMPTS) &
        (delta_df["attempts_B"] >= MIN_COUNTRY_ATTEMPTS)
    ].copy()

    p_values = []
    for _, r in valid.iterrows():
        s_a = r["sr_A"] * r["attempts_A"]
        s_b = r["sr_B"] * r["attempts_B"]
        if r["attempts_A"] > 0 and r["attempts_B"] > 0 and (s_a + s_b) > 0:
            _, p, _, _, _ = two_proportion_z_test(r["attempts_A"], s_a, r["attempts_B"], s_b)
        else:
            p = 1.0
        p_values.append(p)
    valid["p_value"] = p_values
    valid["significant"] = valid["p_value"] < 0.05

    valid = valid.sort_values("sr_delta_pp", ascending=False)

    print(f"Country-level SR delta ({g_a} minus {g_b}), sorted by gap:")
    print(f"{'Country':<10} {'SR_A':>8} {'SR_B':>8} {'Δ(pp)':>8} {'ChR_A':>8} {'ChR_B':>8} {'ΔChR(pp)':>10} {'n_A':>8} {'n_B':>8} {'p':>8} {'sig':>5}")
    print("-" * 100)
    for _, r in valid.iterrows():
        sig_str = sig_label(r.get("p_value", 1))
        print(f"{r['country']:<10} {r['sr_A']:>8.2%} {r['sr_B']:>8.2%} {r['sr_delta_pp']:>+8.2f} "
              f"{r['challenge_rate_A']:>8.2%} {r['challenge_rate_B']:>8.2%} {r['challenge_delta_pp']:>+10.2f} "
              f"{r['attempts_A']:>8,.0f} {r['attempts_B']:>8,.0f} {r.get('p_value', 1):>8.4f} {sig_str:>5}")
else:
    print(f"Expected 2 groups, found {len(country_groups)}: {country_groups}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Country-level delta visualisation

# COMMAND ----------

if g_a is not None and len(valid) > 0:
    plot_valid = valid.sort_values("sr_delta_pp")

    fig, axes = plt.subplots(1, 2, figsize=(18, 8))

    bar_colors = [COLORS[3] if d > 0 else COLORS[2] for d in plot_valid["sr_delta_pp"]]
    axes[0].barh(plot_valid["country"], plot_valid["sr_delta_pp"], color=bar_colors)
    axes[0].axvline(0, color="black", linewidth=0.8)
    axes[0].set_title("SR Delta (A − B) in pp\n(positive = A better)", fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Δ Success Rate (pp)")
    for i, (_, r) in enumerate(plot_valid.iterrows()):
        sig_str = sig_label(r.get("p_value", 1))
        axes[0].text(r["sr_delta_pp"] + 0.1 * np.sign(r["sr_delta_pp"]), i,
                     f"{r['sr_delta_pp']:+.2f}{sig_str}", va="center", fontsize=8)

    bar_colors_ch = [COLORS[3] if d > 0 else COLORS[2] for d in plot_valid["challenge_delta_pp"]]
    axes[1].barh(plot_valid["country"], plot_valid["challenge_delta_pp"], color=bar_colors_ch)
    axes[1].axvline(0, color="black", linewidth=0.8)
    axes[1].set_title("3DS Challenge Rate Delta (A − B) in pp\n(positive = A has more challenges)",
                      fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Δ Challenge Rate (pp)")

    fig.suptitle("Country-Level A vs B Deltas", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Scatter: SR delta vs 3DS challenge rate delta by country
# MAGIC
# MAGIC If the 3DS handling difference is the root cause, we expect countries where
# MAGIC B has a higher challenge rate (negative challenge_delta) to also have a larger
# MAGIC SR gap (positive sr_delta, meaning A is better).

# COMMAND ----------

if g_a is not None and len(valid) > 0:
    fig, ax = plt.subplots(figsize=(10, 8))

    sizes = valid["total_attempts"] / valid["total_attempts"].max() * 500

    scatter = ax.scatter(
        valid["challenge_delta_pp"], valid["sr_delta_pp"],
        s=sizes, alpha=0.6, c=valid["sr_delta_pp"],
        cmap="RdYlGn_r", edgecolors="black", linewidth=0.5,
    )
    ax.axhline(0, color="gray", linewidth=0.8, linestyle="--")
    ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")

    for _, r in valid.iterrows():
        ax.annotate(r["country"], (r["challenge_delta_pp"], r["sr_delta_pp"]),
                    fontsize=8, ha="center", va="bottom",
                    xytext=(0, 5), textcoords="offset points")

    ax.set_xlabel("Δ 3DS Challenge Rate (A − B) in pp", fontsize=11)
    ax.set_ylabel("Δ Attempt SR (A − B) in pp", fontsize=11)
    ax.set_title("SR Gap vs 3DS Challenge Rate Gap by Country\n"
                 "(bubble size = total attempts; red = A better, green = B better)",
                 fontsize=13, fontweight="bold")

    corr = valid[["challenge_delta_pp", "sr_delta_pp"]].corr().iloc[0, 1]
    ax.text(0.05, 0.95, f"Correlation: {corr:.3f}", transform=ax.transAxes,
            fontsize=11, verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))

    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 3DS challenge SR by country × variant (top 15)

# COMMAND ----------

country_3ds_sr = (
    df_attempts
    .filter(F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "bin_issuer_country_code")
    .agg(
        F.count("*").alias("challenged_attempts"),
        F.sum("is_successful").alias("challenge_successes"),
    )
    .withColumn("challenge_sr", F.col("challenge_successes") / F.col("challenged_attempts"))
    .toPandas()
)

top_challenge_countries = (
    country_3ds_sr.groupby("bin_issuer_country_code")["challenged_attempts"]
    .sum().nlargest(15).index
)
plot_ch_country = country_3ds_sr[country_3ds_sr["bin_issuer_country_code"].isin(top_challenge_countries)]

fig, ax = plt.subplots(figsize=(14, 8))
ch_country_order = (plot_ch_country.groupby("bin_issuer_country_code")["challenged_attempts"]
                    .sum().sort_values(ascending=True).index.tolist())
y = np.arange(len(ch_country_order))
h = 0.35
groups = sorted(plot_ch_country["group_name"].unique())

for i, grp in enumerate(groups):
    grp_data = (plot_ch_country[plot_ch_country["group_name"] == grp]
                .set_index("bin_issuer_country_code").reindex(ch_country_order))
    bars = ax.barh(y + i * h, grp_data["challenge_sr"].fillna(0), h,
                   label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (rate, vol) in enumerate(zip(grp_data["challenge_sr"].fillna(0),
                                        grp_data["challenged_attempts"].fillna(0))):
        if vol > 0:
            ax.text(rate + 0.005, y[j] + i * h, f"{rate:.1%} (n={vol:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels(ch_country_order, fontsize=9)
ax.set_title("SR When 3DS Challenge Issued — by Country × Variant (top 15)",
             fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Decomposition: how much of the overall SR gap is explained by 3DS?

# COMMAND ----------

if g_a is not None and g_b is not None:
    decomp = (
        df_attempts
        .withColumn("challenged", F.col("challenge_issued").cast("boolean").cast("int"))
        .groupBy("group_name", "challenged")
        .agg(
            F.count("*").alias("attempts"),
            F.sum("is_successful").alias("successes"),
        )
        .withColumn("sr", F.col("successes") / F.col("attempts"))
        .toPandas()
    )

    overall_a = decomp[decomp["group_name"] == g_a]["successes"].sum() / decomp[decomp["group_name"] == g_a]["attempts"].sum()
    overall_b = decomp[decomp["group_name"] == g_b]["successes"].sum() / decomp[decomp["group_name"] == g_b]["attempts"].sum()
    overall_gap = (overall_a - overall_b) * 100

    a_chall = decomp[(decomp["group_name"] == g_a) & (decomp["challenged"] == 1)]
    b_chall = decomp[(decomp["group_name"] == g_b) & (decomp["challenged"] == 1)]
    a_no_chall = decomp[(decomp["group_name"] == g_a) & (decomp["challenged"] == 0)]
    b_no_chall = decomp[(decomp["group_name"] == g_b) & (decomp["challenged"] == 0)]

    if not a_chall.empty and not b_chall.empty:
        sr_gap_challenged = (a_chall["sr"].values[0] - b_chall["sr"].values[0]) * 100
        weight_challenged = (a_chall["attempts"].values[0] + b_chall["attempts"].values[0]) / decomp["attempts"].sum()
    else:
        sr_gap_challenged = 0
        weight_challenged = 0

    if not a_no_chall.empty and not b_no_chall.empty:
        sr_gap_no_challenge = (a_no_chall["sr"].values[0] - b_no_chall["sr"].values[0]) * 100
        weight_no_challenge = (a_no_chall["attempts"].values[0] + b_no_chall["attempts"].values[0]) / decomp["attempts"].sum()
    else:
        sr_gap_no_challenge = 0
        weight_no_challenge = 0

    print("=" * 60)
    print("GAP DECOMPOSITION: 3DS vs Non-3DS")
    print("=" * 60)
    print(f"  Overall SR gap (A − B)       : {overall_gap:+.3f} pp")
    print(f"  Gap on 3DS-challenged attempts: {sr_gap_challenged:+.3f} pp  (weight: {weight_challenged:.1%})")
    print(f"  Gap on non-challenged attempts: {sr_gap_no_challenge:+.3f} pp  (weight: {weight_no_challenge:.1%})")
    print(f"\n  Contribution from 3DS path   : ~{sr_gap_challenged * weight_challenged / overall_gap * 100:.0f}% of total gap" if overall_gap != 0 else "")
    print(f"  Contribution from non-3DS    : ~{sr_gap_no_challenge * weight_no_challenge / overall_gap * 100:.0f}% of total gap" if overall_gap != 0 else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key areas to inspect after running this notebook:**
# MAGIC
# MAGIC | Section | What to look for |
# MAGIC |---|---|
# MAGIC | 2.1 | Is visitor sample balanced between A and B? |
# MAGIC | 3.1 | Is the overall attempt SR difference statistically significant? |
# MAGIC | 4.1 | Does B issue more/fewer 3DS challenges? Is 3DS completion SR lower? |
# MAGIC | 4.6 | When 3DS challenges fail in B, what are the dominant error codes? |
# MAGIC | 5.2 | Which countries drive the gap? |
# MAGIC | 5.4 | Does the scatter show correlation between 3DS challenge delta and SR delta? |
# MAGIC | 5.6 | How much of the overall gap is attributable to 3DS vs non-3DS paths? |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of experiment analysis*
