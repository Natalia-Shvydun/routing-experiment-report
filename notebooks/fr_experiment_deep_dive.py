# Databricks notebook source
# MAGIC %md
# MAGIC # France (FR) Deep Dive — Payment Orchestration Experiment
# MAGIC
# MAGIC The main experiment analysis identified **FR (France)** as a country with a notable
# MAGIC success-rate regression in variant B (in-house routing). This notebook
# MAGIC investigates the root cause and provides specific `payment_provider_reference`
# MAGIC IDs for further investigation in **Primer**.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction — FR-only attempts
# MAGIC 2. High-level FR comparison — volume, SR, 3DS rates
# MAGIC 3. Error & response code analysis — what fails differently in B?
# MAGIC 4. 3DS deep dive for FR — challenge rate, challenge success, flow
# MAGIC 5. Processor & payment method variant breakdown
# MAGIC 6. Daily trend — is the issue consistent or time-dependent?
# MAGIC 7. Payment references for Primer investigation

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

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-03-16"
PAYMENT_START = "2026-03-18"
TARGET_COUNTRY = "FR"

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
# MAGIC # SECTION 1 — Data Extraction (FR only)

# COMMAND ----------

df_fr = spark.sql(f"""
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
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{ASSIGNMENT_START}'
    AND experiment_id = '{EXPERIMENT_ID}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
)
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
  AND p.bin_issuer_country_code = '{TARGET_COUNTRY}'
""")
df_fr.cache()

total_fr = df_fr.count()
print(f"Total FR payment attempts: {total_fr:,}")

fr_by_group = df_fr.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in fr_by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(fr_by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — High-Level FR Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Overall FR metrics by variant

# COMMAND ----------

fr_summary = (
    df_fr
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum("is_customer_attempt_successful").alias("cust_attempt_successes"),
        F.sum("is_shopping_cart_successful").alias("cart_successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges_issued"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_challenge_attempts"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("no_challenge_successes"),
        F.countDistinct("visitor_id").alias("visitors"),
        F.countDistinct("payment_provider_reference").alias("customer_attempts"),
    )
    .withColumn("attempt_sr", F.col("successes") / F.col("attempts"))
    .withColumn("cust_attempt_sr", F.col("cust_attempt_successes") / F.col("attempts"))
    .withColumn("cart_sr", F.col("cart_successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
    .withColumn("challenge_sr", F.col("challenge_successes") / F.col("challenges_issued"))
    .withColumn("no_challenge_sr", F.col("no_challenge_successes") / F.col("no_challenge_attempts"))
    .orderBy("group_name")
    .toPandas()
)

print("=" * 90)
print(f"FR SUMMARY BY VARIANT")
print("=" * 90)
for _, r in fr_summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Unique visitors          : {r['visitors']:,.0f}")
    print(f"    Customer attempts        : {r['customer_attempts']:,.0f}")
    print(f"    System attempts          : {r['attempts']:,.0f}")
    print(f"    Attempt SR               : {r['attempt_sr']:.2%}")
    print(f"    Cust Attempt SR          : {r['cust_attempt_sr']:.2%}")
    print(f"    Cart SR                  : {r['cart_sr']:.2%}")
    print(f"    3DS Challenge Rate       : {r['challenge_rate']:.2%}")
    print(f"    SR when challenged       : {r['challenge_sr']:.2%}  (n={r['challenges_issued']:,.0f})")
    print(f"    SR when NOT challenged   : {r['no_challenge_sr']:.2%}  (n={r['no_challenge_attempts']:,.0f})")

if len(fr_summary) == 2:
    ra, rb = fr_summary.iloc[0], fr_summary.iloc[1]
    print(f"\n{'─' * 90}")
    print("STATISTICAL TESTS (FR):")
    for label, n_a, s_a, n_b, s_b in [
        ("Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
        ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ("SR when challenged", ra["challenges_issued"], ra["challenge_successes"], rb["challenges_issued"], rb["challenge_successes"]),
        ("SR when NOT challenged", ra["no_challenge_attempts"], ra["no_challenge_successes"], rb["no_challenge_attempts"], rb["no_challenge_successes"]),
    ]:
        if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
            print(f"  {label:<25}  {g_a}={s_a/n_a:.2%}  {g_b}={s_b/n_b:.2%}  "
                  f"Δ={delta_pp:+.2f}pp  95%CI=[{ci_lo:+.2f},{ci_hi:+.2f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Visual comparison

# COMMAND ----------

metrics = [
    ("attempt_sr", "Attempt SR"),
    ("challenge_rate", "3DS Challenge Rate"),
    ("challenge_sr", "SR When Challenged"),
    ("no_challenge_sr", "SR When Not Challenged"),
]

fig, axes = plt.subplots(1, 4, figsize=(22, 5))
for i, (col, title) in enumerate(metrics):
    colors = [A_COLOR, B_COLOR][:len(fr_summary)]
    bars = axes[i].bar(fr_summary["group_name"], fr_summary[col], color=colors)
    axes[i].set_title(title, fontweight="bold", fontsize=11)
    axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, fr_summary[col]):
        axes[i].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + bar.get_height() * 0.005,
                     f"{val:.2%}", ha="center", va="bottom", fontsize=10, fontweight="bold")

fig.suptitle(f"FR — Key Metrics by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Error & Response Code Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Error code distribution — A vs B (FR failed attempts)

# COMMAND ----------

fr_errors = (
    df_fr.filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_errors_by_grp = fr_errors.groupby("group_name")["cnt"].sum()
top_errors = fr_errors.groupby("error_code")["cnt"].sum().nlargest(15).index
plot_err = fr_errors[fr_errors["error_code"].isin(top_errors)].copy()
plot_err["pct"] = plot_err.apply(
    lambda r: r["cnt"] / total_errors_by_grp.get(r["group_name"], 1), axis=1,
)

print("FR Error Code Distribution (share of failed attempts):")
print(f"{'Error Code':<35} ", end="")
for g in groups:
    print(f"{'cnt_'+g:>10} {'pct_'+g:>8}", end="  ")
print()
print("-" * (35 + len(groups) * 22))
for ec in top_errors:
    print(f"{str(ec):<35} ", end="")
    for g in groups:
        row = plot_err[(plot_err["error_code"] == ec) & (plot_err["group_name"] == g)]
        if not row.empty:
            print(f"{row['cnt'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="  ")
        else:
            print(f"{'0':>10} {'0.00%':>8}", end="  ")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Error code comparison chart

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 7))
error_list = plot_err.groupby("error_code")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(error_list))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = plot_err[plot_err["group_name"] == grp].set_index("error_code").reindex(error_list)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.002, y[j] + i * h, f"{pct:.1%} (n={cnt:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels([str(e) for e in error_list], fontsize=9)
ax.set_title("FR — Top Error Codes (share of failed attempts)", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Response code distribution — A vs B

# COMMAND ----------

fr_resp = (
    df_fr.groupBy("group_name", "response_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_grp = fr_resp.groupby("group_name")["cnt"].sum()
top_resp = fr_resp.groupby("response_code")["cnt"].sum().nlargest(15).index
plot_resp = fr_resp[fr_resp["response_code"].isin(top_resp)].copy()
plot_resp["pct"] = plot_resp.apply(
    lambda r: r["cnt"] / total_by_grp.get(r["group_name"], 1), axis=1,
)

fig, ax = plt.subplots(figsize=(14, 7))
resp_list = plot_resp.groupby("response_code")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(resp_list))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = plot_resp[plot_resp["group_name"] == grp].set_index("response_code").reindex(resp_list)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.002, y[j] + i * h, f"{pct:.1%} (n={cnt:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels([str(r) for r in resp_list], fontsize=9)
ax.set_title("FR — Top Response Codes (share of all attempts)", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Error codes unique to or significantly more frequent in B

# COMMAND ----------

error_pivot = fr_errors.pivot_table(
    index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
)

if g_a is not None and g_b is not None and g_a in error_pivot.columns and g_b in error_pivot.columns:
    error_pivot["total"] = error_pivot.sum(axis=1)
    error_pivot["pct_A"] = error_pivot[g_a] / total_errors_by_grp.get(g_a, 1)
    error_pivot["pct_B"] = error_pivot[g_b] / total_errors_by_grp.get(g_b, 1)
    error_pivot["pct_delta_pp"] = (error_pivot["pct_B"] - error_pivot["pct_A"]) * 100
    error_pivot = error_pivot.sort_values("pct_delta_pp", ascending=False)

    print("Error codes MORE frequent in B (top 15 by delta):")
    print(f"{'Error Code':<40} {'n_A':>8} {'pct_A':>8} {'n_B':>8} {'pct_B':>8} {'Δ(pp)':>8}")
    print("-" * 80)
    for ec, r in error_pivot.head(15).iterrows():
        print(f"{str(ec):<40} {r[g_a]:>8,.0f} {r['pct_A']:>8.2%} {r[g_b]:>8,.0f} {r['pct_B']:>8.2%} {r['pct_delta_pp']:>+8.2f}")

    print(f"\nError codes LESS frequent in B (top 5 by delta):")
    for ec, r in error_pivot.tail(5).iterrows():
        print(f"{str(ec):<40} {r[g_a]:>8,.0f} {r['pct_A']:>8.2%} {r[g_b]:>8,.0f} {r['pct_B']:>8.2%} {r['pct_delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Payment attempt status distribution

# COMMAND ----------

status_dist = (
    df_fr.groupBy("group_name", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_grp_status = status_dist.groupby("group_name")["cnt"].sum()
status_dist["pct"] = status_dist.apply(
    lambda r: r["cnt"] / total_by_grp_status.get(r["group_name"], 1), axis=1,
)

fig, ax = plt.subplots(figsize=(14, 6))
statuses = status_dist.groupby("payment_attempt_status")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(statuses))
h = 0.35

for i, grp in enumerate(groups):
    grp_data = status_dist[status_dist["group_name"] == grp].set_index("payment_attempt_status").reindex(statuses)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.002, y[j] + i * h, f"{pct:.1%} (n={cnt:,.0f})",
                    va="center", fontsize=7)

ax.set_yticks(y + h / 2)
ax.set_yticklabels(statuses, fontsize=9)
ax.set_title("FR — Payment Attempt Status Distribution", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — 3DS Deep Dive for FR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 3DS challenge outcome breakdown

# COMMAND ----------

fr_3ds_outcomes = (
    df_fr
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .withColumn("three_ds_path", F.when(F.col("_challenged"), "Challenged").otherwise("Not Challenged"))
    .groupBy("group_name", "three_ds_path", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_grp_path = fr_3ds_outcomes.groupby(["group_name", "three_ds_path"])["cnt"].sum().to_dict()
fr_3ds_outcomes["pct_of_path"] = fr_3ds_outcomes.apply(
    lambda r: r["cnt"] / total_by_grp_path.get((r["group_name"], r["three_ds_path"]), 1), axis=1,
)

for path in ["Challenged", "Not Challenged"]:
    subset = fr_3ds_outcomes[fr_3ds_outcomes["three_ds_path"] == path]
    if subset.empty:
        continue
    print(f"\n{'=' * 70}")
    print(f"FR — {path} attempts — status breakdown:")
    print(f"{'=' * 70}")
    statuses = subset.groupby("payment_attempt_status")["cnt"].sum().sort_values(ascending=False).index
    print(f"{'Status':<30}", end="")
    for g in groups:
        print(f"  {'cnt_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (30 + len(groups) * 22))
    for s in statuses:
        print(f"{str(s):<30}", end="")
        for g in groups:
            row = subset[(subset["payment_attempt_status"] == s) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['cnt'].values[0]:>10,} {row['pct_of_path'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 3DS challenge error codes — what fails after challenge in B?

# COMMAND ----------

fr_3ds_errors = (
    df_fr
    .filter(F.col("challenge_issued").cast("boolean"))
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code", "response_code", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_3ds_fails = fr_3ds_errors.groupby("group_name")["cnt"].sum()

print("FR — Failed 3DS-challenged attempts — error breakdown:")
print(f"{'Error Code':<35} {'Resp Code':<15} {'Status':<20}", end="")
for g in groups:
    print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
print()
print("-" * (70 + len(groups) * 20))

combined_key = fr_3ds_errors.groupby(["error_code", "response_code", "payment_attempt_status"])["cnt"].sum()
for (ec, rc, st), _ in combined_key.sort_values(ascending=False).head(20).items():
    print(f"{str(ec):<35} {str(rc):<15} {str(st):<20}", end="")
    for g in groups:
        row = fr_3ds_errors[
            (fr_3ds_errors["error_code"] == ec) &
            (fr_3ds_errors["response_code"] == rc) &
            (fr_3ds_errors["payment_attempt_status"] == st) &
            (fr_3ds_errors["group_name"] == g)
        ]
        if not row.empty:
            cnt = row["cnt"].values[0]
            pct = cnt / total_3ds_fails.get(g, 1)
            print(f"  {cnt:>8,} {pct:>8.2%}", end="")
        else:
            print(f"  {'0':>8} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Fraud pre-auth result for FR

# COMMAND ----------

fr_fraud = (
    df_fr
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("is_successful").alias("successes"),
    )
    .toPandas()
)

total_by_grp_fraud = fr_fraud.groupby("group_name")["cnt"].sum()
fr_fraud["pct"] = fr_fraud.apply(
    lambda r: r["cnt"] / total_by_grp_fraud.get(r["group_name"], 1), axis=1,
)
fr_fraud["sr"] = fr_fraud["successes"] / fr_fraud["cnt"]

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
fraud_results = fr_fraud.groupby("fraud_pre_auth_result")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(fraud_results))
h = 0.35

for ax_idx, (col, title) in enumerate([("pct", "Share of Attempts"), ("sr", "Success Rate")]):
    for i, grp in enumerate(groups):
        grp_data = fr_fraud[fr_fraud["group_name"] == grp].set_index("fraud_pre_auth_result").reindex(fraud_results)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, cnt) in enumerate(zip(grp_data[col].fillna(0), grp_data["cnt"].fillna(0))):
            if cnt > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={cnt:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels([str(f) for f in fraud_results], fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("FR — Fraud Pre-Auth Result by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Processor & Payment Method Variant

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Success rate by payment processor (FR)

# COMMAND ----------

fr_proc = (
    df_fr
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.col("challenge_issued").cast("boolean").cast("int")).alias("challenges"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
proc_list = fr_proc.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(proc_list))
h = 0.35

for ax_idx, (col, title) in enumerate([("sr", "Attempt SR"), ("challenge_rate", "3DS Challenge Rate")]):
    for i, grp in enumerate(groups):
        grp_data = fr_proc[fr_proc["group_name"] == grp].set_index("payment_processor").reindex(proc_list)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={vol:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(proc_list, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("FR — Processor Performance by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Success rate by payment method variant (FR)

# COMMAND ----------

fr_pmv = (
    df_fr
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "payment_method_variant")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .withColumn("challenge_sr", F.when(F.col("challenges") > 0, F.col("challenge_successes") / F.col("challenges")))
    .toPandas()
)

print("FR — Payment Method Variant Breakdown:")
print(f"{'PMV':<20} {'Group':<20} {'Attempts':>10} {'SR':>8} {'ChRate':>8} {'ChSR':>8}")
print("-" * 80)
for pmv in fr_pmv.groupby("payment_method_variant")["attempts"].sum().sort_values(ascending=False).index:
    for g in groups:
        row = fr_pmv[(fr_pmv["payment_method_variant"] == pmv) & (fr_pmv["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
            print(f"{str(pmv):<20} {g:<20} {r['attempts']:>10,.0f} {r['sr']:>8.2%} {r['challenge_rate']:>8.2%} {ch_sr:>8}")

fig, axes = plt.subplots(1, 3, figsize=(20, 6))
pmv_list = fr_pmv.groupby("payment_method_variant")["attempts"].sum().nlargest(6).sort_values(ascending=True).index.tolist()
y = np.arange(len(pmv_list))
h = 0.35

for ax_idx, (col, title) in enumerate([("sr", "Attempt SR"), ("challenge_rate", "3DS Challenge Rate"), ("challenge_sr", "SR When Challenged")]):
    for i, grp in enumerate(groups):
        grp_data = fr_pmv[fr_pmv["group_name"] == grp].set_index("payment_method_variant").reindex(pmv_list)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%}",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(pmv_list, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=9)
fig.suptitle("FR — Payment Method Variant × Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Processor × 3DS cross-tabulation for FR

# COMMAND ----------

fr_proc_3ds = (
    df_fr
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .withColumn("path", F.when(F.col("_challenged"), "3DS").otherwise("No 3DS"))
    .groupBy("group_name", "payment_processor", "path")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("payment_processor", "path", "group_name")
    .toPandas()
)

print("FR — Processor × 3DS Path × Variant:")
print(f"{'Processor':<20} {'Path':<10} {'Group':<20} {'Attempts':>10} {'Successes':>10} {'SR':>8}")
print("-" * 85)
for proc in fr_proc_3ds["payment_processor"].unique():
    for path in ["No 3DS", "3DS"]:
        for g in groups:
            row = fr_proc_3ds[
                (fr_proc_3ds["payment_processor"] == proc) &
                (fr_proc_3ds["path"] == path) &
                (fr_proc_3ds["group_name"] == g)
            ]
            if not row.empty:
                r = row.iloc[0]
                print(f"{str(proc):<20} {path:<10} {g:<20} {r['attempts']:>10,.0f} {r['successes']:>10,.0f} {r['sr']:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Daily Trend

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Daily attempt SR for FR by variant

# COMMAND ----------

fr_daily = (
    df_fr
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("attempt_date", "group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges") / F.col("attempts"))
    .withColumn("challenge_sr", F.when(F.col("challenges") > 0, F.col("challenge_successes") / F.col("challenges")))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(2, 2, figsize=(18, 10))

for ax_idx, (col, title) in enumerate([
    ("sr", "Daily Attempt SR"),
    ("challenge_rate", "Daily 3DS Challenge Rate"),
    ("challenge_sr", "Daily SR When Challenged"),
    ("attempts", "Daily Volume"),
]):
    ax = axes[ax_idx // 2][ax_idx % 2]
    for i, (grp, grp_data) in enumerate(fr_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        if col == "attempts":
            ax.bar(grp_data["attempt_date"].values, grp_data[col].values,
                   alpha=0.5, color=color, label=grp, width=0.4)
        else:
            ax.plot(grp_data["attempt_date"], grp_data[col], color=color,
                    linewidth=1.5, label=grp, marker=".", markersize=4)
    ax.set_title(f"FR — {title}", fontsize=12, fontweight="bold")
    if col != "attempts":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Payment References for Primer Investigation
# MAGIC
# MAGIC This section extracts specific `payment_provider_reference` IDs for failed
# MAGIC attempts in variant B (FR) to enable direct lookup in Primer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Failed B-variant 3DS-challenged attempts (FR) — payment references

# COMMAND ----------

fr_b_3ds_failed = (
    df_fr
    .filter(F.col("group_name") == g_b)
    .filter(F.col("challenge_issued").cast("boolean"))
    .filter(F.col("is_successful") == 0)
    .select(
        "payment_provider_reference",
        "customer_system_attempt_reference",
        "payment_attempt_timestamp",
        "payment_processor",
        "payment_method_variant",
        "payment_attempt_status",
        "error_code",
        "response_code",
        "fraud_pre_auth_result",
        "customer_attempt_rank",
        "system_attempt_rank",
    )
    .orderBy(F.desc("payment_attempt_timestamp"))
)

fr_b_3ds_failed_count = fr_b_3ds_failed.count()
print(f"Total failed 3DS-challenged attempts in {g_b} (FR): {fr_b_3ds_failed_count:,}")
print(f"\nShowing up to 200 payment references for Primer investigation:\n")

display(fr_b_3ds_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Failed B-variant non-3DS attempts (FR) — payment references

# COMMAND ----------

fr_b_non3ds_failed = (
    df_fr
    .filter(F.col("group_name") == g_b)
    .filter(~F.col("challenge_issued").cast("boolean"))
    .filter(F.col("is_successful") == 0)
    .select(
        "payment_provider_reference",
        "customer_system_attempt_reference",
        "payment_attempt_timestamp",
        "payment_processor",
        "payment_method_variant",
        "payment_attempt_status",
        "error_code",
        "response_code",
        "fraud_pre_auth_result",
        "customer_attempt_rank",
        "system_attempt_rank",
    )
    .orderBy(F.desc("payment_attempt_timestamp"))
)

fr_b_non3ds_failed_count = fr_b_non3ds_failed.count()
print(f"Total failed non-3DS attempts in {g_b} (FR): {fr_b_non3ds_failed_count:,}")
print(f"\nShowing up to 200 payment references for Primer investigation:\n")

display(fr_b_non3ds_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Payment reference lists grouped by error code (B variant, FR)
# MAGIC
# MAGIC For each top error code in B, provide a sample of `payment_provider_reference`
# MAGIC values for targeted Primer investigation.

# COMMAND ----------

fr_b_failed_refs = (
    df_fr
    .filter(F.col("group_name") == g_b)
    .filter(F.col("is_successful") == 0)
    .groupBy("error_code")
    .agg(
        F.count("*").alias("cnt"),
        F.collect_list("payment_provider_reference").alias("refs"),
    )
    .orderBy(F.desc("cnt"))
    .toPandas()
)

print("FR B-variant failed attempts — sample payment_provider_references by error code:")
print("=" * 100)
for _, r in fr_b_failed_refs.head(10).iterrows():
    refs = r["refs"][:20]
    print(f"\n  Error: {r['error_code']}  (n={r['cnt']:,})")
    print(f"  Sample references ({len(refs)}):")
    for ref in refs:
        print(f"    {ref}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Side-by-side comparison — same error code, A vs B references
# MAGIC
# MAGIC Pick the top error codes that are disproportionately higher in B and provide
# MAGIC references from both A and B for direct comparison in Primer.

# COMMAND ----------

if g_a is not None and g_b is not None:
    top_b_errors = (
        fr_errors[fr_errors["group_name"] == g_b]
        .nlargest(5, "cnt")["error_code"]
        .tolist()
    )

    for ec in top_b_errors:
        print(f"\n{'=' * 100}")
        print(f"Error Code: {ec}")
        print(f"{'=' * 100}")

        for grp in [g_a, g_b]:
            refs_df = (
                df_fr
                .filter(F.col("group_name") == grp)
                .filter(F.col("is_successful") == 0)
                .filter(F.col("error_code") == ec)
                .select(
                    "payment_provider_reference",
                    "payment_processor",
                    "payment_method_variant",
                    "response_code",
                    "payment_attempt_status",
                    F.col("challenge_issued").cast("boolean").alias("challenged"),
                )
                .limit(10)
                .toPandas()
            )
            cnt = df_fr.filter(
                (F.col("group_name") == grp) &
                (F.col("is_successful") == 0) &
                (F.col("error_code") == ec)
            ).count()
            print(f"\n  {grp} — {cnt:,} failed attempts with this error")
            if not refs_df.empty:
                print(refs_df.to_string(index=False))
            else:
                print("  (no attempts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 Exportable reference lists
# MAGIC
# MAGIC Full list of `payment_provider_reference` for B-variant FR failures,
# MAGIC saved as a temp view for easy export or further querying.

# COMMAND ----------

fr_b_all_failed_refs = (
    df_fr
    .filter(F.col("group_name") == g_b)
    .filter(F.col("is_successful") == 0)
    .select(
        "payment_provider_reference",
        "customer_system_attempt_reference",
        "payment_attempt_timestamp",
        "payment_processor",
        "payment_method_variant",
        "payment_attempt_status",
        "error_code",
        "response_code",
        F.col("challenge_issued").cast("boolean").alias("challenged"),
        "fraud_pre_auth_result",
    )
    .orderBy("error_code", F.desc("payment_attempt_timestamp"))
)

fr_b_all_failed_refs.createOrReplaceTempView("fr_b_failed_references")
ref_count = fr_b_all_failed_refs.count()

print(f"Created temp view: fr_b_failed_references")
print(f"Total rows: {ref_count:,}")
print(f"\nYou can now query this view directly:")
print(f"  SELECT * FROM fr_b_failed_references WHERE error_code = '<code>' LIMIT 50")
print(f"  SELECT DISTINCT payment_provider_reference FROM fr_b_failed_references WHERE challenged = true")

display(fr_b_all_failed_refs.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key questions to answer from this notebook:**
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 2.1 | How large is the SR gap for FR specifically? Is the 3DS challenge rate different? |
# MAGIC | 3.4 | Which error codes are disproportionately more frequent in B? |
# MAGIC | 4.1 | When 3DS challenges are issued in B, what share fail? What status do they end up in? |
# MAGIC | 4.2 | What are the specific error/response codes for 3DS failures? |
# MAGIC | 5.3 | Is the issue isolated to a specific processor + 3DS path combination? |
# MAGIC | 6.1 | Is the FR drop consistent across all days or concentrated in a specific period? |
# MAGIC | 7.1–7.4 | Use the payment references to look up specific transactions in Primer |
# MAGIC | 7.5 | Query `fr_b_failed_references` for ad-hoc investigation |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of FR deep-dive analysis*
