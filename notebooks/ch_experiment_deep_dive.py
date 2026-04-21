# Databricks notebook source
# MAGIC %md
# MAGIC # Switzerland (CH) Deep Dive — Payment Orchestration Experiment
# MAGIC
# MAGIC The experiment (restarted 2026-04-02) shows **CH (Switzerland)** performing
# MAGIC significantly worse in variant B (in-house routing). This notebook investigates
# MAGIC root causes with a focus on **provider split, provider performance, attempt types,
# MAGIC and 3DS handling**, and provides `payment_provider_reference` IDs for Primer.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction — CH-only attempts (with funnel columns)
# MAGIC 2. High-level CH comparison — volume, SR, 3DS rates
# MAGIC 3. Provider split & performance — share, SR, funnel by processor
# MAGIC 4. Attempt type & system rank analysis
# MAGIC 5. First customer attempt routing
# MAGIC 6. Error & response code analysis
# MAGIC 7. 3DS deep dive for CH
# MAGIC 8. Processor × payment method variant breakdown
# MAGIC 9. Daily trends
# MAGIC 10. Payment references for Primer investigation

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
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START = "2026-04-02"
TARGET_COUNTRY = "CH"

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test. Returns (z_stat, p_value, delta_pp, ci_lower_pp, ci_upper_pp)."""
    if n1 == 0 or n2 == 0 or (s1 + s2) == 0:
        return 0.0, 1.0, 0.0, 0.0, 0.0
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
# MAGIC # SECTION 1 — Data Extraction (CH only, with funnel columns)

# COMMAND ----------

df = spark.sql(f"""
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
  p.payment_initiated,
  p.sent_to_issuer,
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
df.cache()

total = df.count()
print(f"Total CH payment attempts: {total:,}")

by_group = df.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — High-Level CH Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Overall CH metrics by variant

# COMMAND ----------

summary = (
    df
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
print(f"CH SUMMARY BY VARIANT")
print("=" * 90)
for _, r in summary.iterrows():
    print(f"\n  {r['group_name']}:")
    print(f"    Unique visitors          : {r['visitors']:,.0f}")
    print(f"    Customer attempts        : {r['customer_attempts']:,.0f}")
    print(f"    System attempts          : {r['attempts']:,.0f}")
    print(f"    Attempt SR               : {r['attempt_sr']:.2%}")
    print(f"    Cust Attempt SR          : {r['cust_attempt_sr']:.2%}")
    print(f"    Cart SR                  : {r['cart_sr']:.2%}")
    print(f"    3DS Challenge Rate       : {r['challenge_rate']:.2%}" if pd.notna(r['challenge_rate']) else f"    3DS Challenge Rate       : N/A")
    print(f"    SR when challenged       : {r['challenge_sr']:.2%}  (n={r['challenges_issued']:,.0f})" if pd.notna(r['challenge_sr']) else f"    SR when challenged       : N/A  (n={r['challenges_issued']:,.0f})")
    print(f"    SR when NOT challenged   : {r['no_challenge_sr']:.2%}  (n={r['no_challenge_attempts']:,.0f})" if pd.notna(r['no_challenge_sr']) else f"    SR when NOT challenged   : N/A  (n={r['no_challenge_attempts']:,.0f})")

if len(summary) == 2:
    ra, rb = summary.iloc[0], summary.iloc[1]
    print(f"\n{'─' * 90}")
    print("STATISTICAL TESTS (CH):")
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
    colors = [A_COLOR, B_COLOR][:len(summary)]
    bars = axes[i].bar(summary["group_name"], summary[col], color=colors)
    axes[i].set_title(title, fontweight="bold", fontsize=11)
    axes[i].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, summary[col]):
        axes[i].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + bar.get_height() * 0.005,
                     f"{val:.2%}", ha="center", va="bottom", fontsize=10, fontweight="bold")

fig.suptitle(f"CH — Key Metrics by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Provider Split & Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Processor volume and share by variant

# COMMAND ----------

proc_dist = (
    df
    .groupBy("group_name", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp = proc_dist.groupby("group_name")["attempts"].sum()
proc_dist["pct"] = proc_dist.apply(
    lambda r: r["attempts"] / total_by_grp.get(r["group_name"], 1), axis=1,
)

proc_pivot = proc_dist.pivot_table(
    index="payment_processor", columns="group_name",
    values=["attempts", "pct"], aggfunc="first", fill_value=0,
)

print("CH — Processor Distribution by Variant (all system attempts)")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ share(pp)':>12}", end="")
print()
print("-" * (25 + len(groups) * 22 + 14))

for proc in proc_pivot.index:
    print(f"{str(proc):<25}", end="")
    pcts = []
    for g in groups:
        n = proc_pivot.loc[proc, ("attempts", g)] if ("attempts", g) in proc_pivot.columns else 0
        p = proc_pivot.loc[proc, ("pct", g)] if ("pct", g) in proc_pivot.columns else 0
        pcts.append(p)
        print(f"  {n:>10,.0f} {p:>8.2%}", end="")
    if len(pcts) == 2:
        delta = (pcts[1] - pcts[0]) * 100
        print(f"  {delta:>+12.2f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Processor share comparison chart

# COMMAND ----------

all_procs = proc_dist.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(all_procs))
h = 0.35

fig, axes = plt.subplots(1, 2, figsize=(20, max(5, len(all_procs) * 0.6)))

for ax_idx, (col, title) in enumerate([("pct", "Share of CH Attempts"), ("attempts", "Volume")]):
    for i, grp in enumerate(groups):
        grp_data = proc_dist[proc_dist["group_name"] == grp].set_index("payment_processor").reindex(all_procs)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                fmt = f"{val:.1%}" if col == "pct" else f"{vol:,.0f}"
                axes[ax_idx].text(val + val * 0.02, y[j] + i * h, fmt, va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(all_procs, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    if col == "pct":
        axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("CH — Processor Distribution by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Processor share — statistical tests

# COMMAND ----------

if len(groups) == 2:
    all_proc_names = proc_dist["payment_processor"].unique()
    print("CH — Processor share z-tests (A vs B):")
    print(f"{'Processor':<25} {'share_A':>8} {'share_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
    print("-" * 70)
    for proc_name in sorted(all_proc_names, key=str):
        n_a_total = total_by_grp.get(g_a, 0)
        n_b_total = total_by_grp.get(g_b, 0)
        s_a = proc_dist[(proc_dist["payment_processor"] == proc_name) &
                        (proc_dist["group_name"] == g_a)]["attempts"].sum()
        s_b = proc_dist[(proc_dist["payment_processor"] == proc_name) &
                        (proc_dist["group_name"] == g_b)]["attempts"].sum()
        if n_a_total > 0 and n_b_total > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_total, s_a, n_b_total, s_b)
            print(f"  {str(proc_name):<23} {s_a/n_a_total:>8.2%} {s_b/n_b_total:>8.2%} {delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Success rate by processor — A vs B

# COMMAND ----------

proc_sr = (
    df
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

print("CH — Success Rate by Processor × Variant:")
print(f"{'Processor':<25} {'Group':<20} {'Attempts':>10} {'SR':>8} {'ChRate':>8}")
print("-" * 80)
for proc in proc_sr.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index:
    for g in groups:
        row = proc_sr[(proc_sr["payment_processor"] == proc) & (proc_sr["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            ch_r = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
            print(f"{str(proc):<25} {g:<20} {r['attempts']:>10,.0f} {r['sr']:>8.2%} {ch_r:>8}")
    print()

if len(groups) == 2:
    print(f"\n{'─' * 90}")
    print("SR Z-TESTS BY PROCESSOR:")
    for proc in proc_sr.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index:
        a_row = proc_sr[(proc_sr["payment_processor"] == proc) & (proc_sr["group_name"] == g_a)]
        b_row = proc_sr[(proc_sr["payment_processor"] == proc) & (proc_sr["group_name"] == g_b)]
        if not a_row.empty and not b_row.empty:
            ra_p, rb_p = a_row.iloc[0], b_row.iloc[0]
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                ra_p["attempts"], ra_p["successes"], rb_p["attempts"], rb_p["successes"])
            print(f"  {str(proc):<23} {g_a}={ra_p['sr']:.2%}  {g_b}={rb_p['sr']:.2%}  "
                  f"Δ={delta_pp:+.2f}pp  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 SR by processor — visual

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
proc_list = proc_sr.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(proc_list))
h = 0.35

for ax_idx, (col, title) in enumerate([("sr", "Attempt SR"), ("challenge_rate", "3DS Challenge Rate")]):
    for i, grp in enumerate(groups):
        grp_data = proc_sr[proc_sr["group_name"] == grp].set_index("payment_processor").reindex(proc_list)
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
fig.suptitle("CH — Processor Performance by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Pre-processor funnel by processor × variant

# COMMAND ----------

funnel_by_proc = (
    df
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("initiation_rate", F.col("initiated") / F.col("total_attempts"))
    .withColumn("sent_to_issuer_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .withColumn("auth_rate", F.when(F.col("sent_to_issuer") > 0, F.col("successes") / F.col("sent_to_issuer")))
    .withColumn("overall_sr", F.col("successes") / F.col("total_attempts"))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

print("CH — Payment Funnel by Processor × Variant")
print(f"{'Processor':<20} {'Group':<20} {'Total':>8} {'Init':>8} {'InitR':>7} {'SentIss':>8} {'SentR':>7} {'Succ':>8} {'AuthR':>7} {'SR':>7}")
print("-" * 115)
for proc in funnel_by_proc["payment_processor"].unique():
    for g in groups:
        row = funnel_by_proc[(funnel_by_proc["payment_processor"] == proc) &
                             (funnel_by_proc["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            init_r = f"{r['initiation_rate']:.1%}" if pd.notna(r["initiation_rate"]) else "N/A"
            sent_r = f"{r['sent_to_issuer_rate']:.1%}" if pd.notna(r["sent_to_issuer_rate"]) else "N/A"
            auth_r = f"{r['auth_rate']:.1%}" if pd.notna(r["auth_rate"]) else "N/A"
            sr = f"{r['overall_sr']:.1%}" if pd.notna(r["overall_sr"]) else "N/A"
            print(f"{str(proc):<20} {g:<20} {r['total_attempts']:>8,.0f} {r['initiated']:>8,.0f} {init_r:>7} "
                  f"{r['sent_to_issuer']:>8,.0f} {sent_r:>7} {r['successes']:>8,.0f} {auth_r:>7} {sr:>7}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Customer-attempt-level provider split (system_attempt_rank = 1)

# COMMAND ----------

cust_proc_dist = (
    df
    .filter(F.col("system_attempt_rank") == 1)
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("customer_attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("customer_attempts"))
    .toPandas()
)

cust_total_by_grp = cust_proc_dist.groupby("group_name")["customer_attempts"].sum()
cust_proc_dist["pct"] = cust_proc_dist.apply(
    lambda r: r["customer_attempts"] / cust_total_by_grp.get(r["group_name"], 1), axis=1,
)

print("CH — Customer-Attempt-Level Processor Distribution (system_attempt_rank = 1)")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'share':>8} {'SR':>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ share':>8}", end="")
print()
print("-" * (25 + len(groups) * 30 + 10))

for proc in cust_proc_dist.groupby("payment_processor")["customer_attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(proc):<25}", end="")
    pcts = []
    for g in groups:
        row = cust_proc_dist[(cust_proc_dist["payment_processor"] == proc) & (cust_proc_dist["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            pcts.append(r["pct"])
            print(f"  {r['customer_attempts']:>10,.0f} {r['pct']:>8.2%} {r['sr']:>8.2%}", end="")
        else:
            pcts.append(0)
            print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
    if len(pcts) == 2:
        print(f"  {(pcts[1]-pcts[0])*100:>+8.2f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Attempt Type & System Rank Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Attempt type distribution by variant

# COMMAND ----------

attempt_type_dist = (
    df
    .groupBy("group_name", "attempt_type")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_at = attempt_type_dist.groupby("group_name")["attempts"].sum()
attempt_type_dist["pct"] = attempt_type_dist.apply(
    lambda r: r["attempts"] / total_by_grp_at.get(r["group_name"], 1), axis=1,
)

print("CH — Attempt Type Distribution")
print(f"{'Attempt Type':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 30))
for at in attempt_type_dist.groupby("attempt_type")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(at):<25}", end="")
    for g in groups:
        row = attempt_type_dist[(attempt_type_dist["attempt_type"] == at) &
                                (attempt_type_dist["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {r['sr']:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Processor distribution by attempt type × variant

# COMMAND ----------

proc_by_attempt_type = (
    df
    .groupBy("group_name", "attempt_type", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_at_type = proc_by_attempt_type.groupby(["group_name", "attempt_type"])["attempts"].sum().to_dict()
proc_by_attempt_type["pct"] = proc_by_attempt_type.apply(
    lambda r: r["attempts"] / total_by_grp_at_type.get((r["group_name"], r["attempt_type"]), 1), axis=1,
)

for at in proc_by_attempt_type.groupby("attempt_type")["attempts"].sum().sort_values(ascending=False).head(4).index:
    print(f"\n{'=' * 90}")
    print(f"Attempt Type: {at}")
    print(f"{'=' * 90}")
    subset = proc_by_attempt_type[proc_by_attempt_type["attempt_type"] == at]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 System attempt rank — volume and SR

# COMMAND ----------

rank_dist = (
    df
    .filter(F.col("system_attempt_rank") <= 5)
    .groupBy("group_name", "system_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("system_attempt_rank", "group_name")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 5))
ranks = sorted(rank_dist["system_attempt_rank"].unique())
x = np.arange(len(ranks))
w = 0.35

for i, grp in enumerate(groups):
    grp_data = rank_dist[rank_dist["group_name"] == grp].set_index("system_attempt_rank").reindex(ranks)
    axes[0].bar(x + i * w, grp_data["attempts"].fillna(0), w,
                label=grp, color=A_COLOR if i == 0 else B_COLOR)
    axes[1].bar(x + i * w, grp_data["sr"].fillna(0), w,
                label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (vol, sr) in enumerate(zip(grp_data["attempts"].fillna(0), grp_data["sr"].fillna(0))):
        axes[0].text(x[j] + i * w, vol + vol * 0.02, f"{vol:,.0f}", ha="center", va="bottom", fontsize=7)
        axes[1].text(x[j] + i * w, sr + 0.005, f"{sr:.1%}", ha="center", va="bottom", fontsize=7)

for ax in axes:
    ax.set_xticks(x + w / 2)
    ax.set_xticklabels([str(r) for r in ranks])
    ax.set_xlabel("System Attempt Rank")
    ax.legend(fontsize=9)

axes[0].set_title("Volume by System Attempt Rank", fontweight="bold")
axes[1].set_title("SR by System Attempt Rank", fontweight="bold")
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("CH — System Attempt Rank Analysis", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Processor routing by system attempt rank

# COMMAND ----------

proc_by_rank = (
    df
    .filter(F.col("system_attempt_rank") <= 3)
    .groupBy("group_name", "system_attempt_rank", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_rank = proc_by_rank.groupby(["group_name", "system_attempt_rank"])["attempts"].sum().to_dict()
proc_by_rank["pct"] = proc_by_rank.apply(
    lambda r: r["attempts"] / total_by_grp_rank.get((r["group_name"], r["system_attempt_rank"]), 1), axis=1,
)

for rank in sorted(proc_by_rank["system_attempt_rank"].unique()):
    print(f"\n{'=' * 90}")
    print(f"System Attempt Rank = {rank}")
    print(f"{'=' * 90}")
    subset = proc_by_rank[proc_by_rank["system_attempt_rank"] == rank]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — First Customer Attempt Routing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 First-attempt processor distribution

# COMMAND ----------

first_attempt = (
    df
    .filter((F.col("customer_attempt_rank") == 1) & (F.col("system_attempt_rank") == 1))
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("init_rate", F.col("initiated") / F.col("attempts"))
    .withColumn("sent_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .toPandas()
)

total_first_by_grp = first_attempt.groupby("group_name")["attempts"].sum()
first_attempt["pct_share"] = first_attempt.apply(
    lambda r: r["attempts"] / total_first_by_grp.get(r["group_name"], 1), axis=1,
)

print("CH — First Attempt (rank 1/1) Processor Distribution")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>8} {'share':>7} {'SR':>7} {'InitR':>7} {'SentR':>7}", end="")
print()
print("-" * (25 + len(groups) * 42))
procs_sorted = first_attempt.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
for proc in procs_sorted:
    print(f"{str(proc):<25}", end="")
    for g in groups:
        row = first_attempt[(first_attempt["payment_processor"] == proc) &
                            (first_attempt["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sent_r = f"{r['sent_rate']:.1%}" if pd.notna(r["sent_rate"]) else "N/A"
            print(f"  {r['attempts']:>8,.0f} {r['pct_share']:>7.1%} {r['sr']:>7.1%} {r['init_rate']:>7.1%} {sent_r:>7}", end="")
        else:
            print(f"  {'0':>8} {'0.0%':>7} {'N/A':>7} {'N/A':>7} {'N/A':>7}", end="")
    print()

print(f"\nTotal first attempts: ", end="")
for g in groups:
    print(f"  {g}={total_first_by_grp.get(g, 0):,.0f}", end="")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 First-attempt routing chart

# COMMAND ----------

procs_first = first_attempt.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(procs_first))
h = 0.35

fig, axes = plt.subplots(1, 2, figsize=(18, max(5, len(procs_first) * 0.6)))

for ax_idx, (col, title) in enumerate([("pct_share", "Share of 1st Attempts"), ("sr", "Success Rate")]):
    for i, grp in enumerate(groups):
        grp_data = first_attempt[first_attempt["group_name"] == grp].set_index("payment_processor").reindex(procs_first)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={vol:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(procs_first, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("CH — First Attempt Routing (customer_rank=1, system_rank=1)",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 First-attempt routing by payment method variant (card brand)

# COMMAND ----------

first_by_pmv = (
    df
    .filter((F.col("customer_attempt_rank") == 1) & (F.col("system_attempt_rank") == 1))
    .groupBy("group_name", "payment_method_variant", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_first_by_grp_pmv = first_by_pmv.groupby(["group_name", "payment_method_variant"])["attempts"].sum().to_dict()
first_by_pmv["pct"] = first_by_pmv.apply(
    lambda r: r["attempts"] / total_first_by_grp_pmv.get((r["group_name"], r["payment_method_variant"]), 1), axis=1,
)

top_pmvs = first_by_pmv.groupby("payment_method_variant")["attempts"].sum().nlargest(4).index
for pmv in top_pmvs:
    print(f"\n{'=' * 90}")
    print(f"Payment Method Variant: {pmv} — First Attempt Routing (CH)")
    print(f"{'=' * 90}")
    subset = first_by_pmv[first_by_pmv["payment_method_variant"] == pmv]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Error & Response Code Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Error code distribution — A vs B (CH failed attempts)

# COMMAND ----------

errors = (
    df.filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_errors_by_grp = errors.groupby("group_name")["cnt"].sum()
top_errors = errors.groupby("error_code")["cnt"].sum().nlargest(15).index
plot_err = errors[errors["error_code"].isin(top_errors)].copy()
plot_err["pct"] = plot_err.apply(
    lambda r: r["cnt"] / total_errors_by_grp.get(r["group_name"], 1), axis=1,
)

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
ax.set_title("CH — Top Error Codes (share of failed attempts)", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Error codes more/less frequent in B

# COMMAND ----------

error_pivot = errors.pivot_table(
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
# MAGIC ## 6.3 Error codes per processor — B vs A

# COMMAND ----------

proc_errors = (
    df.filter(F.col("is_successful") == 0)
    .groupBy("group_name", "payment_processor", "error_code", "response_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_proc_fails = proc_errors.groupby(["group_name", "payment_processor"])["cnt"].sum().to_dict()
proc_errors["pct"] = proc_errors.apply(
    lambda r: r["cnt"] / total_proc_fails.get((r["group_name"], r["payment_processor"]), 1), axis=1,
)

top_procs = proc_errors.groupby("payment_processor")["cnt"].sum().nlargest(4).index
for proc in top_procs:
    print(f"\n{'=' * 100}")
    print(f"{proc} — Failed Attempts Error Breakdown (CH)")
    print(f"{'=' * 100}")
    subset = proc_errors[proc_errors["payment_processor"] == proc]
    top_combos = (subset.groupby(["error_code", "response_code"])["cnt"].sum()
                  .sort_values(ascending=False).head(10).index)
    print(f"{'Error Code':<35} {'Resp':>10}", end="")
    for g in groups:
        print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
    print()
    print("-" * (45 + len(groups) * 20))
    for ec, rc in top_combos:
        print(f"{str(ec):<35} {str(rc):>10}", end="")
        for g in groups:
            row = subset[(subset["error_code"] == ec) &
                         (subset["response_code"] == rc) &
                         (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['cnt'].values[0]:>8,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>8} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Payment attempt status distribution

# COMMAND ----------

status_dist = (
    df.groupBy("group_name", "payment_attempt_status")
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
ax.set_title("CH — Payment Attempt Status Distribution", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — 3DS Deep Dive for CH

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 3DS challenge outcome breakdown

# COMMAND ----------

three_ds_outcomes = (
    df
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .withColumn("three_ds_path", F.when(F.col("_challenged"), "Challenged").otherwise("Not Challenged"))
    .groupBy("group_name", "three_ds_path", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_by_grp_path = three_ds_outcomes.groupby(["group_name", "three_ds_path"])["cnt"].sum().to_dict()
three_ds_outcomes["pct_of_path"] = three_ds_outcomes.apply(
    lambda r: r["cnt"] / total_by_grp_path.get((r["group_name"], r["three_ds_path"]), 1), axis=1,
)

for path in ["Challenged", "Not Challenged"]:
    subset = three_ds_outcomes[three_ds_outcomes["three_ds_path"] == path]
    if subset.empty:
        continue
    print(f"\n{'=' * 70}")
    print(f"CH — {path} attempts — status breakdown:")
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
# MAGIC ## 7.2 3DS challenge error codes — what fails after challenge in B?

# COMMAND ----------

three_ds_errors = (
    df
    .filter(F.col("challenge_issued").cast("boolean"))
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code", "response_code", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_3ds_fails = three_ds_errors.groupby("group_name")["cnt"].sum()

print("CH — Failed 3DS-challenged attempts — error breakdown:")
print(f"{'Error Code':<35} {'Resp Code':<15} {'Status':<20}", end="")
for g in groups:
    print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
print()
print("-" * (70 + len(groups) * 20))

combined_key = three_ds_errors.groupby(["error_code", "response_code", "payment_attempt_status"])["cnt"].sum()
for (ec, rc, st), _ in combined_key.sort_values(ascending=False).head(20).items():
    print(f"{str(ec):<35} {str(rc):<15} {str(st):<20}", end="")
    for g in groups:
        row = three_ds_errors[
            (three_ds_errors["error_code"] == ec) &
            (three_ds_errors["response_code"] == rc) &
            (three_ds_errors["payment_attempt_status"] == st) &
            (three_ds_errors["group_name"] == g)
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
# MAGIC ## 7.3 3DS handling by processor

# COMMAND ----------

proc_3ds = (
    df
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("ch_successes"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("no_ch_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_ch_total"),
    )
    .withColumn("challenge_rate", F.col("challenges") / F.col("total"))
    .withColumn("challenge_sr", F.when(F.col("challenges") > 0, F.col("ch_successes") / F.col("challenges")))
    .withColumn("no_challenge_sr", F.when(F.col("no_ch_total") > 0, F.col("no_ch_successes") / F.col("no_ch_total")))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

print("CH — 3DS by Processor × Variant")
print(f"{'Processor':<20} {'Group':<20} {'Total':>8} {'ChRate':>8} {'ChSR':>8} {'NoChSR':>8}")
print("-" * 80)
for _, r in proc_3ds.iterrows():
    ch_rate = f"{r['challenge_rate']:.1%}" if pd.notna(r["challenge_rate"]) else "N/A"
    ch_sr = f"{r['challenge_sr']:.1%}" if pd.notna(r["challenge_sr"]) else "N/A"
    no_ch_sr = f"{r['no_challenge_sr']:.1%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
    print(f"{r['payment_processor']:<20} {r['group_name']:<20} {r['total']:>8,.0f} "
          f"{ch_rate:>8} {ch_sr:>8} {no_ch_sr:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Fraud pre-auth result

# COMMAND ----------

fraud = (
    df
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("is_successful").alias("successes"),
    )
    .toPandas()
)

total_by_grp_fraud = fraud.groupby("group_name")["cnt"].sum()
fraud["pct"] = fraud.apply(
    lambda r: r["cnt"] / total_by_grp_fraud.get(r["group_name"], 1), axis=1,
)
fraud["sr"] = fraud["successes"] / fraud["cnt"]

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
fraud_results = fraud.groupby("fraud_pre_auth_result")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(fraud_results))
h = 0.35

for ax_idx, (col, title) in enumerate([("pct", "Share of Attempts"), ("sr", "Success Rate")]):
    for i, grp in enumerate(groups):
        grp_data = fraud[fraud["group_name"] == grp].set_index("fraud_pre_auth_result").reindex(fraud_results)
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
fig.suptitle("CH — Fraud Pre-Auth Result by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Processor × Payment Method Variant Breakdown

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Payment method variant performance

# COMMAND ----------

pmv = (
    df
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

print("CH — Payment Method Variant Breakdown:")
print(f"{'PMV':<20} {'Group':<20} {'Attempts':>10} {'SR':>8} {'ChRate':>8} {'ChSR':>8}")
print("-" * 80)
for p in pmv.groupby("payment_method_variant")["attempts"].sum().sort_values(ascending=False).index:
    for g in groups:
        row = pmv[(pmv["payment_method_variant"] == p) & (pmv["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
            ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
            print(f"{str(p):<20} {g:<20} {r['attempts']:>10,.0f} {r['sr']:>8.2%} {ch_rate:>8} {ch_sr:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Processor × 3DS cross-tabulation

# COMMAND ----------

proc_3ds_cross = (
    df
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

print("CH — Processor × 3DS Path × Variant:")
print(f"{'Processor':<20} {'Path':<10} {'Group':<20} {'Attempts':>10} {'Successes':>10} {'SR':>8}")
print("-" * 85)
for proc in proc_3ds_cross["payment_processor"].unique():
    for path in ["No 3DS", "3DS"]:
        for g in groups:
            row = proc_3ds_cross[
                (proc_3ds_cross["payment_processor"] == proc) &
                (proc_3ds_cross["path"] == path) &
                (proc_3ds_cross["group_name"] == g)
            ]
            if not row.empty:
                r = row.iloc[0]
                print(f"{str(proc):<20} {path:<10} {g:<20} {r['attempts']:>10,.0f} {r['successes']:>10,.0f} {r['sr']:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Daily Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Daily SR and volume

# COMMAND ----------

daily = (
    df
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
    for i, (grp, grp_data) in enumerate(daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        if col == "attempts":
            ax.bar(grp_data["attempt_date"].values, grp_data[col].values,
                   alpha=0.5, color=color, label=grp, width=0.4)
        else:
            ax.plot(grp_data["attempt_date"], grp_data[col], color=color,
                    linewidth=1.5, label=grp, marker=".", markersize=4)
    ax.set_title(f"CH — {title}", fontsize=12, fontweight="bold")
    if col != "attempts":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Daily processor share

# COMMAND ----------

daily_proc = (
    df
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

daily_total = daily_proc.groupby(["attempt_date", "group_name"])["attempts"].sum().reset_index()
daily_total.columns = ["attempt_date", "group_name", "total"]
daily_proc = daily_proc.merge(daily_total, on=["attempt_date", "group_name"])
daily_proc["pct"] = daily_proc["attempts"] / daily_proc["total"]

for grp in groups:
    grp_daily = daily_proc[daily_proc["group_name"] == grp].copy()
    procs_order = grp_daily.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index.tolist()

    pivot = grp_daily.pivot_table(index="attempt_date", columns="payment_processor",
                                  values="pct", aggfunc="first", fill_value=0)
    pivot = pivot.reindex(columns=procs_order)

    fig, ax = plt.subplots(figsize=(16, 5))
    pivot.plot.area(ax=ax, stacked=True, alpha=0.7,
                    color=COLORS[:len(procs_order)])
    ax.set_title(f"{grp} — Daily Processor Share (CH)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=8, loc="upper right", ncol=2)
    ax.set_xlabel("")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 Daily SR by processor

# COMMAND ----------

daily_proc_sr = (
    df
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

top_procs_for_daily = daily_proc_sr.groupby("payment_processor")["attempts"].sum().nlargest(4).index.tolist()

fig, axes = plt.subplots(len(top_procs_for_daily), 1, figsize=(16, 4 * len(top_procs_for_daily)))
if len(top_procs_for_daily) == 1:
    axes = [axes]

for ax_idx, proc in enumerate(top_procs_for_daily):
    proc_daily = daily_proc_sr[daily_proc_sr["payment_processor"] == proc]
    for i, (grp, grp_data) in enumerate(proc_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[ax_idx].plot(grp_data["attempt_date"], grp_data["sr"], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[ax_idx].set_title(f"{proc} — Daily SR (CH)", fontsize=12, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=9)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Payment References for Primer Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Failed B-variant 3DS-challenged attempts — payment references

# COMMAND ----------

if g_b is not None:
    b_3ds_failed = (
        df
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

    cnt = b_3ds_failed.count()
    print(f"Total failed 3DS-challenged attempts in {g_b} (CH): {cnt:,}")
    print(f"\nShowing up to 200 payment references for Primer investigation:\n")
    display(b_3ds_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.2 Failed B-variant non-3DS attempts — payment references

# COMMAND ----------

if g_b is not None:
    b_non3ds_failed = (
        df
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

    cnt = b_non3ds_failed.count()
    print(f"Total failed non-3DS attempts in {g_b} (CH): {cnt:,}")
    print(f"\nShowing up to 200 payment references for Primer investigation:\n")
    display(b_non3ds_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.3 Payment references by error code (B variant)

# COMMAND ----------

if g_b is not None:
    b_failed_refs = (
        df
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

    print("CH B-variant failed attempts — sample payment_provider_references by error code:")
    print("=" * 100)
    for _, r in b_failed_refs.head(10).iterrows():
        refs = r["refs"][:20]
        print(f"\n  Error: {r['error_code']}  (n={r['cnt']:,})")
        print(f"  Sample references ({len(refs)}):")
        for ref in refs:
            print(f"    {ref}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.4 Side-by-side: A vs B references per top error code

# COMMAND ----------

if g_a is not None and g_b is not None:
    top_b_errors = (
        errors[errors["group_name"] == g_b]
        .nlargest(5, "cnt")["error_code"]
        .tolist()
    )

    for ec in top_b_errors:
        print(f"\n{'=' * 100}")
        print(f"Error Code: {ec}")
        print(f"{'=' * 100}")

        for grp in [g_a, g_b]:
            refs_sample = (
                df
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
            cnt = df.filter(
                (F.col("group_name") == grp) &
                (F.col("is_successful") == 0) &
                (F.col("error_code") == ec)
            ).count()
            print(f"\n  {grp} — {cnt:,} failed attempts with this error")
            if not refs_sample.empty:
                print(refs_sample.to_string(index=False))
            else:
                print("  (no attempts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.5 Exportable temp view

# COMMAND ----------

if g_b is not None:
    b_all_failed_refs = (
        df
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
            "attempt_type",
            "customer_attempt_rank",
            "system_attempt_rank",
            "payment_initiated",
            "sent_to_issuer",
        )
        .orderBy("error_code", F.desc("payment_attempt_timestamp"))
    )

    b_all_failed_refs.createOrReplaceTempView("ch_b_failed_references")
    ref_count = b_all_failed_refs.count()

    print(f"Created temp view: ch_b_failed_references")
    print(f"Total rows: {ref_count:,}")
    print(f"\nYou can now query this view directly:")
    print(f"  SELECT * FROM ch_b_failed_references WHERE error_code = '<code>' LIMIT 50")
    print(f"  SELECT DISTINCT payment_provider_reference FROM ch_b_failed_references WHERE challenged = true")

    display(b_all_failed_refs.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key questions to answer from this notebook:**
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 2.1 | How large is the SR gap for CH? Is the 3DS challenge rate different? |
# MAGIC | 3.1–3.3 | Does the provider split (share of Adyen/Checkout/JPMC) differ between variants? |
# MAGIC | 3.4–3.6 | Which processor has the largest SR gap? Where in the funnel do attempts fail? |
# MAGIC | 3.7 | At the customer-attempt level, how does routing differ? |
# MAGIC | 4.1–4.2 | Does B generate more/fewer retries? Does attempt type distribution differ? |
# MAGIC | 4.3–4.4 | Does B route system retries (rank 2, 3) to different processors? |
# MAGIC | 5.1–5.3 | For the very first attempt, does B route differently? Does it depend on card brand? |
# MAGIC | 6.1–6.3 | Which error codes are disproportionately more frequent in B? Per processor? |
# MAGIC | 7.1–7.3 | Is 3DS handling worse in B? For which processors? |
# MAGIC | 9.1–9.3 | Is the drop consistent across days? Did processor share shift over time? |
# MAGIC | 10.1–10.5 | Payment references for Primer investigation |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of CH deep-dive analysis*
