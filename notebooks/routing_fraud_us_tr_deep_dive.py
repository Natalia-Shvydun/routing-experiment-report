# Databricks notebook source
# MAGIC %md
# MAGIC # Routing Experiment Deep Dive — Fraud Shift + US & TR Analysis
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (restarted 2026-04-02)
# MAGIC
# MAGIC **Observed problems:**
# MAGIC 1. Attempt SR is significantly lower in Test (~0.38pp)
# MAGIC 2. Error distribution shift: less SUSPECTED_FRAUD, more INSUFFICIENT_FUNDS in Test —
# MAGIC    suggests a difference in the fraud setup between control/test
# MAGIC 3. US and TR are significantly worse in Test
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction (global + US + TR)
# MAGIC 2. Global SR confirmation & error shift
# MAGIC 3. Fraud deep dive — `fraud_pre_auth_result` analysis
# MAGIC 4. Fraud × processor × error interactions
# MAGIC 5. US deep dive — SR, errors, fraud, processor, 3DS, daily, Primer refs
# MAGIC 6. TR deep dive — SR, errors, fraud, processor, 3DS, daily, Primer refs

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Data Extraction

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

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
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
# MAGIC ## 1.1 Global data extraction (all countries)

# COMMAND ----------

df_all = spark.sql(f"""
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
""")
df_all.cache()

total = df_all.count()
print(f"Total payment card attempts (global): {total:,}")

by_group = df_all.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 US and TR subsets

# COMMAND ----------

df_us = df_all.filter(F.col("bin_issuer_country_code") == "US")
df_tr = df_all.filter(F.col("bin_issuer_country_code") == "TR")

df_us.cache()
df_tr.cache()

print(f"US attempts: {df_us.count():,}")
print(f"TR attempts: {df_tr.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Global SR Confirmation & Error Shift

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Overall SR by variant

# COMMAND ----------

global_summary = (
    df_all
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges_issued"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_challenge_attempts"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("no_challenge_successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
    .withColumn("challenge_sr", F.when(F.col("challenges_issued") > 0, F.col("challenge_successes") / F.col("challenges_issued")))
    .withColumn("no_challenge_sr", F.when(F.col("no_challenge_attempts") > 0, F.col("no_challenge_successes") / F.col("no_challenge_attempts")))
    .orderBy("group_name")
    .toPandas()
)

print("=" * 90)
print("GLOBAL ATTEMPT-LEVEL SUMMARY")
print("=" * 90)
for _, r in global_summary.iterrows():
    ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
    ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
    no_ch_sr = f"{r['no_challenge_sr']:.2%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
    print(f"\n  {r['group_name']}:")
    print(f"    Attempts          : {r['attempts']:,.0f}")
    print(f"    Attempt SR        : {r['sr']:.4%}")
    print(f"    3DS Challenge Rate: {ch_rate}")
    print(f"    SR when challenged: {ch_sr}")
    print(f"    SR when NOT chal. : {no_ch_sr}")

if len(global_summary) == 2:
    ra, rb = global_summary.iloc[0], global_summary.iloc[1]
    print(f"\n{'─' * 90}")
    print("STATISTICAL TESTS:")
    for label, n_a, s_a, n_b, s_b in [
        ("Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
        ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
    ]:
        z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
        print(f"  {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
              f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Error code shift — global view

# COMMAND ----------

errors_global = (
    df_all.filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_errors_by_grp = errors_global.groupby("group_name")["cnt"].sum()

error_pivot = errors_global.pivot_table(
    index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
)

if g_a is not None and g_b is not None and g_a in error_pivot.columns and g_b in error_pivot.columns:
    error_pivot["total"] = error_pivot.sum(axis=1)
    error_pivot["pct_A"] = error_pivot[g_a] / total_errors_by_grp.get(g_a, 1)
    error_pivot["pct_B"] = error_pivot[g_b] / total_errors_by_grp.get(g_b, 1)
    error_pivot["pct_delta_pp"] = (error_pivot["pct_B"] - error_pivot["pct_A"]) * 100
    error_pivot = error_pivot.sort_values("pct_delta_pp", ascending=False)

    print("ERROR CODE SHIFT — Test vs Control (sorted by delta):")
    print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
    print("-" * 85)
    for ec, r in error_pivot.head(20).iterrows():
        print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['pct_delta_pp']:>+8.2f}")
    print(f"\n--- LESS frequent in Test (bottom 10) ---")
    for ec, r in error_pivot.tail(10).iterrows():
        print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['pct_delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Focus: INSUFFICIENT_FUNDS vs SUSPECTED_FRAUD — z-tests

# COMMAND ----------

if g_a is not None and g_b is not None:
    total_attempts_a = global_summary[global_summary["group_name"] == g_a]["attempts"].values[0]
    total_attempts_b = global_summary[global_summary["group_name"] == g_b]["attempts"].values[0]

    for error_focus in ["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD", "ERROR", "DO_NOT_HONOR",
                        "AUTHENTICATION_REQUIRED", "EXPIRED_CARD"]:
        n_a = errors_global[(errors_global["error_code"] == error_focus) &
                            (errors_global["group_name"] == g_a)]["cnt"].sum()
        n_b = errors_global[(errors_global["error_code"] == error_focus) &
                            (errors_global["group_name"] == g_b)]["cnt"].sum()
        if (n_a + n_b) > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                total_attempts_a, n_a, total_attempts_b, n_b)
            print(f"  {error_focus:<30}  {g_a}={n_a/total_attempts_a:.3%} (n={n_a:,})  "
                  f"{g_b}={n_b/total_attempts_b:.3%} (n={n_b:,})  "
                  f"Δ={delta_pp:+.3f}pp  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Error shift visualization

# COMMAND ----------

focus_errors = ["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD", "ERROR", "DO_NOT_HONOR",
                "AUTHENTICATION_REQUIRED", "EXPIRED_CARD", "WITHDRAWAL_LIMIT_EXCEEDED",
                "DECLINED", "REFER_TO_CARD_ISSUER", "INVALID_CARD_NUMBER"]

plot_data = errors_global[errors_global["error_code"].isin(focus_errors)].copy()
plot_data["pct"] = plot_data.apply(
    lambda r: r["cnt"] / total_errors_by_grp.get(r["group_name"], 1), axis=1,
)

err_order = plot_data.groupby("error_code")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(err_order))
h = 0.35

fig, ax = plt.subplots(figsize=(14, 7))
for i, grp in enumerate(groups):
    grp_data = plot_data[plot_data["group_name"] == grp].set_index("error_code").reindex(err_order)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h,
            label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.001, y[j] + i * h, f"{pct:.2%} (n={cnt:,.0f})",
                    va="center", fontsize=7)

for ec_highlight in ["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD"]:
    if ec_highlight in err_order:
        idx = err_order.index(ec_highlight)
        ax.axhspan(idx - 0.4, idx + 0.8, alpha=0.1, color=COLORS[3])

ax.set_yticks(y + h / 2)
ax.set_yticklabels([str(e) for e in err_order], fontsize=9)
ax.set_title("Error Code Distribution — Control vs Test (share of failed attempts)", fontsize=13, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Fraud Deep Dive

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 `fraud_pre_auth_result` distribution by variant

# COMMAND ----------

fraud_dist = (
    df_all
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_fraud = fraud_dist.groupby("group_name")["attempts"].sum()
fraud_dist["pct"] = fraud_dist.apply(
    lambda r: r["attempts"] / total_by_grp_fraud.get(r["group_name"], 1), axis=1,
)

print("FRAUD PRE-AUTH RESULT DISTRIBUTION:")
print(f"{'Fraud Result':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ pct(pp)':>10} {'Δ SR(pp)':>10}", end="")
print()
print("-" * (25 + len(groups) * 30 + 22))

for fr in fraud_dist.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(fr):<25}", end="")
    pcts, srs = [], []
    for g in groups:
        row = fraud_dist[(fraud_dist["fraud_pre_auth_result"] == fr) & (fraud_dist["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
            print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
            pcts.append(r["pct"])
            srs.append(r["sr"] if pd.notna(r["sr"]) else 0)
        else:
            print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
            pcts.append(0)
            srs.append(0)
    if len(pcts) == 2:
        print(f"  {(pcts[1]-pcts[0])*100:>+10.2f} {(srs[1]-srs[0])*100:>+10.2f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Fraud pre-auth result — z-tests

# COMMAND ----------

if len(groups) == 2:
    print("FRAUD PRE-AUTH SHARE Z-TESTS (share of all attempts):")
    print("=" * 90)
    for fr in fraud_dist.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
        n_a_total = total_by_grp_fraud.get(g_a, 0)
        n_b_total = total_by_grp_fraud.get(g_b, 0)
        s_a = fraud_dist[(fraud_dist["fraud_pre_auth_result"] == fr) &
                         (fraud_dist["group_name"] == g_a)]["attempts"].sum()
        s_b = fraud_dist[(fraud_dist["fraud_pre_auth_result"] == fr) &
                         (fraud_dist["group_name"] == g_b)]["attempts"].sum()
        if n_a_total > 0 and n_b_total > 0 and (s_a + s_b) > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_total, s_a, n_b_total, s_b)
            print(f"  {str(fr):<25} share: {g_a}={s_a/n_a_total:.3%}  {g_b}={s_b/n_b_total:.3%}  "
                  f"Δ={delta_pp:+.3f}pp  p={p:.4f}{sig_label(p)}")

    print("\nFRAUD PRE-AUTH SR Z-TESTS (SR within each fraud result):")
    print("=" * 90)
    for fr in fraud_dist.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
        a_row = fraud_dist[(fraud_dist["fraud_pre_auth_result"] == fr) & (fraud_dist["group_name"] == g_a)]
        b_row = fraud_dist[(fraud_dist["fraud_pre_auth_result"] == fr) & (fraud_dist["group_name"] == g_b)]
        if not a_row.empty and not b_row.empty:
            ra_f, rb_f = a_row.iloc[0], b_row.iloc[0]
            if ra_f["attempts"] > 0 and rb_f["attempts"] > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                    ra_f["attempts"], ra_f["successes"], rb_f["attempts"], rb_f["successes"])
                print(f"  {str(fr):<25} SR: {g_a}={ra_f['successes']/ra_f['attempts']:.3%}  "
                      f"{g_b}={rb_f['successes']/rb_f['attempts']:.3%}  "
                      f"Δ={delta_pp:+.3f}pp  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Fraud pre-auth result visualization

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
fraud_results = fraud_dist.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(fraud_results))
h = 0.35

for ax_idx, (col, title) in enumerate([("pct", "Share of All Attempts"), ("sr", "Success Rate")]):
    for i, grp in enumerate(groups):
        grp_data = fraud_dist[fraud_dist["group_name"] == grp].set_index("fraud_pre_auth_result").reindex(fraud_results)
        axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, cnt) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if cnt > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={cnt:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels([str(f) for f in fraud_results], fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("Fraud Pre-Auth Result by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Fraud × Processor × Error Interactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Fraud result by processor

# COMMAND ----------

fraud_by_proc = (
    df_all
    .groupBy("group_name", "payment_processor", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_proc = fraud_by_proc.groupby(["group_name", "payment_processor"])["attempts"].sum().to_dict()
fraud_by_proc["pct_of_proc"] = fraud_by_proc.apply(
    lambda r: r["attempts"] / total_by_grp_proc.get((r["group_name"], r["payment_processor"]), 1), axis=1,
)

top_procs = fraud_by_proc.groupby("payment_processor")["attempts"].sum().nlargest(5).index
for proc in top_procs:
    print(f"\n{'=' * 100}")
    print(f"Processor: {proc} — Fraud Pre-Auth Result")
    print(f"{'=' * 100}")
    subset = fraud_by_proc[fraud_by_proc["payment_processor"] == proc]
    fraud_types = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Fraud Result':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30))
    for fr in fraud_types:
        print(f"{str(fr):<25}", end="")
        for g in groups:
            row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct_of_proc']:>8.2%} {sr_val:>8}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Error codes for attempts with fraud_pre_auth_result differences

# COMMAND ----------

fraud_error_cross = (
    df_all
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "fraud_pre_auth_result", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_failed_by_grp_fraud = fraud_error_cross.groupby(["group_name", "fraud_pre_auth_result"])["cnt"].sum().to_dict()
fraud_error_cross["pct"] = fraud_error_cross.apply(
    lambda r: r["cnt"] / total_failed_by_grp_fraud.get((r["group_name"], r["fraud_pre_auth_result"]), 1), axis=1,
)

key_fraud_results = fraud_dist.groupby("fraud_pre_auth_result")["attempts"].sum().nlargest(4).index
for fr in key_fraud_results:
    print(f"\n{'=' * 100}")
    print(f"Fraud Result: {fr} — Error Code Breakdown (failed attempts only)")
    print(f"{'=' * 100}")
    subset = fraud_error_cross[fraud_error_cross["fraud_pre_auth_result"] == fr]
    top_ec = subset.groupby("error_code")["cnt"].sum().sort_values(ascending=False).head(12).index
    print(f"{'Error Code':<35}", end="")
    for g in groups:
        print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
    print()
    print("-" * (35 + len(groups) * 20))
    for ec in top_ec:
        print(f"{str(ec):<35}", end="")
        for g in groups:
            row = subset[(subset["error_code"] == ec) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['cnt'].values[0]:>8,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>8} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Fraud result × error code — what drives INSUFFICIENT_FUNDS increase?

# COMMAND ----------

insuf_by_fraud = (
    df_all
    .filter(F.col("error_code") == "INSUFFICIENT_FUNDS")
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_insuf_by_grp = insuf_by_fraud.groupby("group_name")["cnt"].sum()

print("INSUFFICIENT_FUNDS attempts by fraud_pre_auth_result:")
print(f"{'Fraud Result':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>8} {'pct_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 20))
for fr in insuf_by_fraud.groupby("fraud_pre_auth_result")["cnt"].sum().sort_values(ascending=False).index:
    print(f"{str(fr):<25}", end="")
    for g in groups:
        row = insuf_by_fraud[(insuf_by_fraud["fraud_pre_auth_result"] == fr) &
                              (insuf_by_fraud["group_name"] == g)]
        if not row.empty:
            cnt = row["cnt"].values[0]
            pct = cnt / total_insuf_by_grp.get(g, 1)
            print(f"  {cnt:>8,} {pct:>8.2%}", end="")
        else:
            print(f"  {'0':>8} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 SUSPECTED_FRAUD → what happens to those attempts in Test?

# COMMAND ----------

suspected_fraud_by_variant = (
    df_all
    .filter(F.col("error_code") == "SUSPECTED_FRAUD")
    .groupBy("group_name", "fraud_pre_auth_result", "payment_processor", "response_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

print("SUSPECTED_FRAUD error — breakdown by fraud_pre_auth_result × processor × response_code:")
print(f"{'Fraud Result':<20} {'Processor':<20} {'Resp Code':<15}", end="")
for g in groups:
    print(f"  {'cnt_'+g:>8}", end="")
print()
print("-" * (55 + len(groups) * 12))
top_combos = (suspected_fraud_by_variant
              .groupby(["fraud_pre_auth_result", "payment_processor", "response_code"])["cnt"]
              .sum().sort_values(ascending=False).head(20).index)
for fr, proc, rc in top_combos:
    print(f"{str(fr):<20} {str(proc):<20} {str(rc):<15}", end="")
    for g in groups:
        row = suspected_fraud_by_variant[
            (suspected_fraud_by_variant["fraud_pre_auth_result"] == fr) &
            (suspected_fraud_by_variant["payment_processor"] == proc) &
            (suspected_fraud_by_variant["response_code"] == rc) &
            (suspected_fraud_by_variant["group_name"] == g)
        ]
        if not row.empty:
            print(f"  {row['cnt'].values[0]:>8,}", end="")
        else:
            print(f"  {'0':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Daily fraud result trend — SUSPECTED_FRAUD & INSUFFICIENT_FUNDS rates

# COMMAND ----------

daily_fraud_errors = (
    df_all
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("error_code") == "SUSPECTED_FRAUD", 1).otherwise(0)).alias("suspected_fraud"),
        F.sum(F.when(F.col("error_code") == "INSUFFICIENT_FUNDS", 1).otherwise(0)).alias("insufficient_funds"),
    )
    .withColumn("suspected_fraud_rate", F.col("suspected_fraud") / F.col("total"))
    .withColumn("insufficient_funds_rate", F.col("insufficient_funds") / F.col("total"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(18, 5))

for ax_idx, (col, title) in enumerate([
    ("suspected_fraud_rate", "SUSPECTED_FRAUD Rate (of all attempts)"),
    ("insufficient_funds_rate", "INSUFFICIENT_FUNDS Rate (of all attempts)"),
]):
    for i, (grp, grp_data) in enumerate(daily_fraud_errors.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[ax_idx].plot(grp_data["attempt_date"], grp_data[col], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[ax_idx].set_title(title, fontsize=11, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=9)
    axes[ax_idx].tick_params(axis="x", rotation=45)

fig.suptitle("Daily Error Rate Trends — Control vs Test", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Fraud pre-auth result by country (top countries by delta)

# COMMAND ----------

fraud_by_country = (
    df_all
    .groupBy("group_name", "bin_issuer_country_code", "fraud_pre_auth_result")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_country = fraud_by_country.groupby(["group_name", "bin_issuer_country_code"])["attempts"].sum().to_dict()
fraud_by_country["pct"] = fraud_by_country.apply(
    lambda r: r["attempts"] / total_by_grp_country.get((r["group_name"], r["bin_issuer_country_code"]), 1), axis=1,
)

top_countries = (
    df_all.groupBy("bin_issuer_country_code").count()
    .orderBy(F.desc("count")).limit(15)
    .select("bin_issuer_country_code").toPandas()["bin_issuer_country_code"].tolist()
)

key_fraud_result = "SUSPECTED_FRAUD"
print(f"SUSPECTED_FRAUD share by country — Control vs Test (top 15 countries):")
print(f"{'Country':<10}", end="")
for g in groups:
    print(f"  {'pct_'+g:>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}", end="")
print()
print("-" * (10 + len(groups) * 12 + 25))

for cc in top_countries:
    print(f"{str(cc):<10}", end="")
    pcts = []
    n_totals = []
    for g in groups:
        row = fraud_by_country[(fraud_by_country["bin_issuer_country_code"] == cc) &
                                (fraud_by_country["fraud_pre_auth_result"] == key_fraud_result) &
                                (fraud_by_country["group_name"] == g)]
        total_cc = total_by_grp_country.get((g, cc), 0)
        n_totals.append(total_cc)
        if not row.empty:
            print(f"  {row['pct'].values[0]:>8.2%}", end="")
            pcts.append(row["pct"].values[0])
        else:
            print(f"  {'0.00%':>8}", end="")
            pcts.append(0)
    if len(pcts) == 2 and n_totals[0] > 0 and n_totals[1] > 0:
        s_a_cnt = fraud_by_country[(fraud_by_country["bin_issuer_country_code"] == cc) &
                                    (fraud_by_country["fraud_pre_auth_result"] == key_fraud_result) &
                                    (fraud_by_country["group_name"] == g_a)]["attempts"].sum()
        s_b_cnt = fraud_by_country[(fraud_by_country["bin_issuer_country_code"] == cc) &
                                    (fraud_by_country["fraud_pre_auth_result"] == key_fraud_result) &
                                    (fraud_by_country["group_name"] == g_b)]["attempts"].sum()
        z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_totals[0], s_a_cnt, n_totals[1], s_b_cnt)
        print(f"  {delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — US Deep Dive

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 US — High-level metrics

# COMMAND ----------

def country_deep_dive(df_country, country_code, groups, g_a, g_b):
    """Run a full deep dive for a single country. Prints tables and charts."""

    cc = country_code
    summary = (
        df_country
        .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
        .groupBy("group_name")
        .agg(
            F.count("*").alias("attempts"),
            F.sum("is_successful").alias("successes"),
            F.sum(F.col("_challenged").cast("int")).alias("challenges_issued"),
            F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
            F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_challenge_attempts"),
            F.sum(F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("no_challenge_successes"),
            F.countDistinct("visitor_id").alias("visitors"),
        )
        .withColumn("sr", F.col("successes") / F.col("attempts"))
        .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
        .withColumn("challenge_sr", F.when(F.col("challenges_issued") > 0, F.col("challenge_successes") / F.col("challenges_issued")))
        .withColumn("no_challenge_sr", F.when(F.col("no_challenge_attempts") > 0, F.col("no_challenge_successes") / F.col("no_challenge_attempts")))
        .orderBy("group_name")
        .toPandas()
    )

    print("=" * 90)
    print(f"{cc} SUMMARY BY VARIANT")
    print("=" * 90)
    for _, r in summary.iterrows():
        ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
        ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
        no_ch_sr = f"{r['no_challenge_sr']:.2%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
        print(f"\n  {r['group_name']}:")
        print(f"    Visitors            : {r['visitors']:,.0f}")
        print(f"    System attempts     : {r['attempts']:,.0f}")
        print(f"    Attempt SR          : {r['sr']:.4%}")
        print(f"    3DS Challenge Rate  : {ch_rate}")
        print(f"    SR when challenged  : {ch_sr}")
        print(f"    SR when NOT chal.   : {no_ch_sr}")

    if len(summary) == 2:
        ra, rb = summary.iloc[0], summary.iloc[1]
        print(f"\n{'─' * 90}")
        print(f"STATISTICAL TESTS ({cc}):")
        for label, n_a, s_a, n_b, s_b in [
            ("Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
            ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ]:
            if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
                print(f"  {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
                      f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

    return summary

us_summary = country_deep_dive(df_us, "US", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 US — Fraud pre-auth result

# COMMAND ----------

def country_fraud_analysis(df_country, country_code, groups, g_a, g_b):
    cc = country_code
    fraud = (
        df_country
        .groupBy("group_name", "fraud_pre_auth_result")
        .agg(
            F.count("*").alias("attempts"),
            F.sum("is_successful").alias("successes"),
        )
        .withColumn("sr", F.col("successes") / F.col("attempts"))
        .toPandas()
    )
    total_by_grp = fraud.groupby("group_name")["attempts"].sum()
    fraud["pct"] = fraud.apply(
        lambda r: r["attempts"] / total_by_grp.get(r["group_name"], 1), axis=1,
    )

    print(f"\n{cc} — Fraud Pre-Auth Result:")
    print(f"{'Fraud Result':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30))
    for fr in fraud.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
        print(f"{str(fr):<25}", end="")
        for g in groups:
            row = fraud[(fraud["fraud_pre_auth_result"] == fr) & (fraud["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
        print()

    if len(groups) == 2:
        print(f"\n  Z-tests ({cc} fraud share):")
        for fr in fraud.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
            n_a_t = total_by_grp.get(g_a, 0)
            n_b_t = total_by_grp.get(g_b, 0)
            s_a = fraud[(fraud["fraud_pre_auth_result"] == fr) & (fraud["group_name"] == g_a)]["attempts"].sum()
            s_b = fraud[(fraud["fraud_pre_auth_result"] == fr) & (fraud["group_name"] == g_b)]["attempts"].sum()
            if n_a_t > 0 and n_b_t > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_t, s_a, n_b_t, s_b)
                if p < 0.1:
                    print(f"    {str(fr):<25} {g_a}={s_a/n_a_t:.3%}  {g_b}={s_b/n_b_t:.3%}  "
                          f"Δ={delta_pp:+.3f}pp  p={p:.4f}{sig_label(p)}")

    return fraud

us_fraud = country_fraud_analysis(df_us, "US", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 US — Error codes

# COMMAND ----------

def country_error_analysis(df_country, country_code, groups, g_a, g_b):
    cc = country_code
    errors = (
        df_country.filter(F.col("is_successful") == 0)
        .groupBy("group_name", "error_code")
        .agg(F.count("*").alias("cnt"))
        .toPandas()
    )
    total_err = errors.groupby("group_name")["cnt"].sum()

    err_pivot = errors.pivot_table(
        index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
    )
    if g_a in err_pivot.columns and g_b in err_pivot.columns:
        err_pivot["pct_A"] = err_pivot[g_a] / total_err.get(g_a, 1)
        err_pivot["pct_B"] = err_pivot[g_b] / total_err.get(g_b, 1)
        err_pivot["delta_pp"] = (err_pivot["pct_B"] - err_pivot["pct_A"]) * 100
        err_pivot = err_pivot.sort_values("delta_pp", ascending=False)

        print(f"\n{cc} — Error Code Shift (sorted by delta, top 15 + bottom 5):")
        print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
        print("-" * 80)
        for ec, r in err_pivot.head(15).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")
        print(f"\n  --- Less frequent in Test ---")
        for ec, r in err_pivot.tail(5).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

    return errors

us_errors = country_error_analysis(df_us, "US", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 US — Processor split & SR

# COMMAND ----------

def country_processor_analysis(df_country, country_code, groups, g_a, g_b):
    cc = country_code
    proc = (
        df_country
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
    total_by_grp = proc.groupby("group_name")["attempts"].sum()
    proc["pct_share"] = proc.apply(
        lambda r: r["attempts"] / total_by_grp.get(r["group_name"], 1), axis=1,
    )

    print(f"\n{cc} — Processor Split & Performance:")
    print(f"{'Processor':<20} {'Group':<15} {'Attempts':>10} {'Share':>8} {'SR':>8} {'InitR':>8} {'SentR':>8}")
    print("-" * 85)
    for p in proc.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index:
        for g in groups:
            row = proc[(proc["payment_processor"] == p) & (proc["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_v = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                sent_v = f"{r['sent_rate']:.1%}" if pd.notna(r["sent_rate"]) else "N/A"
                print(f"{str(p):<20} {g:<15} {r['attempts']:>10,.0f} {r['pct_share']:>8.2%} {sr_v:>8} {r['init_rate']:>8.1%} {sent_v:>8}")
        print()

    if len(groups) == 2:
        print(f"  SR z-tests by processor ({cc}):")
        for p in proc.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index:
            a_row = proc[(proc["payment_processor"] == p) & (proc["group_name"] == g_a)]
            b_row = proc[(proc["payment_processor"] == p) & (proc["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra_p, rb_p = a_row.iloc[0], b_row.iloc[0]
                z, pv, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                    ra_p["attempts"], ra_p["successes"], rb_p["attempts"], rb_p["successes"])
                print(f"    {str(p):<20} {g_a}={ra_p['sr']:.3%}  {g_b}={rb_p['sr']:.3%}  "
                      f"Δ={delta_pp:+.3f}pp  p={pv:.4f}{sig_label(pv)}")

    return proc

us_proc = country_processor_analysis(df_us, "US", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 US — Daily SR trend

# COMMAND ----------

def country_daily_trend(df_country, country_code, groups):
    cc = country_code
    daily = (
        df_country
        .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
        .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
        .groupBy("attempt_date", "group_name")
        .agg(
            F.count("*").alias("attempts"),
            F.sum("is_successful").alias("successes"),
            F.sum(F.when(F.col("error_code") == "SUSPECTED_FRAUD", 1).otherwise(0)).alias("suspected_fraud"),
            F.sum(F.when(F.col("error_code") == "INSUFFICIENT_FUNDS", 1).otherwise(0)).alias("insufficient_funds"),
        )
        .withColumn("sr", F.col("successes") / F.col("attempts"))
        .withColumn("suspected_fraud_rate", F.col("suspected_fraud") / F.col("attempts"))
        .withColumn("insufficient_funds_rate", F.col("insufficient_funds") / F.col("attempts"))
        .orderBy("attempt_date")
        .toPandas()
    )

    fig, axes = plt.subplots(2, 2, figsize=(18, 10))
    for ax_idx, (col, title) in enumerate([
        ("sr", f"{cc} — Daily Attempt SR"),
        ("attempts", f"{cc} — Daily Volume"),
        ("suspected_fraud_rate", f"{cc} — Daily SUSPECTED_FRAUD Rate"),
        ("insufficient_funds_rate", f"{cc} — Daily INSUFFICIENT_FUNDS Rate"),
    ]):
        ax = axes[ax_idx // 2][ax_idx % 2]
        for i, (grp, grp_data) in enumerate(daily.groupby("group_name")):
            color = A_COLOR if i == 0 else B_COLOR
            if col == "attempts":
                ax.plot(grp_data["attempt_date"], grp_data[col], color=color,
                        linewidth=1.5, label=grp, marker=".", markersize=4)
            else:
                ax.plot(grp_data["attempt_date"], grp_data[col], color=color,
                        linewidth=1.5, label=grp, marker=".", markersize=4)
        ax.set_title(title, fontsize=11, fontweight="bold")
        if col != "attempts":
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=9)
        ax.tick_params(axis="x", rotation=45)

    fig.tight_layout()
    plt.show()
    return daily

us_daily = country_daily_trend(df_us, "US", groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 US — 3DS & fraud interaction

# COMMAND ----------

def country_3ds_fraud(df_country, country_code, groups):
    cc = country_code
    cross = (
        df_country
        .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
        .withColumn("path", F.when(F.col("_challenged"), "3DS").otherwise("No 3DS"))
        .groupBy("group_name", "path", "fraud_pre_auth_result")
        .agg(
            F.count("*").alias("attempts"),
            F.sum("is_successful").alias("successes"),
        )
        .withColumn("sr", F.col("successes") / F.col("attempts"))
        .toPandas()
    )

    total_by_grp_path = cross.groupby(["group_name", "path"])["attempts"].sum().to_dict()
    cross["pct"] = cross.apply(
        lambda r: r["attempts"] / total_by_grp_path.get((r["group_name"], r["path"]), 1), axis=1,
    )

    for path in ["No 3DS", "3DS"]:
        subset = cross[cross["path"] == path]
        if subset.empty:
            continue
        print(f"\n{'=' * 90}")
        print(f"{cc} — {path} path — Fraud Pre-Auth Result")
        print(f"{'=' * 90}")
        print(f"{'Fraud Result':<25}", end="")
        for g in groups:
            print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
        print()
        print("-" * (25 + len(groups) * 30))
        for fr in subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index:
            print(f"{str(fr):<25}", end="")
            for g in groups:
                row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
                if not row.empty:
                    r = row.iloc[0]
                    sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                    print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
                else:
                    print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
            print()

country_3ds_fraud(df_us, "US", groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 US — Payment references for Primer

# COMMAND ----------

if g_b is not None:
    us_b_failed = (
        df_us
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
            "fraud_pre_auth_result",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "customer_attempt_rank",
            "system_attempt_rank",
        )
        .orderBy(F.desc("payment_attempt_timestamp"))
    )

    cnt = us_b_failed.count()
    print(f"Total failed attempts in {g_b} (US): {cnt:,}")
    print(f"Showing up to 200 for Primer investigation:\n")
    display(us_b_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.8 US — Exportable temp view

# COMMAND ----------

if g_b is not None:
    us_b_all = (
        df_us
        .filter(F.col("group_name") == g_b)
        .filter(F.col("is_successful") == 0)
        .select(
            "payment_provider_reference", "customer_system_attempt_reference",
            "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
            "payment_attempt_status", "error_code", "response_code",
            "fraud_pre_auth_result",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "attempt_type", "customer_attempt_rank", "system_attempt_rank",
        )
        .orderBy("error_code", F.desc("payment_attempt_timestamp"))
    )
    us_b_all.createOrReplaceTempView("us_b_failed_references")
    print(f"Created temp view: us_b_failed_references ({us_b_all.count():,} rows)")
    print(f"  SELECT * FROM us_b_failed_references WHERE error_code = 'INSUFFICIENT_FUNDS' LIMIT 50")
    print(f"  SELECT * FROM us_b_failed_references WHERE error_code = 'SUSPECTED_FRAUD' LIMIT 50")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — TR Deep Dive

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 TR — High-level metrics

# COMMAND ----------

tr_summary = country_deep_dive(df_tr, "TR", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 TR — Fraud pre-auth result

# COMMAND ----------

tr_fraud = country_fraud_analysis(df_tr, "TR", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 TR — Error codes

# COMMAND ----------

tr_errors = country_error_analysis(df_tr, "TR", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 TR — Processor split & SR

# COMMAND ----------

tr_proc = country_processor_analysis(df_tr, "TR", groups, g_a, g_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 TR — Daily SR trend

# COMMAND ----------

tr_daily = country_daily_trend(df_tr, "TR", groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.6 TR — 3DS & fraud interaction

# COMMAND ----------

country_3ds_fraud(df_tr, "TR", groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.7 TR — Payment references for Primer

# COMMAND ----------

if g_b is not None:
    tr_b_failed = (
        df_tr
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
            "fraud_pre_auth_result",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "customer_attempt_rank",
            "system_attempt_rank",
        )
        .orderBy(F.desc("payment_attempt_timestamp"))
    )

    cnt = tr_b_failed.count()
    print(f"Total failed attempts in {g_b} (TR): {cnt:,}")
    print(f"Showing up to 200 for Primer investigation:\n")
    display(tr_b_failed.limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.8 TR — Exportable temp view

# COMMAND ----------

if g_b is not None:
    tr_b_all = (
        df_tr
        .filter(F.col("group_name") == g_b)
        .filter(F.col("is_successful") == 0)
        .select(
            "payment_provider_reference", "customer_system_attempt_reference",
            "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
            "payment_attempt_status", "error_code", "response_code",
            "fraud_pre_auth_result",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "attempt_type", "customer_attempt_rank", "system_attempt_rank",
        )
        .orderBy("error_code", F.desc("payment_attempt_timestamp"))
    )
    tr_b_all.createOrReplaceTempView("tr_b_failed_references")
    print(f"Created temp view: tr_b_failed_references ({tr_b_all.count():,} rows)")
    print(f"  SELECT * FROM tr_b_failed_references WHERE error_code = 'INSUFFICIENT_FUNDS' LIMIT 50")
    print(f"  SELECT * FROM tr_b_failed_references WHERE fraud_pre_auth_result IS NOT NULL LIMIT 50")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key questions this notebook answers:**
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 2.1 | How large is the global SR gap? |
# MAGIC | 2.2–2.4 | Is the error shift (less SUSPECTED_FRAUD, more INSUFFICIENT_FUNDS) confirmed? |
# MAGIC | 3.1–3.3 | Does `fraud_pre_auth_result` distribution differ between variants? |
# MAGIC | 4.1 | Is the fraud setup difference processor-specific? |
# MAGIC | 4.2 | What error codes appear within each fraud result? |
# MAGIC | 4.3–4.4 | Are INSUFFICIENT_FUNDS attempts coming from a different fraud path in Test? |
# MAGIC | 4.5 | Is the error shift consistent over time? |
# MAGIC | 4.6 | Which countries show the largest fraud share differences? |
# MAGIC | 5.1–5.8 | US deep dive: SR, fraud, errors, processors, daily trends, Primer refs |
# MAGIC | 6.1–6.8 | TR deep dive: SR, fraud, errors, processors, daily trends, Primer refs |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of routing fraud & US/TR deep-dive analysis*
