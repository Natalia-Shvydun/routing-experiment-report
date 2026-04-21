# Databricks notebook source
# MAGIC %md
# MAGIC # US Deep Dive — Fraud Pre-Auth Response Analysis
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (restarted 2026-04-02)
# MAGIC
# MAGIC **Problem:** Test variant shows a consistent shift in `fraud_pre_auth_result`:
# MAGIC - **More** THREE_DS and REFUSE
# MAGIC - **Less** ACCEPT and THREE_DS_EXEMPTION
# MAGIC
# MAGIC This notebook investigates **why** and **in which segments** this happens.
# MAGIC
# MAGIC **Dimensions analysed:**
# MAGIC - Processor (which routing target produces the shift?)
# MAGIC - Card scheme: amex / visa_mc / other (different fraud configs?)
# MAGIC - Currency type: JPM-currency vs non-JPM-currency
# MAGIC - Interactions: processor × card_scheme, processor × currency_type
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Setup & data extraction
# MAGIC 2. Overall fraud result shift & z-tests
# MAGIC 3. Fraud result by processor
# MAGIC 4. Fraud result by card scheme
# MAGIC 5. Fraud result by currency type
# MAGIC 6. Interaction: processor × card_scheme, processor × currency_type
# MAGIC 7. SR conditional on fraud result — is the gap within categories?
# MAGIC 8. Error codes within each fraud result
# MAGIC 9. Daily trends of fraud result shares
# MAGIC 10. Payment references for investigation

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

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

FRAUD_CATEGORIES = ["ACCEPT", "THREE_DS", "THREE_DS_EXEMPTION", "REFUSE"]

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
# MAGIC ## 1.1 Extract US customer attempts

# COMMAND ----------

df_us = spark.sql(f"""
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
  AND p.bin_issuer_country_code = 'US'
""")

df_us = df_us.withColumn(
    "currency_type",
    F.when(F.col("currency").isin(JPM_CURRENCIES), "jpm_currency")
     .otherwise("non_jpm_currency")
).withColumn(
    "card_scheme",
    F.when(F.lower(F.col("payment_method_variant")).contains("amex"), "amex")
     .when(F.lower(F.col("payment_method_variant")).isin("visa", "mastercard"), "visa_mc")
     .otherwise("other")
)

df_cust = df_us.filter(F.col("system_attempt_rank") == 1)
df_cust.cache()

total = df_cust.count()
print(f"Total US customer attempts (system_attempt_rank=1): {total:,}")

by_group = df_cust.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Overall Fraud Result Shift

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Fraud result distribution — Control vs Test

# COMMAND ----------

fraud_overall = (
    df_cust
    .groupBy("group_name", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp = fraud_overall.groupby("group_name")["attempts"].sum().to_dict()
fraud_overall["pct"] = fraud_overall.apply(
    lambda r: r["attempts"] / total_by_grp.get(r["group_name"], 1), axis=1,
)

fraud_types_ordered = fraud_overall.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index

print(f"\n{'=' * 110}")
print(f"US — Overall Fraud Pre-Auth Result Distribution (Customer Attempts)")
print(f"{'=' * 110}")
print(f"{'Fraud Result':<30}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ pct(pp)':>10} {'Δ SR(pp)':>10}", end="")
print()
print("-" * 110)
for fr in fraud_types_ordered:
    print(f"{str(fr):<30}", end="")
    pcts, srs = [], []
    for g in groups:
        row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g)]
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
        print(f"  {(pcts[1]-pcts[0])*100:>+10.3f} {(srs[1]-srs[0])*100:>+10.3f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Z-tests for each fraud result category share

# COMMAND ----------

if len(groups) == 2:
    n_a_total = total_by_grp.get(g_a, 0)
    n_b_total = total_by_grp.get(g_b, 0)

    print(f"\n{'=' * 100}")
    print(f"Z-TESTS — Fraud result SHARE shift (is the share of each category different in test?)")
    print(f"{'=' * 100}")
    print(f"{'Fraud Result':<30} {'share_Ctrl':>10} {'share_Test':>10} {'Δ(pp)':>8} {'95% CI':>18} {'p-value':>10} {'sig':>5}")
    print("-" * 100)
    for fr in fraud_types_ordered:
        a_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_a)]
        b_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_b)]
        s_a = a_row["attempts"].values[0] if not a_row.empty else 0
        s_b = b_row["attempts"].values[0] if not b_row.empty else 0
        if (s_a + s_b) > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_total, s_a, n_b_total, s_b)
            print(f"{str(fr):<30} {s_a/n_a_total:>10.3%} {s_b/n_b_total:>10.3%} "
                  f"{delta_pp:>+8.3f} [{ci_lo:>+7.3f},{ci_hi:>+7.3f}] {p:>10.4f} {sig_label(p):>5}")

    print(f"\n{'=' * 100}")
    print(f"Z-TESTS — SR within each fraud result (is SR different for same fraud result?)")
    print(f"{'=' * 100}")
    print(f"{'Fraud Result':<30} {'SR_Ctrl':>10} {'SR_Test':>10} {'Δ(pp)':>8} {'95% CI':>18} {'p-value':>10} {'sig':>5}")
    print("-" * 100)
    for fr in fraud_types_ordered:
        a_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_a)]
        b_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_b)]
        if not a_row.empty and not b_row.empty:
            ra, rb = a_row.iloc[0], b_row.iloc[0]
            if ra["attempts"] > 0 and rb["attempts"] > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                    ra["attempts"], ra["successes"], rb["attempts"], rb["successes"])
                sr_a = f"{ra['sr']:.3%}" if pd.notna(ra["sr"]) else "N/A"
                sr_b = f"{rb['sr']:.3%}" if pd.notna(rb["sr"]) else "N/A"
                print(f"{str(fr):<30} {sr_a:>10} {sr_b:>10} "
                      f"{delta_pp:>+8.3f} [{ci_lo:>+7.3f},{ci_hi:>+7.3f}] {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Visual — fraud result share comparison

# COMMAND ----------

fr_pivot = fraud_overall.pivot_table(index="fraud_pre_auth_result", columns="group_name",
                                     values="pct", aggfunc="first", fill_value=0)

fig, ax = plt.subplots(figsize=(14, 6))
fr_list = [str(f) for f in fraud_types_ordered]
x = np.arange(len(fr_list))
w = 0.35
for i, g in enumerate(groups):
    vals = [fr_pivot.loc[f, g] if f in fr_pivot.index else 0 for f in fraud_types_ordered]
    bars = ax.bar(x + i * w, vals, w, label=g, color=A_COLOR if i == 0 else B_COLOR)
    for bar, val in zip(bars, vals):
        if val > 0.001:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                    f"{val:.2%}", ha="center", va="bottom", fontsize=8, fontweight="bold")
ax.set_xticks(x + w / 2)
ax.set_xticklabels(fr_list, rotation=30, ha="right", fontsize=9)
ax.set_ylabel("Share of customer attempts")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
ax.set_title("US — Fraud Pre-Auth Result Share — Control vs Test", fontsize=13, fontweight="bold")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Fraud Result by Processor

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Fraud result distribution per processor

# COMMAND ----------

fraud_by_proc = (
    df_cust
    .groupBy("group_name", "payment_processor", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

proc_total = fraud_by_proc.groupby(["group_name", "payment_processor"])["attempts"].sum().to_dict()
fraud_by_proc["pct"] = fraud_by_proc.apply(
    lambda r: r["attempts"] / proc_total.get((r["group_name"], r["payment_processor"]), 1), axis=1,
)

top_procs = fraud_by_proc.groupby("payment_processor")["attempts"].sum().nlargest(6).index

for proc in top_procs:
    subset = fraud_by_proc[fraud_by_proc["payment_processor"] == proc]
    fr_order = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"\n{'=' * 110}")
    print(f"PROCESSOR: {proc}")
    print(f"{'=' * 110}")
    print(f"{'Fraud Result':<30}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct(pp)':>10}", end="")
    print()
    print("-" * 110)
    for fr in fr_order:
        print(f"{str(fr):<30}", end="")
        pcts = []
        for g in groups:
            row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
                pcts.append(r["pct"])
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                pcts.append(0)
        if len(pcts) == 2:
            print(f"  {(pcts[1]-pcts[0])*100:>+10.3f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Z-tests — fraud share shift by processor

# COMMAND ----------

if len(groups) == 2:
    for proc in top_procs:
        subset = fraud_by_proc[fraud_by_proc["payment_processor"] == proc]
        n_a_p = proc_total.get((g_a, proc), 0)
        n_b_p = proc_total.get((g_b, proc), 0)
        if n_a_p == 0 or n_b_p == 0:
            continue

        print(f"\n{'=' * 100}")
        print(f"Fraud share z-tests — {proc}  (Ctrl: {n_a_p:,}  Test: {n_b_p:,})")
        print(f"{'=' * 100}")
        print(f"{'Fraud Result':<30} {'pct_Ctrl':>9} {'pct_Test':>9} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 80)
        for fr in FRAUD_CATEGORIES:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            if (a_cnt + b_cnt) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_p, a_cnt, n_b_p, b_cnt)
                print(f"{str(fr):<30} {a_cnt/n_a_p:>9.3%} {b_cnt/n_b_p:>9.3%} "
                      f"{delta_pp:>+8.3f} {p:>10.4f} {sig_label(p):>5}")

        other_frs = subset[~subset["fraud_pre_auth_result"].isin(FRAUD_CATEGORIES)]["fraud_pre_auth_result"].unique()
        for fr in sorted(other_frs, key=str):
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            if (a_cnt + b_cnt) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_p, a_cnt, n_b_p, b_cnt)
                print(f"{str(fr):<30} {a_cnt/n_a_p:>9.3%} {b_cnt/n_b_p:>9.3%} "
                      f"{delta_pp:>+8.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Heatmap — Δ fraud share (pp) by processor × fraud result

# COMMAND ----------

if len(groups) == 2:
    heat_data = []
    for proc in top_procs:
        subset = fraud_by_proc[fraud_by_proc["payment_processor"] == proc]
        n_a_p = proc_total.get((g_a, proc), 0)
        n_b_p = proc_total.get((g_b, proc), 0)
        if n_a_p == 0 or n_b_p == 0:
            continue
        row = {"processor": str(proc)}
        for fr in FRAUD_CATEGORIES:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            row[fr] = (b_cnt / n_b_p - a_cnt / n_a_p) * 100
        heat_data.append(row)

    heat_df = pd.DataFrame(heat_data).set_index("processor")

    fig, ax = plt.subplots(figsize=(10, max(4, len(heat_df) * 0.8)))
    im = ax.imshow(heat_df.values, cmap="RdYlGn_r", aspect="auto",
                   vmin=-max(abs(heat_df.values.min()), abs(heat_df.values.max())),
                   vmax=max(abs(heat_df.values.min()), abs(heat_df.values.max())))
    ax.set_xticks(range(len(FRAUD_CATEGORIES)))
    ax.set_xticklabels(FRAUD_CATEGORIES, rotation=30, ha="right", fontsize=10)
    ax.set_yticks(range(len(heat_df)))
    ax.set_yticklabels(heat_df.index, fontsize=10)
    for i in range(len(heat_df)):
        for j in range(len(FRAUD_CATEGORIES)):
            val = heat_df.values[i, j]
            ax.text(j, i, f"{val:+.2f}", ha="center", va="center",
                    fontsize=10, fontweight="bold",
                    color="white" if abs(val) > 2 else "black")
    plt.colorbar(im, ax=ax, label="Δ share (pp, test − ctrl)", shrink=0.8)
    ax.set_title("Fraud Result Share Shift by Processor (pp)", fontsize=13, fontweight="bold")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Fraud Result by Card Scheme

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Fraud result distribution per card scheme

# COMMAND ----------

SCHEME_ORDER = ["visa_mc", "amex", "other"]

fraud_by_scheme = (
    df_cust
    .groupBy("group_name", "card_scheme", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

scheme_total = fraud_by_scheme.groupby(["group_name", "card_scheme"])["attempts"].sum().to_dict()
fraud_by_scheme["pct"] = fraud_by_scheme.apply(
    lambda r: r["attempts"] / scheme_total.get((r["group_name"], r["card_scheme"]), 1), axis=1,
)

for cs in SCHEME_ORDER:
    subset = fraud_by_scheme[fraud_by_scheme["card_scheme"] == cs]
    if subset.empty:
        continue
    fr_order = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"\n{'=' * 110}")
    print(f"CARD SCHEME: {cs.upper()}")
    print(f"{'=' * 110}")
    print(f"{'Fraud Result':<30}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct(pp)':>10}", end="")
    print()
    print("-" * 110)
    for fr in fr_order:
        print(f"{str(fr):<30}", end="")
        pcts = []
        for g in groups:
            row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
                pcts.append(r["pct"])
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                pcts.append(0)
        if len(pcts) == 2:
            print(f"  {(pcts[1]-pcts[0])*100:>+10.3f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Z-tests — fraud share shift by card scheme

# COMMAND ----------

if len(groups) == 2:
    for cs in SCHEME_ORDER:
        n_a_cs = scheme_total.get((g_a, cs), 0)
        n_b_cs = scheme_total.get((g_b, cs), 0)
        if n_a_cs == 0 or n_b_cs == 0:
            continue
        subset = fraud_by_scheme[fraud_by_scheme["card_scheme"] == cs]

        print(f"\n{'=' * 100}")
        print(f"Fraud share z-tests — {cs.upper()}  (Ctrl: {n_a_cs:,}  Test: {n_b_cs:,})")
        print(f"{'=' * 100}")
        print(f"{'Fraud Result':<30} {'pct_Ctrl':>9} {'pct_Test':>9} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 80)
        all_frs = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
        for fr in all_frs:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            if (a_cnt + b_cnt) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_cs, a_cnt, n_b_cs, b_cnt)
                print(f"{str(fr):<30} {a_cnt/n_a_cs:>9.3%} {b_cnt/n_b_cs:>9.3%} "
                      f"{delta_pp:>+8.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Heatmap — Δ fraud share by card scheme

# COMMAND ----------

if len(groups) == 2:
    heat_data = []
    for cs in SCHEME_ORDER:
        n_a_cs = scheme_total.get((g_a, cs), 0)
        n_b_cs = scheme_total.get((g_b, cs), 0)
        if n_a_cs == 0 or n_b_cs == 0:
            continue
        subset = fraud_by_scheme[fraud_by_scheme["card_scheme"] == cs]
        row = {"card_scheme": cs.upper()}
        for fr in FRAUD_CATEGORIES:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            row[fr] = (b_cnt / n_b_cs - a_cnt / n_a_cs) * 100
        heat_data.append(row)

    heat_df = pd.DataFrame(heat_data).set_index("card_scheme")
    vmax = max(abs(heat_df.values.min()), abs(heat_df.values.max()))

    fig, ax = plt.subplots(figsize=(10, max(3, len(heat_df) * 1.2)))
    im = ax.imshow(heat_df.values, cmap="RdYlGn_r", aspect="auto", vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(FRAUD_CATEGORIES)))
    ax.set_xticklabels(FRAUD_CATEGORIES, rotation=30, ha="right", fontsize=10)
    ax.set_yticks(range(len(heat_df)))
    ax.set_yticklabels(heat_df.index, fontsize=10)
    for i in range(len(heat_df)):
        for j in range(len(FRAUD_CATEGORIES)):
            val = heat_df.values[i, j]
            ax.text(j, i, f"{val:+.2f}", ha="center", va="center",
                    fontsize=11, fontweight="bold",
                    color="white" if abs(val) > vmax * 0.5 else "black")
    plt.colorbar(im, ax=ax, label="Δ share (pp)", shrink=0.8)
    ax.set_title("Fraud Result Share Shift by Card Scheme (pp)", fontsize=13, fontweight="bold")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Fraud Result by Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Fraud result distribution per currency type

# COMMAND ----------

fraud_by_ct = (
    df_cust
    .groupBy("group_name", "currency_type", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

ct_total = fraud_by_ct.groupby(["group_name", "currency_type"])["attempts"].sum().to_dict()
fraud_by_ct["pct"] = fraud_by_ct.apply(
    lambda r: r["attempts"] / ct_total.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = fraud_by_ct[fraud_by_ct["currency_type"] == ct]
    if subset.empty:
        continue
    fr_order = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"\n{'=' * 110}")
    print(f"CURRENCY TYPE: {ct.upper()}")
    print(f"{'=' * 110}")
    print(f"{'Fraud Result':<30}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct(pp)':>10}", end="")
    print()
    print("-" * 110)
    for fr in fr_order:
        print(f"{str(fr):<30}", end="")
        pcts = []
        for g in groups:
            row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
                pcts.append(r["pct"])
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                pcts.append(0)
        if len(pcts) == 2:
            print(f"  {(pcts[1]-pcts[0])*100:>+10.3f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Z-tests — fraud share shift by currency type

# COMMAND ----------

if len(groups) == 2:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        n_a_ct = ct_total.get((g_a, ct), 0)
        n_b_ct = ct_total.get((g_b, ct), 0)
        if n_a_ct == 0 or n_b_ct == 0:
            continue
        subset = fraud_by_ct[fraud_by_ct["currency_type"] == ct]

        print(f"\n{'=' * 100}")
        print(f"Fraud share z-tests — {ct.upper()}  (Ctrl: {n_a_ct:,}  Test: {n_b_ct:,})")
        print(f"{'=' * 100}")
        print(f"{'Fraud Result':<30} {'pct_Ctrl':>9} {'pct_Test':>9} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 80)
        all_frs = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
        for fr in all_frs:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            if (a_cnt + b_cnt) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_ct, a_cnt, n_b_ct, b_cnt)
                print(f"{str(fr):<30} {a_cnt/n_a_ct:>9.3%} {b_cnt/n_b_ct:>9.3%} "
                      f"{delta_pp:>+8.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Interaction: Processor × Card Scheme, Processor × Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Processor × card_scheme — fraud share delta (pp) for key categories

# COMMAND ----------

fraud_proc_scheme = (
    df_cust
    .groupBy("group_name", "payment_processor", "card_scheme", "fraud_pre_auth_result")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

ps_total = fraud_proc_scheme.groupby(["group_name", "payment_processor", "card_scheme"])["attempts"].sum().to_dict()

if len(groups) == 2:
    print(f"\n{'=' * 130}")
    print(f"Δ Fraud Share (pp) — Processor × Card Scheme (Test − Control)")
    print(f"{'=' * 130}")
    print(f"{'Processor':<22} {'Scheme':<10} {'n_Ctrl':>8} {'n_Test':>8}", end="")
    for fr in FRAUD_CATEGORIES:
        print(f"  {'Δ'+fr:>18}", end="")
    print()
    print("-" * 130)

    for proc in top_procs:
        for cs in SCHEME_ORDER:
            n_a_ps = ps_total.get((g_a, proc, cs), 0)
            n_b_ps = ps_total.get((g_b, proc, cs), 0)
            if n_a_ps < 30 or n_b_ps < 30:
                continue
            subset = fraud_proc_scheme[
                (fraud_proc_scheme["payment_processor"] == proc) &
                (fraud_proc_scheme["card_scheme"] == cs)
            ]
            print(f"{str(proc):<22} {cs:<10} {n_a_ps:>8,} {n_b_ps:>8,}", end="")
            for fr in FRAUD_CATEGORIES:
                a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
                b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
                delta = (b_cnt / n_b_ps - a_cnt / n_a_ps) * 100
                z, p_val, _, _, _ = two_proportion_z_test(n_a_ps, a_cnt, n_b_ps, b_cnt)
                print(f"  {delta:>+13.3f}{sig_label(p_val):>4}", end="")
            print()
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Processor × currency_type — fraud share delta (pp) for key categories

# COMMAND ----------

fraud_proc_ct = (
    df_cust
    .groupBy("group_name", "payment_processor", "currency_type", "fraud_pre_auth_result")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

pct_total = fraud_proc_ct.groupby(["group_name", "payment_processor", "currency_type"])["attempts"].sum().to_dict()

if len(groups) == 2:
    print(f"\n{'=' * 130}")
    print(f"Δ Fraud Share (pp) — Processor × Currency Type (Test − Control)")
    print(f"{'=' * 130}")
    print(f"{'Processor':<22} {'CurrType':<18} {'n_Ctrl':>8} {'n_Test':>8}", end="")
    for fr in FRAUD_CATEGORIES:
        print(f"  {'Δ'+fr:>18}", end="")
    print()
    print("-" * 130)

    for proc in top_procs:
        for ct in ["jpm_currency", "non_jpm_currency"]:
            n_a_pc = pct_total.get((g_a, proc, ct), 0)
            n_b_pc = pct_total.get((g_b, proc, ct), 0)
            if n_a_pc < 30 or n_b_pc < 30:
                continue
            subset = fraud_proc_ct[
                (fraud_proc_ct["payment_processor"] == proc) &
                (fraud_proc_ct["currency_type"] == ct)
            ]
            print(f"{str(proc):<22} {ct:<18} {n_a_pc:>8,} {n_b_pc:>8,}", end="")
            for fr in FRAUD_CATEGORIES:
                a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
                b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
                delta = (b_cnt / n_b_pc - a_cnt / n_a_pc) * 100
                z, p_val, _, _, _ = two_proportion_z_test(n_a_pc, a_cnt, n_b_pc, b_cnt)
                print(f"  {delta:>+13.3f}{sig_label(p_val):>4}", end="")
            print()
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Three-way: processor × card_scheme × currency_type (top combos only)

# COMMAND ----------

fraud_3way = (
    df_cust
    .groupBy("group_name", "payment_processor", "card_scheme", "currency_type", "fraud_pre_auth_result")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

three_total = fraud_3way.groupby(
    ["group_name", "payment_processor", "card_scheme", "currency_type"]
)["attempts"].sum().to_dict()

if len(groups) == 2:
    combo_sizes = []
    for (g, proc, cs, ct), n in three_total.items():
        if g == g_a:
            n_b_3 = three_total.get((g_b, proc, cs, ct), 0)
            combo_sizes.append({"proc": proc, "scheme": cs, "ct": ct, "n_ctrl": n, "n_test": n_b_3})
    combo_df = pd.DataFrame(combo_sizes)
    combo_df = combo_df[(combo_df["n_ctrl"] >= 50) & (combo_df["n_test"] >= 50)]
    combo_df["total"] = combo_df["n_ctrl"] + combo_df["n_test"]
    combo_df = combo_df.sort_values("total", ascending=False).head(20)

    print(f"\n{'=' * 150}")
    print(f"Δ Fraud Share (pp) — Processor × Card Scheme × Currency Type (top combos, ≥50 per group)")
    print(f"{'=' * 150}")
    print(f"{'Processor':<22} {'Scheme':<10} {'CurrType':<18} {'n_Ctrl':>7} {'n_Test':>7}", end="")
    for fr in FRAUD_CATEGORIES:
        print(f"  {'Δ'+fr:>18}", end="")
    print()
    print("-" * 150)

    for _, combo in combo_df.iterrows():
        proc, cs, ct = combo["proc"], combo["scheme"], combo["ct"]
        n_a_3 = combo["n_ctrl"]
        n_b_3 = combo["n_test"]
        subset = fraud_3way[
            (fraud_3way["payment_processor"] == proc) &
            (fraud_3way["card_scheme"] == cs) &
            (fraud_3way["currency_type"] == ct)
        ]
        print(f"{str(proc):<22} {cs:<10} {ct:<18} {n_a_3:>7,} {n_b_3:>7,}", end="")
        for fr in FRAUD_CATEGORIES:
            a_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_a)]["attempts"].sum()
            b_cnt = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g_b)]["attempts"].sum()
            delta = (b_cnt / n_b_3 - a_cnt / n_a_3) * 100
            z, p_val, _, _, _ = two_proportion_z_test(int(n_a_3), a_cnt, int(n_b_3), b_cnt)
            print(f"  {delta:>+13.3f}{sig_label(p_val):>4}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — SR Conditional on Fraud Result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Is the SR gap present even WITHIN the same fraud result?
# MAGIC
# MAGIC If yes → the problem is not only the fraud shift, but also worse performance after the same decision.

# COMMAND ----------

if len(groups) == 2:
    print(f"\n{'=' * 110}")
    print(f"SR within each fraud result — does the gap persist even for same fraud outcome?")
    print(f"{'=' * 110}")
    print(f"{'Fraud Result':<30} {'n_Ctrl':>8} {'SR_Ctrl':>8} {'n_Test':>8} {'SR_Test':>8} "
          f"{'Δ SR(pp)':>9} {'p-value':>10} {'sig':>5}")
    print("-" * 100)
    for fr in fraud_types_ordered:
        a_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_a)]
        b_row = fraud_overall[(fraud_overall["fraud_pre_auth_result"] == fr) & (fraud_overall["group_name"] == g_b)]
        if not a_row.empty and not b_row.empty:
            ra, rb = a_row.iloc[0], b_row.iloc[0]
            if ra["attempts"] > 0 and rb["attempts"] > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                    ra["attempts"], ra["successes"], rb["attempts"], rb["successes"])
                sr_a = f"{ra['sr']:.3%}" if pd.notna(ra["sr"]) else "N/A"
                sr_b = f"{rb['sr']:.3%}" if pd.notna(rb["sr"]) else "N/A"
                print(f"{str(fr):<30} {ra['attempts']:>8,.0f} {sr_a:>8} {rb['attempts']:>8,.0f} {sr_b:>8} "
                      f"{delta_pp:>+9.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 SR within fraud result × processor

# COMMAND ----------

if len(groups) == 2:
    for fr in FRAUD_CATEGORIES:
        subset = fraud_by_proc[fraud_by_proc["fraud_pre_auth_result"] == fr]
        if subset.empty:
            continue

        print(f"\n{'─' * 100}")
        print(f"SR within {fr} — by processor")
        print(f"{'─' * 100}")
        print(f"{'Processor':<25} {'n_Ctrl':>8} {'SR_Ctrl':>8} {'n_Test':>8} {'SR_Test':>8} "
              f"{'Δ SR(pp)':>9} {'p-value':>10} {'sig':>5}")
        print("-" * 90)
        proc_order = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
        for proc in proc_order:
            a_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]
            b_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra, rb = a_row.iloc[0], b_row.iloc[0]
                if ra["attempts"] >= 20 and rb["attempts"] >= 20:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra["attempts"], ra["successes"], rb["attempts"], rb["successes"])
                    sr_a = f"{ra['sr']:.2%}" if pd.notna(ra["sr"]) else "N/A"
                    sr_b = f"{rb['sr']:.2%}" if pd.notna(rb["sr"]) else "N/A"
                    print(f"{str(proc):<25} {ra['attempts']:>8,.0f} {sr_a:>8} {rb['attempts']:>8,.0f} {sr_b:>8} "
                          f"{delta_pp:>+9.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 SR within fraud result × card scheme

# COMMAND ----------

if len(groups) == 2:
    for fr in FRAUD_CATEGORIES:
        subset = fraud_by_scheme[fraud_by_scheme["fraud_pre_auth_result"] == fr]
        if subset.empty:
            continue

        print(f"\n{'─' * 100}")
        print(f"SR within {fr} — by card scheme")
        print(f"{'─' * 100}")
        print(f"{'Card Scheme':<15} {'n_Ctrl':>8} {'SR_Ctrl':>8} {'n_Test':>8} {'SR_Test':>8} "
              f"{'Δ SR(pp)':>9} {'p-value':>10} {'sig':>5}")
        print("-" * 80)
        for cs in SCHEME_ORDER:
            a_row = subset[(subset["card_scheme"] == cs) & (subset["group_name"] == g_a)]
            b_row = subset[(subset["card_scheme"] == cs) & (subset["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra, rb = a_row.iloc[0], b_row.iloc[0]
                if ra["attempts"] >= 20 and rb["attempts"] >= 20:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra["attempts"], ra["successes"], rb["attempts"], rb["successes"])
                    sr_a = f"{ra['sr']:.2%}" if pd.notna(ra["sr"]) else "N/A"
                    sr_b = f"{rb['sr']:.2%}" if pd.notna(rb["sr"]) else "N/A"
                    print(f"{cs:<15} {ra['attempts']:>8,.0f} {sr_a:>8} {rb['attempts']:>8,.0f} {sr_b:>8} "
                          f"{delta_pp:>+9.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Error Codes Within Each Fraud Result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 What error codes happen after each fraud result?
# MAGIC
# MAGIC Focuses on failed attempts only — helps understand if, e.g.,
# MAGIC THREE_DS failures have different error patterns between control and test.

# COMMAND ----------

fraud_error = (
    df_cust
    .filter(F.col("is_customer_attempt_successful") == 0)
    .groupBy("group_name", "fraud_pre_auth_result", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

fraud_err_total = fraud_error.groupby(["group_name", "fraud_pre_auth_result"])["cnt"].sum().to_dict()

for fr in FRAUD_CATEGORIES:
    subset = fraud_error[fraud_error["fraud_pre_auth_result"] == fr]
    if subset.empty:
        continue

    err_pivot = subset.pivot_table(index="error_code", columns="group_name",
                                   values="cnt", aggfunc="sum", fill_value=0)
    if g_a in err_pivot.columns and g_b in err_pivot.columns:
        total_a_fr = fraud_err_total.get((g_a, fr), 1)
        total_b_fr = fraud_err_total.get((g_b, fr), 1)
        err_pivot["pct_A"] = err_pivot[g_a] / total_a_fr
        err_pivot["pct_B"] = err_pivot[g_b] / total_b_fr
        err_pivot["delta_pp"] = (err_pivot["pct_B"] - err_pivot["pct_A"]) * 100
        err_pivot = err_pivot.sort_values(g_a + g_b if g_a and g_b else "pct_A", ascending=False,
                                          key=lambda x: err_pivot[g_a] + err_pivot[g_b] if g_a and g_b else x)
        err_pivot = err_pivot.head(15)

        print(f"\n{'=' * 100}")
        print(f"Error codes after {fr} (failed customer attempts)")
        print(f"Total failed: Ctrl={total_a_fr:,}  Test={total_b_fr:,}")
        print(f"{'=' * 100}")
        print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
        print("-" * 80)

        err_sorted = err_pivot.sort_values([g_a], ascending=False) if g_a in err_pivot.columns else err_pivot
        for ec, r in err_sorted.iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 REFUSE — detailed error + response code breakdown

# COMMAND ----------

refuse_detail = (
    df_cust
    .filter(F.col("fraud_pre_auth_result") == "REFUSE")
    .groupBy("group_name", "error_code", "response_code")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .toPandas()
)

refuse_total = refuse_detail.groupby("group_name")["attempts"].sum().to_dict()

if len(groups) == 2:
    pivot = refuse_detail.pivot_table(
        index=["error_code", "response_code"], columns="group_name",
        values="attempts", aggfunc="sum", fill_value=0
    )
    pivot["total"] = pivot[g_a] + pivot[g_b]
    pivot["pct_A"] = pivot[g_a] / refuse_total.get(g_a, 1)
    pivot["pct_B"] = pivot[g_b] / refuse_total.get(g_b, 1)
    pivot["delta_pp"] = (pivot["pct_B"] - pivot["pct_A"]) * 100
    pivot = pivot.sort_values("total", ascending=False).head(20)

    print(f"\n{'=' * 110}")
    print(f"REFUSE — error_code × response_code breakdown")
    print(f"Ctrl: {refuse_total.get(g_a, 0):,}   Test: {refuse_total.get(g_b, 0):,}")
    print(f"{'=' * 110}")
    print(f"{'Error Code':<30} {'RespCode':<15} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
    print("-" * 100)
    for (ec, rc), r in pivot.iterrows():
        print(f"{str(ec):<30} {str(rc):<15} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} "
              f"{r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 THREE_DS — error + response code breakdown

# COMMAND ----------

tds_detail = (
    df_cust
    .filter(F.col("fraud_pre_auth_result") == "THREE_DS")
    .groupBy("group_name", "error_code", "response_code")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .toPandas()
)

tds_total = tds_detail.groupby("group_name")["attempts"].sum().to_dict()

if len(groups) == 2:
    pivot = tds_detail.pivot_table(
        index=["error_code", "response_code"], columns="group_name",
        values="attempts", aggfunc="sum", fill_value=0
    )
    pivot["total"] = pivot[g_a] + pivot[g_b]
    pivot["pct_A"] = pivot[g_a] / tds_total.get(g_a, 1)
    pivot["pct_B"] = pivot[g_b] / tds_total.get(g_b, 1)
    pivot["delta_pp"] = (pivot["pct_B"] - pivot["pct_A"]) * 100
    pivot = pivot.sort_values("total", ascending=False).head(20)

    print(f"\n{'=' * 110}")
    print(f"THREE_DS — error_code × response_code breakdown")
    print(f"Ctrl: {tds_total.get(g_a, 0):,}   Test: {tds_total.get(g_b, 0):,}")
    print(f"{'=' * 110}")
    print(f"{'Error Code':<30} {'RespCode':<15} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
    print("-" * 100)
    for (ec, rc), r in pivot.iterrows():
        print(f"{str(ec):<30} {str(rc):<15} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} "
              f"{r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Daily Trends of Fraud Result Shares

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Daily share of each fraud result category

# COMMAND ----------

daily_fraud = (
    df_cust
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "fraud_pre_auth_result")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

daily_total = daily_fraud.groupby(["attempt_date", "group_name"])["cnt"].sum().reset_index()
daily_total.columns = ["attempt_date", "group_name", "total"]
daily_fraud = daily_fraud.merge(daily_total, on=["attempt_date", "group_name"])
daily_fraud["pct"] = daily_fraud["cnt"] / daily_fraud["total"]

fig, axes = plt.subplots(2, 2, figsize=(20, 10))
for idx, fr in enumerate(FRAUD_CATEGORIES):
    ax = axes[idx // 2][idx % 2]
    fr_daily = daily_fraud[daily_fraud["fraud_pre_auth_result"] == fr]
    for i, (grp, grp_data) in enumerate(fr_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        ax.plot(grp_data["attempt_date"], grp_data["pct"], color=color,
                linewidth=1.5, label=grp, marker=".", markersize=3)
    ax.set_title(f"{fr} share", fontsize=11, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=8)
    ax.tick_params(axis="x", rotation=45)

fig.suptitle("US — Daily Fraud Pre-Auth Result Share", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Daily fraud share by top processors

# COMMAND ----------

daily_fraud_proc = (
    df_cust
    .filter(F.col("payment_processor").isin([str(p) for p in top_procs]))
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "payment_processor", "fraud_pre_auth_result")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

daily_fp_total = daily_fraud_proc.groupby(
    ["attempt_date", "group_name", "payment_processor"]
)["cnt"].sum().reset_index()
daily_fp_total.columns = ["attempt_date", "group_name", "payment_processor", "total"]
daily_fraud_proc = daily_fraud_proc.merge(
    daily_fp_total, on=["attempt_date", "group_name", "payment_processor"]
)
daily_fraud_proc["pct"] = daily_fraud_proc["cnt"] / daily_fraud_proc["total"]

for fr in ["THREE_DS", "REFUSE", "ACCEPT", "THREE_DS_EXEMPTION"]:
    fr_data = daily_fraud_proc[daily_fraud_proc["fraud_pre_auth_result"] == fr]
    n_procs = min(len(top_procs), 4)
    fig, axes = plt.subplots(1, n_procs, figsize=(6 * n_procs, 4), squeeze=False)
    for p_idx, proc in enumerate(list(top_procs)[:n_procs]):
        ax = axes[0][p_idx]
        proc_data = fr_data[fr_data["payment_processor"] == proc]
        for i, (grp, grp_data) in enumerate(proc_data.groupby("group_name")):
            color = A_COLOR if i == 0 else B_COLOR
            ax.plot(grp_data["attempt_date"], grp_data["pct"], color=color,
                    linewidth=1.3, label=grp, marker=".", markersize=2)
        ax.set_title(f"{proc}\n{fr} share", fontsize=9, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=7)
        ax.tick_params(axis="x", rotation=45, labelsize=7)

    fig.suptitle(f"Daily {fr} Share by Processor", fontsize=12, fontweight="bold", y=1.04)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 Daily fraud share by card scheme

# COMMAND ----------

daily_fraud_scheme = (
    df_cust
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "card_scheme", "fraud_pre_auth_result")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

daily_fs_total = daily_fraud_scheme.groupby(
    ["attempt_date", "group_name", "card_scheme"]
)["cnt"].sum().reset_index()
daily_fs_total.columns = ["attempt_date", "group_name", "card_scheme", "total"]
daily_fraud_scheme = daily_fraud_scheme.merge(
    daily_fs_total, on=["attempt_date", "group_name", "card_scheme"]
)
daily_fraud_scheme["pct"] = daily_fraud_scheme["cnt"] / daily_fraud_scheme["total"]

for fr in ["THREE_DS", "REFUSE", "ACCEPT", "THREE_DS_EXEMPTION"]:
    fr_data = daily_fraud_scheme[daily_fraud_scheme["fraud_pre_auth_result"] == fr]
    fig, axes = plt.subplots(1, 3, figsize=(18, 4), squeeze=False)
    for cs_idx, cs in enumerate(SCHEME_ORDER):
        ax = axes[0][cs_idx]
        cs_data = fr_data[fr_data["card_scheme"] == cs]
        for i, (grp, grp_data) in enumerate(cs_data.groupby("group_name")):
            color = A_COLOR if i == 0 else B_COLOR
            ax.plot(grp_data["attempt_date"], grp_data["pct"], color=color,
                    linewidth=1.3, label=grp, marker=".", markersize=2)
        ax.set_title(f"{cs.upper()}\n{fr} share", fontsize=9, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=7)
        ax.tick_params(axis="x", rotation=45, labelsize=7)

    fig.suptitle(f"Daily {fr} Share by Card Scheme", fontsize=12, fontweight="bold", y=1.04)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Payment References for Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Test REFUSE attempts — sample for Primer

# COMMAND ----------

if g_b is not None:
    refuse_refs = (
        df_cust
        .filter(F.col("group_name") == g_b)
        .filter(F.col("fraud_pre_auth_result") == "REFUSE")
        .select(
            "payment_provider_reference", "customer_system_attempt_reference",
            "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
            "card_scheme", "currency", "currency_type",
            "payment_attempt_status", "error_code", "response_code",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "customer_attempt_rank",
        )
        .orderBy(F.desc("payment_attempt_timestamp"))
    )
    cnt = refuse_refs.count()
    print(f"Test REFUSE attempts: {cnt:,}")
    display(refuse_refs.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.2 Test THREE_DS failed attempts — sample

# COMMAND ----------

if g_b is not None:
    tds_failed_refs = (
        df_cust
        .filter(F.col("group_name") == g_b)
        .filter(F.col("fraud_pre_auth_result") == "THREE_DS")
        .filter(F.col("is_customer_attempt_successful") == 0)
        .select(
            "payment_provider_reference", "customer_system_attempt_reference",
            "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
            "card_scheme", "currency", "currency_type",
            "payment_attempt_status", "error_code", "response_code",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "customer_attempt_rank",
        )
        .orderBy(F.desc("payment_attempt_timestamp"))
    )
    cnt = tds_failed_refs.count()
    print(f"Test THREE_DS failed attempts: {cnt:,}")
    display(tds_failed_refs.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.3 Exportable temp views

# COMMAND ----------

if g_b is not None:
    for fr, view_name in [("REFUSE", "us_test_refuse"), ("THREE_DS", "us_test_three_ds_failed")]:
        filt = df_cust.filter(F.col("group_name") == g_b).filter(F.col("fraud_pre_auth_result") == fr)
        if fr == "THREE_DS":
            filt = filt.filter(F.col("is_customer_attempt_successful") == 0)
        refs = filt.select(
            "payment_provider_reference", "customer_system_attempt_reference",
            "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
            "card_scheme", "currency", "currency_type",
            "payment_attempt_status", "error_code", "response_code",
            "fraud_pre_auth_result",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "customer_attempt_rank",
        ).orderBy("payment_processor", "error_code", F.desc("payment_attempt_timestamp"))
        refs.createOrReplaceTempView(view_name)
        print(f"Created temp view: {view_name} ({refs.count():,} rows)")

    print(f"\nExample queries:")
    print(f"  SELECT * FROM us_test_refuse WHERE payment_processor ILIKE '%jpmc%' LIMIT 50")
    print(f"  SELECT * FROM us_test_three_ds_failed WHERE card_scheme = 'amex' LIMIT 50")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | Question answered |
# MAGIC |---|---|
# MAGIC | 2 | Overall: how big is the fraud result shift? Which categories changed? Is the SR gap sig. within same fraud result? |
# MAGIC | 3 | **Processor:** which processor(s) drive the THREE_DS/REFUSE increase? Heatmap shows it at a glance. |
# MAGIC | 4 | **Card scheme:** is the shift amex-specific, visa/mc-specific, or universal? |
# MAGIC | 5 | **Currency type:** JPM-currency vs non-JPM — different fraud config? |
# MAGIC | 6 | **Interactions:** processor × scheme, processor × currency_type, and full 3-way — isolate the exact segment. |
# MAGIC | 7 | **SR within fraud result:** even for the SAME fraud decision, is SR worse in test? (routing vs fraud setup issue) |
# MAGIC | 8 | **Error codes after fraud result:** what errors follow THREE_DS and REFUSE? Detailed response_code breakdown. |
# MAGIC | 9 | **Daily trends:** is the shift consistent from day 1, or did it start at a specific point? Per processor and scheme. |
# MAGIC | 10 | **Payment references** for manual investigation in Primer. |
# MAGIC
# MAGIC See also: **`us_matched_pairs_investigation`** notebook for control-success vs test-failure matched pairs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of US fraud response deep-dive analysis*
