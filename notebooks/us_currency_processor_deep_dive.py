# Databricks notebook source
# MAGIC %md
# MAGIC # US Deep Dive — JPM Currency vs Non-JPM Currency & Processor Split
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (restarted 2026-04-02)
# MAGIC
# MAGIC US-issued card attempts are split into two segments:
# MAGIC - **JPM currency:** CZK, DKK, EUR, GBP, HKD, JPY, MXN, NOK, NZD, PLN, SEK, SGD, USD, ZAR
# MAGIC - **Non-JPM currency:** everything else
# MAGIC
# MAGIC Focus: How do SR, processor split, fraud, and errors differ between
# MAGIC these two currency segments in Control vs Test?
# MAGIC
# MAGIC Card schemes are also segmented: **amex** / **visa_mc** (Visa+Mastercard) / **other**
# MAGIC because they follow different routing paths.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction (US only)
# MAGIC 2. High-level metrics by currency type
# MAGIC 3. Processor split & SR by currency type
# MAGIC 4. Processor share shift — statistical tests
# MAGIC 5. Funnel by processor × currency type
# MAGIC 6. Error & fraud analysis by currency type
# MAGIC 7. Daily trends by currency type
# MAGIC 8. Payment references for Primer
# MAGIC 9. Card scheme analysis (Amex vs Visa/MC vs Other)

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
# MAGIC ## 1.1 Extract US attempts

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
df_us.cache()

total = df_us.count()
print(f"Total US payment card attempts: {total:,}")

by_group = df_us.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

by_currency_type = df_us.groupBy("currency_type").count().orderBy("currency_type").toPandas()
print(f"\nCurrency type split (all system attempts):")
for _, r in by_currency_type.iterrows():
    print(f"  {r['currency_type']}: {r['count']:,} ({r['count']/total:.1%})")

by_scheme = df_us.groupBy("card_scheme").count().orderBy("card_scheme").toPandas()
print(f"\nCard scheme split:")
for _, r in by_scheme.iterrows():
    print(f"  {r['card_scheme']}: {r['count']:,} ({r['count']/total:.1%})")

df_us_cust = df_us.filter(F.col("system_attempt_rank") == 1)
df_us_cust.cache()

total_cust = df_us_cust.count()
print(f"\nCustomer attempts (system_attempt_rank=1): {total_cust:,}")

by_ct_cust = df_us_cust.groupBy("currency_type").count().orderBy("currency_type").toPandas()
print(f"Currency type split (customer attempts only):")
for _, r in by_ct_cust.iterrows():
    print(f"  {r['currency_type']}: {r['count']:,} ({r['count']/total_cust:.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Currency distribution within each type

# COMMAND ----------

currency_dist = (
    df_us
    .groupBy("currency_type", "currency", "group_name")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = currency_dist[currency_dist["currency_type"] == ct]
    if subset.empty:
        continue
    print(f"\n{'=' * 80}")
    print(f"Currency Type: {ct}")
    print(f"{'=' * 80}")
    total_by_grp_ct = subset.groupby("group_name")["attempts"].sum()
    currencies = subset.groupby("currency")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Currency':<10}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (10 + len(groups) * 22))
    for cur in currencies:
        print(f"{str(cur):<10}", end="")
        for g in groups:
            row = subset[(subset["currency"] == cur) & (subset["group_name"] == g)]
            if not row.empty:
                n = row["attempts"].values[0]
                pct = n / total_by_grp_ct.get(g, 1)
                print(f"  {n:>10,} {pct:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — High-Level Metrics by Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 SR and 3DS metrics by currency_type × variant

# COMMAND ----------

summary_by_ct = (
    df_us
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "currency_type")
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
    .orderBy("currency_type", "group_name")
    .toPandas()
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = summary_by_ct[summary_by_ct["currency_type"] == ct]
    print(f"\n{'=' * 90}")
    print(f"US — {ct.upper()}")
    print(f"{'=' * 90}")
    for _, r in subset.iterrows():
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

    if len(subset) == 2:
        ra, rb = subset.iloc[0], subset.iloc[1]
        print(f"\n  Z-TESTS ({ct}):")
        for label, n_a, s_a, n_b, s_b in [
            ("Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
            ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ]:
            if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
                print(f"    {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
                      f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Visual comparison — SR by currency type

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 5))
for ax_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    subset = summary_by_ct[summary_by_ct["currency_type"] == ct].sort_values("group_name")
    colors = [A_COLOR, B_COLOR][:len(subset)]
    bars = axes[ax_idx].bar(subset["group_name"], subset["sr"], color=colors)
    axes[ax_idx].set_title(f"Attempt SR — {ct}", fontweight="bold", fontsize=12)
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, subset["sr"]):
        if pd.notna(val):
            axes[ax_idx].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                              f"{val:.2%}", ha="center", va="bottom", fontsize=11, fontweight="bold")

fig.suptitle("US — Attempt SR by Currency Type × Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Customer-attempt-only metrics by currency_type × variant

# COMMAND ----------

cust_summary_by_ct = (
    df_us_cust
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "currency_type")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges_issued"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_challenge_attempts"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("no_challenge_successes"),
        F.countDistinct("visitor_id").alias("visitors"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
    .withColumn("challenge_sr", F.when(F.col("challenges_issued") > 0, F.col("challenge_successes") / F.col("challenges_issued")))
    .withColumn("no_challenge_sr", F.when(F.col("no_challenge_attempts") > 0, F.col("no_challenge_successes") / F.col("no_challenge_attempts")))
    .orderBy("currency_type", "group_name")
    .toPandas()
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = cust_summary_by_ct[cust_summary_by_ct["currency_type"] == ct]
    print(f"\n{'=' * 90}")
    print(f"US — {ct.upper()} — CUSTOMER ATTEMPTS ONLY (system_attempt_rank=1)")
    print(f"{'=' * 90}")
    for _, r in subset.iterrows():
        ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
        ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
        no_ch_sr = f"{r['no_challenge_sr']:.2%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
        print(f"\n  {r['group_name']}:")
        print(f"    Visitors            : {r['visitors']:,.0f}")
        print(f"    Customer attempts   : {r['attempts']:,.0f}")
        print(f"    Customer Attempt SR : {r['sr']:.4%}")
        print(f"    3DS Challenge Rate  : {ch_rate}")
        print(f"    SR when challenged  : {ch_sr}")
        print(f"    SR when NOT chal.   : {no_ch_sr}")

    if len(subset) == 2:
        ra, rb = subset.iloc[0], subset.iloc[1]
        print(f"\n  Z-TESTS ({ct}, customer attempts):")
        for label, n_a, s_a, n_b, s_b in [
            ("Customer Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
            ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ]:
            if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
                print(f"    {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
                      f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Processor Split & SR by Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Processor distribution — all system attempts

# COMMAND ----------

proc_dist = (
    df_us
    .groupBy("group_name", "currency_type", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_ct = proc_dist.groupby(["group_name", "currency_type"])["attempts"].sum().to_dict()
proc_dist["pct_share"] = proc_dist.apply(
    lambda r: r["attempts"] / total_by_grp_ct.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = proc_dist[proc_dist["currency_type"] == ct]
    print(f"\n{'=' * 100}")
    print(f"US — Processor Distribution — {ct} (all system attempts)")
    print(f"{'=' * 100}")
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'share_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ share':>8} {'Δ SR':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 18))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        shares, srs = [], []
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>10,} {r['pct_share']:>8.2%} {sr_val:>8}", end="")
                shares.append(r["pct_share"])
                srs.append(r["sr"] if pd.notna(r["sr"]) else 0)
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                shares.append(0)
                srs.append(0)
        if len(shares) == 2:
            print(f"  {(shares[1]-shares[0])*100:>+8.2f} {(srs[1]-srs[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Processor distribution — customer attempts only (system_attempt_rank = 1)

# COMMAND ----------

cust_proc_dist = (
    df_us
    .filter(F.col("system_attempt_rank") == 1)
    .groupBy("group_name", "currency_type", "payment_processor")
    .agg(
        F.count("*").alias("customer_attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("customer_attempts"))
    .toPandas()
)

cust_total_by_grp_ct = cust_proc_dist.groupby(["group_name", "currency_type"])["customer_attempts"].sum().to_dict()
cust_proc_dist["pct_share"] = cust_proc_dist.apply(
    lambda r: r["customer_attempts"] / cust_total_by_grp_ct.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = cust_proc_dist[cust_proc_dist["currency_type"] == ct]
    print(f"\n{'=' * 100}")
    print(f"US — Customer Attempt Processor Distribution — {ct} (system_attempt_rank = 1)")
    print(f"{'=' * 100}")
    procs = subset.groupby("payment_processor")["customer_attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'share_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ share':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 10))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        shares = []
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['customer_attempts']:>10,} {r['pct_share']:>8.2%} {sr_val:>8}", end="")
                shares.append(r["pct_share"])
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                shares.append(0)
        if len(shares) == 2:
            print(f"  {(shares[1]-shares[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Processor share charts

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(20, 6))

for ax_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    subset = proc_dist[proc_dist["currency_type"] == ct]
    proc_list = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
    y = np.arange(len(proc_list))
    h = 0.35

    for i, grp in enumerate(groups):
        grp_data = subset[subset["group_name"] == grp].set_index("payment_processor").reindex(proc_list)
        axes[ax_idx].barh(y + i * h, grp_data["pct_share"].fillna(0), h,
                          label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data["pct_share"].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={vol:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(proc_list, fontsize=9)
    axes[ax_idx].set_title(f"Processor Share — {ct}", fontweight="bold", fontsize=11)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[0].legend(fontsize=10)
fig.suptitle("US — Processor Share by Currency Type × Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Processor Share Shift — Statistical Tests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Processor share z-tests per currency type

# COMMAND ----------

if len(groups) == 2:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        subset = proc_dist[proc_dist["currency_type"] == ct]
        total_a = total_by_grp_ct.get((g_a, ct), 0)
        total_b = total_by_grp_ct.get((g_b, ct), 0)
        all_procs = subset["payment_processor"].unique()

        print(f"\n{'=' * 90}")
        print(f"Processor SHARE z-tests — {ct} (all system attempts)")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'share_A':>8} {'share_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        for proc in sorted(all_procs, key=str):
            s_a = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]["attempts"].sum()
            s_b = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]["attempts"].sum()
            if total_a > 0 and total_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(total_a, s_a, total_b, s_b)
                print(f"  {str(proc):<23} {s_a/total_a:>8.2%} {s_b/total_b:>8.2%} "
                      f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Processor SR z-tests per currency type

# COMMAND ----------

if len(groups) == 2:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        subset = proc_dist[proc_dist["currency_type"] == ct]
        all_procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index

        print(f"\n{'=' * 90}")
        print(f"Processor SR z-tests — {ct}")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'SR_A':>8} {'SR_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        for proc in all_procs:
            a_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]
            b_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra_p, rb_p = a_row.iloc[0], b_row.iloc[0]
                if ra_p["attempts"] > 0 and rb_p["attempts"] > 0:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra_p["attempts"], ra_p["successes"], rb_p["attempts"], rb_p["successes"])
                    print(f"  {str(proc):<23} {ra_p['sr']:>8.2%} {rb_p['sr']:>8.2%} "
                          f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Customer-attempt processor share z-tests per currency type

# COMMAND ----------

if len(groups) == 2:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        subset = cust_proc_dist[cust_proc_dist["currency_type"] == ct]
        total_a_c = cust_total_by_grp_ct.get((g_a, ct), 0)
        total_b_c = cust_total_by_grp_ct.get((g_b, ct), 0)
        all_procs = subset["payment_processor"].unique()

        print(f"\n{'=' * 90}")
        print(f"Processor SHARE z-tests — {ct} (CUSTOMER ATTEMPTS ONLY)")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'share_A':>8} {'share_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        for proc in sorted(all_procs, key=str):
            s_a = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]["customer_attempts"].sum()
            s_b = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]["customer_attempts"].sum()
            if total_a_c > 0 and total_b_c > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(total_a_c, s_a, total_b_c, s_b)
                print(f"  {str(proc):<23} {s_a/total_a_c:>8.2%} {s_b/total_b_c:>8.2%} "
                      f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Customer-attempt processor SR z-tests per currency type

# COMMAND ----------

if len(groups) == 2:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        subset = cust_proc_dist[cust_proc_dist["currency_type"] == ct]
        all_procs = subset.groupby("payment_processor")["customer_attempts"].sum().sort_values(ascending=False).index

        print(f"\n{'=' * 90}")
        print(f"Processor Customer-Attempt SR z-tests — {ct}")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'SR_A':>8} {'SR_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        for proc in all_procs:
            a_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]
            b_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra_p, rb_p = a_row.iloc[0], b_row.iloc[0]
                if ra_p["customer_attempts"] > 0 and rb_p["customer_attempts"] > 0:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra_p["customer_attempts"], ra_p["successes"], rb_p["customer_attempts"], rb_p["successes"])
                    sr_a = ra_p["sr"] if pd.notna(ra_p["sr"]) else 0
                    sr_b = rb_p["sr"] if pd.notna(rb_p["sr"]) else 0
                    print(f"  {str(proc):<23} {sr_a:>8.2%} {sr_b:>8.2%} "
                          f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Funnel by Processor × Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Payment funnel by processor × currency type (all system attempts)

# COMMAND ----------

funnel = (
    df_us
    .groupBy("group_name", "currency_type", "payment_processor")
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
    .orderBy("currency_type", "payment_processor", "group_name")
    .toPandas()
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = funnel[funnel["currency_type"] == ct]
    print(f"\n{'=' * 120}")
    print(f"US — Payment Funnel — {ct}")
    print(f"{'=' * 120}")
    print(f"{'Processor':<20} {'Group':<15} {'Total':>8} {'Init':>8} {'InitR':>7} {'SentIss':>8} {'SentR':>7} {'Succ':>8} {'AuthR':>7} {'SR':>7}")
    print("-" * 115)
    for proc in subset.groupby("payment_processor")["total_attempts"].sum().sort_values(ascending=False).index:
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                init_r = f"{r['initiation_rate']:.1%}" if pd.notna(r["initiation_rate"]) else "N/A"
                sent_r = f"{r['sent_to_issuer_rate']:.1%}" if pd.notna(r["sent_to_issuer_rate"]) else "N/A"
                auth_r = f"{r['auth_rate']:.1%}" if pd.notna(r["auth_rate"]) else "N/A"
                sr = f"{r['overall_sr']:.1%}" if pd.notna(r["overall_sr"]) else "N/A"
                print(f"{str(proc):<20} {g:<15} {r['total_attempts']:>8,.0f} {r['initiated']:>8,.0f} {init_r:>7} "
                      f"{r['sent_to_issuer']:>8,.0f} {sent_r:>7} {r['successes']:>8,.0f} {auth_r:>7} {sr:>7}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1b Payment funnel — customer attempts only (system_attempt_rank = 1)

# COMMAND ----------

cust_funnel = (
    df_us_cust
    .groupBy("group_name", "currency_type", "payment_processor")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("initiation_rate", F.col("initiated") / F.col("total_attempts"))
    .withColumn("sent_to_issuer_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .withColumn("auth_rate", F.when(F.col("sent_to_issuer") > 0, F.col("successes") / F.col("sent_to_issuer")))
    .withColumn("overall_sr", F.col("successes") / F.col("total_attempts"))
    .orderBy("currency_type", "payment_processor", "group_name")
    .toPandas()
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = cust_funnel[cust_funnel["currency_type"] == ct]
    print(f"\n{'=' * 120}")
    print(f"US — Customer-Attempt Funnel — {ct} (system_attempt_rank=1)")
    print(f"{'=' * 120}")
    print(f"{'Processor':<20} {'Group':<15} {'CustAtt':>8} {'Init':>8} {'InitR':>7} {'SentIss':>8} {'SentR':>7} {'Succ':>8} {'AuthR':>7} {'SR':>7}")
    print("-" * 115)
    for proc in subset.groupby("payment_processor")["total_attempts"].sum().sort_values(ascending=False).index:
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                init_r = f"{r['initiation_rate']:.1%}" if pd.notna(r["initiation_rate"]) else "N/A"
                sent_r = f"{r['sent_to_issuer_rate']:.1%}" if pd.notna(r["sent_to_issuer_rate"]) else "N/A"
                auth_r = f"{r['auth_rate']:.1%}" if pd.notna(r["auth_rate"]) else "N/A"
                sr = f"{r['overall_sr']:.1%}" if pd.notna(r["overall_sr"]) else "N/A"
                print(f"{str(proc):<20} {g:<15} {r['total_attempts']:>8,.0f} {r['initiated']:>8,.0f} {init_r:>7} "
                      f"{r['sent_to_issuer']:>8,.0f} {sent_r:>7} {r['successes']:>8,.0f} {auth_r:>7} {sr:>7}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 First customer attempt routing (rank 1/1)

# COMMAND ----------

first_attempt = (
    df_us
    .filter((F.col("customer_attempt_rank") == 1) & (F.col("system_attempt_rank") == 1))
    .groupBy("group_name", "currency_type", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

first_total = first_attempt.groupby(["group_name", "currency_type"])["attempts"].sum().to_dict()
first_attempt["pct_share"] = first_attempt.apply(
    lambda r: r["attempts"] / first_total.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = first_attempt[first_attempt["currency_type"] == ct]
    print(f"\n{'=' * 90}")
    print(f"US — First Customer Attempt Routing (rank 1/1) — {ct}")
    print(f"{'=' * 90}")
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>8} {'share':>7} {'SR':>7}", end="")
    if len(groups) == 2:
        print(f"  {'Δ share':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 26 + 10))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        shares = []
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.1%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['attempts']:>8,.0f} {r['pct_share']:>7.1%} {sr_val:>7}", end="")
                shares.append(r["pct_share"])
            else:
                print(f"  {'0':>8} {'0.0%':>7} {'N/A':>7}", end="")
                shares.append(0)
        if len(shares) == 2:
            print(f"  {(shares[1]-shares[0])*100:>+8.2f}", end="")
        print()

    print(f"\n  Total 1st attempts: ", end="")
    for g in groups:
        t = first_total.get((g, ct), 0)
        print(f"  {g}={t:,.0f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 System attempt rank distribution by currency type

# COMMAND ----------

rank_dist = (
    df_us
    .filter(F.col("system_attempt_rank") <= 5)
    .groupBy("group_name", "currency_type", "system_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("currency_type", "system_attempt_rank", "group_name")
    .toPandas()
)

fig, axes = plt.subplots(2, 2, figsize=(18, 10))
for ct_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    ct_data = rank_dist[rank_dist["currency_type"] == ct]
    ranks = sorted(ct_data["system_attempt_rank"].unique())
    x = np.arange(len(ranks))
    w = 0.35

    for i, grp in enumerate(groups):
        grp_data = ct_data[ct_data["group_name"] == grp].set_index("system_attempt_rank").reindex(ranks)
        axes[ct_idx][0].bar(x + i * w, grp_data["attempts"].fillna(0), w,
                            label=grp, color=A_COLOR if i == 0 else B_COLOR)
        axes[ct_idx][1].bar(x + i * w, grp_data["sr"].fillna(0), w,
                            label=grp, color=A_COLOR if i == 0 else B_COLOR)
        for j, (vol, sr) in enumerate(zip(grp_data["attempts"].fillna(0), grp_data["sr"].fillna(0))):
            axes[ct_idx][0].text(x[j] + i * w, vol + vol * 0.02, f"{vol:,.0f}", ha="center", va="bottom", fontsize=7)
            if pd.notna(sr) and sr > 0:
                axes[ct_idx][1].text(x[j] + i * w, sr + 0.005, f"{sr:.1%}", ha="center", va="bottom", fontsize=7)

    for ax in axes[ct_idx]:
        ax.set_xticks(x + w / 2)
        ax.set_xticklabels([str(r) for r in ranks])
        ax.set_xlabel("System Attempt Rank")
        ax.legend(fontsize=8)
    axes[ct_idx][0].set_title(f"{ct} — Volume by Rank", fontweight="bold")
    axes[ct_idx][1].set_title(f"{ct} — SR by Rank", fontweight="bold")
    axes[ct_idx][1].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("US — System Attempt Rank by Currency Type", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Error & Fraud Analysis by Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Error code distribution by currency type

# COMMAND ----------

errors_by_ct = (
    df_us.filter(F.col("is_successful") == 0)
    .groupBy("group_name", "currency_type", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_err_by_grp_ct = errors_by_ct.groupby(["group_name", "currency_type"])["cnt"].sum().to_dict()

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = errors_by_ct[errors_by_ct["currency_type"] == ct]
    if subset.empty:
        continue

    err_pivot = subset.pivot_table(
        index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
    )
    if g_a in err_pivot.columns and g_b in err_pivot.columns:
        err_pivot["pct_A"] = err_pivot[g_a] / total_err_by_grp_ct.get((g_a, ct), 1)
        err_pivot["pct_B"] = err_pivot[g_b] / total_err_by_grp_ct.get((g_b, ct), 1)
        err_pivot["delta_pp"] = (err_pivot["pct_B"] - err_pivot["pct_A"]) * 100
        err_pivot = err_pivot.sort_values("delta_pp", ascending=False)

        print(f"\n{'=' * 90}")
        print(f"US — Error Code Shift — {ct}")
        print(f"{'=' * 90}")
        print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
        print("-" * 80)
        for ec, r in err_pivot.head(12).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")
        print(f"\n  --- Less frequent in Test ---")
        for ec, r in err_pivot.tail(5).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Fraud pre-auth result by currency type

# COMMAND ----------

fraud_by_ct = (
    df_us
    .groupBy("group_name", "currency_type", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_ct_fraud = fraud_by_ct.groupby(["group_name", "currency_type"])["attempts"].sum().to_dict()
fraud_by_ct["pct"] = fraud_by_ct.apply(
    lambda r: r["attempts"] / total_by_grp_ct_fraud.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = fraud_by_ct[fraud_by_ct["currency_type"] == ct]
    print(f"\n{'=' * 100}")
    print(f"US — Fraud Pre-Auth Result — {ct}")
    print(f"{'=' * 100}")
    fraud_types = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Fraud Result':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 10))
    for fr in fraud_types:
        print(f"{str(fr):<25}", end="")
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
            print(f"  {(pcts[1]-pcts[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Fraud × processor breakdown per currency type

# COMMAND ----------

fraud_proc_ct = (
    df_us
    .groupBy("group_name", "currency_type", "payment_processor", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_by_grp_ct_proc = fraud_proc_ct.groupby(["group_name", "currency_type", "payment_processor"])["attempts"].sum().to_dict()
fraud_proc_ct["pct"] = fraud_proc_ct.apply(
    lambda r: r["attempts"] / total_by_grp_ct_proc.get((r["group_name"], r["currency_type"], r["payment_processor"]), 1), axis=1,
)

top_procs_us = proc_dist.groupby("payment_processor")["attempts"].sum().nlargest(4).index
for ct in ["jpm_currency", "non_jpm_currency"]:
    for proc in top_procs_us:
        subset = fraud_proc_ct[(fraud_proc_ct["currency_type"] == ct) &
                                (fraud_proc_ct["payment_processor"] == proc)]
        if subset.empty:
            continue
        print(f"\n{'─' * 80}")
        print(f"{ct} / {proc} — Fraud Pre-Auth Result:")
        fraud_types = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
        print(f"  {'Fraud Result':<25}", end="")
        for g in groups:
            print(f"  {'n_'+g:>8} {'pct_'+g:>7} {'SR_'+g:>7}", end="")
        print()
        for fr in fraud_types:
            print(f"  {str(fr):<25}", end="")
            for g in groups:
                row = subset[(subset["fraud_pre_auth_result"] == fr) & (subset["group_name"] == g)]
                if not row.empty:
                    r = row.iloc[0]
                    sr_val = f"{r['sr']:.1%}" if pd.notna(r["sr"]) else "N/A"
                    print(f"  {r['attempts']:>8,} {r['pct']:>7.1%} {sr_val:>7}", end="")
                else:
                    print(f"  {'0':>8} {'0.0%':>7} {'N/A':>7}", end="")
            print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Customer-attempt error code distribution by currency type

# COMMAND ----------

cust_errors_by_ct = (
    df_us_cust.filter(F.col("is_customer_attempt_successful") == 0)
    .groupBy("group_name", "currency_type", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

cust_total_err_by_grp_ct = cust_errors_by_ct.groupby(["group_name", "currency_type"])["cnt"].sum().to_dict()

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = cust_errors_by_ct[cust_errors_by_ct["currency_type"] == ct]
    if subset.empty:
        continue

    err_pivot = subset.pivot_table(
        index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
    )
    if g_a in err_pivot.columns and g_b in err_pivot.columns:
        err_pivot["pct_A"] = err_pivot[g_a] / cust_total_err_by_grp_ct.get((g_a, ct), 1)
        err_pivot["pct_B"] = err_pivot[g_b] / cust_total_err_by_grp_ct.get((g_b, ct), 1)
        err_pivot["delta_pp"] = (err_pivot["pct_B"] - err_pivot["pct_A"]) * 100
        err_pivot = err_pivot.sort_values("delta_pp", ascending=False)

        print(f"\n{'=' * 90}")
        print(f"US — Error Code Shift — {ct} — CUSTOMER ATTEMPTS ONLY")
        print(f"{'=' * 90}")
        print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
        print("-" * 80)
        for ec, r in err_pivot.head(12).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")
        print(f"\n  --- Less frequent in Test ---")
        for ec, r in err_pivot.tail(5).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Customer-attempt fraud pre-auth result by currency type

# COMMAND ----------

cust_fraud_by_ct = (
    df_us_cust
    .groupBy("group_name", "currency_type", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

cust_total_by_grp_ct_fraud = cust_fraud_by_ct.groupby(["group_name", "currency_type"])["attempts"].sum().to_dict()
cust_fraud_by_ct["pct"] = cust_fraud_by_ct.apply(
    lambda r: r["attempts"] / cust_total_by_grp_ct_fraud.get((r["group_name"], r["currency_type"]), 1), axis=1,
)

for ct in ["jpm_currency", "non_jpm_currency"]:
    subset = cust_fraud_by_ct[cust_fraud_by_ct["currency_type"] == ct]
    print(f"\n{'=' * 100}")
    print(f"US — Fraud Pre-Auth Result — {ct} — CUSTOMER ATTEMPTS ONLY")
    print(f"{'=' * 100}")
    fraud_types = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Fraud Result':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 10))
    for fr in fraud_types:
        print(f"{str(fr):<25}", end="")
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
            print(f"  {(pcts[1]-pcts[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Daily Trends by Currency Type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Daily SR by currency type

# COMMAND ----------

daily = (
    df_us
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "currency_type")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(18, 5))
for ax_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    ct_daily = daily[daily["currency_type"] == ct]
    for i, (grp, grp_data) in enumerate(ct_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[ax_idx].plot(grp_data["attempt_date"], grp_data["sr"], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[ax_idx].set_title(f"{ct} — Daily SR", fontsize=11, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=9)
    axes[ax_idx].tick_params(axis="x", rotation=45)

fig.suptitle("US — Daily Attempt SR by Currency Type", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1b Daily customer-attempt SR by currency type

# COMMAND ----------

daily_cust = (
    df_us_cust
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "currency_type")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(18, 5))
for ax_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    ct_daily = daily_cust[daily_cust["currency_type"] == ct]
    for i, (grp, grp_data) in enumerate(ct_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[ax_idx].plot(grp_data["attempt_date"], grp_data["sr"], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[ax_idx].set_title(f"{ct} — Daily Customer-Attempt SR", fontsize=11, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=9)
    axes[ax_idx].tick_params(axis="x", rotation=45)

fig.suptitle("US — Daily Customer-Attempt SR by Currency Type", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Daily processor share by currency type

# COMMAND ----------

daily_proc = (
    df_us
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "currency_type", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

daily_proc_total = daily_proc.groupby(["attempt_date", "group_name", "currency_type"])["attempts"].sum().reset_index()
daily_proc_total.columns = ["attempt_date", "group_name", "currency_type", "total"]
daily_proc = daily_proc.merge(daily_proc_total, on=["attempt_date", "group_name", "currency_type"])
daily_proc["pct"] = daily_proc["attempts"] / daily_proc["total"]

for ct in ["jpm_currency", "non_jpm_currency"]:
    ct_daily = daily_proc[daily_proc["currency_type"] == ct]
    for grp in groups:
        grp_daily = ct_daily[ct_daily["group_name"] == grp]
        procs_order = grp_daily.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index.tolist()
        pivot = grp_daily.pivot_table(index="attempt_date", columns="payment_processor",
                                      values="pct", aggfunc="first", fill_value=0)
        pivot = pivot.reindex(columns=procs_order)

        fig, ax = plt.subplots(figsize=(16, 4))
        pivot.plot.area(ax=ax, stacked=True, alpha=0.7, color=COLORS[:len(procs_order)])
        ax.set_title(f"{grp} — Daily Processor Share — {ct} (US)", fontsize=11, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=7, loc="upper right", ncol=2)
        ax.set_xlabel("")
        fig.tight_layout()
        plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Daily SUSPECTED_FRAUD & INSUFFICIENT_FUNDS rate by currency type

# COMMAND ----------

daily_err = (
    df_us
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "currency_type")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("error_code") == "SUSPECTED_FRAUD", 1).otherwise(0)).alias("suspected_fraud"),
        F.sum(F.when(F.col("error_code") == "INSUFFICIENT_FUNDS", 1).otherwise(0)).alias("insufficient_funds"),
    )
    .withColumn("sf_rate", F.col("suspected_fraud") / F.col("total"))
    .withColumn("if_rate", F.col("insufficient_funds") / F.col("total"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(2, 2, figsize=(18, 10))
for ct_idx, ct in enumerate(["jpm_currency", "non_jpm_currency"]):
    ct_data = daily_err[daily_err["currency_type"] == ct]
    for col_idx, (col, label) in enumerate([("sf_rate", "SUSPECTED_FRAUD"), ("if_rate", "INSUFFICIENT_FUNDS")]):
        ax = axes[col_idx][ct_idx]
        for i, (grp, grp_data) in enumerate(ct_data.groupby("group_name")):
            color = A_COLOR if i == 0 else B_COLOR
            ax.plot(grp_data["attempt_date"], grp_data[col], color=color,
                    linewidth=1.5, label=grp, marker=".", markersize=3)
        ax.set_title(f"{label} rate — {ct}", fontsize=10, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=8)
        ax.tick_params(axis="x", rotation=45)

fig.suptitle("US — Daily Error Rates by Currency Type", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Payment References for Primer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Failed Test attempts — JPM currency

# COMMAND ----------

if g_b is not None:
    for ct in ["jpm_currency", "non_jpm_currency"]:
        refs = (
            df_us
            .filter(F.col("group_name") == g_b)
            .filter(F.col("currency_type") == ct)
            .filter(F.col("is_successful") == 0)
            .select(
                "payment_provider_reference",
                "customer_system_attempt_reference",
                "payment_attempt_timestamp",
                "payment_processor",
                "payment_method_variant",
                "currency",
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
        cnt = refs.count()
        print(f"\n{'=' * 80}")
        print(f"Failed {g_b} attempts — {ct} (US): {cnt:,}")
        print(f"{'=' * 80}")
        display(refs.limit(150))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Exportable temp views

# COMMAND ----------

if g_b is not None:
    for ct, view_name in [("jpm_currency", "us_b_jpm_failed"), ("non_jpm_currency", "us_b_nonjpm_failed")]:
        refs = (
            df_us
            .filter(F.col("group_name") == g_b)
            .filter(F.col("currency_type") == ct)
            .filter(F.col("is_successful") == 0)
            .select(
                "payment_provider_reference", "customer_system_attempt_reference",
                "payment_attempt_timestamp", "payment_processor", "payment_method_variant",
                "currency", "payment_attempt_status", "error_code", "response_code",
                "fraud_pre_auth_result",
                F.col("challenge_issued").cast("boolean").alias("challenged"),
                "attempt_type", "customer_attempt_rank", "system_attempt_rank",
            )
            .orderBy("error_code", F.desc("payment_attempt_timestamp"))
        )
        refs.createOrReplaceTempView(view_name)
        print(f"Created temp view: {view_name} ({refs.count():,} rows)")

    print(f"\nExample queries:")
    print(f"  SELECT * FROM us_b_jpm_failed WHERE error_code = 'INSUFFICIENT_FUNDS' LIMIT 50")
    print(f"  SELECT * FROM us_b_nonjpm_failed WHERE payment_processor ILIKE '%jpmc%' LIMIT 50")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Card Scheme Analysis (Amex vs Visa/MC vs Other)
# MAGIC
# MAGIC Amex, Visa/Mastercard and other schemes have different routing.
# MAGIC This section isolates scheme-level effects to understand where
# MAGIC the SR gap originates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 High-level metrics by card_scheme × variant

# COMMAND ----------

SCHEME_ORDER = ["visa_mc", "amex", "other"]

scheme_summary = (
    df_us
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "card_scheme")
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
    .orderBy("card_scheme", "group_name")
    .toPandas()
)

for cs in SCHEME_ORDER:
    subset = scheme_summary[scheme_summary["card_scheme"] == cs]
    if subset.empty:
        continue
    print(f"\n{'=' * 90}")
    print(f"US — CARD SCHEME: {cs.upper()} — ALL SYSTEM ATTEMPTS")
    print(f"{'=' * 90}")
    for _, r in subset.iterrows():
        ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
        ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
        no_ch_sr = f"{r['no_challenge_sr']:.2%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
        print(f"\n  {r['group_name']}:")
        print(f"    Visitors            : {r['visitors']:,.0f}")
        print(f"    Attempts            : {r['attempts']:,.0f}")
        print(f"    Attempt SR          : {r['sr']:.4%}")
        print(f"    3DS Challenge Rate  : {ch_rate}")
        print(f"    SR when challenged  : {ch_sr}")
        print(f"    SR when NOT chal.   : {no_ch_sr}")

    if len(subset) == 2:
        ra, rb = subset.iloc[0], subset.iloc[1]
        print(f"\n  Z-TESTS ({cs}):")
        for label, n_a, s_a, n_b, s_b in [
            ("Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
            ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ]:
            if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
                print(f"    {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
                      f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1b Customer-attempt metrics by card_scheme

# COMMAND ----------

cust_scheme_summary = (
    df_us_cust
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "card_scheme")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges_issued"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("challenge_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_challenge_attempts"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("no_challenge_successes"),
        F.countDistinct("visitor_id").alias("visitors"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("challenge_rate", F.col("challenges_issued") / F.col("attempts"))
    .withColumn("challenge_sr", F.when(F.col("challenges_issued") > 0, F.col("challenge_successes") / F.col("challenges_issued")))
    .withColumn("no_challenge_sr", F.when(F.col("no_challenge_attempts") > 0, F.col("no_challenge_successes") / F.col("no_challenge_attempts")))
    .orderBy("card_scheme", "group_name")
    .toPandas()
)

for cs in SCHEME_ORDER:
    subset = cust_scheme_summary[cust_scheme_summary["card_scheme"] == cs]
    if subset.empty:
        continue
    print(f"\n{'=' * 90}")
    print(f"US — CARD SCHEME: {cs.upper()} — CUSTOMER ATTEMPTS ONLY")
    print(f"{'=' * 90}")
    for _, r in subset.iterrows():
        ch_rate = f"{r['challenge_rate']:.2%}" if pd.notna(r["challenge_rate"]) else "N/A"
        ch_sr = f"{r['challenge_sr']:.2%}" if pd.notna(r["challenge_sr"]) else "N/A"
        no_ch_sr = f"{r['no_challenge_sr']:.2%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
        print(f"\n  {r['group_name']}:")
        print(f"    Customer Attempts   : {r['attempts']:,.0f}")
        print(f"    Customer Attempt SR : {r['sr']:.4%}")
        print(f"    3DS Challenge Rate  : {ch_rate}")
        print(f"    SR when challenged  : {ch_sr}")
        print(f"    SR when NOT chal.   : {no_ch_sr}")

    if len(subset) == 2:
        ra, rb = subset.iloc[0], subset.iloc[1]
        print(f"\n  Z-TESTS ({cs}, customer attempts):")
        for label, n_a, s_a, n_b, s_b in [
            ("Customer Attempt SR", ra["attempts"], ra["successes"], rb["attempts"], rb["successes"]),
            ("3DS Challenge Rate", ra["attempts"], ra["challenges_issued"], rb["attempts"], rb["challenges_issued"]),
        ]:
            if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
                print(f"    {label:<25}  {g_a}={s_a/n_a:.4%}  {g_b}={s_b/n_b:.4%}  "
                      f"Δ={delta_pp:+.3f}pp  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 SR chart by card scheme

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
for ax_idx, cs in enumerate(SCHEME_ORDER):
    subset = scheme_summary[scheme_summary["card_scheme"] == cs].sort_values("group_name")
    colors = [A_COLOR, B_COLOR][:len(subset)]
    bars = axes[ax_idx].bar(subset["group_name"], subset["sr"], color=colors)
    axes[ax_idx].set_title(f"Attempt SR — {cs.upper()}", fontweight="bold", fontsize=12)
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    for bar, val in zip(bars, subset["sr"]):
        if pd.notna(val):
            axes[ax_idx].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                              f"{val:.2%}", ha="center", va="bottom", fontsize=11, fontweight="bold")

fig.suptitle("US — Attempt SR by Card Scheme × Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 Processor split by card_scheme × variant (customer attempts)

# COMMAND ----------

scheme_proc = (
    df_us_cust
    .groupBy("group_name", "card_scheme", "payment_processor")
    .agg(
        F.count("*").alias("customer_attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("customer_attempts"))
    .toPandas()
)

scheme_proc_total = scheme_proc.groupby(["group_name", "card_scheme"])["customer_attempts"].sum().to_dict()
scheme_proc["pct_share"] = scheme_proc.apply(
    lambda r: r["customer_attempts"] / scheme_proc_total.get((r["group_name"], r["card_scheme"]), 1), axis=1,
)

for cs in SCHEME_ORDER:
    subset = scheme_proc[scheme_proc["card_scheme"] == cs]
    if subset.empty:
        continue
    print(f"\n{'=' * 110}")
    print(f"US — Processor Split — {cs.upper()} — CUSTOMER ATTEMPTS ONLY")
    print(f"{'=' * 110}")
    procs = subset.groupby("payment_processor")["customer_attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'share_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ share':>8} {'Δ SR':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 18))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        shares, srs = [], []
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
                print(f"  {r['customer_attempts']:>10,} {r['pct_share']:>8.2%} {sr_val:>8}", end="")
                shares.append(r["pct_share"])
                srs.append(r["sr"] if pd.notna(r["sr"]) else 0)
            else:
                print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
                shares.append(0)
                srs.append(0)
        if len(shares) == 2:
            print(f"  {(shares[1]-shares[0])*100:>+8.2f} {(srs[1]-srs[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.4 Processor share & SR z-tests by card_scheme (customer attempts)

# COMMAND ----------

if len(groups) == 2:
    for cs in SCHEME_ORDER:
        subset = scheme_proc[scheme_proc["card_scheme"] == cs]
        if subset.empty:
            continue
        total_a_cs = scheme_proc_total.get((g_a, cs), 0)
        total_b_cs = scheme_proc_total.get((g_b, cs), 0)

        print(f"\n{'=' * 90}")
        print(f"Processor SHARE z-tests — {cs.upper()} (customer attempts)")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'share_A':>8} {'share_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        for proc in sorted(subset["payment_processor"].unique(), key=str):
            s_a = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]["customer_attempts"].sum()
            s_b = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]["customer_attempts"].sum()
            if total_a_cs > 0 and total_b_cs > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(total_a_cs, s_a, total_b_cs, s_b)
                print(f"  {str(proc):<23} {s_a/total_a_cs:>8.2%} {s_b/total_b_cs:>8.2%} "
                      f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

        print(f"\n{'=' * 90}")
        print(f"Processor SR z-tests — {cs.upper()} (customer attempts)")
        print(f"{'=' * 90}")
        print(f"{'Processor':<25} {'SR_A':>8} {'SR_B':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
        print("-" * 70)
        all_procs = subset.groupby("payment_processor")["customer_attempts"].sum().sort_values(ascending=False).index
        for proc in all_procs:
            a_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_a)]
            b_row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g_b)]
            if not a_row.empty and not b_row.empty:
                ra_p, rb_p = a_row.iloc[0], b_row.iloc[0]
                if ra_p["customer_attempts"] > 0 and rb_p["customer_attempts"] > 0:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra_p["customer_attempts"], ra_p["successes"],
                        rb_p["customer_attempts"], rb_p["successes"])
                    sr_a = ra_p["sr"] if pd.notna(ra_p["sr"]) else 0
                    sr_b = rb_p["sr"] if pd.notna(rb_p["sr"]) else 0
                    print(f"  {str(proc):<23} {sr_a:>8.2%} {sr_b:>8.2%} "
                          f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.5 Error code distribution by card_scheme (customer attempts)

# COMMAND ----------

scheme_errors = (
    df_us_cust.filter(F.col("is_customer_attempt_successful") == 0)
    .groupBy("group_name", "card_scheme", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

scheme_err_total = scheme_errors.groupby(["group_name", "card_scheme"])["cnt"].sum().to_dict()

for cs in SCHEME_ORDER:
    subset = scheme_errors[scheme_errors["card_scheme"] == cs]
    if subset.empty:
        continue
    err_pivot = subset.pivot_table(
        index="error_code", columns="group_name", values="cnt", aggfunc="sum", fill_value=0
    )
    if g_a in err_pivot.columns and g_b in err_pivot.columns:
        err_pivot["pct_A"] = err_pivot[g_a] / scheme_err_total.get((g_a, cs), 1)
        err_pivot["pct_B"] = err_pivot[g_b] / scheme_err_total.get((g_b, cs), 1)
        err_pivot["delta_pp"] = (err_pivot["pct_B"] - err_pivot["pct_A"]) * 100
        err_pivot = err_pivot.sort_values("delta_pp", ascending=False)

        print(f"\n{'=' * 90}")
        print(f"US — Error Code Shift — {cs.upper()} — CUSTOMER ATTEMPTS")
        print(f"{'=' * 90}")
        print(f"{'Error Code':<35} {'n_Ctrl':>8} {'pct_Ctrl':>9} {'n_Test':>8} {'pct_Test':>9} {'Δ(pp)':>8}")
        print("-" * 80)
        for ec, r in err_pivot.head(12).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")
        print(f"\n  --- Less frequent in Test ---")
        for ec, r in err_pivot.tail(5).iterrows():
            print(f"{str(ec):<35} {r[g_a]:>8,.0f} {r['pct_A']:>9.2%} {r[g_b]:>8,.0f} {r['pct_B']:>9.2%} {r['delta_pp']:>+8.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.6 Fraud pre-auth result by card_scheme (customer attempts)

# COMMAND ----------

scheme_fraud = (
    df_us_cust
    .groupBy("group_name", "card_scheme", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

scheme_fraud_total = scheme_fraud.groupby(["group_name", "card_scheme"])["attempts"].sum().to_dict()
scheme_fraud["pct"] = scheme_fraud.apply(
    lambda r: r["attempts"] / scheme_fraud_total.get((r["group_name"], r["card_scheme"]), 1), axis=1,
)

for cs in SCHEME_ORDER:
    subset = scheme_fraud[scheme_fraud["card_scheme"] == cs]
    if subset.empty:
        continue
    print(f"\n{'=' * 100}")
    print(f"US — Fraud Pre-Auth Result — {cs.upper()} — CUSTOMER ATTEMPTS")
    print(f"{'=' * 100}")
    fraud_types = subset.groupby("fraud_pre_auth_result")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Fraud Result':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
    if len(groups) == 2:
        print(f"  {'Δ pct':>8}", end="")
    print()
    print("-" * (25 + len(groups) * 30 + 10))
    for fr in fraud_types:
        print(f"{str(fr):<25}", end="")
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
            print(f"  {(pcts[1]-pcts[0])*100:>+8.2f}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.7 Processor × card_scheme × variant — funnel (customer attempts)

# COMMAND ----------

scheme_funnel = (
    df_us_cust
    .groupBy("group_name", "card_scheme", "payment_processor")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("initiation_rate", F.col("initiated") / F.col("total_attempts"))
    .withColumn("sent_to_issuer_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .withColumn("auth_rate", F.when(F.col("sent_to_issuer") > 0, F.col("successes") / F.col("sent_to_issuer")))
    .withColumn("overall_sr", F.col("successes") / F.col("total_attempts"))
    .orderBy("card_scheme", "payment_processor", "group_name")
    .toPandas()
)

for cs in SCHEME_ORDER:
    subset = scheme_funnel[scheme_funnel["card_scheme"] == cs]
    if subset.empty:
        continue
    print(f"\n{'=' * 120}")
    print(f"US — Customer-Attempt Funnel — {cs.upper()}")
    print(f"{'=' * 120}")
    print(f"{'Processor':<20} {'Group':<15} {'CustAtt':>8} {'Init':>8} {'InitR':>7} {'SentIss':>8} {'SentR':>7} {'Succ':>8} {'AuthR':>7} {'SR':>7}")
    print("-" * 115)
    for proc in subset.groupby("payment_processor")["total_attempts"].sum().sort_values(ascending=False).index:
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                r = row.iloc[0]
                init_r = f"{r['initiation_rate']:.1%}" if pd.notna(r["initiation_rate"]) else "N/A"
                sent_r = f"{r['sent_to_issuer_rate']:.1%}" if pd.notna(r["sent_to_issuer_rate"]) else "N/A"
                auth_r = f"{r['auth_rate']:.1%}" if pd.notna(r["auth_rate"]) else "N/A"
                sr = f"{r['overall_sr']:.1%}" if pd.notna(r["overall_sr"]) else "N/A"
                print(f"{str(proc):<20} {g:<15} {r['total_attempts']:>8,.0f} {r['initiated']:>8,.0f} {init_r:>7} "
                      f"{r['sent_to_issuer']:>8,.0f} {sent_r:>7} {r['successes']:>8,.0f} {auth_r:>7} {sr:>7}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.8 Daily SR by card_scheme (customer attempts)

# COMMAND ----------

daily_scheme = (
    df_us_cust
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "card_scheme")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, axes = plt.subplots(1, 3, figsize=(22, 5))
for ax_idx, cs in enumerate(SCHEME_ORDER):
    cs_daily = daily_scheme[daily_scheme["card_scheme"] == cs]
    for i, (grp, grp_data) in enumerate(cs_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[ax_idx].plot(grp_data["attempt_date"], grp_data["sr"], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[ax_idx].set_title(f"{cs.upper()} — Daily Cust-Att SR", fontsize=11, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=9)
    axes[ax_idx].tick_params(axis="x", rotation=45)

fig.suptitle("US — Daily Customer-Attempt SR by Card Scheme", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.9 Daily processor share by card_scheme

# COMMAND ----------

daily_scheme_proc = (
    df_us_cust
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "card_scheme", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

daily_sp_total = daily_scheme_proc.groupby(["attempt_date", "group_name", "card_scheme"])["attempts"].sum().reset_index()
daily_sp_total.columns = ["attempt_date", "group_name", "card_scheme", "total"]
daily_scheme_proc = daily_scheme_proc.merge(daily_sp_total, on=["attempt_date", "group_name", "card_scheme"])
daily_scheme_proc["pct"] = daily_scheme_proc["attempts"] / daily_scheme_proc["total"]

for cs in SCHEME_ORDER:
    cs_data = daily_scheme_proc[daily_scheme_proc["card_scheme"] == cs]
    for grp in groups:
        grp_daily = cs_data[cs_data["group_name"] == grp]
        if grp_daily.empty:
            continue
        procs_order = grp_daily.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index.tolist()
        pivot = grp_daily.pivot_table(index="attempt_date", columns="payment_processor",
                                      values="pct", aggfunc="first", fill_value=0)
        pivot = pivot.reindex(columns=procs_order)

        fig, ax = plt.subplots(figsize=(16, 4))
        pivot.plot.area(ax=ax, stacked=True, alpha=0.7, color=COLORS[:len(procs_order)])
        ax.set_title(f"{grp} — Daily Processor Share — {cs.upper()} (US, customer attempts)", fontsize=11, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=7, loc="upper right", ncol=2)
        ax.set_xlabel("")
        fig.tight_layout()
        plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.10 Card scheme × currency type interaction (customer attempts)

# COMMAND ----------

scheme_ct_summary = (
    df_us_cust
    .groupBy("group_name", "card_scheme", "currency_type")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("card_scheme", "currency_type", "group_name")
    .toPandas()
)

if len(groups) == 2:
    print(f"\n{'=' * 110}")
    print(f"US — Card Scheme × Currency Type — Customer Attempt SR & Z-Tests")
    print(f"{'=' * 110}")
    print(f"{'Scheme':<10} {'CurrType':<18} {'n_Ctrl':>8} {'SR_Ctrl':>8} {'n_Test':>8} {'SR_Test':>8} "
          f"{'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
    print("-" * 100)
    for cs in SCHEME_ORDER:
        for ct in ["jpm_currency", "non_jpm_currency"]:
            a_row = scheme_ct_summary[
                (scheme_ct_summary["card_scheme"] == cs) &
                (scheme_ct_summary["currency_type"] == ct) &
                (scheme_ct_summary["group_name"] == g_a)
            ]
            b_row = scheme_ct_summary[
                (scheme_ct_summary["card_scheme"] == cs) &
                (scheme_ct_summary["currency_type"] == ct) &
                (scheme_ct_summary["group_name"] == g_b)
            ]
            if not a_row.empty and not b_row.empty:
                ra, rb = a_row.iloc[0], b_row.iloc[0]
                if ra["attempts"] > 0 and rb["attempts"] > 0:
                    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
                        ra["attempts"], ra["successes"], rb["attempts"], rb["successes"])
                    sr_a = f"{ra['sr']:.2%}" if pd.notna(ra["sr"]) else "N/A"
                    sr_b = f"{rb['sr']:.2%}" if pd.notna(rb["sr"]) else "N/A"
                    print(f"{cs:<10} {ct:<18} {ra['attempts']:>8,.0f} {sr_a:>8} {rb['attempts']:>8,.0f} {sr_b:>8} "
                          f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 2.1–2.2 | Is the SR gap different for JPM vs non-JPM currencies in US? (all system attempts) |
# MAGIC | 2.3 | Same as above but for **customer attempts only** (system_attempt_rank=1) |
# MAGIC | 3.1–3.3 | Does the processor split differ between currency types? Is JPMC routing more in JPM currencies? |
# MAGIC | 4.1–4.2 | Are processor share and SR shifts statistically significant per currency type? (all system attempts) |
# MAGIC | 4.3–4.4 | Same z-tests but for **customer attempts only** |
# MAGIC | 5.1 | Where in the funnel (init → sent → auth) do attempts fail, by processor × currency type? (all system attempts) |
# MAGIC | 5.1b | Same funnel but for **customer attempts only** |
# MAGIC | 5.2 | Does the first-attempt routing differ between variants for each currency type? |
# MAGIC | 6.1–6.3 | Does the error/fraud shift pattern differ between JPM and non-JPM currencies? (all system attempts) |
# MAGIC | 6.4–6.5 | Same error & fraud analysis but for **customer attempts only** |
# MAGIC | 7.1 | Daily SR trends (all system attempts) |
# MAGIC | 7.1b | Daily SR trends — **customer attempts only** |
# MAGIC | 7.2–7.3 | Are the effects consistent over time for each currency type? (daily processor share, error rates) |
# MAGIC | 8.1–8.2 | Payment references for Primer, segmented by currency type |
# MAGIC | 9.1–9.1b | SR & 3DS by card scheme (amex / visa_mc / other) — all system attempts + customer attempts |
# MAGIC | 9.2 | SR bar chart by card scheme |
# MAGIC | 9.3–9.4 | Processor split & z-tests by card scheme (customer attempts) |
# MAGIC | 9.5–9.6 | Error & fraud by card scheme (customer attempts) |
# MAGIC | 9.7 | Funnel by processor × card scheme (customer attempts) |
# MAGIC | 9.8–9.9 | Daily SR & processor share by card scheme |
# MAGIC | 9.10 | Card scheme × currency type interaction (isolating which combination drives the gap) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of US currency × processor deep-dive analysis*
