# Databricks notebook source
# MAGIC %md
# MAGIC # Agnostic Payment Methods Experiment Analysis
# MAGIC
# MAGIC **Experiment:** `pay-show-agnostic-payment-methods`
# MAGIC
# MAGIC **Variants:**
# MAGIC - **A (control):** Standard payment methods display
# MAGIC - **B:** Agnostic payment methods (faster payment page loading)
# MAGIC - **C:** Agnostic + card scheme icons displayed + unsupported-card validation
# MAGIC
# MAGIC **Period:** 2026-03-12 onwards
# MAGIC
# MAGIC **Key questions:**
# MAGIC 1. How are B and C performing vs A?
# MAGIC 2. Why is C negative vs B?
# MAGIC 3. Do users in C attempt less (card validation deterrent)?
# MAGIC 4. Does the payment method split change (card icon more prominent in C)?
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Setup & constants
# MAGIC 2. Data extraction — visitor-level funnel
# MAGIC 3. Data extraction — attempt-level
# MAGIC 4. High-level funnel comparison (A vs B vs C)
# MAGIC 5. C vs B deep dive — do users attempt less?
# MAGIC 6. Payment method split analysis
# MAGIC 7. Card-specific analysis (C's validation effect)
# MAGIC 8. Payment method availability
# MAGIC 9. Platform breakdown
# MAGIC 10. Attempt-level performance
# MAGIC 11. Daily trends

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Constants

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

EXPERIMENT_ID = "pay-show-agnostic-payment-methods"
START_DATE = "2026-03-12"

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

def run_pairwise_tests(df, groups, metric_label, n_col, s_col):
    """Run pairwise z-tests for all group pairs and print results."""
    pairs = [(groups[0], groups[1]), (groups[0], groups[2]), (groups[1], groups[2])] if len(groups) == 3 else []
    if len(groups) == 2:
        pairs = [(groups[0], groups[1])]
    for ga, gb in pairs:
        ra = df[df["group_name"] == ga]
        rb = df[df["group_name"] == gb]
        if ra.empty or rb.empty:
            continue
        ra, rb = ra.iloc[0], rb.iloc[0]
        n_a, s_a = ra[n_col], ra[s_col]
        n_b, s_b = rb[n_col], rb[s_col]
        if n_a > 0 and n_b > 0 and (s_a + s_b) > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
            print(f"  {metric_label:<30} {ga} vs {gb}:  "
                  f"{ga}={s_a/n_a:.2%}  {gb}={s_b/n_b:.2%}  "
                  f"Δ={delta_pp:+.2f}pp  95%CI=[{ci_lo:+.2f},{ci_hi:+.2f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Data Extraction (Visitor Funnel)

# COMMAND ----------

df_visitor = spark.sql(f"""
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
  WHERE DATE(timestamp) >= '{START_DATE}'
    AND experiment_id = '{EXPERIMENT_ID}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
),
payment_methods_loaded AS (
  SELECT
    a.group_name,
    user.visitor_id AS visitor_id,
    a.assigned_at,
    a.platform,
    from_json(json_event:payment_methods_list, 'ARRAY<STRING>') AS payment_methods_list,
    json_event:payment_terms_preselected AS payment_terms_preselected,
    json_event:payment_method_preselected AS payment_method_preselected,
    json_event:currency_preselected AS currency_preselected,
    array_size(from_json(json_event:payment_methods_list, 'ARRAY<STRING>')) AS cnt_payment_methods,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'applepay') THEN 1 ELSE 0 END AS if_applepay,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'paypal') THEN 1 ELSE 0 END AS if_paypal,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'klarna') THEN 1 ELSE 0 END AS if_klarna,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'creditcard') THEN 1 ELSE 0 END AS if_creditcard,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'googlepay') THEN 1 ELSE 0 END AS if_googlepay,
    CASE WHEN array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'blik')
           OR array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'ideal')
           OR array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'vipps')
           OR array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'mbway')
           OR array_contains(from_json(json_event:payment_methods_list, 'ARRAY<STRING>'), 'mobilepay')
         THEN 1 ELSE 0 END AS if_local_apm,
    event_properties.timestamp AS event_ts,
    sc.shopping_cart_id
  FROM assignment a
  LEFT JOIN production.events.events e
    ON a.visitor_id = user.visitor_id
    AND event_properties.timestamp > a.assigned_at - INTERVAL 120 SECONDS
  LEFT JOIN production.db_mirror_dbz.gyg__shopping_cart sc
    ON json_event:cart_hash = sc.hash_code
    AND sc.date_of_creation::date >= '{START_DATE}'
    AND user.visitor_id = sc.visitor_id
  WHERE event_name IN ('TravelerWebPaymentOptionsLoaded', 'MobileAppPaymentOptionsLoaded')
    AND e.date >= '{START_DATE}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp) = 1
),
payment_data AS (
  SELECT
    a.visitor_id,
    a.shopping_cart_id,
    p.payment_method,
    p.payment_method_variant,
    p.payment_processor,
    p.payment_initiated,
    p.status,
    p.author_boolean,
    p.fraud_check_success_outcome,
    p.`3ds_outcome`,
    p.booked,
    p.payment_attempt_timestamp
  FROM payment_methods_loaded a
  LEFT JOIN production.analytics.fact_payment p
    ON a.shopping_cart_id = p.shopping_cart_id
    AND p.payment_attempt_timestamp::date >= '{START_DATE}'
    AND p.payment_attempt_timestamp > a.assigned_at
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.visitor_id ORDER BY p.payment_attempt_timestamp DESC) = 1
),
pay_button_submit AS (
  SELECT
    user.visitor_id,
    COALESCE(ui.metadata:paymentMethodIdentifier, ui.id) AS last_method_submit_click,
    collect_set(COALESCE(ui.metadata:paymentMethodIdentifier, ui.id))
      OVER (PARTITION BY user.visitor_id, ui.metadata:shoppingCartHash) AS payment_methods_pay_submit_list
  FROM production.events.events e
  WHERE e.date >= '{START_DATE}'
    AND e.event_name = 'UISubmit'
    AND ui.id = 'submit-payment'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp DESC) = 1
)
SELECT
  a.group_name,
  a.assigned_at::date AS assigned_dt,
  a.platform,
  a.payment_terms_preselected AS last_payment_terms_preselected,
  a.payment_methods_list AS last_payment_methods_list,
  a.payment_method_preselected AS last_payment_method_preselected,
  a.if_applepay,
  a.if_paypal,
  a.if_klarna,
  a.if_creditcard,
  a.if_googlepay,
  a.if_local_apm,
  a.cnt_payment_methods AS last_cnt_payment_methods_loaded,
  p.payment_method,
  p.payment_method_variant,
  p.payment_processor,
  p.status,
  p.fraud_check_success_outcome,
  p.`3ds_outcome`,
  pbs.last_method_submit_click,
  a.visitor_id,
  CASE WHEN p.payment_initiated = 1 THEN 1 ELSE 0 END AS payment_initiated,
  CASE WHEN p.booked = 1 THEN 1 ELSE 0 END AS booked,
  CASE WHEN p.author_boolean = 1 THEN 1 ELSE 0 END AS authorized,
  CASE WHEN pbs.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS pay_submitted
FROM payment_methods_loaded a
LEFT JOIN payment_data p ON a.visitor_id = p.visitor_id
LEFT JOIN pay_button_submit pbs ON pbs.visitor_id = a.visitor_id
""")
df_visitor.cache()

total_visitors = df_visitor.count()
print(f"Total visitors in experiment: {total_visitors:,}")

vis_by_group = df_visitor.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in vis_by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(vis_by_group["group_name"].tolist())
print(f"\nDetected groups: {groups}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Data Extraction (Attempt Level)

# COMMAND ----------

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
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{START_DATE}'
    AND experiment_id = '{EXPERIMENT_ID}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
)
SELECT
  a.group_name,
  a.visitor_id,
  p.payment_provider_reference,
  p.customer_system_attempt_reference,
  p.payment_flow,
  p.payment_method,
  p.payment_method_variant,
  p.currency,
  p.payment_processor,
  p.payment_attempt_status,
  p.challenge_issued,
  p.response_code,
  p.error_code,
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.attempt_type,
  p.is_successful,
  p.is_customer_attempt_successful,
  p.is_shopping_cart_successful,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date >= '{START_DATE}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
""")
df_attempts.cache()

total_attempts = df_attempts.count()
print(f"Total payment attempts: {total_attempts:,}")

att_by_group = df_attempts.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in att_by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — High-Level Funnel Comparison (A vs B vs C)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Visitor-level funnel by variant

# COMMAND ----------

funnel = (
    df_visitor
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors_loaded"),
        F.sum("pay_submitted").alias("visitors_submitted"),
        F.sum("payment_initiated").alias("visitors_initiated"),
        F.sum("authorized").alias("visitors_authorized"),
        F.sum("booked").alias("visitors_booked"),
    )
    .orderBy("group_name")
    .toPandas()
)

funnel["submit_rate"] = funnel["visitors_submitted"] / funnel["visitors_loaded"]
funnel["initiation_rate"] = funnel["visitors_initiated"] / funnel["visitors_loaded"]
funnel["auth_rate"] = funnel["visitors_authorized"] / funnel["visitors_loaded"]
funnel["booking_rate"] = funnel["visitors_booked"] / funnel["visitors_loaded"]

print("=" * 100)
print("VISITOR FUNNEL BY VARIANT")
print("=" * 100)
print(f"{'Group':<20} {'Loaded':>10} {'Submitted':>10} {'Initiated':>10} {'Authorized':>10} {'Booked':>10}")
print("-" * 80)
for _, r in funnel.iterrows():
    print(f"{r['group_name']:<20} {r['visitors_loaded']:>10,.0f} {r['visitors_submitted']:>10,.0f} "
          f"{r['visitors_initiated']:>10,.0f} {r['visitors_authorized']:>10,.0f} {r['visitors_booked']:>10,.0f}")

print(f"\n{'Group':<20} {'SubmitR':>10} {'InitR':>10} {'AuthR':>10} {'BookR':>10}")
print("-" * 60)
for _, r in funnel.iterrows():
    print(f"{r['group_name']:<20} {r['submit_rate']:>10.2%} {r['initiation_rate']:>10.2%} "
          f"{r['auth_rate']:>10.2%} {r['booking_rate']:>10.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Statistical tests — pairwise

# COMMAND ----------

print("PAIRWISE Z-TESTS:")
print("=" * 100)
for label, n_col, s_col in [
    ("Submit Rate", "visitors_loaded", "visitors_submitted"),
    ("Initiation Rate", "visitors_loaded", "visitors_initiated"),
    ("Auth Rate", "visitors_loaded", "visitors_authorized"),
    ("Booking Rate", "visitors_loaded", "visitors_booked"),
]:
    run_pairwise_tests(funnel, groups, label, n_col, s_col)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Funnel visualization

# COMMAND ----------

stages = ["submit_rate", "initiation_rate", "auth_rate", "booking_rate"]
stage_labels = ["Pay Submit", "Payment\nInitiated", "Authorized", "Booked"]

fig, ax = plt.subplots(figsize=(14, 6))
x = np.arange(len(stages))
n_groups = len(groups)
w = 0.8 / n_groups

for i, grp in enumerate(groups):
    grp_data = funnel[funnel["group_name"] == grp].iloc[0]
    vals = [grp_data[s] for s in stages]
    bars = ax.bar(x + i * w, vals, w, label=grp, color=COLORS[i % len(COLORS)])
    for j, v in enumerate(vals):
        ax.text(x[j] + i * w, v + 0.002, f"{v:.2%}", ha="center", va="bottom", fontsize=8, fontweight="bold")

ax.set_xticks(x + w * (n_groups - 1) / 2)
ax.set_xticklabels(stage_labels, fontsize=11)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Visitor Funnel Conversion Rates by Variant", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Conditional conversion (given prior step)

# COMMAND ----------

funnel["submit_to_init"] = funnel.apply(
    lambda r: r["visitors_initiated"] / r["visitors_submitted"] if r["visitors_submitted"] > 0 else None, axis=1)
funnel["init_to_auth"] = funnel.apply(
    lambda r: r["visitors_authorized"] / r["visitors_initiated"] if r["visitors_initiated"] > 0 else None, axis=1)
funnel["auth_to_book"] = funnel.apply(
    lambda r: r["visitors_booked"] / r["visitors_authorized"] if r["visitors_authorized"] > 0 else None, axis=1)

print("CONDITIONAL CONVERSION RATES:")
print(f"{'Group':<20} {'Submit→Init':>12} {'Init→Auth':>12} {'Auth→Book':>12}")
print("-" * 60)
for _, r in funnel.iterrows():
    s2i = f"{r['submit_to_init']:.2%}" if pd.notna(r["submit_to_init"]) else "N/A"
    i2a = f"{r['init_to_auth']:.2%}" if pd.notna(r["init_to_auth"]) else "N/A"
    a2b = f"{r['auth_to_book']:.2%}" if pd.notna(r["auth_to_book"]) else "N/A"
    print(f"{r['group_name']:<20} {s2i:>12} {i2a:>12} {a2b:>12}")

print("\nPAIRWISE TESTS (conditional):")
run_pairwise_tests(funnel, groups, "Submit → Initiation", "visitors_submitted", "visitors_initiated")
run_pairwise_tests(funnel, groups, "Initiation → Auth", "visitors_initiated", "visitors_authorized")
run_pairwise_tests(funnel, groups, "Auth → Booking", "visitors_authorized", "visitors_booked")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — C vs B Deep Dive: Do Users Attempt Less?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Attempts per visitor by variant (from attempt-level data)

# COMMAND ----------

attempts_per_visitor = (
    df_attempts
    .groupBy("group_name", "visitor_id")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum(F.when(F.col("system_attempt_rank") == 1, 1).otherwise(0)).alias("customer_attempts"),
        F.max("is_shopping_cart_successful").alias("any_success"),
    )
)

attempts_summary = (
    attempts_per_visitor
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors_with_attempts"),
        F.avg("total_attempts").alias("avg_system_attempts"),
        F.avg("customer_attempts").alias("avg_customer_attempts"),
        F.sum(F.when(F.col("any_success") == 1, 1).otherwise(0)).alias("visitors_successful"),
    )
    .orderBy("group_name")
    .toPandas()
)

print("ATTEMPTS PER VISITOR BY VARIANT:")
print(f"{'Group':<20} {'Visitors':>12} {'Avg Sys Att':>14} {'Avg Cust Att':>14} {'Successful':>12} {'SR':>8}")
print("-" * 85)
for _, r in attempts_summary.iterrows():
    sr = r["visitors_successful"] / r["visitors_with_attempts"] if r["visitors_with_attempts"] > 0 else 0
    print(f"{r['group_name']:<20} {r['visitors_with_attempts']:>12,.0f} {r['avg_system_attempts']:>14.2f} "
          f"{r['avg_customer_attempts']:>14.2f} {r['visitors_successful']:>12,.0f} {sr:>8.2%}")

print("\nPAIRWISE TESTS — Visitor-level attempt success rate:")
run_pairwise_tests(attempts_summary, groups, "Visitor SR", "visitors_with_attempts", "visitors_successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Distribution of customer attempt counts

# COMMAND ----------

attempt_count_dist = (
    attempts_per_visitor
    .withColumn("attempt_bucket",
        F.when(F.col("customer_attempts") == 1, "1")
         .when(F.col("customer_attempts") == 2, "2")
         .when(F.col("customer_attempts") == 3, "3")
         .otherwise("4+"))
    .groupBy("group_name", "attempt_bucket")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

total_by_grp_att = attempt_count_dist.groupby("group_name")["visitors"].sum()
attempt_count_dist["pct"] = attempt_count_dist.apply(
    lambda r: r["visitors"] / total_by_grp_att.get(r["group_name"], 1), axis=1,
)

print("CUSTOMER ATTEMPT COUNT DISTRIBUTION:")
print(f"{'Bucket':<10}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
print()
print("-" * (10 + len(groups) * 22))
for bucket in ["1", "2", "3", "4+"]:
    print(f"{bucket:<10}", end="")
    for g in groups:
        row = attempt_count_dist[(attempt_count_dist["attempt_bucket"] == bucket) &
                                  (attempt_count_dist["group_name"] == g)]
        if not row.empty:
            print(f"  {row['visitors'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Visitors who loaded payment page but never submitted

# COMMAND ----------

never_submitted = (
    df_visitor
    .groupBy("group_name")
    .agg(
        F.count("*").alias("loaded"),
        F.sum(F.when(F.col("pay_submitted") == 0, 1).otherwise(0)).alias("never_submitted"),
    )
    .withColumn("never_submit_rate", F.col("never_submitted") / F.col("loaded"))
    .orderBy("group_name")
    .toPandas()
)

print("VISITORS WHO LOADED BUT NEVER SUBMITTED:")
print(f"{'Group':<20} {'Loaded':>10} {'Never Sub':>10} {'Rate':>10}")
print("-" * 55)
for _, r in never_submitted.iterrows():
    print(f"{r['group_name']:<20} {r['loaded']:>10,.0f} {r['never_submitted']:>10,.0f} {r['never_submit_rate']:>10.2%}")

print("\nPAIRWISE TESTS — Never-submitted rate:")
run_pairwise_tests(never_submitted, groups, "Never Submit Rate", "loaded", "never_submitted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Payment Method Split Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Payment method distribution (visitor level — last payment method used)

# COMMAND ----------

pm_dist = (
    df_visitor
    .filter(F.col("payment_method").isNotNull())
    .groupBy("group_name", "payment_method")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

total_pm_by_grp = pm_dist.groupby("group_name")["visitors"].sum()
pm_dist["pct"] = pm_dist.apply(
    lambda r: r["visitors"] / total_pm_by_grp.get(r["group_name"], 1), axis=1,
)

print("PAYMENT METHOD DISTRIBUTION (visitor level):")
print(f"{'Payment Method':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 22))
for pm in pm_dist.groupby("payment_method")["visitors"].sum().sort_values(ascending=False).index:
    print(f"{str(pm):<25}", end="")
    for g in groups:
        row = pm_dist[(pm_dist["payment_method"] == pm) & (pm_dist["group_name"] == g)]
        if not row.empty:
            print(f"  {row['visitors'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Payment method share — z-tests (C vs B focus)

# COMMAND ----------

if len(groups) >= 2:
    all_pms = pm_dist["payment_method"].unique()
    print("PAYMENT METHOD SHARE Z-TESTS:")
    print(f"{'Payment Method':<25} {'Comparison':<15} {'share_1':>8} {'share_2':>8} {'Δ(pp)':>8} {'p-value':>10} {'sig':>5}")
    print("-" * 85)
    pairs = [(groups[0], groups[1]), (groups[0], groups[2]), (groups[1], groups[2])] if len(groups) == 3 else [(groups[0], groups[1])]
    for pm in sorted(all_pms, key=str):
        for ga, gb in pairs:
            n_a_t = total_pm_by_grp.get(ga, 0)
            n_b_t = total_pm_by_grp.get(gb, 0)
            s_a = pm_dist[(pm_dist["payment_method"] == pm) & (pm_dist["group_name"] == ga)]["visitors"].sum()
            s_b = pm_dist[(pm_dist["payment_method"] == pm) & (pm_dist["group_name"] == gb)]["visitors"].sum()
            if n_a_t > 0 and n_b_t > 0 and (s_a + s_b) > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_t, s_a, n_b_t, s_b)
                if p < 0.1:
                    print(f"  {str(pm):<23} {ga+' vs '+gb:<15} {s_a/n_a_t:>8.2%} {s_b/n_b_t:>8.2%} "
                          f"{delta_pp:>+8.2f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Payment method share chart

# COMMAND ----------

top_pms = pm_dist.groupby("payment_method")["visitors"].sum().nlargest(8).sort_values(ascending=True).index.tolist()
y = np.arange(len(top_pms))
n_groups = len(groups)
h = 0.8 / n_groups

fig, ax = plt.subplots(figsize=(14, max(5, len(top_pms) * 0.7)))
for i, grp in enumerate(groups):
    grp_data = pm_dist[pm_dist["group_name"] == grp].set_index("payment_method").reindex(top_pms)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h, label=grp, color=COLORS[i % len(COLORS)])
    for j, (val, vol) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["visitors"].fillna(0))):
        if vol > 0:
            ax.text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={vol:,.0f})", va="center", fontsize=7)

ax.set_yticks(y + h * (n_groups - 1) / 2)
ax.set_yticklabels(top_pms, fontsize=10)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Payment Method Share by Variant (visitor level)", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Payment method preselected distribution

# COMMAND ----------

preselected_dist = (
    df_visitor
    .filter(F.col("last_payment_method_preselected").isNotNull())
    .groupBy("group_name", "last_payment_method_preselected")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

total_presel_by_grp = preselected_dist.groupby("group_name")["visitors"].sum()
preselected_dist["pct"] = preselected_dist.apply(
    lambda r: r["visitors"] / total_presel_by_grp.get(r["group_name"], 1), axis=1,
)

print("PRESELECTED PAYMENT METHOD DISTRIBUTION:")
print(f"{'Preselected':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 22))
for pm in preselected_dist.groupby("last_payment_method_preselected")["visitors"].sum().sort_values(ascending=False).index:
    print(f"{str(pm):<25}", end="")
    for g in groups:
        row = preselected_dist[(preselected_dist["last_payment_method_preselected"] == pm) &
                                (preselected_dist["group_name"] == g)]
        if not row.empty:
            print(f"  {row['visitors'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Last method submitted via pay button

# COMMAND ----------

submit_method_dist = (
    df_visitor
    .filter(F.col("last_method_submit_click").isNotNull())
    .groupBy("group_name", "last_method_submit_click")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

total_submit_by_grp = submit_method_dist.groupby("group_name")["visitors"].sum()
submit_method_dist["pct"] = submit_method_dist.apply(
    lambda r: r["visitors"] / total_submit_by_grp.get(r["group_name"], 1), axis=1,
)

print("LAST METHOD SUBMITTED (pay button click):")
print(f"{'Method Submitted':<30}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
print()
print("-" * (30 + len(groups) * 22))
for pm in submit_method_dist.groupby("last_method_submit_click")["visitors"].sum().sort_values(ascending=False).index:
    print(f"{str(pm):<30}", end="")
    for g in groups:
        row = submit_method_dist[(submit_method_dist["last_method_submit_click"] == pm) &
                                  (submit_method_dist["group_name"] == g)]
        if not row.empty:
            print(f"  {row['visitors'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Card-Specific Analysis (C's Validation Effect)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Card users — funnel by variant

# COMMAND ----------

card_funnel = (
    df_visitor
    .filter(F.col("payment_method") == "payment_card")
    .groupBy("group_name")
    .agg(
        F.count("*").alias("card_visitors"),
        F.sum("payment_initiated").alias("card_initiated"),
        F.sum("authorized").alias("card_authorized"),
        F.sum("booked").alias("card_booked"),
    )
    .orderBy("group_name")
    .toPandas()
)

card_funnel["init_rate"] = card_funnel.apply(
    lambda r: r["card_initiated"] / r["card_visitors"] if r["card_visitors"] > 0 else None, axis=1)
card_funnel["auth_rate"] = card_funnel.apply(
    lambda r: r["card_authorized"] / r["card_visitors"] if r["card_visitors"] > 0 else None, axis=1)
card_funnel["book_rate"] = card_funnel.apply(
    lambda r: r["card_booked"] / r["card_visitors"] if r["card_visitors"] > 0 else None, axis=1)

print("CARD USERS ONLY — Funnel by Variant:")
print(f"{'Group':<20} {'Card Vis':>10} {'InitR':>10} {'AuthR':>10} {'BookR':>10}")
print("-" * 65)
for _, r in card_funnel.iterrows():
    init_r = f"{r['init_rate']:.2%}" if pd.notna(r["init_rate"]) else "N/A"
    auth_r = f"{r['auth_rate']:.2%}" if pd.notna(r["auth_rate"]) else "N/A"
    book_r = f"{r['book_rate']:.2%}" if pd.notna(r["book_rate"]) else "N/A"
    print(f"{r['group_name']:<20} {r['card_visitors']:>10,.0f} {init_r:>10} {auth_r:>10} {book_r:>10}")

print("\nPAIRWISE TESTS (card users):")
run_pairwise_tests(card_funnel, groups, "Card Initiation", "card_visitors", "card_initiated")
run_pairwise_tests(card_funnel, groups, "Card Auth", "card_visitors", "card_authorized")
run_pairwise_tests(card_funnel, groups, "Card Booking", "card_visitors", "card_booked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Card scheme distribution (from attempt-level data)

# COMMAND ----------

card_attempts = df_attempts.filter(F.col("payment_method") == "payment_card")

card_scheme_dist = (
    card_attempts
    .groupBy("group_name", "payment_method_variant")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_card_by_grp = card_scheme_dist.groupby("group_name")["attempts"].sum()
card_scheme_dist["pct"] = card_scheme_dist.apply(
    lambda r: r["attempts"] / total_card_by_grp.get(r["group_name"], 1), axis=1,
)

print("CARD SCHEME DISTRIBUTION (attempt level):")
print(f"{'Card Scheme':<20}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8} {'SR_'+g:>8}", end="")
print()
print("-" * (20 + len(groups) * 30))
for scheme in card_scheme_dist.groupby("payment_method_variant")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(scheme):<20}", end="")
    for g in groups:
        row = card_scheme_dist[(card_scheme_dist["payment_method_variant"] == scheme) &
                                (card_scheme_dist["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
            print(f"  {r['attempts']:>10,} {r['pct']:>8.2%} {sr_val:>8}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8} {'N/A':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Error codes for card attempts — C vs B vs A

# COMMAND ----------

card_errors = (
    card_attempts
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_card_errors_by_grp = card_errors.groupby("group_name")["cnt"].sum()
top_card_errors = card_errors.groupby("error_code")["cnt"].sum().nlargest(15).index

print("CARD ATTEMPT ERROR CODES (share of failed card attempts):")
print(f"{'Error Code':<40}", end="")
for g in groups:
    print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
print()
print("-" * (40 + len(groups) * 20))
for ec in top_card_errors:
    print(f"{str(ec):<40}", end="")
    for g in groups:
        row = card_errors[(card_errors["error_code"] == ec) & (card_errors["group_name"] == g)]
        if not row.empty:
            cnt = row["cnt"].values[0]
            pct = cnt / total_card_errors_by_grp.get(g, 1)
            print(f"  {cnt:>8,} {pct:>8.2%}", end="")
        else:
            print(f"  {'0':>8} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Card attempt SR by variant (attempt level)

# COMMAND ----------

card_sr_by_group = (
    card_attempts
    .groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.when(F.col("system_attempt_rank") == 1, 1).otherwise(0)).alias("customer_attempts"),
        F.sum(F.when((F.col("system_attempt_rank") == 1) & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("cust_att_successes"),
    )
    .withColumn("system_sr", F.col("successes") / F.col("attempts"))
    .withColumn("customer_sr", F.col("cust_att_successes") / F.col("customer_attempts"))
    .orderBy("group_name")
    .toPandas()
)

print("CARD ATTEMPT SR BY VARIANT:")
print(f"{'Group':<20} {'Sys Att':>10} {'Sys SR':>10} {'Cust Att':>10} {'Cust SR':>10}")
print("-" * 65)
for _, r in card_sr_by_group.iterrows():
    sys_sr = f"{r['system_sr']:.2%}" if pd.notna(r["system_sr"]) else "N/A"
    cust_sr = f"{r['customer_sr']:.2%}" if pd.notna(r["customer_sr"]) else "N/A"
    print(f"{r['group_name']:<20} {r['attempts']:>10,.0f} {sys_sr:>10} {r['customer_attempts']:>10,.0f} {cust_sr:>10}")

print("\nPAIRWISE TESTS:")
run_pairwise_tests(card_sr_by_group, groups, "Card System SR", "attempts", "successes")
run_pairwise_tests(card_sr_by_group, groups, "Card Customer SR", "customer_attempts", "cust_att_successes")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Payment Method Availability

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Number of payment methods loaded

# COMMAND ----------

pm_count_summary = (
    df_visitor
    .filter(F.col("last_cnt_payment_methods_loaded").isNotNull())
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors"),
        F.avg("last_cnt_payment_methods_loaded").alias("avg_methods"),
        F.min("last_cnt_payment_methods_loaded").alias("min_methods"),
        F.max("last_cnt_payment_methods_loaded").alias("max_methods"),
        F.expr("percentile_approx(last_cnt_payment_methods_loaded, 0.5)").alias("median_methods"),
    )
    .orderBy("group_name")
    .toPandas()
)

print("PAYMENT METHODS LOADED STATISTICS:")
print(f"{'Group':<20} {'Visitors':>10} {'Avg':>8} {'Median':>8} {'Min':>6} {'Max':>6}")
print("-" * 60)
for _, r in pm_count_summary.iterrows():
    print(f"{r['group_name']:<20} {r['visitors']:>10,.0f} {r['avg_methods']:>8.2f} "
          f"{r['median_methods']:>8.0f} {r['min_methods']:>6.0f} {r['max_methods']:>6.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Specific payment method availability flags

# COMMAND ----------

availability_flags = ["if_applepay", "if_paypal", "if_klarna", "if_creditcard", "if_googlepay", "if_local_apm"]

pm_avail = (
    df_visitor
    .groupBy("group_name")
    .agg(
        F.count("*").alias("visitors"),
        *[F.sum(flag).alias(flag) for flag in availability_flags],
    )
    .orderBy("group_name")
    .toPandas()
)

print("PAYMENT METHOD AVAILABILITY BY VARIANT:")
print(f"{'Group':<20} {'Visitors':>10}", end="")
for flag in availability_flags:
    label = flag.replace("if_", "")
    print(f"  {label:>10}", end="")
print()
print("-" * (30 + len(availability_flags) * 12))
for _, r in pm_avail.iterrows():
    print(f"{r['group_name']:<20} {r['visitors']:>10,.0f}", end="")
    for flag in availability_flags:
        pct = r[flag] / r["visitors"] if r["visitors"] > 0 else 0
        print(f"  {pct:>10.2%}", end="")
    print()

print("\nPAIRWISE TESTS (payment method availability):")
for flag in availability_flags:
    label = flag.replace("if_", "") + " avail."
    run_pairwise_tests(pm_avail, groups, label, "visitors", flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Platform Breakdown

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Funnel by platform × variant

# COMMAND ----------

platform_funnel = (
    df_visitor
    .groupBy("group_name", "platform")
    .agg(
        F.count("*").alias("visitors_loaded"),
        F.sum("pay_submitted").alias("visitors_submitted"),
        F.sum("payment_initiated").alias("visitors_initiated"),
        F.sum("authorized").alias("visitors_authorized"),
        F.sum("booked").alias("visitors_booked"),
    )
    .toPandas()
)

platform_funnel["submit_rate"] = platform_funnel["visitors_submitted"] / platform_funnel["visitors_loaded"]
platform_funnel["book_rate"] = platform_funnel["visitors_booked"] / platform_funnel["visitors_loaded"]

platforms = platform_funnel["platform"].dropna().unique()

for plat in sorted(platforms, key=str):
    print(f"\n{'=' * 90}")
    print(f"Platform: {plat}")
    print(f"{'=' * 90}")
    subset = platform_funnel[platform_funnel["platform"] == plat].sort_values("group_name")
    print(f"{'Group':<20} {'Loaded':>10} {'Submitted':>10} {'Initiated':>10} {'Auth':>10} {'Booked':>10} {'SubR':>8} {'BookR':>8}")
    print("-" * 95)
    for _, r in subset.iterrows():
        sub_r = f"{r['submit_rate']:.2%}" if pd.notna(r["submit_rate"]) else "N/A"
        book_r = f"{r['book_rate']:.2%}" if pd.notna(r["book_rate"]) else "N/A"
        print(f"{r['group_name']:<20} {r['visitors_loaded']:>10,.0f} {r['visitors_submitted']:>10,.0f} "
              f"{r['visitors_initiated']:>10,.0f} {r['visitors_authorized']:>10,.0f} "
              f"{r['visitors_booked']:>10,.0f} {sub_r:>8} {book_r:>8}")

    if len(groups) >= 2:
        plat_data = subset.sort_values("group_name").reset_index(drop=True)
        if len(plat_data) >= 2:
            print(f"\n  Pairwise tests ({plat}):")
            run_pairwise_tests(plat_data, sorted(plat_data["group_name"].tolist()), "Submit Rate", "visitors_loaded", "visitors_submitted")
            run_pairwise_tests(plat_data, sorted(plat_data["group_name"].tolist()), "Booking Rate", "visitors_loaded", "visitors_booked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Payment method split by platform × variant

# COMMAND ----------

pm_by_platform = (
    df_visitor
    .filter(F.col("payment_method").isNotNull())
    .groupBy("group_name", "platform", "payment_method")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

total_pm_by_grp_plat = pm_by_platform.groupby(["group_name", "platform"])["visitors"].sum().to_dict()
pm_by_platform["pct"] = pm_by_platform.apply(
    lambda r: r["visitors"] / total_pm_by_grp_plat.get((r["group_name"], r["platform"]), 1), axis=1,
)

for plat in sorted(platforms, key=str):
    subset = pm_by_platform[pm_by_platform["platform"] == plat]
    if subset.empty:
        continue
    print(f"\n{'=' * 90}")
    print(f"Payment Method Split — Platform: {plat}")
    print(f"{'=' * 90}")
    top_pms_plat = subset.groupby("payment_method")["visitors"].sum().sort_values(ascending=False).head(8).index
    print(f"{'Payment Method':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for pm in top_pms_plat:
        print(f"{str(pm):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_method"] == pm) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['visitors'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Attempt-Level Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Overall attempt SR by variant

# COMMAND ----------

attempt_sr = (
    df_attempts
    .groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum(F.when(F.col("system_attempt_rank") == 1, 1).otherwise(0)).alias("customer_attempts"),
        F.sum(F.when((F.col("system_attempt_rank") == 1) & (F.col("is_customer_attempt_successful") == 1), 1).otherwise(0)).alias("cust_att_successes"),
        F.countDistinct("visitor_id").alias("unique_visitors"),
    )
    .withColumn("system_sr", F.col("successes") / F.col("attempts"))
    .withColumn("customer_sr", F.col("cust_att_successes") / F.col("customer_attempts"))
    .withColumn("att_per_visitor", F.col("attempts") / F.col("unique_visitors"))
    .orderBy("group_name")
    .toPandas()
)

print("ATTEMPT-LEVEL SR BY VARIANT:")
print(f"{'Group':<20} {'Sys Att':>10} {'Sys SR':>10} {'Cust Att':>10} {'Cust SR':>10} {'Att/Vis':>10}")
print("-" * 75)
for _, r in attempt_sr.iterrows():
    sys_sr = f"{r['system_sr']:.2%}" if pd.notna(r["system_sr"]) else "N/A"
    cust_sr = f"{r['customer_sr']:.2%}" if pd.notna(r["customer_sr"]) else "N/A"
    print(f"{r['group_name']:<20} {r['attempts']:>10,.0f} {sys_sr:>10} {r['customer_attempts']:>10,.0f} "
          f"{cust_sr:>10} {r['att_per_visitor']:>10.2f}")

print("\nPAIRWISE TESTS:")
run_pairwise_tests(attempt_sr, groups, "System SR", "attempts", "successes")
run_pairwise_tests(attempt_sr, groups, "Customer SR", "customer_attempts", "cust_att_successes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.2 SR by payment method (attempt level)

# COMMAND ----------

pm_attempt_sr = (
    df_attempts
    .groupBy("group_name", "payment_method")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

print("ATTEMPT SR BY PAYMENT METHOD:")
print(f"{'Payment Method':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'SR_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 22))
for pm in pm_attempt_sr.groupby("payment_method")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(pm):<25}", end="")
    for g in groups:
        row = pm_attempt_sr[(pm_attempt_sr["payment_method"] == pm) & (pm_attempt_sr["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
            print(f"  {r['attempts']:>10,} {sr_val:>8}", end="")
        else:
            print(f"  {'0':>10} {'N/A':>8}", end="")
    print()

if len(groups) >= 2:
    print(f"\nPAIRWISE TESTS BY PAYMENT METHOD:")
    for pm in pm_attempt_sr.groupby("payment_method")["attempts"].sum().sort_values(ascending=False).head(6).index:
        pm_subset = pm_attempt_sr[pm_attempt_sr["payment_method"] == pm].copy()
        run_pairwise_tests(pm_subset, groups, f"SR ({pm})", "attempts", "successes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.3 SR by payment processor

# COMMAND ----------

proc_sr = (
    df_attempts
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .toPandas()
)

total_att_by_grp = proc_sr.groupby("group_name")["attempts"].sum()
proc_sr["pct_share"] = proc_sr.apply(
    lambda r: r["attempts"] / total_att_by_grp.get(r["group_name"], 1), axis=1,
)

print("ATTEMPT SR & SHARE BY PROCESSOR:")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'share':>7} {'SR':>8}", end="")
print()
print("-" * (25 + len(groups) * 28))
for proc in proc_sr.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(proc):<25}", end="")
    for g in groups:
        row = proc_sr[(proc_sr["payment_processor"] == proc) & (proc_sr["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sr_val = f"{r['sr']:.2%}" if pd.notna(r["sr"]) else "N/A"
            print(f"  {r['attempts']:>10,} {r['pct_share']:>7.1%} {sr_val:>8}", end="")
        else:
            print(f"  {'0':>10} {'0.0%':>7} {'N/A':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.4 Error code distribution — all payment methods

# COMMAND ----------

all_errors = (
    df_attempts
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "error_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_all_errors_by_grp = all_errors.groupby("group_name")["cnt"].sum()
top_all_errors = all_errors.groupby("error_code")["cnt"].sum().nlargest(15).index

fig, ax = plt.subplots(figsize=(14, 7))
error_list = all_errors[all_errors["error_code"].isin(top_all_errors)].copy()
error_list["pct"] = error_list.apply(
    lambda r: r["cnt"] / total_all_errors_by_grp.get(r["group_name"], 1), axis=1,
)
err_order = error_list.groupby("error_code")["cnt"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(err_order))
n_groups = len(groups)
h = 0.8 / n_groups

for i, grp in enumerate(groups):
    grp_data = error_list[error_list["group_name"] == grp].set_index("error_code").reindex(err_order)
    ax.barh(y + i * h, grp_data["pct"].fillna(0), h, label=grp, color=COLORS[i % len(COLORS)])
    for j, (pct, cnt) in enumerate(zip(grp_data["pct"].fillna(0), grp_data["cnt"].fillna(0))):
        if cnt > 0:
            ax.text(pct + 0.002, y[j] + i * h, f"{pct:.1%} (n={cnt:,.0f})", va="center", fontsize=6)

ax.set_yticks(y + h * (n_groups - 1) / 2)
ax.set_yticklabels([str(e) for e in err_order], fontsize=8)
ax.set_title("Top Error Codes (share of failed attempts) by Variant", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 11 — Daily Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.1 Daily funnel conversion rates by variant

# COMMAND ----------

daily_funnel = (
    df_visitor
    .groupBy("assigned_dt", "group_name")
    .agg(
        F.count("*").alias("visitors_loaded"),
        F.sum("pay_submitted").alias("visitors_submitted"),
        F.sum("payment_initiated").alias("visitors_initiated"),
        F.sum("booked").alias("visitors_booked"),
    )
    .withColumn("submit_rate", F.col("visitors_submitted") / F.col("visitors_loaded"))
    .withColumn("book_rate", F.col("visitors_booked") / F.col("visitors_loaded"))
    .orderBy("assigned_dt")
    .toPandas()
)

fig, axes = plt.subplots(2, 2, figsize=(18, 10))

for ax_idx, (col, title) in enumerate([
    ("submit_rate", "Daily Submit Rate"),
    ("book_rate", "Daily Booking Rate"),
    ("visitors_loaded", "Daily Visitors Loaded"),
    ("visitors_booked", "Daily Visitors Booked"),
]):
    ax = axes[ax_idx // 2][ax_idx % 2]
    for i, (grp, grp_data) in enumerate(daily_funnel.groupby("group_name")):
        color = COLORS[i % len(COLORS)]
        if col in ("visitors_loaded", "visitors_booked"):
            ax.plot(grp_data["assigned_dt"], grp_data[col], color=color,
                    linewidth=1.5, label=grp, marker=".", markersize=4)
        else:
            ax.plot(grp_data["assigned_dt"], grp_data[col], color=color,
                    linewidth=1.5, label=grp, marker=".", markersize=4)
    ax.set_title(title, fontsize=12, fontweight="bold")
    if col in ("submit_rate", "book_rate"):
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)
    ax.tick_params(axis="x", rotation=45)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.2 Daily payment method split

# COMMAND ----------

daily_pm = (
    df_visitor
    .filter(F.col("payment_method").isNotNull())
    .withColumn("pm_group",
        F.when(F.col("payment_method") == "payment_card", "card")
         .when(F.col("payment_method") == "paypal", "paypal")
         .when(F.col("payment_method").isin("applepay", "googlepay"), "wallet")
         .otherwise("other"))
    .groupBy("assigned_dt", "group_name", "pm_group")
    .agg(F.count("*").alias("visitors"))
    .toPandas()
)

daily_pm_total = daily_pm.groupby(["assigned_dt", "group_name"])["visitors"].sum().reset_index()
daily_pm_total.columns = ["assigned_dt", "group_name", "total"]
daily_pm = daily_pm.merge(daily_pm_total, on=["assigned_dt", "group_name"])
daily_pm["pct"] = daily_pm["visitors"] / daily_pm["total"]

pm_groups_to_plot = ["card", "paypal", "wallet", "other"]
n_pm_groups = len(pm_groups_to_plot)

fig, axes = plt.subplots(1, n_pm_groups, figsize=(6 * n_pm_groups, 5))
if n_pm_groups == 1:
    axes = [axes]

for ax_idx, pm_grp in enumerate(pm_groups_to_plot):
    subset = daily_pm[daily_pm["pm_group"] == pm_grp]
    for i, (grp, grp_data) in enumerate(subset.groupby("group_name")):
        color = COLORS[i % len(COLORS)]
        axes[ax_idx].plot(grp_data["assigned_dt"], grp_data["pct"], color=color,
                          linewidth=1.5, label=grp, marker=".", markersize=3)
    axes[ax_idx].set_title(f"{pm_grp} share", fontsize=11, fontweight="bold")
    axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=8)
    axes[ax_idx].tick_params(axis="x", rotation=45)

fig.suptitle("Daily Payment Method Share by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.3 Daily attempt SR by variant

# COMMAND ----------

daily_att_sr = (
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

fig, axes = plt.subplots(1, 2, figsize=(18, 5))

for i, (grp, grp_data) in enumerate(daily_att_sr.groupby("group_name")):
    color = COLORS[i % len(COLORS)]
    axes[0].plot(grp_data["attempt_date"], grp_data["sr"], color=color,
                 linewidth=1.5, label=grp, marker=".", markersize=4)
    axes[1].plot(grp_data["attempt_date"], grp_data["attempts"], color=color,
                 linewidth=1.5, label=grp, marker=".", markersize=4)

axes[0].set_title("Daily Attempt SR", fontsize=12, fontweight="bold")
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
axes[0].legend(fontsize=9)
axes[0].tick_params(axis="x", rotation=45)

axes[1].set_title("Daily Attempt Volume", fontsize=12, fontweight="bold")
axes[1].legend(fontsize=9)
axes[1].tick_params(axis="x", rotation=45)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key questions this notebook answers:**
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 4.1–4.4 | How does the full funnel compare across A, B, C? Where does C lose vs B? |
# MAGIC | 5.1–5.3 | Do users in C attempt less (card validation deterrent)? |
# MAGIC | 6.1–6.5 | Does the payment method split change? Is card more prominent in C? |
# MAGIC | 7.1–7.4 | Among card users, does C's validation improve SR or deter users? |
# MAGIC | 8.1–8.2 | Does agnostic loading change which methods are available? |
# MAGIC | 9.1–9.2 | Do effects differ by platform (web vs mobile)? |
# MAGIC | 10.1–10.4 | Attempt-level SR, processor, and error code differences |
# MAGIC | 11.1–11.3 | Are effects consistent over time or time-dependent? |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of agnostic payment methods experiment analysis*
