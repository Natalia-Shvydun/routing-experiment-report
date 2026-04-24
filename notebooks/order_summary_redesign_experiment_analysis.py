# Databricks notebook source
# MAGIC %md
# MAGIC # Order Summary Card Redesign (iOS) — Experiment Analysis
# MAGIC
# MAGIC **Experiment:**
# MAGIC - **iOS:** `pay-order-summary-card-redesign-ios::2` (from 2026-03-24)
# MAGIC
# MAGIC **Experimentation Plan:** [Order Summary Card Redesign Experimentation Plan](https://getyourguide.atlassian.net/wiki/spaces/PAYT/pages/4052877337/Order+Summary+Card+Redesign+Experimentation+Plan)
# MAGIC
# MAGIC **Hypothesis:** The redesigned order summary card on the checkout page improves clarity and
# MAGIC reduces user confusion, leading to higher conversion and fewer drop-offs.
# MAGIC
# MAGIC **Metrics:**
# MAGIC 1. **Total Conversion Rate** — visitors booked / visitors on checkout page
# MAGIC 2. **Funnel Step Rates:**
# MAGIC    - CheckoutView → PaymentView conversion
# MAGIC    - PaymentView → payment initiation (initiation rate)
# MAGIC    - Payment success rate (booked / initiated)
# MAGIC 3. **Order Summary Interaction** — % of visitors who interacted with order summary
# MAGIC 4. **Navigation Back** — % of visitors who navigated back from PersonalDetails
# MAGIC 5. **Cart SR / Customer Attempt SR** — payment attempt-level quality
# MAGIC 6. **Sample size adequacy & power analysis**
# MAGIC
# MAGIC **Base for assignment filter:** Visitors who reached **CheckoutView** (not PaymentView).
# MAGIC
# MAGIC **Sections:**
# MAGIC | # | Section |
# MAGIC |---|---|
# MAGIC | 1 | Setup & helpers |
# MAGIC | 2 | Data extraction — assignments, visitor-level events, payment attempts |
# MAGIC | 3 | SRM check |
# MAGIC | 4 | Overall metrics & significance |
# MAGIC | 5 | Funnel step analysis (CheckoutView → PaymentView → Initiation → Booked) |
# MAGIC | 6 | Order summary interaction & navigation back |
# MAGIC | 7 | Payment attempt–level analysis |
# MAGIC | 8 | Daily trends |
# MAGIC | 9 | Sample size & power analysis |
# MAGIC | 10 | Summary |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from scipy.stats import norm, chi2

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]
A_COLOR, B_COLOR = COLORS[0], COLORS[1]

EXPERIMENT_HASH = "pay-order-summary-card-redesign-ios::2"
EXPERIMENT_START = "2026-03-24"
PLATFORM = "ios"


def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"


def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test (group1 vs group2).
    Returns (z_stat, p_value, delta_pp, ci_lower_pp, ci_upper_pp).
    Delta = group1 - group2."""
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


def required_sample_size_per_group(p_baseline, mde_relative, alpha=0.05, power=0.8):
    """Minimum sample size per group for a two-proportion test."""
    z_alpha = norm.ppf(1 - alpha / 2)
    z_beta = norm.ppf(power)
    p2 = p_baseline * (1 + mde_relative)
    delta = abs(p2 - p_baseline)
    if delta == 0:
        return float("inf")
    numerator = (z_alpha + z_beta) ** 2 * (p_baseline * (1 - p_baseline) + p2 * (1 - p2))
    return int(np.ceil(numerator / delta ** 2))


def print_z_test(label, n_a, s_a, n_b, s_b, name_a, name_b):
    if n_a == 0 or n_b == 0:
        return
    z, p, d, ci_lo, ci_hi = two_proportion_z_test(n_a, s_a, n_b, s_b)
    r_a, r_b = s_a / n_a, s_b / n_b
    rel = (r_a - r_b) / r_b * 100 if r_b > 0 else 0
    print(f"  {label:<45} {name_a}={r_a:.4%}  {name_b}={r_b:.4%}  "
          f"Δ={d:+.3f}pp ({rel:+.2f}%)  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  "
          f"p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Assignments (deduplicated, filtered)

# COMMAND ----------

df_assignment = spark.sql(f"""
WITH assignment_raw AS (
    SELECT
        variation,
        visitor_id,
        timestamp AS assigned_at,
        date,
        experiment_hash,
        '{PLATFORM}' AS platform
    FROM production.experimentation_product.assignment
    WHERE experiment_hash = '{EXPERIMENT_HASH}'
      AND date >= '{EXPERIMENT_START}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY visitor_id ORDER BY timestamp
    ) = 1
),
filtered_customers AS (
    SELECT DISTINCT visitor_id
    FROM production.dwh.dim_customer c
    JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
    WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
),
non_bot_visitors AS (
    SELECT DISTINCT visitor_id, date
    FROM production.experimentation_product.fact_non_bot_visitors
    WHERE date >= '{EXPERIMENT_START}'
)
SELECT a.*
FROM assignment_raw a
LEFT ANTI JOIN filtered_customers USING (visitor_id)
JOIN non_bot_visitors USING (visitor_id, date)
""")
df_assignment.cache()

assignment_counts = df_assignment.groupBy("variation").count().toPandas()
print("Assignment counts (deduplicated, filtered):")
for _, row in assignment_counts.sort_values("variation").iterrows():
    print(f"  {row['variation']:<20}  {row['count']:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Visitor-level data — funnel events
# MAGIC
# MAGIC Base: visitors who reached **CheckoutView** (container_name = 'Checkout', event = 'CheckoutView').
# MAGIC Funnel: CheckoutView → PaymentView → Payment Initiation → Booked

# COMMAND ----------

df_assignment.createOrReplaceTempView("assignment")

df_visitors = spark.sql(f"""
WITH
checkout_view AS (
    SELECT
        user.visitor_id AS visitor_id,
        MIN(event_properties.timestamp) AS checkout_timestamp
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at - INTERVAL 120 SECONDS
    WHERE event_name = 'CheckoutView'
      AND header.platform = '{PLATFORM}'
      AND e.date >= '{EXPERIMENT_START}'
    GROUP BY user.visitor_id
),

payment_view AS (
    SELECT
        user.visitor_id AS visitor_id,
        MIN(event_properties.timestamp) AS payment_view_timestamp
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE event_name = 'MobileAppPaymentOptionsLoaded'
      AND header.platform = '{PLATFORM}'
      AND e.date >= '{EXPERIMENT_START}'
    GROUP BY user.visitor_id
),

payment_initiated AS (
    SELECT DISTINCT user.visitor_id AS visitor_id
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE (
        (event_name = 'MobileAppUITap' AND ui.target = 'payment')
        OR (event_name = 'UISubmit' AND ui.id = 'submit-payment')
        OR event_name = 'MobileAppPaymentSubmit'
    )
      AND header.platform = '{PLATFORM}'
      AND e.date >= '{EXPERIMENT_START}'
),

booked AS (
    SELECT DISTINCT user.visitor_id AS visitor_id
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE event_name = 'BookAction'
      AND e.date >= '{EXPERIMENT_START}'
),

payment_submit AS (
    SELECT
        user.visitor_id AS visitor_id,
        json_event:payment_method AS payment_method_submitted,
        event_properties.timestamp AS submit_timestamp
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE event_name = 'MobileAppPaymentSubmit'
      AND header.platform = '{PLATFORM}'
      AND e.date >= '{EXPERIMENT_START}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp) = 1
)

SELECT
    a.visitor_id,
    a.variation,
    a.platform,
    a.assigned_at,
    a.date AS assigned_date,

    CASE WHEN cv.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS reached_checkout,
    CASE WHEN pv.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS reached_payment_view,
    CASE WHEN ps.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS has_submitted,
    CASE WHEN pi.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS is_initiated,
    CASE WHEN b.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS is_booked,
    ps.payment_method_submitted

FROM assignment a
LEFT JOIN checkout_view cv ON a.visitor_id = cv.visitor_id
LEFT JOIN payment_view pv ON a.visitor_id = pv.visitor_id
LEFT JOIN payment_submit ps ON a.visitor_id = ps.visitor_id
LEFT JOIN payment_initiated pi ON a.visitor_id = pi.visitor_id
LEFT JOIN booked b ON a.visitor_id = b.visitor_id
""")
df_visitors.cache()

total = df_visitors.count()
print(f"Total visitor rows: {total:,}")
print(f"\nVisitor counts by variation:")
df_visitors.groupBy("variation").count().orderBy("variation").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Payment attempt data

# COMMAND ----------

df_attempts = spark.sql(f"""
WITH raw_attempts AS (
    SELECT
        a.visitor_id,
        a.variation,
        p.payment_provider_reference,
        p.shopping_cart_id,
        p.is_customer_attempt_successful,
        p.is_shopping_cart_successful,
        p.payment_method,
        p.payment_processor,
        p.customer_attempt_rank,
        p.payment_attempt_timestamp,
        p.payment_attempt_timestamp::date AS attempt_date
    FROM assignment a
    JOIN production.payments.fact_payment_attempt p
        ON a.visitor_id = p.visitor_id
        AND p.payment_attempt_timestamp > a.assigned_at
        AND p.payment_attempt_timestamp::date >= '{EXPERIMENT_START}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.payment_provider_reference
        ORDER BY p.payment_attempt_timestamp
    ) = 1
)
SELECT * FROM raw_attempts
""")
df_attempts.cache()

total_attempts = df_attempts.count()
distinct_ppr = df_attempts.select(F.countDistinct("payment_provider_reference")).collect()[0][0]
print(f"Customer attempts (distinct payment_provider_reference): {total_attempts:,}")
print(f"Sanity check — distinct PPR: {distinct_ppr:,}")
df_attempts.groupBy("variation").count().orderBy("variation").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Order Summary Interaction & Navigation Back data
# MAGIC
# MAGIC Based on PersonalDetails page events. Captures OrderSummaryInteraction,
# MAGIC nav_back taps, edit_cart_item taps, and next_cta taps.

# COMMAND ----------

df_os_interaction = spark.sql(f"""
WITH personal_details AS (
    SELECT
        user.visitor_id AS visitor_id,
        attribution_session_id,
        container_name AS prev_page
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at - INTERVAL 120 SECONDS
    WHERE container_name = 'PersonalDetails'
      AND header.platform = '{PLATFORM}'
      AND event_name = 'PersonalDetailsView'
      AND e.date >= '{EXPERIMENT_START}'
    GROUP BY ALL
),
interaction_events AS (
    SELECT
        p.visitor_id,
        e.event_name,
        e.ui.target AS ui_target
    FROM personal_details p
    JOIN production.events.events e
        ON p.attribution_session_id = e.attribution_session_id
        AND p.visitor_id = e.user.visitor_id
    WHERE e.container_name = 'PersonalDetails'
      AND e.header.platform = '{PLATFORM}'
      AND e.event_name IN ('OrderSummaryInteraction', 'MobileAppUITap')
      AND e.date >= '{EXPERIMENT_START}'
)
SELECT
    a.variation,
    COUNT(DISTINCT pd.visitor_id) AS pd_visitors,

    COUNT(DISTINCT CASE
        WHEN ie.event_name = 'MobileAppUITap' AND ie.ui_target = 'nav_back'
        THEN ie.visitor_id
    END) AS nav_back_visitors,

    COUNT(DISTINCT CASE
        WHEN ie.event_name = 'OrderSummaryInteraction'
        THEN ie.visitor_id
    END) AS order_summary_interaction_visitors,

    COUNT(DISTINCT CASE
        WHEN ie.event_name = 'MobileAppUITap' AND ie.ui_target = 'edit_cart_item'
        THEN ie.visitor_id
    END) AS edit_cart_visitors,

    COUNT(DISTINCT CASE
        WHEN ie.event_name = 'MobileAppUITap' AND ie.ui_target = 'next_cta'
        THEN ie.visitor_id
    END) AS next_cta_visitors,

    COUNT(DISTINCT CASE
        WHEN (ie.event_name = 'MobileAppUITap' AND ie.ui_target = 'nav_back')
          OR ie.event_name = 'OrderSummaryInteraction'
        THEN ie.visitor_id
    END) AS nav_back_or_interaction_visitors

FROM assignment a
LEFT JOIN personal_details pd ON a.visitor_id = pd.visitor_id
LEFT JOIN interaction_events ie ON pd.visitor_id = ie.visitor_id
GROUP BY a.variation
ORDER BY a.variation
""")
df_os_interaction.cache()

print("Order Summary Interaction & Navigation Back (base: PersonalDetails visitors):")
df_os_interaction.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — SRM Check
# MAGIC
# MAGIC Verify balanced allocation. Base: visitors who reached CheckoutView.

# COMMAND ----------

df_checkout = df_visitors.filter(F.col("reached_checkout") == 1)
df_checkout.cache()

checkout_total = df_checkout.count()
all_total = df_visitors.count()
print(f"Assigned visitors:          {all_total:,}")
print(f"Reached CheckoutView:       {checkout_total:,}  ({checkout_total/all_total:.1%})")
print(f"Did not reach Checkout:     {all_total - checkout_total:,}")

print("\n" + "=" * 80)
print("SAMPLE RATIO MISMATCH (SRM) CHECK — base = CheckoutView visitors")
print("=" * 80)

srm_counts = (
    df_checkout.groupBy("variation").count().toPandas()
    .sort_values("variation").reset_index(drop=True)
)
total_v = srm_counts["count"].sum()
n_groups = len(srm_counts)
expected = total_v / n_groups

chi2_stat = sum((row["count"] - expected) ** 2 / expected for _, row in srm_counts.iterrows())
srm_p = 1 - chi2.cdf(chi2_stat, df=n_groups - 1)

print(f"\n  Expected 1/{n_groups} split → {expected:,.0f} per group")
for _, row in srm_counts.iterrows():
    ratio = row["count"] / total_v
    print(f"    {row['variation']}: {row['count']:>10,}  ({ratio:.4%})")
print(f"    χ² = {chi2_stat:.4f},  p = {srm_p:.6f}")
if srm_p < 0.01:
    print("    ⚠️  SRM DETECTED")
else:
    print("    ✓  No SRM — balanced split")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Overall Metrics & Significance
# MAGIC
# MAGIC Base = visitors who reached **CheckoutView**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Overall summary table

# COMMAND ----------

overall = (
    df_checkout
    .groupBy("variation")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("reached_payment_view").alias("reached_pv"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("is_booked").alias("booked"),
        F.sum("has_submitted").alias("submitted"),
    )
    .withColumn("conversion_rate", F.col("booked") / F.col("visitors"))
    .withColumn("checkout_to_pv_rate", F.col("reached_pv") / F.col("visitors"))
    .withColumn("pv_to_initiation_rate",
                F.when(F.col("reached_pv") > 0, F.col("initiated") / F.col("reached_pv")))
    .withColumn("initiation_rate", F.col("initiated") / F.col("visitors"))
    .withColumn("payment_success_rate",
                F.when(F.col("initiated") > 0, F.col("booked") / F.col("initiated")))
    .orderBy("variation")
    .toPandas()
)

print("=" * 130)
print("OVERALL METRICS BY VARIATION (base = CheckoutView visitors)")
print("=" * 130)
print(f"{'Variation':<20} {'Visitors':>10} {'ReachPV':>10} {'CV→PV%':>10} "
      f"{'Init':>8} {'PV→Init%':>10} {'Booked':>8} {'Conv%':>8} {'PaySucc%':>10}")
print("-" * 120)
for _, r in overall.iterrows():
    cr = f"{r['conversion_rate']:.2%}" if pd.notna(r['conversion_rate']) else "N/A"
    cv_pv = f"{r['checkout_to_pv_rate']:.2%}" if pd.notna(r['checkout_to_pv_rate']) else "N/A"
    pv_init = f"{r['pv_to_initiation_rate']:.2%}" if pd.notna(r['pv_to_initiation_rate']) else "N/A"
    psr = f"{r['payment_success_rate']:.2%}" if pd.notna(r['payment_success_rate']) else "N/A"
    ir = f"{r['initiation_rate']:.2%}" if pd.notna(r['initiation_rate']) else "N/A"
    print(f"{r['variation']:<20} {r['visitors']:>10,} {r['reached_pv']:>10,.0f} {cv_pv:>10} "
          f"{r['initiated']:>8,.0f} {pv_init:>10} {r['booked']:>8,.0f} {cr:>8} {psr:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Statistical significance tests

# COMMAND ----------

print("=" * 120)
print("Z-TESTS — OVERALL METRICS")
print("=" * 120)

variations = overall["variation"].tolist()
if len(variations) >= 2:
    v_ctrl_name = variations[0]
    v_test_name = variations[1]
    ctrl = overall[overall["variation"] == v_ctrl_name].iloc[0]
    test = overall[overall["variation"] == v_test_name].iloc[0]

    print(f"\n  {v_test_name} vs {v_ctrl_name}")

    for label, n_col, s_col in [
        ("Conversion rate (CV→Booked)", "visitors", "booked"),
        ("CheckoutView → PaymentView", "visitors", "reached_pv"),
        ("PaymentView → Initiation", "reached_pv", "initiated"),
        ("Payment success rate (Init→Booked)", "initiated", "booked"),
        ("Initiation rate (CV→Init)", "visitors", "initiated"),
    ]:
        if test[n_col] > 0 and ctrl[n_col] > 0:
            print_z_test(label,
                         test[n_col], test[s_col],
                         ctrl[n_col], ctrl[s_col],
                         v_test_name, v_ctrl_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Overall metrics visualisation

# COMMAND ----------

metrics_to_plot = [
    ("conversion_rate", "Conversion Rate\n(CV → Booked)"),
    ("checkout_to_pv_rate", "CheckoutView → PaymentView"),
    ("pv_to_initiation_rate", "PaymentView → Initiation"),
    ("initiation_rate", "Initiation Rate\n(CV → Init)"),
    ("payment_success_rate", "Payment Success\n(Init → Booked)"),
]

fig, axes = plt.subplots(1, len(metrics_to_plot), figsize=(5 * len(metrics_to_plot), 6))

variations = sorted(overall["variation"].unique())
x = np.arange(len(variations))
width = 0.5

for ax_idx, (col, title) in enumerate(metrics_to_plot):
    ax = axes[ax_idx]
    vals = [overall[overall["variation"] == v][col].fillna(0).values[0] for v in variations]
    bars = ax.bar(x, vals, width, color=[COLORS[i] for i in range(len(variations))])
    for bar, val in zip(bars, vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                    f"{val:.2%}", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(variations, fontsize=9)
    ax.set_title(title, fontweight="bold", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("iOS — Order Summary Redesign: Overall Funnel Metrics",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Funnel Step Analysis
# MAGIC
# MAGIC Detailed funnel: CheckoutView → PaymentView → Payment Initiation → Booked
# MAGIC
# MAGIC Visualise both absolute counts and step-wise conversion rates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Funnel counts & step rates

# COMMAND ----------

funnel_data = (
    df_checkout
    .groupBy("variation")
    .agg(
        F.count("*").alias("checkout_view"),
        F.sum("reached_payment_view").alias("payment_view"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("is_booked").alias("booked"),
    )
    .orderBy("variation")
    .toPandas()
)

print("=" * 110)
print("FUNNEL COUNTS & STEP RATES")
print("=" * 110)
print(f"{'Variation':<20} {'CheckoutView':>14} {'PaymentView':>14} {'Initiated':>12} {'Booked':>10} "
      f"{'CV→PV':>8} {'PV→Init':>8} {'Init→Book':>10} {'CV→Book':>10}")
print("-" * 110)
for _, r in funnel_data.iterrows():
    cv_pv = r['payment_view'] / r['checkout_view'] if r['checkout_view'] > 0 else 0
    pv_init = r['initiated'] / r['payment_view'] if r['payment_view'] > 0 else 0
    init_book = r['booked'] / r['initiated'] if r['initiated'] > 0 else 0
    cv_book = r['booked'] / r['checkout_view'] if r['checkout_view'] > 0 else 0
    print(f"{r['variation']:<20} {r['checkout_view']:>14,} {r['payment_view']:>14,.0f} "
          f"{r['initiated']:>12,.0f} {r['booked']:>10,.0f} "
          f"{cv_pv:>8.2%} {pv_init:>8.2%} {init_book:>10.2%} {cv_book:>10.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Funnel visualisation

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(20, 7))

variations = sorted(funnel_data["variation"].unique())
steps = ["CheckoutView", "PaymentView", "Initiated", "Booked"]
step_cols = ["checkout_view", "payment_view", "initiated", "booked"]

ax = axes[0]
x = np.arange(len(steps))
width = 0.35
for i, var in enumerate(variations):
    row = funnel_data[funnel_data["variation"] == var].iloc[0]
    vals = [row[c] for c in step_cols]
    bars = ax.bar(x + i * width, vals, width, label=var, color=COLORS[i])
    for j, v in enumerate(vals):
        ax.text(x[j] + i * width, v + max(vals) * 0.01,
                f"{v:,.0f}", ha="center", va="bottom", fontsize=7)

ax.set_xticks(x + width / 2)
ax.set_xticklabels(steps, fontsize=10)
ax.set_title("Funnel — Absolute Counts", fontweight="bold", fontsize=12)
ax.legend(fontsize=10)
ax.set_ylabel("Visitors")

ax = axes[1]
step_rate_labels = ["CV→PV", "PV→Init", "Init→Book"]
for i, var in enumerate(variations):
    row = funnel_data[funnel_data["variation"] == var].iloc[0]
    rates = [
        row["payment_view"] / row["checkout_view"] if row["checkout_view"] > 0 else 0,
        row["initiated"] / row["payment_view"] if row["payment_view"] > 0 else 0,
        row["booked"] / row["initiated"] if row["initiated"] > 0 else 0,
    ]
    x_rates = np.arange(len(step_rate_labels))
    bars = ax.bar(x_rates + i * width, rates, width, label=var, color=COLORS[i])
    for j, v in enumerate(rates):
        ax.text(x_rates[j] + i * width, v + 0.003,
                f"{v:.2%}", ha="center", va="bottom", fontsize=9)

ax.set_xticks(x_rates + width / 2)
ax.set_xticklabels(step_rate_labels, fontsize=10)
ax.set_title("Funnel — Step Conversion Rates", fontweight="bold", fontsize=12)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)

fig.suptitle("iOS — Order Summary Redesign: Checkout Funnel",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Funnel step z-tests

# COMMAND ----------

if len(variations) >= 2:
    print("=" * 120)
    print("Z-TESTS — FUNNEL STEPS")
    print("=" * 120)

    ctrl_f = funnel_data[funnel_data["variation"] == variations[0]].iloc[0]
    test_f = funnel_data[funnel_data["variation"] == variations[1]].iloc[0]

    for label, n_col, s_col in [
        ("CheckoutView → PaymentView", "checkout_view", "payment_view"),
        ("PaymentView → Initiation", "payment_view", "initiated"),
        ("Initiation → Booked", "initiated", "booked"),
        ("Total: CheckoutView → Booked", "checkout_view", "booked"),
    ]:
        print_z_test(
            label,
            test_f[n_col], test_f[s_col],
            ctrl_f[n_col], ctrl_f[s_col],
            variations[1], variations[0],
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Order Summary Interaction & Navigation Back
# MAGIC
# MAGIC Base: visitors who viewed PersonalDetails page.
# MAGIC Measures: % who interacted with order summary, % who navigated back,
# MAGIC % who tapped edit cart, % who tapped next CTA.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Interaction rates table

# COMMAND ----------

os_pd = df_os_interaction.toPandas()

print("=" * 120)
print("ORDER SUMMARY INTERACTION & NAVIGATION BACK (base = PersonalDetails visitors)")
print("=" * 120)
print(f"{'Variation':<20} {'PD Visitors':>12} {'NavBack':>10} {'NavBack%':>10} "
      f"{'OS Interact':>12} {'OS%':>10} {'EditCart':>10} {'EditCart%':>10} "
      f"{'NextCTA':>10} {'NextCTA%':>10} {'Back|OS':>10} {'Back|OS%':>10}")
print("-" * 140)

for _, r in os_pd.iterrows():
    pd_vis = r['pd_visitors']
    if pd_vis == 0:
        continue
    print(f"{r['variation']:<20} {pd_vis:>12,} "
          f"{r['nav_back_visitors']:>10,} {r['nav_back_visitors']/pd_vis:>10.2%} "
          f"{r['order_summary_interaction_visitors']:>10,} {r['order_summary_interaction_visitors']/pd_vis:>10.2%} "
          f"{r['edit_cart_visitors']:>10,} {r['edit_cart_visitors']/pd_vis:>10.2%} "
          f"{r['next_cta_visitors']:>10,} {r['next_cta_visitors']/pd_vis:>10.2%} "
          f"{r['nav_back_or_interaction_visitors']:>10,} {r['nav_back_or_interaction_visitors']/pd_vis:>10.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Interaction z-tests

# COMMAND ----------

print("=" * 120)
print("Z-TESTS — ORDER SUMMARY INTERACTION & NAVIGATION")
print("=" * 120)

os_sorted = os_pd.sort_values("variation")
if len(os_sorted) >= 2:
    ctrl_os = os_sorted.iloc[0]
    test_os = os_sorted.iloc[1]
    v_ctrl = ctrl_os["variation"]
    v_test = test_os["variation"]

    for label, s_col in [
        ("Navigation back rate", "nav_back_visitors"),
        ("Order summary interaction rate", "order_summary_interaction_visitors"),
        ("Edit cart rate", "edit_cart_visitors"),
        ("Next CTA tap rate", "next_cta_visitors"),
        ("Nav back OR order summary interaction", "nav_back_or_interaction_visitors"),
    ]:
        print_z_test(
            label,
            test_os["pd_visitors"], test_os[s_col],
            ctrl_os["pd_visitors"], ctrl_os[s_col],
            v_test, v_ctrl,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Interaction rates visualisation

# COMMAND ----------

interaction_metrics = [
    ("nav_back_visitors", "Navigation Back"),
    ("order_summary_interaction_visitors", "Order Summary Interaction"),
    ("edit_cart_visitors", "Edit Cart"),
    ("next_cta_visitors", "Next CTA"),
    ("nav_back_or_interaction_visitors", "Nav Back OR OS Interaction"),
]

fig, ax = plt.subplots(figsize=(14, 6))

variations = sorted(os_pd["variation"].unique())
x = np.arange(len(interaction_metrics))
width = 0.35

for i, var in enumerate(variations):
    row = os_pd[os_pd["variation"] == var].iloc[0]
    pd_vis = row["pd_visitors"]
    rates = [row[col] / pd_vis if pd_vis > 0 else 0 for col, _ in interaction_metrics]
    bars = ax.bar(x + i * width, rates, width, label=var, color=COLORS[i])
    for j, v in enumerate(rates):
        ax.text(x[j] + i * width, v + 0.003,
                f"{v:.2%}", ha="center", va="bottom", fontsize=8)

ax.set_xticks(x + width / 2)
ax.set_xticklabels([label for _, label in interaction_metrics], fontsize=9, rotation=15, ha="right")
ax.set_title("iOS — Order Summary Interaction & Navigation (PersonalDetails base)",
             fontweight="bold", fontsize=12)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Payment Attempt–Level Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Attempt success rate by variation

# COMMAND ----------

attempt_summary = (
    df_attempts
    .groupBy("variation")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempts"),
        F.countDistinct("visitor_id").alias("visitors_with_attempts"),
        F.sum("is_customer_attempt_successful").alias("cust_attempt_successes"),
    )
    .withColumn("cust_attempt_sr",
                F.col("cust_attempt_successes") / F.col("customer_attempts"))
    .orderBy("variation")
    .toPandas()
)

cart_sr_df = (
    df_attempts
    .groupBy("variation", "shopping_cart_id")
    .agg(F.max("is_shopping_cart_successful").alias("cart_successful"))
    .groupBy("variation")
    .agg(
        F.count("*").alias("distinct_carts"),
        F.sum("cart_successful").alias("successful_carts"),
    )
    .withColumn("cart_sr", F.col("successful_carts") / F.col("distinct_carts"))
    .orderBy("variation")
    .toPandas()
)

attempt_summary = attempt_summary.merge(
    cart_sr_df[["variation", "distinct_carts", "successful_carts", "cart_sr"]],
    on=["variation"],
)

print("=" * 130)
print("PAYMENT ATTEMPT–LEVEL METRICS")
print("  Customer attempts = distinct payment_provider_reference")
print("  Cart SR = by distinct shopping_cart_id")
print("=" * 130)
print(f"{'Variation':<18} {'CustAttempts':>14} {'Visitors':>10} "
      f"{'Carts':>10} {'CustAttemptSR':>14} {'CartSR':>14}")
print("-" * 90)
for _, r in attempt_summary.iterrows():
    print(f"{r['variation']:<18} {r['customer_attempts']:>14,} "
          f"{r['visitors_with_attempts']:>10,} {r['distinct_carts']:>10,} "
          f"{r['cust_attempt_sr']:>14.4%} {r['cart_sr']:>14.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Attempt-level z-tests

# COMMAND ----------

print("Z-TESTS — Attempt & Cart success rate:\n")

att_sorted = attempt_summary.sort_values("variation")
if len(att_sorted) >= 2:
    ctrl, test = att_sorted.iloc[0], att_sorted.iloc[1]

    print_z_test(
        "Customer attempt SR (per distinct PPR)",
        test["customer_attempts"], test["cust_attempt_successes"],
        ctrl["customer_attempts"], ctrl["cust_attempt_successes"],
        test["variation"], ctrl["variation"],
    )
    print_z_test(
        "Cart SR (per distinct cart)",
        test["distinct_carts"], test["successful_carts"],
        ctrl["distinct_carts"], ctrl["successful_carts"],
        test["variation"], ctrl["variation"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Attempt success rate by payment method

# COMMAND ----------

method_attempt = (
    df_attempts
    .groupBy("variation", "payment_method")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
        F.countDistinct("visitor_id").alias("visitors"),
    )
    .withColumn("sr", F.col("successes") / F.col("customer_attempts"))
    .orderBy("payment_method", "variation")
    .toPandas()
)

top_methods = method_attempt.groupby("payment_method")["customer_attempts"].sum().nlargest(8).index
print(f"\n{'=' * 90}")
print(f"  Customer Attempt SR by Payment Method (distinct PPR)")
print(f"{'=' * 90}")
for method in top_methods:
    m_data = method_attempt[method_attempt["payment_method"] == method].sort_values("variation")
    for _, r in m_data.iterrows():
        print(f"  {method:<25} {r['variation']:<18} SR={r['sr']:.2%}  "
              f"(n={r['customer_attempts']:,}, visitors={r['visitors']:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Multiple attempts per visitor

# COMMAND ----------

visitor_attempts = (
    df_attempts
    .groupBy("variation", "visitor_id")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempt_count"),
        F.max("is_customer_attempt_successful").alias("any_success"),
    )
)

multi_attempt = (
    visitor_attempts
    .groupBy("variation")
    .agg(
        F.count("*").alias("visitors"),
        F.mean("customer_attempt_count").alias("avg_customer_attempts"),
        F.sum(F.when(F.col("customer_attempt_count") > 1, 1).otherwise(0)).alias("multi_attempt_visitors"),
        F.sum(F.when(F.col("customer_attempt_count") > 2, 1).otherwise(0)).alias("three_plus_attempt_visitors"),
    )
    .withColumn("multi_attempt_rate", F.col("multi_attempt_visitors") / F.col("visitors"))
    .orderBy("variation")
    .toPandas()
)

print("Multi-attempt visitor rates (by distinct customer attempts):")
print(f"{'Variation':<18} {'Visitors':>10} {'AvgCustAtt':>12} "
      f"{'Multi%':>8} {'3+%':>8}")
print("-" * 70)
for _, r in multi_attempt.iterrows():
    print(f"{r['variation']:<18} {r['visitors']:>10,} "
          f"{r['avg_customer_attempts']:>12.2f} {r['multi_attempt_rate']:>8.2%} "
          f"{r['three_plus_attempt_visitors']/r['visitors']:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Daily Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Daily conversion rate by variation

# COMMAND ----------

daily = (
    df_checkout
    .groupBy("assigned_date", "variation")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("reached_payment_view").alias("reached_pv"),
        F.sum("is_booked").alias("booked"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("has_submitted").alias("submitted"),
    )
    .withColumn("conversion_rate", F.col("booked") / F.col("visitors"))
    .withColumn("checkout_to_pv_rate", F.col("reached_pv") / F.col("visitors"))
    .withColumn("pv_to_initiation_rate",
                F.when(F.col("reached_pv") > 0, F.col("initiated") / F.col("reached_pv")))
    .withColumn("initiation_rate", F.col("initiated") / F.col("visitors"))
    .orderBy("assigned_date")
    .toPandas()
)
daily["assigned_date"] = pd.to_datetime(daily["assigned_date"])

# COMMAND ----------

variations = sorted(daily["variation"].unique())

fig, axes = plt.subplots(2, 2, figsize=(18, 10))
metrics_daily = [
    ("conversion_rate", "Conversion Rate (CV → Booked)"),
    ("checkout_to_pv_rate", "CheckoutView → PaymentView"),
    ("pv_to_initiation_rate", "PaymentView → Initiation"),
    ("visitors", "Daily Visitors (balance check)"),
]

for ax_idx, (col, title) in enumerate(metrics_daily):
    ax = axes[ax_idx // 2][ax_idx % 2]
    for i, var in enumerate(variations):
        var_data = daily[daily["variation"] == var]
        ax.plot(var_data["assigned_date"], var_data[col],
                color=COLORS[i], linewidth=1.5, label=var, marker=".", markersize=4)
    ax.set_title(title, fontsize=12, fontweight="bold")
    if col != "visitors":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)
    ax.tick_params(axis="x", rotation=45)

fig.suptitle("iOS — Order Summary Redesign: Daily Trends",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Cumulative conversion over time

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(22, 6))

for ax_idx, (num_col, den_col, title) in enumerate([
    ("booked", "visitors", "Cumulative Conversion Rate"),
    ("reached_pv", "visitors", "Cumulative CV → PV Rate"),
    ("initiated", "reached_pv", "Cumulative PV → Init Rate"),
]):
    ax = axes[ax_idx]
    for i, var in enumerate(variations):
        var_data = daily[daily["variation"] == var].sort_values("assigned_date")
        cum_num = var_data[num_col].cumsum()
        cum_den = var_data[den_col].cumsum()
        cum_rate = cum_num / cum_den
        ax.plot(var_data["assigned_date"], cum_rate,
                color=COLORS[i], linewidth=1.8, label=var)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=10)
    ax.tick_params(axis="x", rotation=45)

fig.suptitle("iOS — Order Summary Redesign: Cumulative Rates Over Time",
             fontsize=13, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Sample Size & Power Analysis
# MAGIC
# MAGIC Required sample size per group for several MDE levels,
# MAGIC and estimated remaining days.

# COMMAND ----------

MDE_LEVELS = [0.02, 0.05, 0.10]

print("=" * 130)
print("SAMPLE SIZE & POWER ANALYSIS")
print("=" * 130)

variations = overall["variation"].tolist()
if len(variations) >= 2:
    ctrl = overall[overall["variation"] == variations[0]].iloc[0]
    test = overall[overall["variation"] == variations[1]].iloc[0]
    current_n_per_group = min(ctrl["visitors"], test["visitors"])
    days_elapsed = (pd.Timestamp.now() - pd.Timestamp(EXPERIMENT_START)).days
    daily_visitors_per_group = current_n_per_group / max(days_elapsed, 1)

    print(f"\n  Current n per group: {current_n_per_group:,.0f}  "
          f"({days_elapsed} days elapsed, ~{daily_visitors_per_group:,.0f}/day/group)")
    print(f"  {'Metric':<40} {'Baseline':>10} ", end="")
    for mde in MDE_LEVELS:
        print(f"{'MDE=' + f'{mde:.0%}':>14} ", end="")
    print()
    print(f"  {'':<40} {'':>10} ", end="")
    for _ in MDE_LEVELS:
        print(f"{'n_req / +days':>14} ", end="")
    print()
    print("  " + "-" * 110)

    metrics_for_power = [
        ("Conversion rate (CV→Booked)",
         ctrl["booked"] / ctrl["visitors"] if ctrl["visitors"] > 0 else 0),
        ("CheckoutView → PaymentView",
         ctrl["reached_pv"] / ctrl["visitors"] if ctrl["visitors"] > 0 else 0),
        ("PaymentView → Initiation",
         ctrl["initiated"] / ctrl["reached_pv"] if ctrl["reached_pv"] > 0 else 0),
        ("Payment success (Init→Booked)",
         ctrl["booked"] / ctrl["initiated"] if ctrl["initiated"] > 0 else 0),
    ]

    for label, baseline in metrics_for_power:
        if baseline == 0 or baseline >= 1:
            continue
        print(f"  {label:<40} {baseline:>10.4%} ", end="")
        for mde in MDE_LEVELS:
            n_req = required_sample_size_per_group(baseline, mde)
            extra_days = max(0, (n_req - current_n_per_group) / daily_visitors_per_group)
            print(f"{n_req:>8,}/{extra_days:>4.0f}d ", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Summary
# MAGIC
# MAGIC **Key areas to inspect after running:**
# MAGIC
# MAGIC | Section | What to look for |
# MAGIC |---|---|
# MAGIC | 3 | SRM — is randomisation balanced? |
# MAGIC | 4.2 | Are overall conversion / funnel step differences significant? |
# MAGIC | 5 | Which funnel step shows the biggest difference? |
# MAGIC | 6 | Did order summary interaction / nav back rates change? |
# MAGIC | 7.1 | Does attempt-level success rate change? |
# MAGIC | 8.1 | Are daily trends stable, or is there a novelty / ramp-up effect? |
# MAGIC | 8.2 | Are cumulative rates converging or diverging? |
# MAGIC | 9 | Is current sample size sufficient? How many more days to run? |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - If order summary interaction rate changed, investigate whether it correlates with conversion changes
# MAGIC - If nav back rate increased, check whether users are exploring more or abandoning
# MAGIC - Consider slicing by new vs returning visitors if the overall effect is ambiguous

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of order summary card redesign experiment analysis*
