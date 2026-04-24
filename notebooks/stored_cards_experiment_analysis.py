# Databricks notebook source
# MAGIC %md
# MAGIC # Stored Cards on Apps — Experiment Analysis
# MAGIC
# MAGIC **Experiments:**
# MAGIC - **iOS:** `pay-payment-options-enable-stored-cards-ios::5` (from 2026-03-13)
# MAGIC - **Android:** `pay-payment-options-enable-stored-cards-android::6` (from 2026-03-19)
# MAGIC
# MAGIC **Hypothesis:** Enabling stored cards on mobile apps improves the payment experience
# MAGIC for returning customers by allowing them to reuse saved card details, reducing friction
# MAGIC and increasing conversion.
# MAGIC
# MAGIC **Segments:**
# MAGIC - **With previously stored cards** — `stored_card_references_list` is non-empty at payment page load
# MAGIC - **Without previously stored cards** — no stored card references at payment page load
# MAGIC - iOS / Android split applied everywhere
# MAGIC
# MAGIC **Metrics:**
# MAGIC 1. Conversion rate (visitors booked / visitors on payment page)
# MAGIC 2. Save card opt-in rate — **credit card submitters only** (`save_card_consent` in `MobileAppPaymentSubmit` where `payment_method` = creditcard)
# MAGIC 3. Stored cards adoption (% visitors submitting with `stored_card_reference`)
# MAGIC 4. Initiation rate (visitors initiating / visitors on payment page)
# MAGIC 5. Payment success rate (visitors booked / visitors initiated)
# MAGIC 6. Cart SR (by distinct `shopping_cart_id`), Customer attempt SR (by attempt)
# MAGIC 7. Payment method mix (with storedcard as separate method)
# MAGIC 8. Sample size adequacy & estimated days to significance
# MAGIC
# MAGIC **Sections:**
# MAGIC | # | Section |
# MAGIC |---|---|
# MAGIC | 1 | Setup & helpers |
# MAGIC | 2 | Data extraction — assignments, visitor-level events, payment attempts |
# MAGIC | 3 | Stored card events exploration |
# MAGIC | 4 | SRM check |
# MAGIC | 5 | Overall metrics & significance (platform × variation) |
# MAGIC | 6 | Segmented analysis — with / without stored cards |
# MAGIC | 7 | Stored cards adoption & opt-in |
# MAGIC | 8 | Payment attempt–level analysis |
# MAGIC | 9 | Daily trends |
# MAGIC | 10 | Sample size & power analysis |
# MAGIC | 11 | Summary |

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

IOS_HASH = "pay-payment-options-enable-stored-cards-ios::5"
ANDROID_HASH = "pay-payment-options-enable-stored-cards-android::6"
IOS_START = "2026-03-13"
ANDROID_START = "2026-03-19"
GLOBAL_START = "2026-03-13"


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
    print(f"  {label:<40} {name_a}={r_a:.4%}  {name_b}={r_b:.4%}  "
          f"Δ={d:+.3f}pp ({rel:+.2f}%)  95%CI=[{ci_lo:+.3f},{ci_hi:+.3f}]  "
          f"p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Assignments (both experiments, deduplicated)

# COMMAND ----------

df_assignment = spark.sql(f"""
WITH assignment_raw AS (
    SELECT
        variation,
        visitor_id,
        timestamp AS assigned_at,
        date,
        experiment_hash,
        CASE
            WHEN experiment_hash = '{IOS_HASH}' THEN 'ios'
            WHEN experiment_hash = '{ANDROID_HASH}' THEN 'android'
        END AS platform
    FROM production.experimentation_product.assignment
    WHERE (
        (experiment_hash = '{IOS_HASH}' AND date >= '{IOS_START}')
        OR (experiment_hash = '{ANDROID_HASH}' AND date >= '{ANDROID_START}')
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY visitor_id, experiment_hash ORDER BY timestamp
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
    WHERE date >= '{GLOBAL_START}'
)
SELECT a.*
FROM assignment_raw a
LEFT ANTI JOIN filtered_customers USING (visitor_id)
JOIN non_bot_visitors USING (visitor_id, date)
""")
df_assignment.cache()

assignment_counts = df_assignment.groupBy("platform", "variation").count().toPandas()
print("Assignment counts (deduplicated, filtered):")
for _, row in assignment_counts.sort_values(["platform", "variation"]).iterrows():
    print(f"  {row['platform']:>10}  {row['variation']:<20}  {row['count']:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Visitor-level data — events + funnel flags

# COMMAND ----------

df_assignment.createOrReplaceTempView("assignment")

df_visitors = spark.sql(f"""
WITH
payment_options_loaded AS (
    SELECT
        user.visitor_id AS visitor_id,
        CASE
            WHEN from_json(json_event:stored_card_references_list, 'ARRAY<STRING>') IS NOT NULL
                AND array_size(from_json(json_event:stored_card_references_list, 'ARRAY<STRING>')) > 0
            THEN 'with_stored_cards'
            ELSE 'without_stored_cards'
        END AS stored_cards_segment,
        array_size(
            COALESCE(from_json(json_event:stored_card_references_list, 'ARRAY<STRING>'), array())
        ) AS cnt_stored_cards,
        from_json(json_event:payment_methods_list, 'ARRAY<STRING>') AS payment_methods_list,
        json_event:payment_method_preselected AS payment_method_preselected,
        event_properties.timestamp AS pol_timestamp
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at - INTERVAL 120 SECONDS
    WHERE event_name = 'MobileAppPaymentOptionsLoaded'
      AND e.date >= '{GLOBAL_START}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp) = 1
),

payment_submit AS (
    SELECT
        user.visitor_id AS visitor_id,
        json_event:save_card_consent AS save_card_consent,
        json_event:stored_card_reference AS stored_card_reference,
        json_event:payment_method AS payment_method_submitted,
        event_properties.timestamp AS submit_timestamp
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE event_name = 'MobileAppPaymentSubmit'
      AND e.date >= '{GLOBAL_START}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user.visitor_id ORDER BY event_properties.timestamp) = 1
),

booked AS (
    SELECT DISTINCT user.visitor_id AS visitor_id
    FROM assignment a
    JOIN production.events.events e
        ON a.visitor_id = user.visitor_id
        AND event_properties.timestamp > a.assigned_at
    WHERE event_name = 'BookAction'
      AND e.date >= '{GLOBAL_START}'
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
      AND e.date >= '{GLOBAL_START}'
)

SELECT
    a.visitor_id,
    a.variation,
    a.platform,
    a.assigned_at,
    a.date AS assigned_date,

    COALESCE(pol.stored_cards_segment, 'no_payment_page') AS stored_cards_segment,
    COALESCE(pol.cnt_stored_cards, 0) AS cnt_stored_cards,

    CASE WHEN pol.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS reached_payment_page,
    pol.payment_method_preselected,

    CASE WHEN ps.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS has_submitted,
    CASE
        WHEN LOWER(CAST(ps.save_card_consent AS STRING)) IN ('true', '1', 'yes')
            AND LOWER(CAST(ps.payment_method_submitted AS STRING)) IN ('creditcard', 'payment_card')
        THEN 1
        ELSE 0
    END AS save_card_opt_in,
    CASE
        WHEN ps.stored_card_reference IS NOT NULL
            AND TRIM(CAST(ps.stored_card_reference AS STRING)) != ''
            THEN 1
        ELSE 0
    END AS used_stored_card,
    ps.payment_method_submitted,

    CASE WHEN pi.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS is_initiated,
    CASE WHEN b.visitor_id IS NOT NULL THEN 1 ELSE 0 END AS is_booked

FROM assignment a
LEFT JOIN payment_options_loaded pol ON a.visitor_id = pol.visitor_id
LEFT JOIN payment_submit ps ON a.visitor_id = ps.visitor_id
LEFT JOIN payment_initiated pi ON a.visitor_id = pi.visitor_id
LEFT JOIN booked b ON a.visitor_id = b.visitor_id
""")
df_visitors.cache()

total = df_visitors.count()
print(f"Total visitor rows: {total:,}")
print(f"\nVisitor counts by platform × variation:")
df_visitors.groupBy("platform", "variation").count().orderBy("platform", "variation").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Payment attempt data

# COMMAND ----------

df_attempts = spark.sql(f"""
WITH raw_attempts AS (
    SELECT
        a.visitor_id,
        a.variation,
        a.platform,
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
        AND p.payment_attempt_timestamp::date >= '{GLOBAL_START}'
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
df_attempts.groupBy("platform", "variation").count().orderBy("platform", "variation").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Stored Card Events Exploration
# MAGIC
# MAGIC Search for any events with "stored_card" or "save_card" in the name to identify
# MAGIC experiment-specific events from the experimentation plan.

# COMMAND ----------

df_stored_events = spark.sql(f"""
SELECT
    event_name,
    COUNT(*) AS cnt_events,
    COUNT(DISTINCT user.visitor_id) AS cnt_visitors
FROM assignment a
JOIN production.events.events e
    ON a.visitor_id = user.visitor_id
    AND event_properties.timestamp > a.assigned_at
WHERE e.date >= '{GLOBAL_START}'
  AND (
      LOWER(event_name) LIKE '%stored%card%'
      OR LOWER(event_name) LIKE '%save%card%'
      OR LOWER(event_name) LIKE '%saved%card%'
      OR LOWER(event_name) LIKE '%storedcard%'
  )
GROUP BY event_name
ORDER BY cnt_events DESC
""")

print("Stored card–related events found in experiment population:")
df_stored_events.show(30, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Check `MobileAppPaymentSubmit` field values for stored card fields:

# COMMAND ----------

df_submit_fields = spark.sql(f"""
SELECT
    json_event:save_card_consent AS save_card_consent,
    json_event:stored_card_reference AS stored_card_reference,
    json_event:is_stored_card AS is_stored_card,
    json_event:payment_method AS payment_method,
    COUNT(*) AS cnt
FROM assignment a
JOIN production.events.events e
    ON a.visitor_id = user.visitor_id
    AND event_properties.timestamp > a.assigned_at
WHERE event_name = 'MobileAppPaymentSubmit'
  AND e.date >= '{GLOBAL_START}'
GROUP BY 1, 2, 3, 4
ORDER BY cnt DESC
LIMIT 30
""")

print("MobileAppPaymentSubmit field value combinations:")
df_submit_fields.show(30, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — SRM Check
# MAGIC
# MAGIC Verify balanced allocation per platform (separate experiments = separate checks).
# MAGIC Base: visitors who reached the payment page (`MobileAppPaymentOptionsLoaded`).

# COMMAND ----------

df_pp = df_visitors.filter(F.col("reached_payment_page") == 1)
df_pp.cache()

pp_total = df_pp.count()
all_total = df_visitors.count()
print(f"Assigned visitors:            {all_total:,}")
print(f"Reached payment page:         {pp_total:,}  ({pp_total/all_total:.1%})")
print(f"Did not reach payment page:   {all_total - pp_total:,}")

print("\n" + "=" * 80)
print("SAMPLE RATIO MISMATCH (SRM) CHECK")
print("=" * 80)

for platform in ["ios", "android"]:
    platform_data = df_pp.filter(F.col("platform") == platform)
    srm_counts = (
        platform_data.groupBy("variation").count().toPandas()
        .sort_values("variation").reset_index(drop=True)
    )
    total_v = srm_counts["count"].sum()
    n_groups = len(srm_counts)
    expected = total_v / n_groups

    chi2_stat = sum((row["count"] - expected) ** 2 / expected for _, row in srm_counts.iterrows())
    srm_p = 1 - chi2.cdf(chi2_stat, df=n_groups - 1)

    print(f"\n  [{platform.upper()}]  Expected 1/{n_groups} split → {expected:,.0f} per group")
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
# MAGIC # SECTION 5 — Overall Metrics & Significance
# MAGIC
# MAGIC All metrics split by **platform × variation**.
# MAGIC Base = visitors who reached the payment page (`MobileAppPaymentOptionsLoaded`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Overall summary table

# COMMAND ----------

overall = (
    df_pp
    .groupBy("platform", "variation")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("is_booked").alias("booked"),
        F.sum("has_submitted").alias("submitted"),
        F.sum(
            F.when(
                F.lower(F.col("payment_method_submitted")).isin("creditcard", "payment_card"), 1
            ).otherwise(0)
        ).alias("card_submitters"),
        F.sum("save_card_opt_in").alias("save_card_opt_in"),
        F.sum("used_stored_card").alias("used_stored_card"),
    )
    .withColumn("conversion_rate", F.col("booked") / F.col("visitors"))
    .withColumn("initiation_rate", F.col("initiated") / F.col("visitors"))
    .withColumn("payment_success_rate",
                F.when(F.col("initiated") > 0, F.col("booked") / F.col("initiated")))
    .withColumn("save_card_opt_in_rate",
                F.when(F.col("card_submitters") > 0,
                       F.col("save_card_opt_in") / F.col("card_submitters")))
    .withColumn("stored_card_adoption_rate",
                F.when(F.col("submitted") > 0, F.col("used_stored_card") / F.col("submitted")))
    .orderBy("platform", "variation")
    .toPandas()
)

print("=" * 120)
print("OVERALL METRICS BY PLATFORM × VARIATION (base = payment page visitors)")
print("=" * 120)
print(f"{'Platform':<10} {'Variation':<20} {'Visitors':>10} {'Booked':>8} {'Conv%':>8} "
      f"{'Init':>8} {'InitR%':>8} {'PaySucc%':>8} {'Submit':>8} "
      f"{'SaveOpt':>8} {'SaveR%':>8} {'UsedSC':>8} {'AdoptR%':>8}")
print("-" * 140)
for _, r in overall.iterrows():
    cr = f"{r['conversion_rate']:.2%}" if pd.notna(r['conversion_rate']) else "N/A"
    ir = f"{r['initiation_rate']:.2%}" if pd.notna(r['initiation_rate']) else "N/A"
    psr = f"{r['payment_success_rate']:.2%}" if pd.notna(r['payment_success_rate']) else "N/A"
    sor = f"{r['save_card_opt_in_rate']:.2%}" if pd.notna(r['save_card_opt_in_rate']) else "N/A"
    scar = f"{r['stored_card_adoption_rate']:.2%}" if pd.notna(r['stored_card_adoption_rate']) else "N/A"
    print(f"{r['platform']:<10} {r['variation']:<20} {r['visitors']:>10,} {r['booked']:>8,.0f} {cr:>8} "
          f"{r['initiated']:>8,.0f} {ir:>8} {psr:>8} {r['submitted']:>8,.0f} "
          f"{r['save_card_opt_in']:>8,.0f} {sor:>8} {r['used_stored_card']:>8,.0f} {scar:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Statistical significance tests — per platform

# COMMAND ----------

print("=" * 120)
print("Z-TESTS — PER PLATFORM")
print("=" * 120)

for platform in ["ios", "android"]:
    plat = overall[overall["platform"] == platform].sort_values("variation")
    variations = plat["variation"].tolist()
    if len(variations) < 2:
        print(f"\n  [{platform.upper()}] — fewer than 2 variations, skipping")
        continue

    v_ctrl_name = variations[0]
    v_test_name = variations[1]
    ctrl = plat[plat["variation"] == v_ctrl_name].iloc[0]
    test = plat[plat["variation"] == v_test_name].iloc[0]

    print(f"\n  [{platform.upper()}]  {v_test_name} vs {v_ctrl_name}")

    for label, n_col, s_col in [
        ("Conversion rate", "visitors", "booked"),
        ("Initiation rate", "visitors", "initiated"),
        ("Payment success rate (init→book)", "initiated", "booked"),
        ("Save card opt-in rate (card only)", "card_submitters", "save_card_opt_in"),
        ("Stored card adoption rate", "submitted", "used_stored_card"),
    ]:
        if test[n_col] > 0 and ctrl[n_col] > 0:
            print_z_test(label,
                         test[n_col], test[s_col],
                         ctrl[n_col], ctrl[s_col],
                         v_test_name, v_ctrl_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Overall metrics visualisation

# COMMAND ----------

fig, axes = plt.subplots(2, 3, figsize=(22, 10))
metrics_plot = [
    ("conversion_rate", "Conversion Rate"),
    ("initiation_rate", "Initiation Rate"),
    ("payment_success_rate", "Payment Success\n(Init → Booked)"),
    ("save_card_opt_in_rate", "Save Card Opt-in Rate"),
    ("stored_card_adoption_rate", "Stored Card Adoption"),
]

for platform_idx, platform in enumerate(["ios", "android"]):
    plat = overall[overall["platform"] == platform].sort_values("variation")
    for i, (col, title) in enumerate(metrics_plot):
        ax = axes[platform_idx][i] if i < 3 else axes[platform_idx][i - 2] if platform_idx == 1 else None
        if ax is None:
            continue
        row_idx = platform_idx
        ax_idx = i

for ax_idx, (col, title) in enumerate(metrics_plot):
    ax = axes[ax_idx // 3][ax_idx % 3]
    for platform in ["ios", "android"]:
        plat = overall[overall["platform"] == platform].sort_values("variation")
        variations = plat["variation"].tolist()
        x_positions = np.arange(len(variations))
        width = 0.35
        offset = -width / 2 if platform == "ios" else width / 2
        color_base = COLORS[0] if platform == "ios" else COLORS[1]

        vals = plat[col].fillna(0).values
        bars = ax.bar(x_positions + offset, vals, width, label=platform, color=color_base,
                      alpha=0.9 if platform == "ios" else 0.6)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                        f"{val:.2%}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x_positions)
    ax.set_xticklabels(variations, fontsize=9)
    ax.set_title(title, fontweight="bold", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    if ax_idx == 0:
        ax.legend(fontsize=9)

axes[1][2].set_visible(False)
fig.suptitle("Overall Metrics — iOS vs Android by Variation", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Segmented Analysis (With / Without Stored Cards)
# MAGIC
# MAGIC Only visitors who reached the payment page (have `MobileAppPaymentOptionsLoaded`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Segment sizes

# COMMAND ----------

segment_summary = (
    df_pp
    .groupBy("platform", "variation", "stored_cards_segment")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("is_booked").alias("booked"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("has_submitted").alias("submitted"),
        F.sum(
            F.when(
                F.lower(F.col("payment_method_submitted")).isin("creditcard", "payment_card"), 1
            ).otherwise(0)
        ).alias("card_submitters"),
        F.sum("save_card_opt_in").alias("save_card_opt_in"),
        F.sum("used_stored_card").alias("used_stored_card"),
    )
    .withColumn("conversion_rate", F.col("booked") / F.col("visitors"))
    .withColumn("initiation_rate", F.col("initiated") / F.col("visitors"))
    .withColumn("payment_success_rate",
                F.when(F.col("initiated") > 0, F.col("booked") / F.col("initiated")))
    .withColumn("save_card_opt_in_rate",
                F.when(F.col("card_submitters") > 0,
                       F.col("save_card_opt_in") / F.col("card_submitters")))
    .withColumn("stored_card_adoption_rate",
                F.when(F.col("submitted") > 0, F.col("used_stored_card") / F.col("submitted")))
    .orderBy("platform", "stored_cards_segment", "variation")
    .toPandas()
)

print("=" * 140)
print("SEGMENTED METRICS (visitors who reached payment page)")
print("=" * 140)
print(f"{'Platform':<10} {'Segment':<22} {'Variation':<18} {'Visitors':>10} {'Booked':>8} "
      f"{'Conv%':>8} {'Init':>8} {'InitR%':>8} {'PaySR%':>8} "
      f"{'SaveOpt':>8} {'UsedSC':>8}")
print("-" * 150)
for _, r in segment_summary.iterrows():
    cr = f"{r['conversion_rate']:.2%}" if pd.notna(r['conversion_rate']) else "N/A"
    ir = f"{r['initiation_rate']:.2%}" if pd.notna(r['initiation_rate']) else "N/A"
    psr = f"{r['payment_success_rate']:.2%}" if pd.notna(r['payment_success_rate']) else "N/A"
    print(f"{r['platform']:<10} {r['stored_cards_segment']:<22} {r['variation']:<18} "
          f"{r['visitors']:>10,} {r['booked']:>8,.0f} {cr:>8} "
          f"{r['initiated']:>8,.0f} {ir:>8} {psr:>8} "
          f"{r['save_card_opt_in']:>8,.0f} {r['used_stored_card']:>8,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Significance tests — per segment × platform

# COMMAND ----------

print("=" * 120)
print("Z-TESTS — PER SEGMENT × PLATFORM")
print("=" * 120)

for platform in ["ios", "android"]:
    for segment in ["with_stored_cards", "without_stored_cards"]:
        seg = segment_summary[
            (segment_summary["platform"] == platform) &
            (segment_summary["stored_cards_segment"] == segment)
        ].sort_values("variation")

        if len(seg) < 2:
            continue

        v_ctrl_name = seg["variation"].iloc[0]
        v_test_name = seg["variation"].iloc[1]
        ctrl = seg.iloc[0]
        test = seg.iloc[1]

        print(f"\n  [{platform.upper()} | {segment}]  {v_test_name} vs {v_ctrl_name}")

        for label, n_col, s_col in [
            ("Conversion rate", "visitors", "booked"),
            ("Initiation rate", "visitors", "initiated"),
            ("Payment success (init→book)", "initiated", "booked"),
            ("Save card opt-in (card only)", "card_submitters", "save_card_opt_in"),
            ("Stored card adoption", "submitted", "used_stored_card"),
        ]:
            if test[n_col] > 0 and ctrl[n_col] > 0:
                print_z_test(label,
                             test[n_col], test[s_col],
                             ctrl[n_col], ctrl[s_col],
                             v_test_name, v_ctrl_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Segmented conversion rate chart

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(18, 6))

for ax_idx, platform in enumerate(["ios", "android"]):
    ax = axes[ax_idx]
    plat_seg = segment_summary[segment_summary["platform"] == platform]
    segments = sorted(plat_seg["stored_cards_segment"].unique())
    variations = sorted(plat_seg["variation"].unique())

    x = np.arange(len(segments))
    width = 0.8 / len(variations)

    for i, var in enumerate(variations):
        var_data = plat_seg[plat_seg["variation"] == var].set_index("stored_cards_segment").reindex(segments)
        vals = var_data["conversion_rate"].fillna(0).values
        bars = ax.bar(x + i * width, vals, width, label=var, color=COLORS[i])
        for j, (v, n) in enumerate(zip(vals, var_data["visitors"].fillna(0))):
            ax.text(x[j] + i * width, v + 0.002,
                    f"{v:.2%}\nn={n:,.0f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x + width * (len(variations) - 1) / 2)
    ax.set_xticklabels(segments, fontsize=9)
    ax.set_title(f"{platform.upper()} — Conversion Rate by Segment", fontweight="bold", fontsize=12)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Segmented initiation & payment success charts

# COMMAND ----------

for metric, title_suffix in [
    ("initiation_rate", "Initiation Rate"),
    ("payment_success_rate", "Payment Success Rate (Init → Booked)"),
]:
    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    for ax_idx, platform in enumerate(["ios", "android"]):
        ax = axes[ax_idx]
        plat_seg = segment_summary[segment_summary["platform"] == platform]
        segments = sorted(plat_seg["stored_cards_segment"].unique())
        variations = sorted(plat_seg["variation"].unique())
        x = np.arange(len(segments))
        width = 0.8 / len(variations)

        for i, var in enumerate(variations):
            var_data = plat_seg[plat_seg["variation"] == var].set_index("stored_cards_segment").reindex(segments)
            vals = var_data[metric].fillna(0).values
            bars = ax.bar(x + i * width, vals, width, label=var, color=COLORS[i])
            for j, v in enumerate(vals):
                if v > 0:
                    ax.text(x[j] + i * width, v + 0.002, f"{v:.2%}",
                            ha="center", va="bottom", fontsize=8)

        ax.set_xticks(x + width * (len(variations) - 1) / 2)
        ax.set_xticklabels(segments, fontsize=9)
        ax.set_title(f"{platform.upper()} — {title_suffix}", fontweight="bold", fontsize=12)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=9)

    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Stored Cards Adoption & Opt-in
# MAGIC
# MAGIC Deeper look at save-card consent and stored-card usage patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Save card opt-in — among submitters

# COMMAND ----------

df_submitters = df_pp.filter(F.col("has_submitted") == 1)

opt_in_summary = (
    df_submitters
    .groupBy("platform", "variation", "stored_cards_segment")
    .agg(
        F.count("*").alias("submitters"),
        F.sum(
            F.when(
                F.lower(F.col("payment_method_submitted")).isin("creditcard", "payment_card"), 1
            ).otherwise(0)
        ).alias("card_submitters"),
        F.sum("save_card_opt_in").alias("opted_in"),
        F.sum("used_stored_card").alias("used_stored"),
    )
    .withColumn("opt_in_rate",
                F.when(F.col("card_submitters") > 0,
                       F.col("opted_in") / F.col("card_submitters")))
    .withColumn("stored_card_usage_rate", F.col("used_stored") / F.col("submitters"))
    .orderBy("platform", "stored_cards_segment", "variation")
    .toPandas()
)

print("=" * 130)
print("SAVE CARD OPT-IN (card submitters only) & STORED CARD USAGE (all submitters)")
print("=" * 130)
print(f"{'Platform':<10} {'Segment':<22} {'Variation':<18} {'Submitters':>12} "
      f"{'CardSub':>10} {'OptedIn':>10} {'OptInR%':>10} {'UsedStored':>12} {'UsageR%':>10}")
print("-" * 130)
for _, r in opt_in_summary.iterrows():
    oir = f"{r['opt_in_rate']:.2%}" if pd.notna(r['opt_in_rate']) else "N/A"
    sur = f"{r['stored_card_usage_rate']:.2%}" if pd.notna(r['stored_card_usage_rate']) else "N/A"
    print(f"{r['platform']:<10} {r['stored_cards_segment']:<22} {r['variation']:<18} "
          f"{r['submitters']:>12,} {r['card_submitters']:>10,.0f} "
          f"{r['opted_in']:>10,.0f} {oir:>10} "
          f"{r['used_stored']:>12,.0f} {sur:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Save card opt-in z-tests

# COMMAND ----------

print("Z-TESTS — Save card opt-in rate (per segment × platform):\n")

for platform in ["ios", "android"]:
    for segment in ["with_stored_cards", "without_stored_cards"]:
        seg = opt_in_summary[
            (opt_in_summary["platform"] == platform) &
            (opt_in_summary["stored_cards_segment"] == segment)
        ].sort_values("variation")

        if len(seg) < 2:
            continue

        ctrl, test = seg.iloc[0], seg.iloc[1]
        v_ctrl_name, v_test_name = ctrl["variation"], test["variation"]

        if test["card_submitters"] > 0 and ctrl["card_submitters"] > 0:
            print_z_test(
                f"[{platform.upper()}|{segment}] Opt-in rate (card)",
                test["card_submitters"], test["opted_in"],
                ctrl["card_submitters"], ctrl["opted_in"],
                v_test_name, v_ctrl_name,
            )
        if test["submitters"] > 0 and ctrl["submitters"] > 0:
            print_z_test(
                f"[{platform.upper()}|{segment}] Stored card usage",
                test["submitters"], test["used_stored"],
                ctrl["submitters"], ctrl["used_stored"],
                v_test_name, v_ctrl_name,
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Opt-in & adoption chart

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(20, 10))

for col_idx, (metric, title) in enumerate([
    ("opt_in_rate", "Save Card Opt-in Rate"),
    ("stored_card_usage_rate", "Stored Card Usage Rate"),
]):
    for row_idx, platform in enumerate(["ios", "android"]):
        ax = axes[row_idx][col_idx]
        plat_data = opt_in_summary[opt_in_summary["platform"] == platform]
        segments = sorted(plat_data["stored_cards_segment"].unique())
        variations = sorted(plat_data["variation"].unique())
        x = np.arange(len(segments))
        width = 0.8 / max(len(variations), 1)

        for i, var in enumerate(variations):
            var_data = plat_data[plat_data["variation"] == var].set_index("stored_cards_segment").reindex(segments)
            vals = var_data[metric].fillna(0).values
            bars = ax.bar(x + i * width, vals, width, label=var, color=COLORS[i])
            for j, (v, n) in enumerate(zip(vals, var_data["submitters"].fillna(0))):
                if v > 0:
                    ax.text(x[j] + i * width, v + 0.003,
                            f"{v:.2%}\nn={n:,.0f}", ha="center", va="bottom", fontsize=7)

        ax.set_xticks(x + width * (len(variations) - 1) / 2)
        ax.set_xticklabels(segments, fontsize=9)
        ax.set_title(f"{platform.upper()} — {title}", fontweight="bold", fontsize=11)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=8)

fig.suptitle("Stored Card Opt-in & Usage by Segment × Platform × Variation",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7B — Payment Method Mix (with storedcard as separate method)
# MAGIC
# MAGIC Split by stored-cards segment to see how the feature shifts usage
# MAGIC from other payment methods to stored cards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7B.1 Payment method mix table

# COMMAND ----------

df_method_mix = (
    df_pp
    .filter(F.col("has_submitted") == 1)
    .withColumn(
        "payment_method_display",
        F.when(
            (F.col("used_stored_card") == 1), F.lit("storedcard")
        ).otherwise(F.col("payment_method_submitted"))
    )
    .groupBy("platform", "variation", "stored_cards_segment", "payment_method_display")
    .agg(F.countDistinct("visitor_id").alias("visitors"))
)

df_method_totals = (
    df_method_mix
    .groupBy("platform", "variation", "stored_cards_segment")
    .agg(F.sum("visitors").alias("total_visitors"))
)

df_method_share = (
    df_method_mix
    .join(df_method_totals, on=["platform", "variation", "stored_cards_segment"])
    .withColumn("share", F.col("visitors") / F.col("total_visitors"))
    .orderBy("platform", "stored_cards_segment", "variation",
             F.desc("visitors"))
    .toPandas()
)

for platform in ["ios", "android"]:
    for segment in ["with_stored_cards", "without_stored_cards"]:
        seg = df_method_share[
            (df_method_share["platform"] == platform) &
            (df_method_share["stored_cards_segment"] == segment)
        ]
        if seg.empty:
            continue

        print(f"\n{'=' * 100}")
        print(f"  [{platform.upper()} | {segment}] Payment Method Mix")
        print(f"{'=' * 100}")
        print(f"  {'Method':<25}", end="")

        variations = sorted(seg["variation"].unique())
        for v in variations:
            print(f"  {v + ' visitors':>14} {v + ' share':>10}", end="")
        print()
        print("  " + "-" * 90)

        all_methods = (
            seg.groupby("payment_method_display")["visitors"]
            .sum().nlargest(15).index.tolist()
        )
        for method in all_methods:
            print(f"  {method:<25}", end="")
            for v in variations:
                row = seg[
                    (seg["variation"] == v) &
                    (seg["payment_method_display"] == method)
                ]
                if not row.empty:
                    r = row.iloc[0]
                    print(f"  {r['visitors']:>14,} {r['share']:>10.2%}", end="")
                else:
                    print(f"  {'0':>14} {'0.00%':>10}", end="")
            print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7B.2 Payment method mix visualisation

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(22, 14))

for row_idx, segment in enumerate(["with_stored_cards", "without_stored_cards"]):
    for col_idx, platform in enumerate(["ios", "android"]):
        ax = axes[row_idx][col_idx]
        seg = df_method_share[
            (df_method_share["platform"] == platform) &
            (df_method_share["stored_cards_segment"] == segment)
        ]
        if seg.empty:
            ax.set_visible(False)
            continue

        top_methods = (
            seg.groupby("payment_method_display")["visitors"]
            .sum().nlargest(6).index.tolist()
        )
        seg_top = seg[seg["payment_method_display"].isin(top_methods)]
        variations = sorted(seg_top["variation"].unique())
        x = np.arange(len(top_methods))
        width = 0.8 / max(len(variations), 1)

        for i, var in enumerate(variations):
            var_data = (
                seg_top[seg_top["variation"] == var]
                .set_index("payment_method_display")
                .reindex(top_methods)
            )
            vals = var_data["share"].fillna(0).values
            bars = ax.bar(x + i * width, vals, width, label=var, color=COLORS[i])
            for j, v in enumerate(vals):
                if v > 0.005:
                    ax.text(x[j] + i * width, v + 0.005,
                            f"{v:.1%}", ha="center", va="bottom", fontsize=7)

        ax.set_xticks(x + width * (len(variations) - 1) / 2)
        ax.set_xticklabels(top_methods, fontsize=8, rotation=30, ha="right")
        ax.set_title(f"{platform.upper()} — {segment}", fontweight="bold", fontsize=11)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=8)

fig.suptitle("Payment Method Mix by Segment × Platform × Variation\n(storedcard shown separately)",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Payment Attempt–Level Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Attempt success rate by platform × variation

# COMMAND ----------

attempt_summary = (
    df_attempts
    .groupBy("platform", "variation")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempts"),
        F.countDistinct("visitor_id").alias("visitors_with_attempts"),
        F.sum("is_customer_attempt_successful").alias("cust_attempt_successes"),
    )
    .withColumn("cust_attempt_sr",
                F.col("cust_attempt_successes") / F.col("customer_attempts"))
    .orderBy("platform", "variation")
    .toPandas()
)

cart_sr_df = (
    df_attempts
    .groupBy("platform", "variation", "shopping_cart_id")
    .agg(F.max("is_shopping_cart_successful").alias("cart_successful"))
    .groupBy("platform", "variation")
    .agg(
        F.count("*").alias("distinct_carts"),
        F.sum("cart_successful").alias("successful_carts"),
    )
    .withColumn("cart_sr", F.col("successful_carts") / F.col("distinct_carts"))
    .orderBy("platform", "variation")
    .toPandas()
)

attempt_summary = attempt_summary.merge(
    cart_sr_df[["platform", "variation", "successful_carts", "cart_sr"]],
    on=["platform", "variation"],
)

print("=" * 130)
print("PAYMENT ATTEMPT–LEVEL METRICS")
print("  Customer attempts = distinct payment_provider_reference")
print("  Cart SR = by distinct shopping_cart_id")
print("=" * 130)
print(f"{'Platform':<10} {'Variation':<18} {'CustAttempts':>14} {'Visitors':>10} "
      f"{'Carts':>10} {'CustAttemptSR':>14} {'CartSR':>14}")
print("-" * 100)
for _, r in attempt_summary.iterrows():
    print(f"{r['platform']:<10} {r['variation']:<18} {r['customer_attempts']:>14,} "
          f"{r['visitors_with_attempts']:>10,} {r['distinct_carts']:>10,} "
          f"{r['cust_attempt_sr']:>14.4%} {r['cart_sr']:>14.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Attempt-level z-tests

# COMMAND ----------

print("Z-TESTS — Attempt & Cart success rate:\n")

for platform in ["ios", "android"]:
    plat = attempt_summary[attempt_summary["platform"] == platform].sort_values("variation")
    if len(plat) < 2:
        continue
    ctrl, test = plat.iloc[0], plat.iloc[1]

    print_z_test(
        f"[{platform.upper()}] Customer attempt SR (per distinct PPR)",
        test["customer_attempts"], test["cust_attempt_successes"],
        ctrl["customer_attempts"], ctrl["cust_attempt_successes"],
        test["variation"], ctrl["variation"],
    )
    print_z_test(
        f"[{platform.upper()}] Cart SR (per distinct cart)",
        test["distinct_carts"], test["successful_carts"],
        ctrl["distinct_carts"], ctrl["successful_carts"],
        test["variation"], ctrl["variation"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 Attempt success rate by payment method

# COMMAND ----------

method_attempt = (
    df_attempts
    .groupBy("platform", "variation", "payment_method")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempts"),
        F.sum("is_customer_attempt_successful").alias("successes"),
        F.countDistinct("visitor_id").alias("visitors"),
    )
    .withColumn("sr", F.col("successes") / F.col("customer_attempts"))
    .orderBy("platform", "payment_method", "variation")
    .toPandas()
)

for platform in ["ios", "android"]:
    plat = method_attempt[method_attempt["platform"] == platform]
    top_methods = plat.groupby("payment_method")["customer_attempts"].sum().nlargest(8).index
    print(f"\n{'=' * 90}")
    print(f"  [{platform.upper()}] Customer Attempt SR by Payment Method (distinct PPR)")
    print(f"{'=' * 90}")
    for method in top_methods:
        m_data = plat[plat["payment_method"] == method].sort_values("variation")
        for _, r in m_data.iterrows():
            print(f"  {method:<25} {r['variation']:<18} SR={r['sr']:.2%}  "
                  f"(n={r['customer_attempts']:,}, visitors={r['visitors']:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.4 Multiple attempts per visitor

# COMMAND ----------

visitor_attempts = (
    df_attempts
    .groupBy("platform", "variation", "visitor_id")
    .agg(
        F.countDistinct("payment_provider_reference").alias("customer_attempt_count"),
        F.max("is_customer_attempt_successful").alias("any_success"),
    )
)

multi_attempt = (
    visitor_attempts
    .groupBy("platform", "variation")
    .agg(
        F.count("*").alias("visitors"),
        F.mean("customer_attempt_count").alias("avg_customer_attempts"),
        F.sum(F.when(F.col("customer_attempt_count") > 1, 1).otherwise(0)).alias("multi_attempt_visitors"),
        F.sum(F.when(F.col("customer_attempt_count") > 2, 1).otherwise(0)).alias("three_plus_attempt_visitors"),
    )
    .withColumn("multi_attempt_rate", F.col("multi_attempt_visitors") / F.col("visitors"))
    .orderBy("platform", "variation")
    .toPandas()
)

print("Multi-attempt visitor rates (by distinct customer attempts):")
print(f"{'Platform':<10} {'Variation':<18} {'Visitors':>10} {'AvgCustAtt':>12} "
      f"{'Multi%':>8} {'3+%':>8}")
print("-" * 80)
for _, r in multi_attempt.iterrows():
    print(f"{r['platform']:<10} {r['variation']:<18} {r['visitors']:>10,} "
          f"{r['avg_customer_attempts']:>12.2f} {r['multi_attempt_rate']:>8.2%} "
          f"{r['three_plus_attempt_visitors']/r['visitors']:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Daily Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Daily conversion rate by platform × variation

# COMMAND ----------

daily = (
    df_pp
    .groupBy("assigned_date", "platform", "variation")
    .agg(
        F.count("*").alias("visitors"),
        F.sum("is_booked").alias("booked"),
        F.sum("is_initiated").alias("initiated"),
        F.sum("has_submitted").alias("submitted"),
        F.sum("save_card_opt_in").alias("save_card_opt_in"),
        F.sum("used_stored_card").alias("used_stored_card"),
    )
    .withColumn("conversion_rate", F.col("booked") / F.col("visitors"))
    .withColumn("initiation_rate", F.col("initiated") / F.col("visitors"))
    .withColumn("save_card_opt_in_rate",
                F.when(F.col("submitted") > 0, F.col("save_card_opt_in") / F.col("submitted")))
    .orderBy("assigned_date")
    .toPandas()
)
daily["assigned_date"] = pd.to_datetime(daily["assigned_date"])

# COMMAND ----------

for platform in ["ios", "android"]:
    plat = daily[daily["platform"] == platform]
    variations = sorted(plat["variation"].unique())

    fig, axes = plt.subplots(2, 2, figsize=(18, 10))
    metrics_daily = [
        ("conversion_rate", "Conversion Rate"),
        ("initiation_rate", "Initiation Rate"),
        ("save_card_opt_in_rate", "Save Card Opt-in Rate"),
        ("visitors", "Daily Visitors (balance check)"),
    ]

    for ax_idx, (col, title) in enumerate(metrics_daily):
        ax = axes[ax_idx // 2][ax_idx % 2]
        for i, var in enumerate(variations):
            var_data = plat[plat["variation"] == var]
            ax.plot(var_data["assigned_date"], var_data[col],
                    color=COLORS[i], linewidth=1.5, label=var, marker=".", markersize=4)
        ax.set_title(title, fontsize=12, fontweight="bold")
        if col != "visitors":
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=9)
        ax.tick_params(axis="x", rotation=45)

    fig.suptitle(f"{platform.upper()} — Daily Trends",
                 fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Cumulative conversion over time

# COMMAND ----------

for platform in ["ios", "android"]:
    plat = daily[daily["platform"] == platform]
    variations = sorted(plat["variation"].unique())

    fig, axes = plt.subplots(1, 2, figsize=(18, 5))

    for ax_idx, (num_col, title) in enumerate([
        ("booked", "Cumulative Conversion Rate"),
        ("initiated", "Cumulative Initiation Rate"),
    ]):
        ax = axes[ax_idx]
        for i, var in enumerate(variations):
            var_data = plat[plat["variation"] == var].sort_values("assigned_date")
            cum_num = var_data[num_col].cumsum()
            cum_den = var_data["visitors"].cumsum()
            cum_rate = cum_num / cum_den
            ax.plot(var_data["assigned_date"], cum_rate,
                    color=COLORS[i], linewidth=1.8, label=var)
        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        ax.legend(fontsize=10)
        ax.tick_params(axis="x", rotation=45)

    fig.suptitle(f"{platform.upper()} — Cumulative Rates Over Time",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Sample Size & Power Analysis
# MAGIC
# MAGIC For each metric, compute the required sample size per group for several
# MAGIC minimum detectable effect (MDE) levels, and estimate how many more days
# MAGIC of data collection are needed.

# COMMAND ----------

MDE_LEVELS = [0.02, 0.05, 0.10]

print("=" * 130)
print("SAMPLE SIZE & POWER ANALYSIS")
print("=" * 130)

for platform in ["ios", "android"]:
    plat = overall[overall["platform"] == platform].sort_values("variation")
    if len(plat) < 2:
        continue

    ctrl = plat.iloc[0]
    test = plat.iloc[1]
    current_n_per_group = min(ctrl["visitors"], test["visitors"])
    days_elapsed = (pd.Timestamp.now() - pd.Timestamp(IOS_START if platform == "ios" else ANDROID_START)).days
    daily_visitors_per_group = current_n_per_group / max(days_elapsed, 1)

    print(f"\n  [{platform.upper()}]  Current n per group: {current_n_per_group:,.0f}  "
          f"({days_elapsed} days elapsed, ~{daily_visitors_per_group:,.0f}/day/group)")
    print(f"  {'Metric':<35} {'Baseline':>10} ", end="")
    for mde in MDE_LEVELS:
        print(f"{'MDE=' + f'{mde:.0%}':>14} ", end="")
    print()
    print(f"  {'':<35} {'':>10} ", end="")
    for _ in MDE_LEVELS:
        print(f"{'n_req / +days':>14} ", end="")
    print()
    print("  " + "-" * 100)

    metrics_for_power = [
        ("Conversion rate", ctrl["booked"] / ctrl["visitors"] if ctrl["visitors"] > 0 else 0),
        ("Initiation rate", ctrl["initiated"] / ctrl["visitors"] if ctrl["visitors"] > 0 else 0),
        ("Pay success (init→book)",
         ctrl["booked"] / ctrl["initiated"] if ctrl["initiated"] > 0 else 0),
        ("Save card opt-in",
         ctrl["save_card_opt_in"] / ctrl["submitted"] if ctrl["submitted"] > 0 else 0),
    ]

    for label, baseline in metrics_for_power:
        if baseline == 0 or baseline >= 1:
            continue
        print(f"  {label:<35} {baseline:>10.4%} ", end="")
        for mde in MDE_LEVELS:
            n_req = required_sample_size_per_group(baseline, mde)
            extra_days = max(0, (n_req - current_n_per_group) / daily_visitors_per_group)
            print(f"{n_req:>8,}/{extra_days:>4.0f}d ", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 11 — Summary
# MAGIC
# MAGIC **Key areas to inspect after running:**
# MAGIC
# MAGIC | Section | What to look for |
# MAGIC |---|---|
# MAGIC | 3 | What stored-card-specific events exist? Add them to the analysis if relevant. |
# MAGIC | 4 | SRM — is randomisation balanced per platform? |
# MAGIC | 5.2 | Are overall conversion / initiation / payment success differences significant? |
# MAGIC | 6.2 | Do results differ for visitors **with** vs **without** stored cards? |
# MAGIC | 7.1 | What share of submitters opt in to save their card? |
# MAGIC | 7.2 | Is opt-in / adoption significantly different between test and control? |
# MAGIC | 8.1 | Does attempt-level success rate change? |
# MAGIC | 9.1 | Are daily trends stable, or is there a novelty / ramp-up effect? |
# MAGIC | 9.2 | Are cumulative rates converging or diverging? |
# MAGIC | 10 | Is current sample size sufficient? How many more days to run? |
# MAGIC
# MAGIC **Next steps based on Section 3 output:**
# MAGIC If the event exploration reveals stored-card-specific events (e.g.
# MAGIC `MobileAppStoredCardSelected`, `MobileAppSaveCardConsentToggled`), add cells
# MAGIC to extract and analyse those events for richer funnel insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of stored cards experiment analysis*
