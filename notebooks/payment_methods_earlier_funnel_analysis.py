# Databricks notebook source
# MAGIC %md
# MAGIC # Payment Methods Earlier in the Funnel — Experiment Analysis
# MAGIC
# MAGIC **Experiment:** `pay-showing-payment-methods-earlier-in-the-funnel`
# MAGIC
# MAGIC **Hypothesis:** Showing available payment methods earlier (on Cart and Checkout
# MAGIC pages, before the Payment page) gives customers transparency and confidence,
# MAGIC potentially lifting overall conversion.
# MAGIC
# MAGIC **Observation:** Overall conversion is slightly **down**, driven by fewer visitors
# MAGIC proceeding to the payment page.
# MAGIC
# MAGIC **Goal of this notebook:**
# MAGIC 1. Determine whether the observed drop is **statistically significant**.
# MAGIC 2. Pinpoint **which funnel step** and **which segments** drive the effect.
# MAGIC 3. Check for sample-ratio mismatch, novelty effects, and platform differences.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Setup & helpers
# MAGIC 2. Data extraction (visitor-level funnel, daily × platform × group)
# MAGIC 3. Sample Ratio Mismatch (SRM) check
# MAGIC 4. Overall funnel — significance tests
# MAGIC 5. Conditional conversion — where exactly does the funnel break?
# MAGIC 6. Platform-level breakdown
# MAGIC 7. Daily trends — novelty / time effects
# MAGIC 8. Cumulative conversion over time
# MAGIC 9. Summary & recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Helpers

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from scipy.stats import norm, chi2

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]

EXPERIMENT_ID = "pay-showing-payment-methods-earlier-in-the-funnel"
START_DATE = "2026-02-27"

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test (control=1, test=2).
    Returns (z_stat, p_value, delta_pp, ci_lower_pp, ci_upper_pp).
    Delta = test - control (positive means test is higher)."""
    if n1 == 0 or n2 == 0 or (s1 + s2) == 0:
        return 0.0, 1.0, 0.0, 0.0, 0.0
    p1, p2 = s1 / n1, s2 / n2
    delta = p2 - p1
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

def print_test_result(label, n_ctrl, s_ctrl, n_test, s_test, ctrl_name, test_name):
    """Print a formatted z-test result line."""
    if n_ctrl == 0 or n_test == 0:
        return
    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_ctrl, s_ctrl, n_test, s_test)
    r_ctrl = s_ctrl / n_ctrl
    r_test = s_test / n_test
    rel_change = (r_test - r_ctrl) / r_ctrl * 100 if r_ctrl > 0 else 0
    print(f"  {label:<35} {ctrl_name}={r_ctrl:.4%}  {test_name}={r_test:.4%}  "
          f"Δ={delta_pp:+.3f}pp ({rel_change:+.2f}%)  "
          f"95%CI=[{ci_lo:+.3f}, {ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Run the funnel query (daily × platform × group)

# COMMAND ----------

df_raw = spark.sql(f"""
WITH
filtered_customers AS (
  SELECT DISTINCT ctv.visitor_id
  FROM production.dwh.dim_customer c
  JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
  WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
),

assignment_raw AS (
  SELECT
    e.group_name,
    e.user_id AS visitor_id,
    COALESCE(
      e.user_dimensions:custom.visitorPlatform,
      e.user_dimensions:visitorPlatform
    ) AS visitor_platform,
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc
    ON e.user_id = fc.visitor_id
  WHERE DATE(e.timestamp) >= '{START_DATE}'
    AND e.experiment_id = '{EXPERIMENT_ID}'
),

assignment AS (
  SELECT
    visitor_id,
    group_name,
    visitor_platform,
    assigned_at,
    CAST(assigned_at AS DATE) AS assigned_dt
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY visitor_id
        ORDER BY assigned_at ASC
      ) AS rn
    FROM assignment_raw
    WHERE visitor_platform IS NOT NULL
  )
  WHERE rn = 1
),

payment_initiation AS (
  SELECT
    user.visitor_id,
    attribution_session_id
  FROM production.events.events s
  WHERE (
      (event_name IN ('UISubmit') AND ui.id = 'submit-payment')
      OR (event_name IN ('MobileAppUITap') AND ui.target IN ('payment'))
    )
    AND container_name IN ('Payment','PaymentInformation')
    AND date >= '{START_DATE}'
),

sessions_post_assignment AS (
  SELECT
    a.visitor_id,
    a.group_name,
    a.visitor_platform,
    a.assigned_at,
    a.assigned_dt,
    s.session_id,
    s.started_at,
    s.ended_at,
    s.events
  FROM assignment a
  JOIN production.dwh.fact_session s
    ON s.visitor_id = a.visitor_id
   AND s.ended_at > a.assigned_at
  WHERE CAST(s.started_at AS DATE) >= '{START_DATE}'
    AND NOT s.is_office_ip
),

session_flags AS (
  SELECT
    spa.visitor_id,
    spa.group_name,
    spa.visitor_platform,
    spa.assigned_dt,
    spa.session_id,

    CASE
      WHEN array_contains(transform(spa.events, x -> x.event_name), 'CheckoutView')
        OR array_contains(transform(spa.events, x -> x.event_name), 'CheckoutPageRequest')
      THEN 1 ELSE 0
    END AS has_checkout,

    CASE
      WHEN array_contains(transform(spa.events, x -> x.event_name), 'PaymentView')
        OR array_contains(transform(spa.events, x -> x.event_name), 'PaymentPageRequest')
      THEN 1 ELSE 0
    END AS has_payment,

    CASE WHEN pi.attribution_session_id IS NOT NULL THEN 1 ELSE 0 END AS has_payment_initiated,

    CASE
      WHEN array_contains(transform(spa.events, x -> x.event_name), 'BookAction')
      THEN 1 ELSE 0
    END AS has_booked

  FROM sessions_post_assignment spa
  LEFT JOIN payment_initiation pi
    ON spa.session_id = pi.attribution_session_id
   AND spa.visitor_id = pi.visitor_id
),

visitor_funnel AS (
  SELECT
    assigned_dt AS dt,
    visitor_platform,
    group_name,
    visitor_id,
    MAX(has_checkout) AS has_checkout,
    MAX(has_payment) AS has_payment,
    MAX(has_payment_initiated) AS has_payment_initiated,
    MAX(has_booked) AS has_booked
  FROM session_flags
  GROUP BY assigned_dt, visitor_platform, group_name, visitor_id
),

agg AS (
  SELECT
    dt,
    visitor_platform,
    group_name,
    COUNT(DISTINCT visitor_id) AS cnt_visitors,
    COUNT(DISTINCT CASE WHEN has_checkout = 1 THEN visitor_id END) AS cnt_visitors_checkout,
    COUNT(DISTINCT CASE WHEN has_payment = 1 THEN visitor_id END) AS cnt_visitors_payment,
    COUNT(DISTINCT CASE WHEN has_payment_initiated = 1 THEN visitor_id END) AS cnt_visitors_payment_initiated,
    COUNT(DISTINCT CASE WHEN has_booked = 1 THEN visitor_id END) AS cnt_visitors_booked
  FROM visitor_funnel
  GROUP BY dt, visitor_platform, group_name
)

SELECT * FROM agg
ORDER BY dt, visitor_platform, group_name
""")

df = df_raw.toPandas()
print(f"Raw rows: {len(df):,}")
print(f"Date range: {df['dt'].min()} — {df['dt'].max()}")
print(f"Platforms: {sorted(df['visitor_platform'].dropna().unique())}")
print(f"Groups: {sorted(df['group_name'].unique())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Detect control & test groups

# COMMAND ----------

groups = sorted(df["group_name"].unique())
print(f"Experiment groups: {groups}")

total_by_group = df.groupby("group_name")[["cnt_visitors"]].sum().sort_values("cnt_visitors", ascending=False)
print("\nTotal visitors by group:")
for grp, row in total_by_group.iterrows():
    print(f"  {grp}: {row['cnt_visitors']:,.0f}")

CONTROL = total_by_group.index[0]
TEST_GROUPS = [g for g in groups if g != CONTROL]
print(f"\nAssumed control (largest): {CONTROL}")
print(f"Test group(s): {TEST_GROUPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Sample Ratio Mismatch (SRM) Check
# MAGIC
# MAGIC Before analysing conversion we verify that the randomisation unit allocation
# MAGIC is balanced. An SRM indicates a data-quality or bucketing issue that can
# MAGIC invalidate the experiment entirely.

# COMMAND ----------

srm_data = df.groupby("group_name")["cnt_visitors"].sum()
n_groups = len(srm_data)
total_visitors = srm_data.sum()
expected_per_group = total_visitors / n_groups

chi2_stat = sum((obs - expected_per_group) ** 2 / expected_per_group for obs in srm_data.values)
srm_p = 1 - chi2.cdf(chi2_stat, df=n_groups - 1)

print("=" * 70)
print("SAMPLE RATIO MISMATCH (SRM) CHECK")
print("=" * 70)
print(f"  Expected 1/{n_groups} split → {expected_per_group:,.0f} per group")
for grp, cnt in srm_data.items():
    ratio = cnt / total_visitors
    print(f"  {grp}: {cnt:,.0f}  ({ratio:.4%})")
print(f"\n  χ² = {chi2_stat:.4f},  p = {srm_p:.6f}")
if srm_p < 0.01:
    print("  ⚠️  SRM DETECTED — group sizes differ more than expected by chance.")
    print("     Check bucketing logic, bot filtering, or data pipeline issues.")
else:
    print("  ✓  No SRM detected — group sizes consistent with a balanced split.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Overall Funnel: Significance Tests
# MAGIC
# MAGIC Aggregate across all dates and platforms, then run two-proportion z-tests
# MAGIC for every funnel step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Aggregate funnel numbers

# COMMAND ----------

overall = (
    df
    .groupby("group_name")[
        ["cnt_visitors", "cnt_visitors_checkout", "cnt_visitors_payment",
         "cnt_visitors_payment_initiated", "cnt_visitors_booked"]
    ]
    .sum()
    .reset_index()
)

for col in ["cnt_visitors_checkout", "cnt_visitors_payment",
            "cnt_visitors_payment_initiated", "cnt_visitors_booked"]:
    rate_col = "r_" + col.replace("cnt_visitors_", "").replace("cnt_visitors", "all")
    overall[rate_col] = overall[col] / overall["cnt_visitors"]

print("=" * 100)
print("OVERALL VISITOR FUNNEL")
print("=" * 100)
print(f"{'Group':<20} {'Visitors':>10} {'Checkout':>10} {'Payment':>10} {'Initiated':>10} {'Booked':>10}")
print("-" * 75)
for _, r in overall.iterrows():
    print(f"{r['group_name']:<20} {r['cnt_visitors']:>10,.0f} {r['cnt_visitors_checkout']:>10,.0f} "
          f"{r['cnt_visitors_payment']:>10,.0f} {r['cnt_visitors_payment_initiated']:>10,.0f} "
          f"{r['cnt_visitors_booked']:>10,.0f}")

print(f"\n{'Group':<20} {'ChkoutR':>10} {'PaymentR':>10} {'InitR':>10} {'BookR':>10}")
print("-" * 65)
for _, r in overall.iterrows():
    print(f"{r['group_name']:<20} {r['r_checkout']:>10.4%} {r['r_payment']:>10.4%} "
          f"{r['r_payment_initiated']:>10.4%} {r['r_booked']:>10.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Statistical tests — each funnel step vs control

# COMMAND ----------

ctrl = overall[overall["group_name"] == CONTROL].iloc[0]

print("=" * 110)
print("TWO-PROPORTION Z-TESTS vs CONTROL")
print("(Δ = test − control; positive = test is higher)")
print("=" * 110)

for test_grp in TEST_GROUPS:
    tst = overall[overall["group_name"] == test_grp].iloc[0]
    print(f"\n--- {test_grp} vs {CONTROL} ---")

    for label, s_col in [
        ("Checkout rate", "cnt_visitors_checkout"),
        ("Payment page rate", "cnt_visitors_payment"),
        ("Payment initiated rate", "cnt_visitors_payment_initiated"),
        ("Booking rate", "cnt_visitors_booked"),
    ]:
        print_test_result(
            label,
            ctrl["cnt_visitors"], ctrl[s_col],
            tst["cnt_visitors"], tst[s_col],
            CONTROL, test_grp,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Funnel bar chart

# COMMAND ----------

stages = ["r_checkout", "r_payment", "r_payment_initiated", "r_booked"]
stage_labels = ["Checkout", "Payment\nPage", "Payment\nInitiated", "Booked"]

fig, ax = plt.subplots(figsize=(14, 6))
x = np.arange(len(stages))
n_grps = len(groups)
w = 0.8 / n_grps

for i, grp in enumerate(groups):
    grp_data = overall[overall["group_name"] == grp].iloc[0]
    vals = [grp_data[s] for s in stages]
    bars = ax.bar(x + i * w, vals, w, label=grp, color=COLORS[i % len(COLORS)])
    for j, v in enumerate(vals):
        ax.text(x[j] + i * w, v + 0.001, f"{v:.3%}", ha="center", va="bottom",
                fontsize=8, fontweight="bold")

ax.set_xticks(x + w * (n_grps - 1) / 2)
ax.set_xticklabels(stage_labels, fontsize=11)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Visitor Funnel Conversion Rates by Variant", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Conditional Conversion
# MAGIC
# MAGIC Where exactly does the funnel break? We look at step-to-step conversion to
# MAGIC isolate the problematic transition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Conditional conversion rates

# COMMAND ----------

cond = overall.copy()
cond["checkout_to_payment"] = cond["cnt_visitors_payment"] / cond["cnt_visitors_checkout"]
cond["payment_to_initiated"] = cond["cnt_visitors_payment_initiated"] / cond["cnt_visitors_payment"]
cond["initiated_to_booked"] = cond["cnt_visitors_booked"] / cond["cnt_visitors_payment_initiated"]
cond["checkout_to_booked"] = cond["cnt_visitors_booked"] / cond["cnt_visitors_checkout"]

print("=" * 90)
print("CONDITIONAL CONVERSION RATES")
print("=" * 90)
print(f"{'Group':<20} {'Chkout→Pay':>12} {'Pay→Init':>12} {'Init→Book':>12} {'Chkout→Book':>12}")
print("-" * 72)
for _, r in cond.iterrows():
    print(f"{r['group_name']:<20} {r['checkout_to_payment']:>12.4%} {r['payment_to_initiated']:>12.4%} "
          f"{r['initiated_to_booked']:>12.4%} {r['checkout_to_booked']:>12.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Statistical tests — conditional steps

# COMMAND ----------

ctrl = cond[cond["group_name"] == CONTROL].iloc[0]

print("CONDITIONAL CONVERSION Z-TESTS vs CONTROL:")
print("=" * 110)

for test_grp in TEST_GROUPS:
    tst = cond[cond["group_name"] == test_grp].iloc[0]
    print(f"\n--- {test_grp} vs {CONTROL} ---")

    for label, n_col, s_col in [
        ("Checkout → Payment", "cnt_visitors_checkout", "cnt_visitors_payment"),
        ("Payment → Initiated", "cnt_visitors_payment", "cnt_visitors_payment_initiated"),
        ("Initiated → Booked", "cnt_visitors_payment_initiated", "cnt_visitors_booked"),
        ("Checkout → Booked (end-to-end)", "cnt_visitors_checkout", "cnt_visitors_booked"),
    ]:
        print_test_result(
            label,
            ctrl[n_col], ctrl[s_col],
            tst[n_col], tst[s_col],
            CONTROL, test_grp,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Conditional conversion chart

# COMMAND ----------

cond_stages = ["checkout_to_payment", "payment_to_initiated", "initiated_to_booked"]
cond_labels = ["Checkout →\nPayment Page", "Payment Page →\nPay Initiated", "Pay Initiated →\nBooked"]

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(cond_stages))
n_grps = len(groups)
w = 0.8 / n_grps

for i, grp in enumerate(groups):
    grp_data = cond[cond["group_name"] == grp].iloc[0]
    vals = [grp_data[s] for s in cond_stages]
    bars = ax.bar(x + i * w, vals, w, label=grp, color=COLORS[i % len(COLORS)])
    for j, v in enumerate(vals):
        ax.text(x[j] + i * w, v + 0.002, f"{v:.2%}", ha="center", va="bottom",
                fontsize=9, fontweight="bold")

ax.set_xticks(x + w * (n_grps - 1) / 2)
ax.set_xticklabels(cond_labels, fontsize=11)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Conditional Conversion by Funnel Step", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Platform-Level Breakdown
# MAGIC
# MAGIC The experiment may affect web and mobile differently. We split by platform to
# MAGIC check whether the payment-page drop is platform-specific.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Funnel by platform × variant

# COMMAND ----------

platform_agg = (
    df
    .groupby(["visitor_platform", "group_name"])[
        ["cnt_visitors", "cnt_visitors_checkout", "cnt_visitors_payment",
         "cnt_visitors_payment_initiated", "cnt_visitors_booked"]
    ]
    .sum()
    .reset_index()
)

platform_agg["r_checkout"] = platform_agg["cnt_visitors_checkout"] / platform_agg["cnt_visitors"]
platform_agg["r_payment"] = platform_agg["cnt_visitors_payment"] / platform_agg["cnt_visitors"]
platform_agg["r_payment_initiated"] = platform_agg["cnt_visitors_payment_initiated"] / platform_agg["cnt_visitors"]
platform_agg["r_booked"] = platform_agg["cnt_visitors_booked"] / platform_agg["cnt_visitors"]

platforms = sorted(platform_agg["visitor_platform"].dropna().unique(), key=str)

for plat in platforms:
    plat_data = platform_agg[platform_agg["visitor_platform"] == plat].sort_values("group_name")
    print(f"\n{'=' * 100}")
    print(f"Platform: {plat}")
    print(f"{'=' * 100}")
    print(f"{'Group':<20} {'Visitors':>10} {'ChkoutR':>10} {'PaymentR':>10} {'InitR':>10} {'BookR':>10}")
    print("-" * 75)
    for _, r in plat_data.iterrows():
        print(f"{r['group_name']:<20} {r['cnt_visitors']:>10,.0f} {r['r_checkout']:>10.4%} "
              f"{r['r_payment']:>10.4%} {r['r_payment_initiated']:>10.4%} {r['r_booked']:>10.4%}")

    p_ctrl = plat_data[plat_data["group_name"] == CONTROL]
    if p_ctrl.empty:
        continue
    p_ctrl = p_ctrl.iloc[0]

    for test_grp in TEST_GROUPS:
        p_tst = plat_data[plat_data["group_name"] == test_grp]
        if p_tst.empty:
            continue
        p_tst = p_tst.iloc[0]
        print(f"\n  Z-tests ({test_grp} vs {CONTROL}):")
        for label, s_col in [
            ("Checkout rate", "cnt_visitors_checkout"),
            ("Payment page rate", "cnt_visitors_payment"),
            ("Payment initiated rate", "cnt_visitors_payment_initiated"),
            ("Booking rate", "cnt_visitors_booked"),
        ]:
            print_test_result(
                label,
                p_ctrl["cnt_visitors"], p_ctrl[s_col],
                p_tst["cnt_visitors"], p_tst[s_col],
                CONTROL, test_grp,
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Platform breakdown chart — booking rate

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(18, 6))

for ax_idx, (metric, title) in enumerate([
    ("r_payment", "Payment Page Rate by Platform"),
    ("r_booked", "Booking Rate by Platform"),
]):
    ax = axes[ax_idx]
    plat_list = (platform_agg.groupby("visitor_platform")["cnt_visitors"].sum()
                 .nlargest(6).sort_values(ascending=True).index.tolist())
    y = np.arange(len(plat_list))
    n_grps = len(groups)
    h = 0.8 / n_grps

    for i, grp in enumerate(groups):
        grp_data = (platform_agg[platform_agg["group_name"] == grp]
                    .set_index("visitor_platform").reindex(plat_list))
        bars = ax.barh(y + i * h, grp_data[metric].fillna(0), h,
                       label=grp, color=COLORS[i % len(COLORS)])
        for j, (rate, vol) in enumerate(zip(grp_data[metric].fillna(0),
                                            grp_data["cnt_visitors"].fillna(0))):
            if vol > 0:
                ax.text(rate + 0.002, y[j] + i * h, f"{rate:.2%}\nn={vol:,.0f}",
                        va="center", fontsize=7)

    ax.set_yticks(y + h * (n_grps - 1) / 2)
    ax.set_yticklabels(plat_list, fontsize=10)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.set_title(title, fontsize=13, fontweight="bold")
    if ax_idx == 0:
        ax.legend(fontsize=9)

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Conditional conversion (Checkout → Payment) by platform

# COMMAND ----------

platform_agg["checkout_to_payment"] = (
    platform_agg["cnt_visitors_payment"] / platform_agg["cnt_visitors_checkout"]
)

fig, ax = plt.subplots(figsize=(14, 6))
plat_list = (platform_agg.groupby("visitor_platform")["cnt_visitors"].sum()
             .nlargest(6).sort_values(ascending=True).index.tolist())
y = np.arange(len(plat_list))
n_grps = len(groups)
h = 0.8 / n_grps

for i, grp in enumerate(groups):
    grp_data = (platform_agg[platform_agg["group_name"] == grp]
                .set_index("visitor_platform").reindex(plat_list))
    bars = ax.barh(y + i * h, grp_data["checkout_to_payment"].fillna(0), h,
                   label=grp, color=COLORS[i % len(COLORS)])
    for j, (rate, vol) in enumerate(zip(grp_data["checkout_to_payment"].fillna(0),
                                        grp_data["cnt_visitors_checkout"].fillna(0))):
        if vol > 0:
            ax.text(rate + 0.003, y[j] + i * h,
                    f"{rate:.2%} (n={vol:,.0f})", va="center", fontsize=7)

ax.set_yticks(y + h * (n_grps - 1) / 2)
ax.set_yticklabels(plat_list, fontsize=10)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Checkout → Payment Page Rate by Platform × Variant\n(this is where the drop happens)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Daily Trends
# MAGIC
# MAGIC Check whether the drop is consistent over time or whether there is a
# MAGIC novelty effect (users initially confused, then adapting), ramp-up artefact,
# MAGIC or a specific date driving the overall number.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Daily funnel rates by variant

# COMMAND ----------

daily = (
    df
    .groupby(["dt", "group_name"])[
        ["cnt_visitors", "cnt_visitors_checkout", "cnt_visitors_payment",
         "cnt_visitors_payment_initiated", "cnt_visitors_booked"]
    ]
    .sum()
    .reset_index()
    .sort_values("dt")
)

daily["r_checkout"] = daily["cnt_visitors_checkout"] / daily["cnt_visitors"]
daily["r_payment"] = daily["cnt_visitors_payment"] / daily["cnt_visitors"]
daily["r_payment_initiated"] = daily["cnt_visitors_payment_initiated"] / daily["cnt_visitors"]
daily["r_booked"] = daily["cnt_visitors_booked"] / daily["cnt_visitors"]
daily["checkout_to_payment"] = daily["cnt_visitors_payment"] / daily["cnt_visitors_checkout"]

metrics_daily = [
    ("r_checkout", "Daily Checkout Rate"),
    ("r_payment", "Daily Payment Page Rate"),
    ("checkout_to_payment", "Daily Checkout → Payment Rate"),
    ("r_booked", "Daily Booking Rate"),
]

fig, axes = plt.subplots(2, 2, figsize=(18, 10))

for ax_idx, (col, title) in enumerate(metrics_daily):
    ax = axes[ax_idx // 2][ax_idx % 2]
    for i, (grp, grp_data) in enumerate(daily.groupby("group_name")):
        color = COLORS[i % len(COLORS)]
        ax.plot(grp_data["dt"], grp_data[col], color=color,
                linewidth=1.5, label=grp, marker=".", markersize=4)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=9)
    ax.tick_params(axis="x", rotation=45)

fig.suptitle("Daily Funnel Metrics by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Daily visitor volume (balance check)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(16, 5))
for i, (grp, grp_data) in enumerate(daily.groupby("group_name")):
    color = COLORS[i % len(COLORS)]
    ax.plot(grp_data["dt"], grp_data["cnt_visitors"], color=color,
            linewidth=1.5, label=grp, marker=".", markersize=4)

ax.set_title("Daily Visitor Count by Variant (balance check)", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
ax.tick_params(axis="x", rotation=45)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Daily delta (test − control) for key metrics
# MAGIC
# MAGIC Plotting the daily difference helps visualise whether the gap is widening,
# MAGIC narrowing, or constant. A narrowing gap would suggest a novelty effect.

# COMMAND ----------

daily_ctrl = daily[daily["group_name"] == CONTROL].set_index("dt")

for test_grp in TEST_GROUPS:
    daily_test = daily[daily["group_name"] == test_grp].set_index("dt")
    merged = daily_ctrl[["r_checkout", "r_payment", "checkout_to_payment", "r_booked"]].join(
        daily_test[["r_checkout", "r_payment", "checkout_to_payment", "r_booked"]],
        lsuffix="_ctrl", rsuffix="_test", how="inner",
    )

    delta_cols = [
        ("r_payment", "Δ Payment Page Rate (pp)"),
        ("checkout_to_payment", "Δ Checkout→Payment (pp)"),
        ("r_booked", "Δ Booking Rate (pp)"),
    ]

    fig, axes = plt.subplots(1, len(delta_cols), figsize=(6 * len(delta_cols), 5))
    if len(delta_cols) == 1:
        axes = [axes]

    for ax_idx, (base_col, title) in enumerate(delta_cols):
        delta_series = (merged[f"{base_col}_test"] - merged[f"{base_col}_ctrl"]) * 100
        color = [COLORS[3] if d < 0 else COLORS[2] for d in delta_series]
        axes[ax_idx].bar(range(len(delta_series)), delta_series, color=color, alpha=0.7)
        axes[ax_idx].axhline(0, color="black", linewidth=0.8)

        z = np.polyfit(range(len(delta_series)), delta_series.values, 1)
        p = np.poly1d(z)
        axes[ax_idx].plot(range(len(delta_series)), p(range(len(delta_series))),
                          color="black", linewidth=1.5, linestyle="--", label=f"trend (slope={z[0]:.4f})")

        axes[ax_idx].set_title(title, fontsize=11, fontweight="bold")
        axes[ax_idx].set_ylabel("pp")
        axes[ax_idx].set_xlabel("Day index")
        axes[ax_idx].legend(fontsize=8)

    fig.suptitle(f"Daily Delta: {test_grp} − {CONTROL}", fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Cumulative Conversion Over Time
# MAGIC
# MAGIC This shows how the cumulative conversion rates evolve as more data
# MAGIC accumulates. If the lines are converging the effect may be shrinking; if
# MAGIC diverging, it's strengthening.

# COMMAND ----------

for test_grp in TEST_GROUPS:
    cum_ctrl = (
        daily[daily["group_name"] == CONTROL]
        .sort_values("dt")
        .assign(
            cum_visitors=lambda d: d["cnt_visitors"].cumsum(),
            cum_checkout=lambda d: d["cnt_visitors_checkout"].cumsum(),
            cum_payment=lambda d: d["cnt_visitors_payment"].cumsum(),
            cum_initiated=lambda d: d["cnt_visitors_payment_initiated"].cumsum(),
            cum_booked=lambda d: d["cnt_visitors_booked"].cumsum(),
        )
    )
    cum_test = (
        daily[daily["group_name"] == test_grp]
        .sort_values("dt")
        .assign(
            cum_visitors=lambda d: d["cnt_visitors"].cumsum(),
            cum_checkout=lambda d: d["cnt_visitors_checkout"].cumsum(),
            cum_payment=lambda d: d["cnt_visitors_payment"].cumsum(),
            cum_initiated=lambda d: d["cnt_visitors_payment_initiated"].cumsum(),
            cum_booked=lambda d: d["cnt_visitors_booked"].cumsum(),
        )
    )

    cum_metrics = [
        ("cum_payment", "cum_visitors", "Cumulative Payment Page Rate"),
        ("cum_booked", "cum_visitors", "Cumulative Booking Rate"),
    ]

    fig, axes = plt.subplots(1, len(cum_metrics), figsize=(9 * len(cum_metrics), 5))
    if len(cum_metrics) == 1:
        axes = [axes]

    for ax_idx, (num_col, den_col, title) in enumerate(cum_metrics):
        ctrl_rate = cum_ctrl[num_col] / cum_ctrl[den_col]
        test_rate = cum_test[num_col] / cum_test[den_col]

        axes[ax_idx].plot(cum_ctrl["dt"], ctrl_rate, color=COLORS[0],
                          linewidth=1.8, label=CONTROL)
        axes[ax_idx].plot(cum_test["dt"], test_rate, color=COLORS[1],
                          linewidth=1.8, label=test_grp)
        axes[ax_idx].set_title(title, fontsize=12, fontweight="bold")
        axes[ax_idx].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
        axes[ax_idx].legend(fontsize=10)
        axes[ax_idx].tick_params(axis="x", rotation=45)

    fig.suptitle(f"Cumulative Rates: {test_grp} vs {CONTROL}", fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Deep Dive: What Happens to Users Who See Payment Methods Earlier?
# MAGIC
# MAGIC The key question: do users who now see payment methods on the Cart/Checkout page
# MAGIC drop off **before** reaching the Payment page? Possible reasons:
# MAGIC - **Sticker shock** — seeing "credit card only" or limited options causes abandonment
# MAGIC - **Perceived completion** — users mistake the payment-method info for an actual payment step
# MAGIC - **Decision fatigue** — extra information slows down the flow
# MAGIC
# MAGIC We check whether users who reach checkout but **don't** proceed to payment
# MAGIC have changed in the test group. We also check if the users who **do** reach
# MAGIC payment are more committed (higher initiated/booked rates).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Drop-off at Checkout → Payment: absolute and relative

# COMMAND ----------

for test_grp in TEST_GROUPS:
    ctrl_row = overall[overall["group_name"] == CONTROL].iloc[0]
    test_row = overall[overall["group_name"] == test_grp].iloc[0]

    ctrl_dropoff = ctrl_row["cnt_visitors_checkout"] - ctrl_row["cnt_visitors_payment"]
    test_dropoff = test_row["cnt_visitors_checkout"] - test_row["cnt_visitors_payment"]
    ctrl_dropoff_rate = ctrl_dropoff / ctrl_row["cnt_visitors_checkout"] if ctrl_row["cnt_visitors_checkout"] > 0 else 0
    test_dropoff_rate = test_dropoff / test_row["cnt_visitors_checkout"] if test_row["cnt_visitors_checkout"] > 0 else 0

    print(f"\n{'=' * 80}")
    print(f"DROP-OFF: Checkout → Payment ({test_grp} vs {CONTROL})")
    print(f"{'=' * 80}")
    print(f"  {CONTROL}:  {ctrl_dropoff:,.0f} dropped  ({ctrl_dropoff_rate:.4%} of checkout visitors)")
    print(f"  {test_grp}:  {test_dropoff:,.0f} dropped  ({test_dropoff_rate:.4%} of checkout visitors)")

    z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(
        ctrl_row["cnt_visitors_checkout"], ctrl_dropoff,
        test_row["cnt_visitors_checkout"], test_dropoff,
    )
    rel_change = (test_dropoff_rate - ctrl_dropoff_rate) / ctrl_dropoff_rate * 100 if ctrl_dropoff_rate > 0 else 0
    print(f"\n  Drop-off rate Δ = {delta_pp:+.3f}pp ({rel_change:+.2f}%)  "
          f"95%CI=[{ci_lo:+.3f}, {ci_hi:+.3f}]  p={p:.4f}{sig_label(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Do users who reach the Payment page convert better in test?
# MAGIC
# MAGIC If showing payment methods earlier filters out less-intent visitors, those
# MAGIC who **do** reach the payment page might be more committed → higher
# MAGIC Payment→Initiated→Booked rates.

# COMMAND ----------

print("=" * 110)
print("CONVERSION QUALITY: Among visitors who reached the Payment page")
print("=" * 110)

for test_grp in TEST_GROUPS:
    ctrl_row = overall[overall["group_name"] == CONTROL].iloc[0]
    test_row = overall[overall["group_name"] == test_grp].iloc[0]

    print(f"\n--- {test_grp} vs {CONTROL} ---")

    for label, n_col, s_col in [
        ("Payment → Initiated", "cnt_visitors_payment", "cnt_visitors_payment_initiated"),
        ("Payment → Booked", "cnt_visitors_payment", "cnt_visitors_booked"),
        ("Initiated → Booked", "cnt_visitors_payment_initiated", "cnt_visitors_booked"),
    ]:
        print_test_result(
            label,
            ctrl_row[n_col], ctrl_row[s_col],
            test_row[n_col], test_row[s_col],
            CONTROL, test_grp,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Weekly Cohort Analysis
# MAGIC
# MAGIC Group data by week to smooth out daily noise and see if there's a clear
# MAGIC week-over-week trend in the delta.

# COMMAND ----------

daily_copy = daily.copy()
daily_copy["week"] = pd.to_datetime(daily_copy["dt"]).dt.isocalendar().week.astype(int)
daily_copy["year_week"] = pd.to_datetime(daily_copy["dt"]).dt.strftime("%Y-W%V")

weekly = (
    daily_copy
    .groupby(["year_week", "group_name"])[
        ["cnt_visitors", "cnt_visitors_checkout", "cnt_visitors_payment",
         "cnt_visitors_payment_initiated", "cnt_visitors_booked"]
    ]
    .sum()
    .reset_index()
)

weekly["r_payment"] = weekly["cnt_visitors_payment"] / weekly["cnt_visitors"]
weekly["r_booked"] = weekly["cnt_visitors_booked"] / weekly["cnt_visitors"]
weekly["checkout_to_payment"] = weekly["cnt_visitors_payment"] / weekly["cnt_visitors_checkout"]

print("WEEKLY FUNNEL BY VARIANT:")
print(f"{'Week':<12} {'Group':<20} {'Visitors':>10} {'PayR':>10} {'BookR':>10} {'Chk→Pay':>10}")
print("-" * 80)
for _, r in weekly.sort_values(["year_week", "group_name"]).iterrows():
    print(f"{r['year_week']:<12} {r['group_name']:<20} {r['cnt_visitors']:>10,.0f} "
          f"{r['r_payment']:>10.4%} {r['r_booked']:>10.4%} {r['checkout_to_payment']:>10.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Weekly significance tests

# COMMAND ----------

weeks = sorted(weekly["year_week"].unique())

for test_grp in TEST_GROUPS:
    print(f"\n{'=' * 110}")
    print(f"WEEKLY Z-TESTS: {test_grp} vs {CONTROL}")
    print(f"{'=' * 110}")
    print(f"{'Week':<12} {'Metric':<30} {'Ctrl':>10} {'Test':>10} {'Δ(pp)':>10} {'p-value':>10} {'sig':>5}")
    print("-" * 92)

    for wk in weeks:
        wk_ctrl = weekly[(weekly["year_week"] == wk) & (weekly["group_name"] == CONTROL)]
        wk_test = weekly[(weekly["year_week"] == wk) & (weekly["group_name"] == test_grp)]
        if wk_ctrl.empty or wk_test.empty:
            continue
        wk_ctrl, wk_test = wk_ctrl.iloc[0], wk_test.iloc[0]

        for label, n_col, s_col in [
            ("Payment page rate", "cnt_visitors", "cnt_visitors_payment"),
            ("Booking rate", "cnt_visitors", "cnt_visitors_booked"),
        ]:
            n1, s1 = wk_ctrl[n_col], wk_ctrl[s_col]
            n2, s2 = wk_test[n_col], wk_test[s_col]
            if n1 > 0 and n2 > 0:
                z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n1, s1, n2, s2)
                r1, r2 = s1 / n1, s2 / n2
                print(f"{wk:<12} {label:<30} {r1:>10.4%} {r2:>10.4%} "
                      f"{delta_pp:>+10.3f} {p:>10.4f} {sig_label(p):>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key findings to inspect after running this notebook:**
# MAGIC
# MAGIC | Section | What to look for |
# MAGIC |---|---|
# MAGIC | 3 | **SRM check** — are group sizes balanced? |
# MAGIC | 4.2 | Is the payment page rate drop **statistically significant**? Is the booking rate drop significant? |
# MAGIC | 5.2 | **Conditional conversion** — is the drop isolated to **Checkout → Payment**, or does it cascade? |
# MAGIC | 6.1 | Does the drop appear on **all platforms** or only web / only mobile? |
# MAGIC | 7.3 | Is the daily delta **narrowing** over time (novelty effect) or **constant** (real effect)? |
# MAGIC | 8 | Are cumulative rates **converging** or **diverging**? |
# MAGIC | 9.1 | How large is the **excess drop-off** between Checkout and Payment? |
# MAGIC | 9.2 | Do users who still reach Payment convert **better** (quality filtering) or the same? |
# MAGIC | 10.1 | Is the effect consistent **week over week**? |
# MAGIC
# MAGIC **Interpretation framework:**
# MAGIC - If the Checkout → Payment drop is significant but Payment → Booked is flat/positive:
# MAGIC   the earlier display **filters out** low-intent visitors. Net effect depends on volume vs quality.
# MAGIC - If the drop is only on specific platforms: consider a platform-specific rollout.
# MAGIC - If the delta narrows over time: users are adapting → consider running longer.
# MAGIC - If the delta is constant and significant: the design change genuinely hurts
# MAGIC   progression to the payment page.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of analysis*
