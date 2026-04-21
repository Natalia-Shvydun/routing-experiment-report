# Databricks notebook source
# MAGIC %md
# MAGIC # Incident Impact Analysis — Apr 11 14:00 → Apr 13 10:00
# MAGIC
# MAGIC Estimates lost **non-cancelled bookings** and **GMV** during the incident,
# MAGIC controlling for weekday/hour seasonality and week-over-week growth trend.
# MAGIC
# MAGIC **Methodology:**
# MAGIC 1. Build counterfactual on **total bookings** (active + cancelled) to avoid
# MAGIC    cancellation-maturation bias (recent weeks have artificially low cancel rates)
# MAGIC 2. Fit per weekday×hour linear trend from ~14 clean baseline weeks
# MAGIC 3. Apply **mature cancellation rate** from baseline to convert total impact
# MAGIC    into non-cancelled bookings and GMV
# MAGIC
# MAGIC **Exclusions from baseline:**
# MAGIC - Easter period (Apr 3-6, 2026) — atypical traffic
# MAGIC - The incident window itself

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Load data

# COMMAND ----------

query = """
select
      date_of_checkout::date          as checkout_date,
      weekday(date_of_checkout::date) as weekday,
      weekofyear(date_of_checkout::date) as week,
      hour(date_of_checkout)          as checkout_hour,
      status_id,
      sum(gmv)                        as gmv,
      sum(nr)                         as nr,
      count(distinct booking_id)      as cnt_bookings
from production.dwh.fact_booking b
where date_of_checkout::date >= '2026-01-05'
  and status_id in (1, 2)
group by all
"""

raw = spark.sql(query)
raw.cache()
print(f"2026 rows loaded: {raw.count():,}")

# COMMAND ----------

# Pull last year's data around Easter 2025 for seasonal adjustment.
# Easter 2025: Apr 18-21. We pull Mar-May to have surrounding weeks.
query_ly = """
select
      date_of_checkout::date          as checkout_date,
      weekday(date_of_checkout::date) as weekday,
      weekofyear(date_of_checkout::date) as week,
      hour(date_of_checkout)          as checkout_hour,
      status_id,
      sum(gmv)                        as gmv,
      sum(nr)                         as nr,
      count(distinct booking_id)      as cnt_bookings
from production.dwh.fact_booking b
where date_of_checkout::date >= '2025-03-01'
  and date_of_checkout::date <= '2025-05-31'
  and status_id in (1, 2)
group by all
"""

raw_ly = spark.sql(query_ly)
raw_ly.cache()
print(f"2025 rows loaded: {raw_ly.count():,}")

# COMMAND ----------

import pandas as pd
import numpy as np

pdf = raw.toPandas()
pdf["checkout_date"] = pd.to_datetime(pdf["checkout_date"])
pdf["weekday"] = pdf["weekday"].astype(int)
pdf["checkout_hour"] = pdf["checkout_hour"].astype(int)
for col in ["gmv", "nr", "cnt_bookings"]:
    pdf[col] = pdf[col].astype(float)

pdf_ly = raw_ly.toPandas()
pdf_ly["checkout_date"] = pd.to_datetime(pdf_ly["checkout_date"])
pdf_ly["weekday"] = pdf_ly["weekday"].astype(int)
pdf_ly["checkout_hour"] = pdf_ly["checkout_hour"].astype(int)
for col in ["gmv", "nr", "cnt_bookings"]:
    pdf_ly[col] = pdf_ly[col].astype(float)

print(f"2026 data: {pdf['checkout_date'].min().date()} → {pdf['checkout_date'].max().date()}")
print(f"2025 data: {pdf_ly['checkout_date'].min().date()} → {pdf_ly['checkout_date'].max().date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Define periods & filters

# COMMAND ----------

INCIDENT_START = pd.Timestamp("2026-04-11 14:00")
INCIDENT_END   = pd.Timestamp("2026-04-13 10:00")

EASTER_START = pd.Timestamp("2026-04-03")
EASTER_END   = pd.Timestamp("2026-04-06")

# Use TOTAL bookings (active + cancelled) for the counterfactual model.
# Cancellation rate will be applied later to derive non-cancelled impact.
total = (
    pdf
    .groupby(["checkout_date", "weekday", "week", "checkout_hour"])
    .agg(cnt_bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"), nr=("nr", "sum"))
    .reset_index()
)

total["ts"] = total["checkout_date"] + pd.to_timedelta(total["checkout_hour"], unit="h")

total["is_incident"] = (total["ts"] >= INCIDENT_START) & (total["ts"] < INCIDENT_END)
total["is_easter"]   = (total["checkout_date"] >= EASTER_START) & (total["checkout_date"] <= EASTER_END)

print(f"Total hourly rows (all statuses): {len(total):,}")
print(f"Incident-window rows:             {total['is_incident'].sum():,}")
print(f"Easter rows:                      {total['is_easter'].sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Build baseline: per weekday×hour linear trend regression
# MAGIC
# MAGIC For each (weekday, hour) slot we fit a linear regression on week number
# MAGIC across all clean baseline weeks (excluding Easter & incident).
# MAGIC This captures the growth trend properly and projects it to the incident week.

# COMMAND ----------

from scipy import stats as sp_stats

baseline = total[(~total["is_incident"]) & (~total["is_easter"])].copy()

incident_week_number = INCIDENT_START.isocalendar()[1]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Weekly totals — trend overview

# COMMAND ----------

weekly = (
    baseline
    .groupby("week")
    .agg(bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
    .sort_index()
)
weekly = weekly[weekly.index < incident_week_number]
weekly["bookings_growth"] = weekly["bookings"].pct_change()
weekly["gmv_growth"] = weekly["gmv"].pct_change()

print(f"Baseline weeks used: {len(weekly)} (week {weekly.index.min()} → {weekly.index.max()})")
print(f"Median WoW growth — bookings: {weekly['bookings_growth'].dropna().median():+.2%}")
print(f"Median WoW growth — GMV:      {weekly['gmv_growth'].dropna().median():+.2%}")
display(weekly)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Fit per weekday×hour linear regression on week number

# COMMAND ----------

baseline_for_reg = baseline[baseline["week"] < incident_week_number].copy()

wh_by_week = (
    baseline_for_reg
    .groupby(["week", "weekday", "checkout_hour"])
    .agg(bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
    .reset_index()
)

print(f"Clean baseline rows (week × weekday × hour): {len(wh_by_week):,}")
print(f"Weeks in baseline: {sorted(wh_by_week['week'].unique())}")

# COMMAND ----------

R2_THRESHOLD = 0.5
RECENT_WEEKS_FALLBACK = 4

def predict_at_week(group, target_week):
    """Fit OLS on week number, predict at target_week.
    Falls back to mean of most recent weeks if R² is below threshold."""
    x = group["week"].values.astype(float)
    y_b = group["bookings"].values.astype(float)
    y_g = group["gmv"].values.astype(float)
    n = len(x)

    recent = group.nlargest(RECENT_WEEKS_FALLBACK, "week")
    fallback_b = recent["bookings"].mean()
    fallback_g = recent["gmv"].mean()

    if n < 3:
        return pd.Series({
            "expected_bookings": fallback_b,
            "expected_gmv": fallback_g,
            "n_weeks": n,
            "r2_bookings": np.nan,
            "trend_bookings_per_week": np.nan,
            "method": "fallback_few_weeks",
        })

    slope_b, intercept_b, r_b, _, _ = sp_stats.linregress(x, y_b)
    slope_g, intercept_g, _, _, _ = sp_stats.linregress(x, y_g)
    r2 = r_b ** 2

    if r2 >= R2_THRESHOLD:
        pred_b = max(intercept_b + slope_b * target_week, 0)
        pred_g = max(intercept_g + slope_g * target_week, 0)
        method = "regression"
    else:
        pred_b = fallback_b
        pred_g = fallback_g
        method = "fallback_low_r2"

    return pd.Series({
        "expected_bookings": pred_b,
        "expected_gmv": pred_g,
        "n_weeks": n,
        "r2_bookings": r2,
        "trend_bookings_per_week": slope_b,
        "method": method,
    })

wh_expected = (
    wh_by_week
    .groupby(["weekday", "checkout_hour"])
    .apply(lambda g: predict_at_week(g, incident_week_number))
    .reset_index()
)

n_regression = (wh_expected["method"] == "regression").sum()
n_fallback = (wh_expected["method"] != "regression").sum()

print(f"R² threshold: {R2_THRESHOLD} — slots below use mean of last {RECENT_WEEKS_FALLBACK} weeks")
print(f"Weekday×hour slots: {len(wh_expected)} total")
print(f"  Regression (R²≥{R2_THRESHOLD}): {n_regression}")
print(f"  Fallback (low R²):  {n_fallback}")
print(f"Median R² (all slots): {wh_expected['r2_bookings'].median():.3f}")

display(spark.createDataFrame(wh_expected.sort_values(["weekday", "checkout_hour"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Backtest: regression accuracy on recent non-incident weeks
# MAGIC
# MAGIC Compare model predictions to actuals for each baseline week to check
# MAGIC for systematic over- or under-prediction.

# COMMAND ----------

backtest_weeks = sorted(wh_by_week["week"].unique())

backtest_rows = []
for w in backtest_weeks:
    actual_week = wh_by_week[wh_by_week["week"] == w]
    actual_total_b = actual_week["bookings"].sum()
    actual_total_g = actual_week["gmv"].sum()

    pred_week = (
        wh_by_week
        .groupby(["weekday", "checkout_hour"])
        .apply(lambda g: predict_at_week(g, w))
        .reset_index()
    )
    pred_total_b = pred_week["expected_bookings"].sum()
    pred_total_g = pred_week["expected_gmv"].sum()

    backtest_rows.append({
        "week": w,
        "actual_bookings": actual_total_b,
        "predicted_bookings": pred_total_b,
        "bookings_error_pct": (pred_total_b - actual_total_b) / actual_total_b * 100,
        "actual_gmv": actual_total_g,
        "predicted_gmv": pred_total_g,
        "gmv_error_pct": (pred_total_g - actual_total_g) / actual_total_g * 100,
    })

backtest_df = pd.DataFrame(backtest_rows)

pred_incident_b = wh_expected["expected_bookings"].sum()
pred_incident_g = wh_expected["expected_gmv"].sum()

print("Model prediction for incident week (full week):")
print(f"  Expected total bookings: {pred_incident_b:,.0f}")
print(f"  Expected total GMV:      {pred_incident_g:,.2f}")
print()
print("Backtest — prediction error per baseline week (positive = over-predicting):")

display(spark.createDataFrame(backtest_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Calibrate model using pre-incident hours
# MAGIC
# MAGIC The model predicts an "average" weekend, but this specific weekend may
# MAGIC have been naturally higher or lower. We use Apr 11 hours 0–13
# MAGIC (before the incident started at 14:00) to compute a scaling factor.

# COMMAND ----------

apr11 = pd.Timestamp("2026-04-11")
apr11_weekday = apr11.weekday()

pre_incident_actual = total[
    (total["checkout_date"] == apr11)
    & (total["checkout_hour"] < 14)
    & (~total["is_incident"])
    & (~total["is_easter"])
]

pre_incident_agg = (
    pre_incident_actual
    .groupby("checkout_hour")
    .agg(actual_bookings=("cnt_bookings", "sum"), actual_gmv=("gmv", "sum"))
    .reset_index()
)

pre_incident_expected = wh_expected[
    (wh_expected["weekday"] == apr11_weekday)
    & (wh_expected["checkout_hour"] < 14)
][["checkout_hour", "expected_bookings", "expected_gmv"]]

calibration = pre_incident_agg.merge(pre_incident_expected, on="checkout_hour")

scale_bookings = calibration["actual_bookings"].sum() / calibration["expected_bookings"].sum()
scale_gmv = calibration["actual_gmv"].sum() / calibration["expected_gmv"].sum()

print("Calibration from Apr 11 pre-incident hours (0–13):")
print(f"  Actual total bookings:   {calibration['actual_bookings'].sum():,.0f}")
print(f"  Model expected bookings: {calibration['expected_bookings'].sum():,.0f}")
print(f"  Scale factor (bookings): {scale_bookings:.4f}")
print(f"  Scale factor (GMV):      {scale_gmv:.4f}")
print()

calibration["ratio_bookings"] = calibration["actual_bookings"] / calibration["expected_bookings"]
calibration["ratio_gmv"] = calibration["actual_gmv"] / calibration["expected_gmv"]
display(spark.createDataFrame(calibration))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4b. Post-Easter seasonal adjustment from 2025
# MAGIC
# MAGIC Easter 2025: Apr 18-21. The equivalent "weekend after Easter" was Apr 25-27.
# MAGIC We compare that post-Easter weekend to surrounding non-Easter weekends
# MAGIC to compute a seasonal dip/lift factor, then apply it to 2026 predictions.

# COMMAND ----------

# Easter 2025: Fri Apr 18 – Mon Apr 21 (same structure as 2026: Fri Apr 3 – Mon Apr 6)
EASTER_2025_START = pd.Timestamp("2025-04-18")
EASTER_2025_END   = pd.Timestamp("2025-04-21")
POST_EASTER_2025_START = pd.Timestamp("2025-04-25")
POST_EASTER_2025_END   = pd.Timestamp("2025-04-27")

ly_total = (
    pdf_ly
    .groupby(["checkout_date", "weekday", "checkout_hour"])
    .agg(cnt_bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
    .reset_index()
)

# Post-Easter weekend (Sat Apr 25 – Sun Apr 27)
post_easter_2025 = ly_total[
    (ly_total["checkout_date"] >= POST_EASTER_2025_START)
    & (ly_total["checkout_date"] <= POST_EASTER_2025_END)
]

pe_by_wh = (
    post_easter_2025
    .groupby(["weekday", "checkout_hour"])
    .agg(pe_bookings=("cnt_bookings", "sum"), pe_gmv=("gmv", "sum"))
    .reset_index()
)

# Easter weekend itself (Sat Apr 19 – Sun Apr 20) as the reference
easter_weekend_2025 = ly_total[
    (ly_total["checkout_date"] >= pd.Timestamp("2025-04-19"))
    & (ly_total["checkout_date"] <= pd.Timestamp("2025-04-20"))
]

easter_by_wh = (
    easter_weekend_2025
    .groupby(["weekday", "checkout_hour"])
    .agg(easter_bookings=("cnt_bookings", "sum"), easter_gmv=("gmv", "sum"))
    .reset_index()
)

easter_adj = pe_by_wh.merge(easter_by_wh, on=["weekday", "checkout_hour"], how="inner")
easter_adj["dip_factor_bookings"] = easter_adj["pe_bookings"] / easter_adj["easter_bookings"]
easter_adj["dip_factor_gmv"] = easter_adj["pe_gmv"] / easter_adj["easter_gmv"]

overall_dip_bookings = easter_adj["pe_bookings"].sum() / easter_adj["easter_bookings"].sum()
overall_dip_gmv = easter_adj["pe_gmv"].sum() / easter_adj["easter_gmv"].sum()

print("Post-Easter seasonal adjustment (2025):")
print(f"  Easter weekend 2025 (reference): Apr 19-20")
print(f"  Post-Easter weekend 2025:        Apr 25-27")
print(f"  Overall ratio (post-Easter / Easter) — bookings: {overall_dip_bookings:.4f}, GMV: {overall_dip_gmv:.4f}")
print(f"  (Factor < 1 means post-Easter was lower than Easter weekend)")
print()

display(spark.createDataFrame(
    easter_adj[["weekday", "checkout_hour", "easter_bookings", "pe_bookings",
                "dip_factor_bookings", "dip_factor_gmv"]]
    .sort_values(["weekday", "checkout_hour"])
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Actual vs Expected during the incident window (calibrated + seasonal)

# COMMAND ----------

wh_calibrated = wh_expected.copy()
wh_calibrated["expected_bookings"] = wh_calibrated["expected_bookings"] * scale_bookings
wh_calibrated["expected_gmv"] = wh_calibrated["expected_gmv"] * scale_gmv

# Apply per weekday×hour post-Easter dip factor from 2025
wh_calibrated = wh_calibrated.merge(
    easter_adj[["weekday", "checkout_hour", "dip_factor_bookings", "dip_factor_gmv"]],
    on=["weekday", "checkout_hour"],
    how="left",
)
wh_calibrated["dip_factor_bookings"] = wh_calibrated["dip_factor_bookings"].fillna(1.0)
wh_calibrated["dip_factor_gmv"] = wh_calibrated["dip_factor_gmv"].fillna(1.0)

wh_calibrated["expected_bookings"] = wh_calibrated["expected_bookings"] * wh_calibrated["dip_factor_bookings"]
wh_calibrated["expected_gmv"] = wh_calibrated["expected_gmv"] * wh_calibrated["dip_factor_gmv"]

incident_data = total[total["is_incident"]].copy()

incident_hourly = (
    incident_data
    .groupby(["checkout_date", "weekday", "checkout_hour"])
    .agg(actual_bookings=("cnt_bookings", "sum"), actual_gmv=("gmv", "sum"))
    .reset_index()
)

incident_hourly = incident_hourly.merge(
    wh_calibrated[["weekday", "checkout_hour", "expected_bookings", "expected_gmv"]],
    on=["weekday", "checkout_hour"],
    how="left",
)

incident_hourly["booking_delta"] = incident_hourly["actual_bookings"] - incident_hourly["expected_bookings"]
incident_hourly["gmv_delta"] = incident_hourly["actual_gmv"] - incident_hourly["expected_gmv"]
incident_hourly["booking_delta_pct"] = (
    incident_hourly["booking_delta"] / incident_hourly["expected_bookings"] * 100
)
incident_hourly["gmv_delta_pct"] = (
    incident_hourly["gmv_delta"] / incident_hourly["expected_gmv"] * 100
)

incident_hourly = incident_hourly.sort_values(["checkout_date", "checkout_hour"])

display(spark.createDataFrame(incident_hourly))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4b. Compute mature cancellation rate from baseline
# MAGIC
# MAGIC Only use bookings old enough for cancellations to have fully matured.
# MAGIC Recent weeks (March/April) still have artificially low cancel rates
# MAGIC because not enough time has passed. We cap the period at
# MAGIC `CANCEL_MATURITY_DAYS` before the incident.

# COMMAND ----------

CANCEL_MATURITY_DAYS = 45

cancel_cutoff = INCIDENT_START.normalize() - pd.Timedelta(days=CANCEL_MATURITY_DAYS)

mature_data = pdf[
    (pdf["checkout_date"] < cancel_cutoff)
    & ~((pdf["checkout_date"] >= EASTER_START) & (pdf["checkout_date"] <= EASTER_END))
].copy()

cancel_stats = (
    mature_data
    .groupby("status_id")
    .agg(bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
)

total_baseline_bookings = cancel_stats["bookings"].sum()
total_baseline_gmv = cancel_stats["gmv"].sum()

cancelled_bookings = cancel_stats.loc[2, "bookings"] if 2 in cancel_stats.index else 0
cancelled_gmv = cancel_stats.loc[2, "gmv"] if 2 in cancel_stats.index else 0

cancel_rate_bookings = cancelled_bookings / total_baseline_bookings
cancel_rate_gmv = cancelled_gmv / total_baseline_gmv
survival_rate_bookings = 1 - cancel_rate_bookings
survival_rate_gmv = 1 - cancel_rate_gmv

print(f"Maturity cutoff:              {cancel_cutoff.date()} ({CANCEL_MATURITY_DAYS} days before incident)")
print(f"Mature baseline period:       {mature_data['checkout_date'].min().date()} → {mature_data['checkout_date'].max().date()}")
print(f"Cancellation rate (bookings): {cancel_rate_bookings:.2%}")
print(f"Cancellation rate (GMV):      {cancel_rate_gmv:.2%}")
print(f"Survival rate (bookings):     {survival_rate_bookings:.2%}")
print(f"Survival rate (GMV):          {survival_rate_gmv:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Summary: total incident impact

# COMMAND ----------

total_actual_bookings   = incident_hourly["actual_bookings"].sum()
total_expected_bookings = incident_hourly["expected_bookings"].sum()
total_actual_gmv        = incident_hourly["actual_gmv"].sum()
total_expected_gmv      = incident_hourly["expected_gmv"].sum()

lost_bookings_total = total_expected_bookings - total_actual_bookings
lost_gmv_total      = total_expected_gmv - total_actual_gmv

lost_bookings_net = lost_bookings_total * survival_rate_bookings
lost_gmv_net      = lost_gmv_total * survival_rate_gmv

print("=" * 70)
print("INCIDENT IMPACT SUMMARY")
print(f"  Period: {INCIDENT_START} → {INCIDENT_END}")
print(f"  Calibration scale: bookings {scale_bookings:.4f}, GMV {scale_gmv:.4f}")
print("=" * 70)
print("  TOTAL BOOKINGS (active + cancelled):")
print(f"    Expected: {total_expected_bookings:,.0f}")
print(f"    Actual:   {total_actual_bookings:,.0f}")
print(f"    Lost:     {lost_bookings_total:,.0f}  ({lost_bookings_total/total_expected_bookings:+.1%})")
print("-" * 70)
print("  TOTAL GMV (active + cancelled):")
print(f"    Expected: {total_expected_gmv:,.2f}")
print(f"    Actual:   {total_actual_gmv:,.2f}")
print(f"    Lost:     {lost_gmv_total:,.2f}  ({lost_gmv_total/total_expected_gmv:+.1%})")
print("=" * 70)
print(f"  Applying mature cancellation rate "
      f"(bookings: {cancel_rate_bookings:.2%}, GMV: {cancel_rate_gmv:.2%})")
print("-" * 70)
print("  NON-CANCELLED (estimated after cancellation maturation):")
print(f"    Lost bookings: {lost_bookings_net:,.0f}")
print(f"    Lost GMV:      {lost_gmv_net:,.2f}")
print("=" * 70)


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Hourly impact pattern (chart-ready)
# MAGIC
# MAGIC Shows how the total-bookings deficit evolved hour by hour during the incident.

# COMMAND ----------

chart_df = incident_hourly.copy()
chart_df["hour_label"] = (
    chart_df["checkout_date"].dt.strftime("%a %m/%d") + " " +
    chart_df["checkout_hour"].astype(str).str.zfill(2) + ":00"
)
chart_df["lost_bookings_net"] = -chart_df["booking_delta"] * survival_rate_bookings

display(
    spark.createDataFrame(
        chart_df[["hour_label", "actual_bookings", "expected_bookings",
                  "booking_delta", "booking_delta_pct", "lost_bookings_net"]]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Cross-check: same weekday/hour last week vs incident week
# MAGIC
# MAGIC Direct comparison to build confidence in the counterfactual.

# COMMAND ----------

prev_week_number = incident_week_number - 1

prev_week_data = total[
    (total["week"] == prev_week_number) & (~total["is_easter"])
].copy()

prev_hourly = (
    prev_week_data
    .groupby(["weekday", "checkout_hour"])
    .agg(prev_bookings=("cnt_bookings", "sum"), prev_gmv=("gmv", "sum"))
    .reset_index()
)

cross_check = incident_hourly.merge(
    prev_hourly, on=["weekday", "checkout_hour"], how="left"
)
cross_check["vs_prev_week_bookings_pct"] = (
    (cross_check["actual_bookings"] - cross_check["prev_bookings"])
    / cross_check["prev_bookings"] * 100
)

display(
    spark.createDataFrame(
        cross_check[["checkout_date", "checkout_hour", "weekday",
                     "prev_bookings", "actual_bookings", "expected_bookings",
                     "vs_prev_week_bookings_pct", "booking_delta_pct"]]
        .sort_values(["checkout_date", "checkout_hour"])
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Impact on cancelled bookings (supplementary)
# MAGIC
# MAGIC Check whether cancellation rates also shifted during the incident.

# COMMAND ----------

all_statuses = pdf.copy()
all_statuses["ts"] = all_statuses["checkout_date"] + pd.to_timedelta(all_statuses["checkout_hour"], unit="h")
all_statuses["is_incident"] = (all_statuses["ts"] >= INCIDENT_START) & (all_statuses["ts"] < INCIDENT_END)
all_statuses["is_easter"] = (all_statuses["checkout_date"] >= EASTER_START) & (all_statuses["checkout_date"] <= EASTER_END)

cancel_comparison = (
    all_statuses[~all_statuses["is_easter"]]
    .groupby(["is_incident", "status_id"])
    .agg(bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
    .reset_index()
)

cancel_pivot = cancel_comparison.pivot_table(
    index="is_incident", columns="status_id", values=["bookings", "gmv"], aggfunc="sum"
).fillna(0)

cancel_pivot.columns = [f"{m}_{s}" for m, s in cancel_pivot.columns]

if "bookings_1" in cancel_pivot.columns and "bookings_2" in cancel_pivot.columns:
    cancel_pivot["cancel_rate"] = (
        cancel_pivot["bookings_2"]
        / (cancel_pivot["bookings_1"] + cancel_pivot["bookings_2"]) * 100
    )

print("Cancellation rate comparison:")
display(spark.createDataFrame(cancel_pivot.reset_index()))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 9. Daily-level summary view

# COMMAND ----------

daily_impact = (
    incident_hourly
    .groupby("checkout_date")
    .agg(
        actual_bookings=("actual_bookings", "sum"),
        expected_bookings=("expected_bookings", "sum"),
        actual_gmv=("actual_gmv", "sum"),
        expected_gmv=("expected_gmv", "sum"),
    )
    .reset_index()
)

daily_impact["lost_bookings_total"] = daily_impact["expected_bookings"] - daily_impact["actual_bookings"]
daily_impact["lost_gmv_total"] = daily_impact["expected_gmv"] - daily_impact["actual_gmv"]
daily_impact["booking_loss_pct"] = daily_impact["lost_bookings_total"] / daily_impact["expected_bookings"] * 100
daily_impact["gmv_loss_pct"] = daily_impact["lost_gmv_total"] / daily_impact["expected_gmv"] * 100
daily_impact["lost_bookings_net"] = daily_impact["lost_bookings_total"] * survival_rate_bookings
daily_impact["lost_gmv_net"] = daily_impact["lost_gmv_total"] * survival_rate_gmv

display(spark.createDataFrame(daily_impact))
