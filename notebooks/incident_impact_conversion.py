# Databricks notebook source
# MAGIC %md
# MAGIC # Incident Impact Analysis — Conversion Approach
# MAGIC
# MAGIC Alternative estimate of lost bookings and GMV based on **ADP-to-booking
# MAGIC conversion rate** rather than absolute booking counts.
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. ADP views (traffic) were likely unaffected by the incident
# MAGIC 2. The incident degraded a component on ADP, reducing conversion to booking
# MAGIC 3. We measure the conversion drop during the incident vs the expected rate
# MAGIC 4. Multiply the conversion drop × actual ADP sessions → lost bookings
# MAGIC 5. Apply avg GMV per booking to get lost GMV
# MAGIC
# MAGIC **Exclusions:** Easter (Apr 3-6), incident window from baseline

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Load session data

# COMMAND ----------

query_sessions = """
select
      session_started_at::date              as session_date,
      hour(session_started_at)              as session_hour,
      weekday(session_started_at::date)     as weekday,
      weekofyear(session_started_at::date)  as week,
      count(distinct session_id)            as cnt_sessions,
      count(distinct case when array_size(viewed_activity_ids) > 0
            then session_id else null end)  as cnt_sessions_adp_views,
      count(distinct case when array_size(booking_ids) > 0
            then session_id else null end)  as cnt_sessions_booked
from production.marketplace_reports.agg_session_performance a
where session_started_at::date >= '2026-01-05'
group by all
"""

try:
    raw_sessions.unpersist()
except Exception:
    pass
raw_sessions = spark.sql(query_sessions)
raw_sessions.cache()
print(f"Session rows loaded: {raw_sessions.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1b. Load booking data (for GMV per booking)

# COMMAND ----------

query_bookings = """
select
      date_of_checkout::date          as checkout_date,
      hour(date_of_checkout)          as checkout_hour,
      status_id,
      sum(gmv)                        as gmv,
      count(distinct booking_id)      as cnt_bookings
from production.dwh.fact_booking b
where date_of_checkout::date >= '2026-01-05'
  and status_id in (1, 2)
group by all
"""

try:
    raw_bookings.unpersist()
except Exception:
    pass
raw_bookings = spark.sql(query_bookings)
raw_bookings.cache()
print(f"Booking rows loaded: {raw_bookings.count():,}")

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy import stats as sp_stats

sdf = raw_sessions.toPandas()
sdf["session_date"] = pd.to_datetime(sdf["session_date"])
for col in ["cnt_sessions", "cnt_sessions_adp_views", "cnt_sessions_booked",
            "session_hour", "weekday", "week"]:
    sdf[col] = sdf[col].astype(float if "cnt_" in col else int)

bdf = raw_bookings.toPandas()
bdf["checkout_date"] = pd.to_datetime(bdf["checkout_date"])
bdf["checkout_hour"] = bdf["checkout_hour"].astype(int)
for col in ["gmv", "cnt_bookings"]:
    bdf[col] = bdf[col].astype(float)

print(f"Sessions: {sdf['session_date'].min().date()} → {sdf['session_date'].max().date()}")
print(f"Bookings: {bdf['checkout_date'].min().date()} → {bdf['checkout_date'].max().date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Define periods & compute conversion rates

# COMMAND ----------

INCIDENT_START = pd.Timestamp("2026-04-11 14:00")
INCIDENT_END   = pd.Timestamp("2026-04-13 10:00")

EASTER_START = pd.Timestamp("2026-04-03")
EASTER_END   = pd.Timestamp("2026-04-06")

sdf["ts"] = sdf["session_date"] + pd.to_timedelta(sdf["session_hour"], unit="h")
sdf["is_incident"] = (sdf["ts"] >= INCIDENT_START) & (sdf["ts"] < INCIDENT_END)
sdf["is_easter"] = (sdf["session_date"] >= EASTER_START) & (sdf["session_date"] <= EASTER_END)

sdf["conv_adp_to_booking"] = sdf["cnt_sessions_booked"] / sdf["cnt_sessions_adp_views"]
sdf["conv_session_to_booking"] = sdf["cnt_sessions_booked"] / sdf["cnt_sessions"]

print(f"Total hourly rows:    {len(sdf):,}")
print(f"Incident-window rows: {sdf['is_incident'].sum():,}")
print(f"Easter rows:          {sdf['is_easter'].sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Baseline conversion rate per weekday × hour

# COMMAND ----------

baseline = sdf[(~sdf["is_incident"]) & (~sdf["is_easter"])].copy()

incident_week_number = INCIDENT_START.isocalendar()[1]
baseline = baseline[baseline["week"] < incident_week_number]

# Aggregate per week × weekday × hour
wh_by_week = (
    baseline
    .groupby(["week", "weekday", "session_hour"])
    .agg(
        sessions=("cnt_sessions", "sum"),
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
wh_by_week["conv_rate"] = wh_by_week["booked"] / wh_by_week["adp_views"]

print(f"Baseline weeks: {sorted(wh_by_week['week'].unique())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Expected conversion rate per weekday × hour
# MAGIC
# MAGIC Use median of recent weeks — conversion rates are more stable than
# MAGIC absolute counts, so median is more robust than regression here.

# COMMAND ----------

RECENT_WEEKS = 4

wh_recent = wh_by_week[
    wh_by_week["week"] >= wh_by_week["week"].max() - RECENT_WEEKS + 1
]

def conv_stats_for_slot(group):
    """Compute expected conversion rate with R² for trend stability check."""
    rates = group["conv_rate"].values.astype(float)
    weeks = group["week"].values.astype(float)
    n = len(rates)

    recent = group.nlargest(RECENT_WEEKS, "week")
    median_rate = recent["conv_rate"].median()
    avg_adp = recent["adp_views"].mean()
    avg_booked = recent["booked"].mean()

    if n >= 3:
        _, _, r, _, _ = sp_stats.linregress(weeks, rates)
        r2 = r ** 2
        cv = rates.std() / rates.mean() if rates.mean() > 0 else np.nan
    else:
        r2 = np.nan
        cv = np.nan

    return pd.Series({
        "expected_conv_rate": median_rate,
        "avg_adp_views": avg_adp,
        "avg_booked": avg_booked,
        "n_weeks": n,
        "r2_conv_trend": r2,
        "cv_conv_rate": cv,
    })

wh_expected_conv = (
    wh_by_week
    .groupby(["weekday", "session_hour"])
    .apply(conv_stats_for_slot)
    .reset_index()
)

print(f"Using median of last {RECENT_WEEKS} clean weeks for expected conversion rate")
print(f"Weekday×hour slots: {len(wh_expected_conv)}")
print(f"Median R² (conversion trend):  {wh_expected_conv['r2_conv_trend'].median():.3f}")
print(f"Median CV (conversion rate):   {wh_expected_conv['cv_conv_rate'].median():.3f}")
print(f"  (Low R² + low CV = stable rate, good for median approach)")

display(spark.createDataFrame(wh_expected_conv.sort_values(["weekday", "session_hour"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Weekly conversion rate trend (overview)

# COMMAND ----------

weekly_conv = (
    wh_by_week
    .groupby("week")
    .agg(adp_views=("adp_views", "sum"), booked=("booked", "sum"))
)
weekly_conv["conv_rate"] = weekly_conv["booked"] / weekly_conv["adp_views"]

print("Weekly ADP → Booking conversion rate:")
display(spark.createDataFrame(weekly_conv.reset_index()))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Measure conversion drop during incident
# MAGIC
# MAGIC Uses **expected ADP views** (not actual) because the incident also
# MAGIC reduced repeat ADP traffic — users who saw the issue didn't return.
# MAGIC This captures both the conversion drop and the lost return visits.

# COMMAND ----------

incident_data = sdf[sdf["is_incident"]].copy()

incident_hourly = (
    incident_data
    .groupby(["session_date", "weekday", "session_hour"])
    .agg(
        actual_sessions=("cnt_sessions", "sum"),
        actual_adp_views=("cnt_sessions_adp_views", "sum"),
        actual_booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
incident_hourly["actual_conv_rate"] = (
    incident_hourly["actual_booked"] / incident_hourly["actual_adp_views"]
)

incident_hourly = incident_hourly.merge(
    wh_expected_conv[["weekday", "session_hour", "expected_conv_rate", "avg_adp_views"]],
    on=["weekday", "session_hour"],
    how="left",
)
incident_hourly.rename(columns={"avg_adp_views": "expected_adp_views"}, inplace=True)

incident_hourly["adp_traffic_delta_pct"] = (
    (incident_hourly["actual_adp_views"] - incident_hourly["expected_adp_views"])
    / incident_hourly["expected_adp_views"] * 100
)

# Expected bookings = expected ADP views × expected conversion rate
incident_hourly["expected_booked"] = (
    incident_hourly["expected_adp_views"] * incident_hourly["expected_conv_rate"]
)
incident_hourly["conv_drop_pct"] = (
    (incident_hourly["actual_conv_rate"] - incident_hourly["expected_conv_rate"])
    / incident_hourly["expected_conv_rate"] * 100
)
incident_hourly["lost_bookings"] = (
    incident_hourly["expected_booked"] - incident_hourly["actual_booked"]
)

incident_hourly = incident_hourly.sort_values(["session_date", "session_hour"])

display(spark.createDataFrame(incident_hourly))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Translate lost bookings into GMV

# COMMAND ----------

# Compute average GMV per total booking from recent baseline
gmv_baseline = bdf[
    (bdf["checkout_date"] >= pd.Timestamp("2026-03-02"))
    & (bdf["checkout_date"] < INCIDENT_START.normalize())
    & ~((bdf["checkout_date"] >= EASTER_START) & (bdf["checkout_date"] <= EASTER_END))
]

total_gmv = gmv_baseline["gmv"].sum()
total_bookings = gmv_baseline["cnt_bookings"].sum()
avg_gmv_per_booking = total_gmv / total_bookings

print(f"Avg GMV per booking (all statuses, Mar baseline): {avg_gmv_per_booking:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Cancellation adjustment

# COMMAND ----------

CANCEL_MATURITY_DAYS = 45
cancel_cutoff = INCIDENT_START.normalize() - pd.Timedelta(days=CANCEL_MATURITY_DAYS)

mature_data = bdf[
    (bdf["checkout_date"] < cancel_cutoff)
    & ~((bdf["checkout_date"] >= EASTER_START) & (bdf["checkout_date"] <= EASTER_END))
]

cancel_stats = (
    mature_data
    .groupby("status_id")
    .agg(bookings=("cnt_bookings", "sum"), gmv=("gmv", "sum"))
)

total_mature_bookings = cancel_stats["bookings"].sum()
cancelled_bookings_cnt = cancel_stats.loc[2, "bookings"] if 2 in cancel_stats.index else 0
cancel_rate_bookings = cancelled_bookings_cnt / total_mature_bookings
survival_rate = 1 - cancel_rate_bookings

# GMV per non-cancelled booking (for final output)
active_gmv = cancel_stats.loc[1, "gmv"] if 1 in cancel_stats.index else total_gmv
active_bookings = cancel_stats.loc[1, "bookings"] if 1 in cancel_stats.index else total_bookings
avg_gmv_per_active_booking = active_gmv / active_bookings

print(f"Maturity cutoff:               {cancel_cutoff.date()}")
print(f"Cancellation rate (bookings):  {cancel_rate_bookings:.2%}")
print(f"Survival rate:                 {survival_rate:.2%}")
print(f"Avg GMV per non-cancelled booking: {avg_gmv_per_active_booking:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Summary: incident impact (conversion approach)

# COMMAND ----------

total_lost_sessions_booked = incident_hourly["lost_bookings"].sum()
total_actual_booked = incident_hourly["actual_booked"].sum()
total_expected_booked = incident_hourly["expected_booked"].sum()
total_actual_adp = incident_hourly["actual_adp_views"].sum()
total_expected_adp = incident_hourly["expected_adp_views"].sum()

avg_actual_conv = total_actual_booked / total_actual_adp
avg_expected_conv = total_expected_booked / total_expected_adp
adp_traffic_drop = (total_actual_adp - total_expected_adp) / total_expected_adp

lost_bookings_net = total_lost_sessions_booked * survival_rate
lost_gmv_net = lost_bookings_net * avg_gmv_per_active_booking

print("=" * 70)
print("INCIDENT IMPACT — CONVERSION APPROACH")
print(f"  Period: {INCIDENT_START} → {INCIDENT_END}")
print("=" * 70)
print(f"  Expected ADP sessions:           {total_expected_adp:,.0f}")
print(f"  Actual ADP sessions:             {total_actual_adp:,.0f}  ({adp_traffic_drop:+.1%} from lost return visits)")
print(f"  Expected conversion rate:        {avg_expected_conv:.4%}")
print(f"  Actual conversion rate:          {avg_actual_conv:.4%}  ({(avg_actual_conv - avg_expected_conv)/avg_expected_conv:+.1%})")
print("-" * 70)
print(f"    Expected booked sessions: {total_expected_booked:,.0f}  (expected ADP × expected conv)")
print(f"    Actual booked sessions:   {total_actual_booked:,.0f}")
print(f"    Lost booked sessions:     {total_lost_sessions_booked:,.0f}  ({total_lost_sessions_booked/total_expected_booked:+.1%})")
print("=" * 70)
print(f"  Applying cancellation rate ({cancel_rate_bookings:.2%})"
      f" and avg GMV/booking ({avg_gmv_per_active_booking:,.2f})")
print("-" * 70)
print("  NON-CANCELLED (estimated after cancellation maturation):")
print(f"    Lost bookings: {lost_bookings_net:,.0f}")
print(f"    Lost GMV:      {lost_gmv_net:,.2f}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 8. Hourly conversion pattern (chart-ready)

# COMMAND ----------

chart_df = incident_hourly.copy()
chart_df["hour_label"] = (
    chart_df["session_date"].dt.strftime("%a %m/%d") + " " +
    chart_df["session_hour"].astype(str).str.zfill(2) + ":00"
)

display(
    spark.createDataFrame(
        chart_df[["hour_label", "expected_adp_views", "actual_adp_views",
                  "adp_traffic_delta_pct", "actual_conv_rate",
                  "expected_conv_rate", "conv_drop_pct",
                  "actual_booked", "expected_booked", "lost_bookings"]]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 9. Cross-check: conversion rate previous week vs incident

# COMMAND ----------

prev_week_number = incident_week_number - 1

prev_week = sdf[
    (sdf["week"] == prev_week_number) & (~sdf["is_easter"])
]

prev_hourly = (
    prev_week
    .groupby(["weekday", "session_hour"])
    .agg(
        prev_adp_views=("cnt_sessions_adp_views", "sum"),
        prev_booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
prev_hourly["prev_conv_rate"] = prev_hourly["prev_booked"] / prev_hourly["prev_adp_views"]

cross_check = incident_hourly.merge(
    prev_hourly[["weekday", "session_hour", "prev_conv_rate", "prev_adp_views"]],
    on=["weekday", "session_hour"],
    how="left",
)
cross_check["conv_vs_prev_week_pct"] = (
    (cross_check["actual_conv_rate"] - cross_check["prev_conv_rate"])
    / cross_check["prev_conv_rate"] * 100
)
cross_check["adp_vs_prev_week_pct"] = (
    (cross_check["actual_adp_views"] - cross_check["prev_adp_views"])
    / cross_check["prev_adp_views"] * 100
)

display(
    spark.createDataFrame(
        cross_check[["session_date", "session_hour", "weekday",
                     "prev_conv_rate", "actual_conv_rate", "expected_conv_rate",
                     "conv_vs_prev_week_pct", "conv_drop_pct",
                     "adp_vs_prev_week_pct"]]
        .sort_values(["session_date", "session_hour"])
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 10. Traffic validation: were ADP views actually unaffected?

# COMMAND ----------

traffic_baseline = (
    baseline
    .groupby(["weekday", "session_hour"])
    .agg(avg_adp_views=("cnt_sessions_adp_views", "mean"))
    .reset_index()
)

traffic_check = incident_hourly[["session_date", "weekday", "session_hour", "actual_adp_views"]].merge(
    traffic_baseline, on=["weekday", "session_hour"], how="left"
)
traffic_check["adp_vs_baseline_pct"] = (
    (traffic_check["actual_adp_views"] - traffic_check["avg_adp_views"])
    / traffic_check["avg_adp_views"] * 100
)

overall_traffic_delta = (
    traffic_check["actual_adp_views"].sum() / traffic_check["avg_adp_views"].sum() - 1
) * 100

print(f"Overall ADP traffic during incident vs baseline: {overall_traffic_delta:+.1f}%")
print("(Should be close to 0% if traffic was unaffected)")
print()

display(
    spark.createDataFrame(
        traffic_check.sort_values(["session_date", "session_hour"])
    )
)
