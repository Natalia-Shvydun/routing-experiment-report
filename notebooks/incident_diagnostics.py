# Databricks notebook source
# MAGIC %md
# MAGIC # Incident Diagnostics
# MAGIC
# MAGIC Two checks:
# MAGIC 1. Did the conversion impact persist **after 10am Apr 13**?
# MAGIC 2. Is the impact limited to **specific platforms** (mweb/desktop vs app)?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Load data

# COMMAND ----------

query = """
select
      session_started_at::date              as session_date,
      hour(session_started_at)              as session_hour,
      weekday(session_started_at::date)     as weekday,
      weekofyear(session_started_at::date)  as week,
      platform_id,
      count(distinct session_id)            as cnt_sessions,
      count(distinct case when array_size(viewed_activity_ids) > 0
            then session_id else null end)  as cnt_sessions_adp_views,
      count(distinct case when array_size(booking_ids) > 0
            then session_id else null end)  as cnt_sessions_booked
from production.marketplace_reports.agg_session_performance a
where session_started_at::date >= '2026-03-02'
group by all
"""

try:
    raw.unpersist()
except Exception:
    pass
raw = spark.sql(query)
raw.cache()
print(f"Rows loaded: {raw.count():,}")

# COMMAND ----------

import pandas as pd
import numpy as np

sdf = raw.toPandas()
sdf["session_date"] = pd.to_datetime(sdf["session_date"])
sdf["session_hour"] = sdf["session_hour"].astype(int)
sdf["weekday"] = sdf["weekday"].astype(int)
sdf["week"] = sdf["week"].astype(int)
sdf["platform_id"] = sdf["platform_id"].astype(int)
for col in ["cnt_sessions", "cnt_sessions_adp_views", "cnt_sessions_booked"]:
    sdf[col] = sdf[col].astype(float)

sdf["ts"] = sdf["session_date"] + pd.to_timedelta(sdf["session_hour"], unit="h")
sdf["conv_rate"] = sdf["cnt_sessions_booked"] / sdf["cnt_sessions_adp_views"]

EASTER_START = pd.Timestamp("2026-04-03")
EASTER_END   = pd.Timestamp("2026-04-06")
sdf["is_easter"] = (sdf["session_date"] >= EASTER_START) & (sdf["session_date"] <= EASTER_END)

PLATFORM_NAMES = {1: "desktop", 2: "mweb", 3: "app_ios", 4: "app_android"}
sdf["platform"] = sdf["platform_id"].map(PLATFORM_NAMES).fillna("other")

print(f"Date range: {sdf['session_date'].min().date()} → {sdf['session_date'].max().date()}")
print(f"Platforms: {sorted(sdf['platform_id'].unique())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Baseline conversion rates per weekday × hour × platform
# MAGIC
# MAGIC **Hierarchical model** to avoid noisy per-platform × hour estimates:
# MAGIC 1. **Overall hourly shape**: stable conversion rate per weekday × hour (all platforms)
# MAGIC 2. **Platform scaling factor**: one ratio per platform × weekday (averaged over all hours)
# MAGIC 3. **Expected platform rate** = overall hourly rate × platform ratio
# MAGIC
# MAGIC This separates the intra-day pattern (high volume, stable) from the
# MAGIC platform-level effect (also stable when aggregated across hours).

# COMMAND ----------

INCIDENT_DATE = pd.Timestamp("2026-04-11")
incident_week = INCIDENT_DATE.isocalendar()[1]

baseline = sdf[
    (~sdf["is_easter"])
    & (sdf["week"] < incident_week)
    & (sdf["session_date"] >= pd.Timestamp("2026-03-02"))
].copy()

RECENT_WEEKS = 4
max_week = baseline["week"].max()
baseline_recent = baseline[baseline["week"] >= max_week - RECENT_WEEKS + 1]

# --- Step 1: overall (all-platform) conversion rate per weekday × hour ---
baseline_all = (
    baseline_recent
    .groupby(["weekday", "session_hour"])
    .agg(
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
        n_weeks=("week", "nunique"),
    )
    .reset_index()
)
baseline_all["overall_conv_rate"] = baseline_all["booked"] / baseline_all["adp_views"]
baseline_all["expected_conv_rate"] = baseline_all["overall_conv_rate"]

# --- Step 2: platform scaling factor per platform × weekday ---
# Ratio = platform conv rate / overall conv rate, aggregated over all hours of the day
platform_day = (
    baseline_recent
    .groupby(["platform_id", "platform", "weekday"])
    .agg(p_adp=("cnt_sessions_adp_views", "sum"), p_booked=("cnt_sessions_booked", "sum"))
    .reset_index()
)
platform_day["platform_conv_rate"] = platform_day["p_booked"] / platform_day["p_adp"]

overall_day = (
    baseline_recent
    .groupby("weekday")
    .agg(o_adp=("cnt_sessions_adp_views", "sum"), o_booked=("cnt_sessions_booked", "sum"))
    .reset_index()
)
overall_day["overall_day_conv_rate"] = overall_day["o_booked"] / overall_day["o_adp"]

platform_day = platform_day.merge(overall_day[["weekday", "overall_day_conv_rate"]], on="weekday")
platform_day["platform_ratio"] = platform_day["platform_conv_rate"] / platform_day["overall_day_conv_rate"]

# --- Step 3: build per-platform × weekday × hour expected rates ---
baseline_conv = (
    baseline_recent
    .groupby(["platform_id", "platform", "weekday", "session_hour"])
    .agg(
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
        n_weeks=("week", "nunique"),
    )
    .reset_index()
)
baseline_conv["avg_adp_views"] = baseline_conv["adp_views"] / baseline_conv["n_weeks"]
baseline_conv["avg_booked"] = baseline_conv["booked"] / baseline_conv["n_weeks"]

baseline_conv = baseline_conv.merge(
    baseline_all[["weekday", "session_hour", "overall_conv_rate"]],
    on=["weekday", "session_hour"], how="left",
)
baseline_conv = baseline_conv.merge(
    platform_day[["platform_id", "weekday", "platform_ratio"]],
    on=["platform_id", "weekday"], how="left",
)

baseline_conv["expected_conv_rate"] = baseline_conv["overall_conv_rate"] * baseline_conv["platform_ratio"]

print(f"Baseline weeks: {sorted(baseline_recent['week'].unique())}")
print(f"Platforms: {sorted(baseline_conv['platform_id'].unique())}")
print()
print("Platform scaling factors (ratio vs overall):")
for _, r in platform_day.sort_values(["platform_id", "weekday"]).iterrows():
    print(f"  {r['platform']:15s} wd={int(r['weekday'])}  "
          f"conv={r['platform_conv_rate']:.4f}  overall={r['overall_day_conv_rate']:.4f}  "
          f"ratio={r['platform_ratio']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Check: does the impact persist after 10am Apr 13?
# MAGIC
# MAGIC Compare conversion rate for Apr 13 (all hours) vs baseline,
# MAGIC hour by hour, to see if it normalises after 10am.

# COMMAND ----------

apr13 = sdf[sdf["session_date"] == pd.Timestamp("2026-04-13")].copy()
apr13_weekday = pd.Timestamp("2026-04-13").weekday()

apr13_hourly = (
    apr13
    .groupby("session_hour")
    .agg(
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
apr13_hourly["actual_conv_rate"] = apr13_hourly["booked"] / apr13_hourly["adp_views"]

apr13_baseline = baseline_all[baseline_all["weekday"] == apr13_weekday][
    ["session_hour", "expected_conv_rate"]
]

apr13_hourly = apr13_hourly.merge(apr13_baseline, on="session_hour", how="left")
apr13_hourly["conv_delta_pct"] = (
    (apr13_hourly["actual_conv_rate"] - apr13_hourly["expected_conv_rate"])
    / apr13_hourly["expected_conv_rate"] * 100
)
apr13_hourly["period"] = apr13_hourly["session_hour"].apply(
    lambda h: "INCIDENT (before 10am)" if h < 10 else "AFTER incident"
)

print("Apr 13 — hourly conversion rate vs baseline (Monday):")
print("  Negative delta = conversion below normal")
print()

display(spark.createDataFrame(apr13_hourly))

# COMMAND ----------

# Summary: incident hours vs after
apr13_summary = (
    apr13_hourly
    .groupby("period")
    .apply(lambda g: pd.Series({
        "hours": len(g),
        "avg_conv_delta_pct": g["conv_delta_pct"].mean(),
        "total_adp_views": g["adp_views"].sum(),
        "total_booked": g["booked"].sum(),
    }))
    .reset_index()
)
print("Apr 13 summary — incident hours vs after:")
display(spark.createDataFrame(apr13_summary))

# COMMAND ----------

# MAGIC %md
# MAGIC Also check Apr 12 (Sunday, full incident day) and Apr 14 (Tuesday, day after)
# MAGIC for context.

# COMMAND ----------

for check_date, label in [
    (pd.Timestamp("2026-04-12"), "Apr 12 (Sun, full incident day)"),
    (pd.Timestamp("2026-04-14"), "Apr 14 (Tue, day after incident)"),
]:
    day_data = sdf[sdf["session_date"] == check_date]
    if len(day_data) == 0:
        print(f"{label}: no data available")
        continue

    wd = check_date.weekday()
    day_hourly = (
        day_data
        .groupby("session_hour")
        .agg(adp_views=("cnt_sessions_adp_views", "sum"), booked=("cnt_sessions_booked", "sum"))
        .reset_index()
    )
    day_hourly["actual_conv_rate"] = day_hourly["booked"] / day_hourly["adp_views"]
    day_hourly = day_hourly.merge(
        baseline_all[baseline_all["weekday"] == wd][["session_hour", "expected_conv_rate"]],
        on="session_hour", how="left",
    )
    day_hourly["conv_delta_pct"] = (
        (day_hourly["actual_conv_rate"] - day_hourly["expected_conv_rate"])
        / day_hourly["expected_conv_rate"] * 100
    )

    avg_delta = day_hourly["conv_delta_pct"].mean()
    print(f"\n{label} — avg conversion delta: {avg_delta:+.1f}%")
    display(spark.createDataFrame(day_hourly))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Platform breakdown: which platforms were affected?
# MAGIC
# MAGIC Compare conversion rate during the incident window by platform.

# COMMAND ----------

INCIDENT_START = pd.Timestamp("2026-04-11 14:00")
INCIDENT_END   = pd.Timestamp("2026-04-13 10:00")

sdf["is_incident"] = (sdf["ts"] >= INCIDENT_START) & (sdf["ts"] < INCIDENT_END)

incident_by_platform = (
    sdf[sdf["is_incident"]]
    .groupby(["platform_id", "platform"])
    .agg(
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
incident_by_platform["actual_conv_rate"] = (
    incident_by_platform["booked"] / incident_by_platform["adp_views"]
)

# Expected per platform: use per-slot avg from baseline_conv (already has low-volume fallback)
incident_hours = sdf[sdf["is_incident"]][["weekday", "session_hour"]].drop_duplicates()

expected_slots = baseline_conv.merge(
    incident_hours, on=["weekday", "session_hour"], how="inner",
)
expected_slots["expected_booked_slot"] = expected_slots["avg_adp_views"] * expected_slots["expected_conv_rate"]

expected_by_platform = (
    expected_slots
    .groupby(["platform_id", "platform"])
    .agg(
        expected_adp_views=("avg_adp_views", "sum"),
        expected_booked=("expected_booked_slot", "sum"),
    )
    .reset_index()
)
expected_by_platform["expected_conv_rate"] = (
    expected_by_platform["expected_booked"] / expected_by_platform["expected_adp_views"]
)

platform_impact = incident_by_platform.merge(
    expected_by_platform[["platform_id", "expected_conv_rate", "expected_adp_views"]],
    on="platform_id", how="left",
)
platform_impact["conv_delta_pct"] = (
    (platform_impact["actual_conv_rate"] - platform_impact["expected_conv_rate"])
    / platform_impact["expected_conv_rate"] * 100
)
platform_impact["adp_delta_pct"] = (
    (platform_impact["adp_views"] - platform_impact["expected_adp_views"])
    / platform_impact["expected_adp_views"] * 100
)

print("Platform breakdown during incident window:")
display(spark.createDataFrame(
    platform_impact[["platform_id", "platform", "adp_views", "expected_adp_views",
                     "adp_delta_pct", "actual_conv_rate", "expected_conv_rate",
                     "conv_delta_pct"]]
    .sort_values("platform_id")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Platform × hour detail: before, during, and after incident
# MAGIC
# MAGIC Includes Apr 10 (pre-incident) through Apr 14 (day after) by platform.

# COMMAND ----------

detail_data = sdf[
    (sdf["session_date"] >= pd.Timestamp("2026-04-10"))
    & (sdf["session_date"] <= pd.Timestamp("2026-04-14"))
    & (~sdf["is_easter"])
]

incident_detail = (
    detail_data
    .groupby(["session_date", "session_hour", "weekday", "platform_id", "platform"])
    .agg(
        adp_views=("cnt_sessions_adp_views", "sum"),
        booked=("cnt_sessions_booked", "sum"),
    )
    .reset_index()
)
incident_detail["actual_conv_rate"] = incident_detail["booked"] / incident_detail["adp_views"]

incident_detail = incident_detail.merge(
    baseline_conv[["platform_id", "weekday", "session_hour", "expected_conv_rate"]],
    on=["platform_id", "weekday", "session_hour"],
    how="left",
)
incident_detail["conv_delta_pct"] = (
    (incident_detail["actual_conv_rate"] - incident_detail["expected_conv_rate"])
    / incident_detail["expected_conv_rate"] * 100
)
incident_detail["ts_sort"] = (
    incident_detail["session_date"] + pd.to_timedelta(incident_detail["session_hour"], unit="h")
)
incident_detail["hour_label"] = (
    incident_detail["session_date"].dt.strftime("%a %m/%d") + " " +
    incident_detail["session_hour"].astype(str).str.zfill(2) + ":00"
)
incident_detail["period"] = incident_detail["ts_sort"].apply(
    lambda t: "INCIDENT" if INCIDENT_START <= t < INCIDENT_END
    else ("PRE-INCIDENT" if t < INCIDENT_START else "POST-INCIDENT")
)

display(
    spark.createDataFrame(
        incident_detail[["ts_sort", "hour_label", "period", "platform", "adp_views",
                         "actual_conv_rate", "expected_conv_rate", "conv_delta_pct"]]
        .sort_values(["ts_sort", "platform"])
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Platform summary: avg conversion delta

# COMMAND ----------

# Platform × period summary from the detail data in section 5
period_platform_summary = (
    incident_detail
    .groupby(["period", "platform_id", "platform"])
    .agg(
        hours=("session_hour", "nunique"),
        adp_views=("adp_views", "sum"),
        booked=("booked", "sum"),
        expected_booked=("expected_conv_rate", lambda x: 0),  # placeholder
    )
    .reset_index()
)
# Recompute expected from detail-level data
_detail_expected = (
    incident_detail
    .assign(expected_booked=lambda df: df["adp_views"] * df["expected_conv_rate"])
    .groupby(["period", "platform_id", "platform"])
    .agg(
        adp_views_sum=("adp_views", "sum"),
        booked_sum=("booked", "sum"),
        expected_booked_sum=("expected_booked", "sum"),
    )
    .reset_index()
)
_detail_expected["actual_conv_rate"] = _detail_expected["booked_sum"] / _detail_expected["adp_views_sum"]
_detail_expected["expected_conv_rate"] = _detail_expected["expected_booked_sum"] / _detail_expected["adp_views_sum"]
_detail_expected["conv_delta_pct"] = (
    (_detail_expected["actual_conv_rate"] - _detail_expected["expected_conv_rate"])
    / _detail_expected["expected_conv_rate"] * 100
)

print("Platform × period summary (pre-incident / incident / post-incident):")
display(spark.createDataFrame(
    _detail_expected[["period", "platform_id", "platform", "adp_views_sum",
                      "actual_conv_rate", "expected_conv_rate", "conv_delta_pct"]]
    .sort_values(["platform_id", "period"])
))

print("\nIncident-only platform impact (from section 4):")
print()
for _, row in platform_impact.sort_values("conv_delta_pct").iterrows():
    marker = " ← AFFECTED" if row["conv_delta_pct"] < -5 else ""
    print(f"  {row['platform']:15s}  conv delta: {row['conv_delta_pct']:+6.1f}%  "
          f"  ADP delta: {row['adp_delta_pct']:+6.1f}%{marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Post-incident performance by platform
# MAGIC
# MAGIC Check Apr 13 10:00+ and Apr 14 by platform to see if recovery was
# MAGIC clean or if there's a lingering effect on specific platforms.

# COMMAND ----------

POST_INCIDENT_PERIODS = [
    ("Apr 13 after 10am", pd.Timestamp("2026-04-13"), 10, 23),
    ("Apr 14 (full day)",  pd.Timestamp("2026-04-14"), 0, 23),
]

post_incident_rows = []

for label, date, hour_start, hour_end in POST_INCIDENT_PERIODS:
    day_data = sdf[
        (sdf["session_date"] == date)
        & (sdf["session_hour"] >= hour_start)
        & (sdf["session_hour"] <= hour_end)
    ]
    if len(day_data) == 0:
        print(f"{label}: no data available")
        continue

    wd = date.weekday()

    by_platform = (
        day_data
        .groupby(["platform_id", "platform"])
        .agg(adp_views=("cnt_sessions_adp_views", "sum"), booked=("cnt_sessions_booked", "sum"))
        .reset_index()
    )
    by_platform["actual_conv_rate"] = by_platform["booked"] / by_platform["adp_views"]

    bl_subset = baseline_conv[
        (baseline_conv["weekday"] == wd)
        & (baseline_conv["session_hour"] >= hour_start)
        & (baseline_conv["session_hour"] <= hour_end)
    ].copy()
    bl_subset["expected_booked_slot"] = bl_subset["avg_adp_views"] * bl_subset["expected_conv_rate"]

    expected = (
        bl_subset
        .groupby(["platform_id", "platform"])
        .agg(exp_adp=("avg_adp_views", "sum"), exp_booked=("expected_booked_slot", "sum"))
        .reset_index()
    )
    expected["expected_conv_rate"] = expected["exp_booked"] / expected["exp_adp"]

    by_platform = by_platform.merge(
        expected[["platform_id", "expected_conv_rate", "exp_adp"]],
        on="platform_id", how="left",
    )
    by_platform["conv_delta_pct"] = (
        (by_platform["actual_conv_rate"] - by_platform["expected_conv_rate"])
        / by_platform["expected_conv_rate"] * 100
    )
    by_platform["adp_delta_pct"] = (
        (by_platform["adp_views"] - by_platform["exp_adp"])
        / by_platform["exp_adp"] * 100
    )
    by_platform["period"] = label

    post_incident_rows.append(by_platform)

if post_incident_rows:
    post_df = pd.concat(post_incident_rows, ignore_index=True)

    print("Post-incident performance by platform:")
    display(spark.createDataFrame(
        post_df[["period", "platform_id", "platform", "adp_views",
                 "adp_delta_pct", "actual_conv_rate", "expected_conv_rate",
                 "conv_delta_pct"]]
        .sort_values(["period", "platform_id"])
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Post-incident hourly detail by platform

# COMMAND ----------

post_hourly_data = sdf[
    ((sdf["session_date"] == pd.Timestamp("2026-04-13")) & (sdf["session_hour"] >= 10))
    | (sdf["session_date"] == pd.Timestamp("2026-04-14"))
].copy()

if len(post_hourly_data) > 0:
    post_hourly = (
        post_hourly_data
        .groupby(["session_date", "session_hour", "weekday", "platform_id", "platform"])
        .agg(adp_views=("cnt_sessions_adp_views", "sum"), booked=("cnt_sessions_booked", "sum"))
        .reset_index()
    )
    post_hourly["actual_conv_rate"] = post_hourly["booked"] / post_hourly["adp_views"]

    post_hourly = post_hourly.merge(
        baseline_conv[["platform_id", "weekday", "session_hour", "expected_conv_rate"]],
        on=["platform_id", "weekday", "session_hour"],
        how="left",
    )
    post_hourly["conv_delta_pct"] = (
        (post_hourly["actual_conv_rate"] - post_hourly["expected_conv_rate"])
        / post_hourly["expected_conv_rate"] * 100
    )
    post_hourly["ts_sort"] = (
        post_hourly["session_date"] + pd.to_timedelta(post_hourly["session_hour"], unit="h")
    )
    post_hourly["hour_label"] = (
        post_hourly["session_date"].dt.strftime("%a %m/%d") + " " +
        post_hourly["session_hour"].astype(str).str.zfill(2) + ":00"
    )

    display(
        spark.createDataFrame(
            post_hourly[["ts_sort", "hour_label", "platform", "adp_views",
                         "actual_conv_rate", "expected_conv_rate", "conv_delta_pct"]]
            .sort_values(["ts_sort", "platform"])
        )
    )
else:
    print("No post-incident data available yet")
