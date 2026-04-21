# Databricks notebook source
# MAGIC %md
# MAGIC # Touchpoint Attribution Baseline Verification
# MAGIC
# MAGIC Reproduces the **current production** attribution logic for both platforms and compares
# MAGIC against production `touchpoint_id` values to measure the match rate.
# MAGIC
# MAGIC | Platform | Logic |
# MAGIC |---|---|
# MAGIC | App (platform_id=3) | FIRST `AttributionTracking` in 5-day lookback + 1-day forward window |
# MAGIC | Web (platform_id=1,2) | LAST `AttributionTracking` in 24h lookback, no forward window |
# MAGIC
# MAGIC Key timestamp fix: mirrors `adaptiveTimestamp()` from the Scala pipeline --
# MAGIC mobile AT events use the client timestamp (`event_properties.timestamp`),
# MAGIC non-mobile AT events use the server timestamp (`event_properties.kafka_timestamp / 1000`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Config

# COMMAND ----------

sample_date = "2025-12-01"

APP_PLATFORM_ID = 3
WEB_PLATFORM_IDS = (1, 2)

APP_LOOKBACK_HOURS  = 120   # 5 days
APP_LOOKFORWARD_HOURS = 24  # 1 day
WEB_LOOKBACK_HOURS  = 24
MAX_TP_PER_VISITOR_PER_DAY = 100  # cap applied for both app and web, mirrors Scala pipeline

print(f"Sample date       : {sample_date}")
print(f"App platform_id   : {APP_PLATFORM_ID}")
print(f"Web platform_ids  : {WEB_PLATFORM_IDS}")
print(f"App window        : -{APP_LOOKBACK_HOURS}h to +{APP_LOOKFORWARD_HOURS}h")
print(f"Web window        : -{WEB_LOOKBACK_HOURS}h to 0")
print(f"TP cap per visitor: {MAX_TP_PER_VISITOR_PER_DAY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Build `touchpoint_events_adaptive`
# MAGIC
# MAGIC Mirrors the full `adaptiveTimestamp()` logic from the Scala pipeline:
# MAGIC 1. Mobile (ios/android/traveler_app) AND `event_properties.timestamp` not null → use client ts (seconds)
# MAGIC 2. `event_properties.kafka_timestamp` is null → fallback to client ts (seconds)
# MAGIC 3. Otherwise → use server ts (`kafka_timestamp`, milliseconds → divide by 1000)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW touchpoint_events_adaptive AS
SELECT
    user.visitor_id                  AS visitor_id,
    event_properties.uuid            AS touchpoint_uuid,
    CASE
        WHEN (lower(header.platform) IN ('android', 'ios')
              OR lower(event_properties.sent_by) = 'traveler_app')
             AND event_properties.timestamp IS NOT NULL
        THEN from_unixtime(CAST(event_properties.timestamp AS DOUBLE))
        WHEN kafka_timestamp IS NULL
        THEN from_unixtime(CAST(event_properties.timestamp AS DOUBLE))
        ELSE from_unixtime(CAST(kafka_timestamp AS DOUBLE))
    END                              AS touchpoint_ts,
    header.platform                  AS tp_platform,
    date
FROM events
WHERE event_name = 'AttributionTracking'
  AND date BETWEEN date_sub('{sample_date}', {APP_LOOKBACK_HOURS // 24})
               AND date_add('{sample_date}', {APP_LOOKFORWARD_HOURS // 24})
""")

tp_diagnostics = spark.sql("""
    SELECT
        CASE
            WHEN lower(tp_platform) IN ('android', 'ios') THEN 'mobile'
            ELSE 'non-mobile'
        END AS tp_type,
        count(*) AS total,
        count_if(touchpoint_ts IS NOT NULL) AS has_ts,
        count_if(touchpoint_ts IS NULL) AS null_ts
    FROM touchpoint_events_adaptive
    GROUP BY 1
""")
tp_diagnostics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Build Session Views

# COMMAND ----------

web_platform_ids_sql = ", ".join(str(p) for p in WEB_PLATFORM_IDS)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW app_sessions AS
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id,
    events[0].event_name  AS first_event_name,
    events[0].timestamp   AS first_event_ts
FROM production.dwh.fact_session
WHERE date = '{sample_date}'
  AND platform_id = {APP_PLATFORM_ID}
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW web_sessions AS
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id,
    events[0].event_name  AS first_event_name,
    events[0].timestamp   AS first_event_ts
FROM production.dwh.fact_session
WHERE date = '{sample_date}'
  AND platform_id IN ({web_platform_ids_sql})
""")

app_total  = spark.sql("SELECT count(*) AS cnt FROM app_sessions").collect()[0]["cnt"]
web_total  = spark.sql("SELECT count(*) AS cnt FROM web_sessions").collect()[0]["cnt"]
print(f"App sessions : {app_total:,}")
print(f"Web sessions : {web_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Apply 100-Touchpoint Cap
# MAGIC
# MAGIC The Scala pipeline pre-filters touchpoints to the first 100 per `(visitor_id, date)`
# MAGIC before using them for both app and web (OTHER) event enrichment.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW touchpoint_events_capped AS
SELECT *
FROM touchpoint_events_adaptive
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY visitor_id, date
    ORDER BY touchpoint_ts ASC
) < {MAX_TP_PER_VISITOR_PER_DAY}
""")

uncapped = spark.sql("SELECT count(*) AS cnt FROM touchpoint_events_adaptive").collect()[0]["cnt"]
capped   = spark.sql("SELECT count(*) AS cnt FROM touchpoint_events_capped").collect()[0]["cnt"]
print(f"Touchpoints before cap : {uncapped:,}")
print(f"Touchpoints after cap  : {capped:,}  ({uncapped - capped:,} removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: App Attribution Reproduction
# MAGIC
# MAGIC Uses `first_event_ts` (from `events[0].timestamp` in `fact_session`) instead of
# MAGIC `started_at` to match the Scala pipeline's event-level enrichment.
# MAGIC
# MAGIC Two cases mirroring the Scala three-way enrichment:
# MAGIC 1. **Self-enrichment**: if the first event IS an `AttributionTracking` event,
# MAGIC    the touchpoint is that event itself (matched by `event_id` = `touchpoint_uuid`)
# MAGIC 2. **Window-based**: otherwise, FIRST AT event in the 5-day lookback / 1-day forward window
# MAGIC    around `first_event_ts`, with platform-matching for the forward bound

# COMMAND ----------

app_result = spark.sql(f"""
WITH self_enriched AS (
    SELECT
        s.session_id,
        s.touchpoint_id                AS prod_touchpoint_id,
        t.touchpoint_uuid              AS reproduced_touchpoint_id
    FROM app_sessions s
    JOIN touchpoint_events_capped t
        ON  t.visitor_id = s.visitor_id
        AND t.touchpoint_uuid = s.session_id
    WHERE s.first_event_name = 'AttributionTracking'
),
window_based AS (
    SELECT
        s.session_id,
        s.touchpoint_id                AS prod_touchpoint_id,
        t.touchpoint_uuid              AS reproduced_touchpoint_id
    FROM app_sessions s
    LEFT JOIN touchpoint_events_capped t
        ON  t.visitor_id = s.visitor_id
        AND t.touchpoint_ts > s.first_event_ts - INTERVAL {APP_LOOKBACK_HOURS} HOURS
        AND t.touchpoint_ts <= CASE
            WHEN lower(t.tp_platform) IN ('android', 'ios')
            THEN s.first_event_ts + INTERVAL {APP_LOOKFORWARD_HOURS} HOURS
            ELSE s.first_event_ts
        END
    WHERE s.first_event_name != 'AttributionTracking'
       OR s.first_event_name IS NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts ASC
    ) = 1
),
attributed AS (
    SELECT * FROM self_enriched
    UNION ALL
    SELECT * FROM window_based
)
SELECT
    count(*)                                                                          AS total_sessions,
    count_if(prod_touchpoint_id IS NOT DISTINCT FROM reproduced_touchpoint_id)        AS matched,
    count_if(prod_touchpoint_id IS DISTINCT FROM reproduced_touchpoint_id)            AS mismatched,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                       AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)          AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL)      AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL)      AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                      AS different_tp,
    round(
        count_if(prod_touchpoint_id IS NOT DISTINCT FROM reproduced_touchpoint_id)
        / count(*) * 100, 2
    )                                                                                 AS match_rate_pct
FROM attributed
""")

app_result.display()
r = app_result.collect()[0]
print(f"\nApp match rate: {r['match_rate_pct']}%  ({r['matched']:,} / {r['total_sessions']:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Web Attribution Reproduction
# MAGIC
# MAGIC Reproduces: LAST `AttributionTracking` event within `[started_at - 24h, started_at]`.
# MAGIC Uses `started_at` as the anchor (gives 97% match rate).

# COMMAND ----------

web_result = spark.sql(f"""
WITH attributed AS (
    SELECT
        s.session_id,
        s.touchpoint_id                AS prod_touchpoint_id,
        t.touchpoint_uuid              AS reproduced_touchpoint_id
    FROM web_sessions s
    LEFT JOIN touchpoint_events_capped t
        ON  t.visitor_id = s.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL {WEB_LOOKBACK_HOURS} HOURS
                                AND s.started_at
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts DESC
    ) = 1
)
SELECT
    count(*)                                                                          AS total_sessions,
    count_if(prod_touchpoint_id IS NOT DISTINCT FROM reproduced_touchpoint_id)        AS matched,
    count_if(prod_touchpoint_id IS DISTINCT FROM reproduced_touchpoint_id)            AS mismatched,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                       AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)          AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL)      AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL)      AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                      AS different_tp,
    round(
        count_if(prod_touchpoint_id IS NOT DISTINCT FROM reproduced_touchpoint_id)
        / count(*) * 100, 2
    )                                                                                 AS match_rate_pct
FROM attributed
""")

web_result.display()
r = web_result.collect()[0]
print(f"\nWeb match rate: {r['match_rate_pct']}%  ({r['matched']:,} / {r['total_sessions']:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Summary

# COMMAND ----------

app_row = app_result.collect()[0]
web_row = web_result.collect()[0]

summary = spark.createDataFrame([
    {
        "platform":          "App (platform_id=3)",
        "logic":             "FIRST AT in [-5d, +1d]",
        "total_sessions":    int(app_row["total_sessions"]),
        "matched":           int(app_row["matched"]),
        "exact_match":       int(app_row["exact_match"]),
        "both_null":         int(app_row["both_null"]),
        "prod_only":         int(app_row["prod_only"]),
        "reproduced_only":   int(app_row["reproduced_only"]),
        "different_tp":      int(app_row["different_tp"]),
        "match_rate_pct":    float(app_row["match_rate_pct"]),
    },
    {
        "platform":          "Web (platform_id=1,2)",
        "logic":             "LAST AT in [-24h, 0]",
        "total_sessions":    int(web_row["total_sessions"]),
        "matched":           int(web_row["matched"]),
        "exact_match":       int(web_row["exact_match"]),
        "both_null":         int(web_row["both_null"]),
        "prod_only":         int(web_row["prod_only"]),
        "reproduced_only":   int(web_row["reproduced_only"]),
        "different_tp":      int(web_row["different_tp"]),
        "match_rate_pct":    float(web_row["match_rate_pct"]),
    },
])

summary.display()

print(f"""
=== BASELINE VERIFICATION SUMMARY ({sample_date}) ===

App  match rate: {app_row['match_rate_pct']}%
Web  match rate: {web_row['match_rate_pct']}%

If app match rate < 90%, investigate:
  - Are touchpoints capped correctly?
  - Does the forward window need platform-matching logic?
  - Check Cell 8b/8c in touchpoint_baseline_investigation.py for further analysis.
""")
