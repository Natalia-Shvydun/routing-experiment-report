# Databricks notebook source
# MAGIC %md
# MAGIC # Touchpoint Baseline Investigation
# MAGIC
# MAGIC The baseline reproduction (first touchpoint in 5-day window) only matches **8.4%** of
# MAGIC production `touchpoint_id` values. This notebook investigates why.
# MAGIC
# MAGIC | Category | Count |
# MAGIC |---|---|
# MAGIC | Total sessions | 46.9M |
# MAGIC | Exact match | 481K |
# MAGIC | Both null | 3.5M |
# MAGIC | Prod only | 5M |
# MAGIC | Reproduced only | 4.5M |
# MAGIC | **Mismatch** | **33.4M** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Config

# COMMAND ----------

investigation_date = "2025-12-01"
platform_id = 3
MAX_LOOKBACK_DAYS = 5

print(f"Investigation date: {investigation_date}")
print(f"Platform ID: {platform_id} (App)")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW touchpoint_events AS
SELECT
    user.visitor_id          AS visitor_id,
    event_properties.uuid    AS touchpoint_uuid,
    event_properties.timestamp AS touchpoint_ts,
    event_name
FROM events
WHERE event_name IN ('AttributionTracking', 'AppOpen')
  AND date BETWEEN date_sub('{investigation_date}', {MAX_LOOKBACK_DAYS}) AND '{investigation_date}'
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW sessions AS
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id,
    date
FROM production.dwh.fact_session
WHERE date = '{investigation_date}'
  AND platform_id = {platform_id}
""")

session_count = spark.sql("SELECT count(*) AS cnt FROM sessions").collect()[0]["cnt"]
print(f"Sessions on {investigation_date}: {session_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Schema Inspection
# MAGIC
# MAGIC Inspect both tables to understand available fields and types.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE events

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE production.dwh.fact_session

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Sample Production Touchpoints
# MAGIC
# MAGIC Pull 20 app sessions with a non-null `touchpoint_id` to understand the format.

# COMMAND ----------

sample_prod = spark.sql(f"""
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id,
    date
FROM production.dwh.fact_session
WHERE date = '{investigation_date}'
  AND platform_id = {platform_id}
  AND touchpoint_id IS NOT NULL
LIMIT 20
""")
sample_prod.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Sample Events for Those Sessions
# MAGIC
# MAGIC For a few sessions from Cell 3, pull ALL events within 5 days to see what fields
# MAGIC are available and whether any field matches the production `touchpoint_id`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW sample_sessions AS
SELECT session_id, visitor_id, started_at, touchpoint_id
FROM sessions
WHERE touchpoint_id IS NOT NULL
LIMIT 5
""")

# COMMAND ----------

spark.sql(f"""
SELECT
    ss.session_id,
    ss.touchpoint_id                AS prod_touchpoint_id,
    e.event_name,
    e.event_properties.uuid         AS ep_uuid,
    e.event_properties.timestamp    AS ep_timestamp,
    e.timestamp                     AS event_ts,
    ss.started_at
FROM sample_sessions ss
JOIN events e
    ON  e.user.visitor_id = ss.visitor_id
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
    AND e.timestamp BETWEEN ss.started_at - INTERVAL 5 DAYS AND ss.started_at + INTERVAL 1 HOUR
WHERE e.event_name IN ('AttributionTracking', 'AppOpen')
ORDER BY ss.session_id, e.timestamp
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: does any event field directly match `touchpoint_id`?

# COMMAND ----------

spark.sql(f"""
SELECT
    ss.session_id,
    ss.touchpoint_id                                        AS prod_tp_id,
    e.event_properties.uuid                                 AS ep_uuid,
    (ss.touchpoint_id = e.event_properties.uuid)            AS uuid_matches,
    e.event_name,
    e.event_properties.timestamp                            AS ep_timestamp,
    e.timestamp                                             AS event_ts,
    ss.started_at
FROM sample_sessions ss
JOIN events e
    ON  e.user.visitor_id = ss.visitor_id
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
    AND e.timestamp BETWEEN ss.started_at - INTERVAL 5 DAYS AND ss.started_at + INTERVAL 1 HOUR
WHERE e.event_name IN ('AttributionTracking', 'AppOpen')
ORDER BY ss.session_id, e.timestamp
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Direct Match Test
# MAGIC
# MAGIC For 1 day of sessions, try joining `touchpoint_id` against multiple candidate fields
# MAGIC in the events table. This identifies which event field actually maps to `touchpoint_id`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW day_sessions AS
SELECT session_id, visitor_id, started_at, touchpoint_id
FROM sessions
WHERE touchpoint_id IS NOT NULL
""")

day_session_count = spark.sql("SELECT count(*) AS cnt FROM day_sessions").collect()[0]["cnt"]
print(f"Sessions with non-null touchpoint_id on {investigation_date}: {day_session_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try matching on `event_properties.uuid`

# COMMAND ----------

spark.sql(f"""
SELECT
    'event_properties.uuid' AS join_field,
    count(DISTINCT ds.session_id) AS matched_sessions
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.event_properties.uuid
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try matching on other candidate fields
# MAGIC
# MAGIC Adapt the field names below based on what `DESCRIBE events` revealed in Cell 2.
# MAGIC Uncomment and adjust as needed.

# COMMAND ----------

spark.sql(f"""
SELECT
    'uuid (top-level)' AS join_field,
    count(DISTINCT ds.session_id) AS matched_sessions
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.uuid
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'

UNION ALL

SELECT
    'event_properties.touchpoint_id' AS join_field,
    count(DISTINCT ds.session_id) AS matched_sessions
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.event_properties.touchpoint_id
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'

UNION ALL

SELECT
    'event_properties.tracking_id' AS join_field,
    count(DISTINCT ds.session_id) AS matched_sessions
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.event_properties.tracking_id
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reverse lookup: find the touchpoint_id directly in events
# MAGIC
# MAGIC Take a few production touchpoint IDs and search for them across all string fields.

# COMMAND ----------

spark.sql("SELECT touchpoint_id FROM day_sessions LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Event Name Distribution
# MAGIC
# MAGIC Once the correct join field is identified (from Cell 5), check which event names
# MAGIC the matched touchpoints come from.

# COMMAND ----------

# MAGIC %md
# MAGIC **Instructions**: Replace `e.event_properties.uuid` below with whichever field
# MAGIC matched best in Cell 5.

# COMMAND ----------

spark.sql(f"""
SELECT
    e.event_name,
    count(DISTINCT ds.session_id) AS matched_sessions
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.event_properties.uuid
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
GROUP BY e.event_name
ORDER BY matched_sessions DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Timestamp and Ordering Check
# MAGIC
# MAGIC For matched touchpoints, analyze:
# MAGIC 1. Time difference between touchpoint and session start
# MAGIC 2. Whether touchpoints fall before, during, or after the session
# MAGIC 3. How many sessions have multiple candidate touchpoints (first vs last matters)

# COMMAND ----------

# MAGIC %md
# MAGIC **Instructions**: Replace `e.event_properties.uuid` below with the correct field from Cell 5.

# COMMAND ----------

spark.sql(f"""
SELECT
    CASE
        WHEN (unix_timestamp(e.timestamp) - unix_timestamp(ds.started_at)) / 3600.0 < -120 THEN '< -5 days'
        WHEN (unix_timestamp(e.timestamp) - unix_timestamp(ds.started_at)) / 3600.0 < -24  THEN '-5d to -24h'
        WHEN (unix_timestamp(e.timestamp) - unix_timestamp(ds.started_at)) / 3600.0 < -1   THEN '-24h to -1h'
        WHEN (unix_timestamp(e.timestamp) - unix_timestamp(ds.started_at)) / 3600.0 < 0    THEN '-1h to 0'
        WHEN (unix_timestamp(e.timestamp) - unix_timestamp(ds.started_at)) / 3600.0 < 1    THEN '0 to +1h'
        ELSE '> +1h'
    END AS time_bucket,
    count(*) AS cnt
FROM day_sessions ds
JOIN events e
    ON  ds.touchpoint_id = e.event_properties.uuid
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND date_add('{investigation_date}', 1)
GROUP BY 1
ORDER BY 1
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT
    num_candidates,
    count(*) AS session_count
FROM (
    SELECT
        ds.session_id,
        count(*) AS num_candidates
    FROM day_sessions ds
    JOIN events e
        ON  e.user.visitor_id = ds.visitor_id
        AND e.date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
        AND e.timestamp BETWEEN ds.started_at - INTERVAL 5 DAYS AND ds.started_at
        AND e.event_name = 'AttributionTracking'
    GROUP BY ds.session_id
)
GROUP BY num_candidates
ORDER BY num_candidates
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Mismatch Deep Dive
# MAGIC
# MAGIC Match rate is 70%. Investigate the remaining 30% mismatches.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8a: Sample mismatched sessions
# MAGIC
# MAGIC Look at sessions where both production and reproduction found a touchpoint, but they differ.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW mismatch_sessions AS
WITH attributed AS (
    SELECT
        s.session_id,
        s.visitor_id,
        s.started_at,
        s.touchpoint_id AS prod_touchpoint_id,
        t.touchpoint_uuid AS reproduced_touchpoint_id,
        t.touchpoint_ts AS reproduced_ts
    FROM sessions s
    LEFT JOIN touchpoint_events t
        ON  s.visitor_id = t.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL 120 HOURS AND s.started_at
        AND t.event_name = 'AttributionTracking'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts ASC
    ) = 1
)
SELECT *
FROM attributed
WHERE prod_touchpoint_id IS NOT NULL
  AND reproduced_touchpoint_id IS NOT NULL
  AND prod_touchpoint_id != reproduced_touchpoint_id
""")

mismatch_count = spark.sql("SELECT count(*) AS cnt FROM mismatch_sessions").collect()[0]["cnt"]
print(f"Mismatched sessions (both non-null but different): {{mismatch_count:,}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b: Hypothesis 1 -- Timestamp field difference
# MAGIC
# MAGIC Check if ordering by `event_properties.timestamp` vs top-level `timestamp` gives different results.
# MAGIC The Scala pipeline might use one while we use the other.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare event_properties.timestamp vs top-level timestamp for AttributionTracking events
# MAGIC SELECT
# MAGIC     count(*) AS total_events,
# MAGIC     count_if(event_properties.timestamp = timestamp) AS ts_equal,
# MAGIC     count_if(event_properties.timestamp != timestamp) AS ts_differ,
# MAGIC     round(count_if(event_properties.timestamp != timestamp) / count(*) * 100, 2) AS pct_differ
# MAGIC FROM touchpoint_events
# MAGIC WHERE event_name = 'AttributionTracking'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8c: Hypothesis 2 -- Window boundary
# MAGIC
# MAGIC Check if production touchpoints fall AFTER `started_at` (i.e., the touchpoint event
# MAGIC fires as part of the session, slightly after `started_at`).

# COMMAND ----------

spark.sql(f"""
SELECT
    CASE
        WHEN e.event_properties.timestamp < ms.started_at THEN 'before_session'
        WHEN e.event_properties.timestamp = ms.started_at THEN 'exact_session_start'
        WHEN e.event_properties.timestamp <= ms.started_at + INTERVAL 5 SECONDS THEN 'within_5s_after'
        WHEN e.event_properties.timestamp <= ms.started_at + INTERVAL 1 MINUTE  THEN 'within_1m_after'
        WHEN e.event_properties.timestamp <= ms.started_at + INTERVAL 1 HOUR    THEN 'within_1h_after'
        ELSE 'more_than_1h_after'
    END AS prod_tp_relative_to_session,
    count(*) AS cnt
FROM mismatch_sessions ms
JOIN events e
    ON  e.event_properties.uuid = ms.prod_touchpoint_id
    AND e.date BETWEEN date_sub('{investigation_date}', 5) AND date_add('{investigation_date}', 1)
    AND e.event_name = 'AttributionTracking'
GROUP BY 1
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8d: Hypothesis 3 -- Production picks LAST not FIRST
# MAGIC
# MAGIC Try reproducing with LAST (DESC) instead of FIRST (ASC) and see if match rate improves.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW baseline_last AS
WITH attributed AS (
    SELECT
        s.session_id,
        s.touchpoint_id AS prod_touchpoint_id,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM sessions s
    LEFT JOIN touchpoint_events t
        ON  s.visitor_id = t.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL 120 HOURS AND s.started_at
        AND t.event_name = 'AttributionTracking'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts DESC
    ) = 1
)
SELECT
    count(*)                                                                     AS total_sessions,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                  AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)     AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL) AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL) AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                 AS mismatch
FROM attributed
""")

row = spark.sql("SELECT * FROM baseline_last").collect()[0]
total = row["total_sessions"]
matched = row["exact_match"] + row["both_null"]
print(f"Match rate with LAST (DESC): {matched / total * 100:.2f}%")
spark.sql("SELECT * FROM baseline_last").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8e: Hypothesis 4 -- Extended upper bound
# MAGIC
# MAGIC Try extending the upper bound past `started_at` to catch touchpoints that fire
# MAGIC during session initialization.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW baseline_extended AS
WITH attributed AS (
    SELECT
        s.session_id,
        s.touchpoint_id AS prod_touchpoint_id,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM sessions s
    LEFT JOIN touchpoint_events t
        ON  s.visitor_id = t.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL 120 HOURS
                                 AND s.started_at + INTERVAL 30 SECONDS
        AND t.event_name = 'AttributionTracking'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts ASC
    ) = 1
)
SELECT
    count(*)                                                                     AS total_sessions,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                  AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)     AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL) AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL) AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                 AS mismatch
FROM attributed
""")

row = spark.sql("SELECT * FROM baseline_extended").collect()[0]
total = row["total_sessions"]
matched = row["exact_match"] + row["both_null"]
print(f"Match rate with extended upper bound (+30s): {matched / total * 100:.2f}%")
spark.sql("SELECT * FROM baseline_extended").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8f: Hypothesis 5 -- Use top-level `timestamp` instead of `event_properties.timestamp`
# MAGIC
# MAGIC The touchpoint_events view uses `event_properties.timestamp`. Try with `e.timestamp` directly.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW baseline_top_ts AS
WITH tp AS (
    SELECT
        user.visitor_id AS visitor_id,
        event_properties.uuid AS touchpoint_uuid,
        timestamp AS touchpoint_ts,
        event_name
    FROM events
    WHERE event_name = 'AttributionTracking'
      AND date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
),
attributed AS (
    SELECT
        s.session_id,
        s.touchpoint_id AS prod_touchpoint_id,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM sessions s
    LEFT JOIN tp t
        ON  s.visitor_id = t.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL 120 HOURS AND s.started_at
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts ASC
    ) = 1
)
SELECT
    count(*)                                                                     AS total_sessions,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                  AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)     AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL) AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL) AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                 AS mismatch
FROM attributed
""")

row = spark.sql("SELECT * FROM baseline_top_ts").collect()[0]
total = row["total_sessions"]
matched = row["exact_match"] + row["both_null"]
print(f"Match rate with top-level timestamp: {{matched / total * 100:.2f}}%")
spark.sql("SELECT * FROM baseline_top_ts").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8g: Combined -- top-level timestamp + extended bound
# MAGIC
# MAGIC Try the best combination of fixes.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW baseline_combined AS
WITH tp AS (
    SELECT
        user.visitor_id AS visitor_id,
        event_properties.uuid AS touchpoint_uuid,
        timestamp AS touchpoint_ts,
        event_name
    FROM events
    WHERE event_name = 'AttributionTracking'
      AND date BETWEEN date_sub('{investigation_date}', 5) AND '{investigation_date}'
),
attributed AS (
    SELECT
        s.session_id,
        s.touchpoint_id AS prod_touchpoint_id,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM sessions s
    LEFT JOIN tp t
        ON  s.visitor_id = t.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL 120 HOURS
                                 AND s.started_at + INTERVAL 30 SECONDS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts ASC
    ) = 1
)
SELECT
    count(*)                                                                     AS total_sessions,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id = reproduced_touchpoint_id)                  AS exact_match,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NULL)     AS both_null,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NULL) AS prod_only,
    count_if(prod_touchpoint_id IS NULL AND reproduced_touchpoint_id IS NOT NULL) AS reproduced_only,
    count_if(prod_touchpoint_id IS NOT NULL AND reproduced_touchpoint_id IS NOT NULL
             AND prod_touchpoint_id != reproduced_touchpoint_id)                 AS mismatch
FROM attributed
""")

row = spark.sql("SELECT * FROM baseline_combined").collect()[0]
total = row["total_sessions"]
matched = row["exact_match"] + row["both_null"]
print(f"Match rate (top-level ts + extended bound): {{matched / total * 100:.2f}}%")
spark.sql("SELECT * FROM baseline_combined").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Variant | Description | Match Rate |
# MAGIC |---|---|---|
# MAGIC | Original | `event_properties.timestamp`, first, `<= started_at` | 70% |
# MAGIC | 8d | Same but LAST (DESC) | ? |
# MAGIC | 8e | Same but upper bound `+ 30s` | ? |
# MAGIC | 8f | Top-level `timestamp` instead | ? |
# MAGIC | 8g | Top-level `timestamp` + upper bound `+ 30s` | ? |
# MAGIC
# MAGIC Fill in the results above and pick the best variant to use in the main analysis.
