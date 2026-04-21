# Databricks notebook source
# MAGIC %md
# MAGIC # fact_session_adp: Session-Level Join Impact Analysis
# MAGIC
# MAGIC **Comparing two join strategies** for linking checkout-funnel events to ADP views:
# MAGIC
# MAGIC | | **Current (production)** | **New (proposed)** |
# MAGIC |---|---|---|
# MAGIC | Join keys | date + visitor + tour | date + visitor + tour + **session_id** |
# MAGIC | Time constraint | checkout_time >= adp_event_time | *(none)* |
# MAGIC
# MAGIC **This analysis classifies each checkout-funnel event into 5 categories**:
# MAGIC
# MAGIC | Category | Old matches? | New matches? | Session-level change? | Meaning |
# MAGIC |---|---|---|---|---|
# MAGIC | **BOTH_SAME** | Yes | Yes | No change | Both match; no other ADP sessions are over-attributed |
# MAGIC | **BOTH_IMPROVED** | Yes | Yes | Precision gain | Both match; but old approach also over-attributes to other ADP sessions that lack their own checkout |
# MAGIC | **OLD_ONLY** | Yes | No | Lost | Cross-session: ADP in different session, checkout_time >= adp_time. **Lost with new approach** |
# MAGIC | **NEW_ONLY** | No | Yes | Gained | Same session but checkout_time < adp_time (rare). **Gained with new approach** |
# MAGIC | **NEITHER** | No | No | No change | No matching ADP view in either approach |
# MAGIC
# MAGIC **BOTH_IMPROVED example**: Session 1 has ADP view (tour A), Session 2 has ADP view + add to cart (tour A).
# MAGIC Production gives Session 1 `has_add_to_cart = true` (wrong). New approach correctly limits it to Session 2.
# MAGIC
# MAGIC **BOTH_SAME example**: Session 1 has ADP view + add to cart (tour A), Session 2 has ADP view + add to cart (tour A).
# MAGIC Both sessions have their own checkout — no over-attribution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-04-05", "Start Date")
dbutils.widgets.text("end_date",   "2026-04-07", "End Date")

start_date = dbutils.widgets.get("start_date")
end_date   = dbutils.widgets.get("end_date")

print(f"Analyzing date range: {start_date} to {end_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build base views

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW adp_views AS
SELECT DISTINCT
    date,
    user.visitor_id AS visitor_id,
    attribution_session_id AS session_id,
    pageview_properties.tour_ids[0] AS tour_id,
    event_properties.timestamp AS adp_event_time
FROM production.events.events
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND event_name IN ('ActivityDetailPageRequest', 'ActivityView')
  AND pageview_properties.tour_ids[0] > 0
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW checkout_funnel_events AS
SELECT
    date,
    visitor_id,
    session_id,
    tour_id,
    funnel_step,
    event_time
FROM (
    SELECT
        date,
        user.visitor_id AS visitor_id,
        attribution_session_id AS session_id,
        explode(
            coalesce(
                booking.cart.cart_items.activities.tour_id,
                pageview_properties.tour_ids,
                referral_pageview_properties.tour_ids
            )
        ) AS tour_id,
        event_properties.timestamp AS event_time,
        CASE
            WHEN event_name = 'AddToCartAction' THEN 'add_to_cart'
            WHEN event_name IN ('CheckoutPageRequest', 'CheckoutView') THEN 'checkout'
            WHEN event_name IN ('PaymentPageRequest', 'PaymentView') THEN 'payment'
            WHEN event_name = 'UISubmit' AND ui.id = 'submit-payment' THEN 'payment_initiated'
            WHEN event_name = 'MobileAppUITap' AND ui.target = 'payment' THEN 'payment_initiated'
        END AS funnel_step
    FROM production.events.events
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
      AND (
          event_name IN ('AddToCartAction', 'CheckoutPageRequest', 'CheckoutView',
                         'PaymentPageRequest', 'PaymentView')
          OR (event_name = 'UISubmit' AND ui.id = 'submit-payment')
          OR (event_name = 'MobileAppUITap' AND ui.target = 'payment')
      )
) t
WHERE funnel_step IS NOT NULL
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW availability_events AS
SELECT DISTINCT
    date,
    user.visitor_id AS visitor_id,
    attribution_session_id AS session_id,
    pageview_properties.tour_ids[0] AS tour_id,
    event_properties.timestamp AS event_time
FROM production.events.events
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND event_name = 'CheckTourAvailabilityAction'
  AND pageview_properties.tour_ids[0] > 0
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Classify each checkout-funnel event by both join strategies
# MAGIC
# MAGIC For each distinct (date, visitor, tour, session, funnel_step):
# MAGIC - **old_matches**: exists an ADP view on same date/visitor/tour where `adp_time <= checkout_time` (any session)
# MAGIC - **new_matches**: exists an ADP view on same date/visitor/tour/**session**
# MAGIC - **BOTH** is split into **BOTH_SAME** vs **BOTH_IMPROVED** by checking whether the old approach
# MAGIC   also over-attributes this checkout to other ADP sessions that don't have their own checkout.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW checkout_scenario_classification AS
# MAGIC WITH checkout_sessions AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         tour_id,
# MAGIC         session_id,
# MAGIC         funnel_step,
# MAGIC         MIN(event_time) AS earliest_event_time
# MAGIC     FROM checkout_funnel_events
# MAGIC     GROUP BY date, visitor_id, tour_id, session_id, funnel_step
# MAGIC ),
# MAGIC -- Does the NEW approach match? (ADP view in the SAME session for same tour)
# MAGIC new_match AS (
# MAGIC     SELECT DISTINCT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         tour_id,
# MAGIC         session_id
# MAGIC     FROM adp_views
# MAGIC ),
# MAGIC -- Does the OLD approach match? (ADP view on same date/visitor/tour, any session, with adp_time <= checkout_time)
# MAGIC old_match AS (
# MAGIC     SELECT
# MAGIC         c.date,
# MAGIC         c.visitor_id,
# MAGIC         c.tour_id,
# MAGIC         c.session_id AS checkout_session_id,
# MAGIC         c.funnel_step,
# MAGIC         MAX(1) AS old_matched
# MAGIC     FROM checkout_sessions c
# MAGIC     JOIN adp_views a
# MAGIC         ON c.date = a.date
# MAGIC         AND c.visitor_id = a.visitor_id
# MAGIC         AND c.tour_id = a.tour_id
# MAGIC         AND c.earliest_event_time >= a.adp_event_time
# MAGIC     GROUP BY c.date, c.visitor_id, c.tour_id, c.session_id, c.funnel_step
# MAGIC ),
# MAGIC -- How many distinct sessions have ADP views for this (date, visitor, tour)?
# MAGIC adp_session_counts AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         tour_id,
# MAGIC         COUNT(DISTINCT session_id) AS num_adp_sessions
# MAGIC     FROM adp_views
# MAGIC     GROUP BY date, visitor_id, tour_id
# MAGIC ),
# MAGIC -- For BOTH sub-classification: count OTHER ADP sessions that
# MAGIC --   (a) would match this checkout via old join (adp_time <= checkout_time)
# MAGIC --   (b) do NOT have their own checkout for the same tour + funnel_step
# MAGIC -- If count > 0 → those sessions are over-attributed in production → BOTH_IMPROVED
# MAGIC over_attribution AS (
# MAGIC     SELECT
# MAGIC         c.date,
# MAGIC         c.visitor_id,
# MAGIC         c.tour_id,
# MAGIC         c.session_id AS checkout_session_id,
# MAGIC         c.funnel_step,
# MAGIC         COUNT(DISTINCT a.session_id) AS overattributed_adp_sessions
# MAGIC     FROM checkout_sessions c
# MAGIC     JOIN adp_views a
# MAGIC         ON c.date = a.date
# MAGIC         AND c.visitor_id = a.visitor_id
# MAGIC         AND c.tour_id = a.tour_id
# MAGIC         AND a.session_id != c.session_id
# MAGIC         AND c.earliest_event_time >= a.adp_event_time
# MAGIC     LEFT JOIN checkout_sessions c_other
# MAGIC         ON c_other.date = a.date
# MAGIC         AND c_other.visitor_id = a.visitor_id
# MAGIC         AND c_other.tour_id = c.tour_id
# MAGIC         AND c_other.session_id = a.session_id
# MAGIC         AND c_other.funnel_step = c.funnel_step
# MAGIC     WHERE c_other.session_id IS NULL
# MAGIC     GROUP BY c.date, c.visitor_id, c.tour_id, c.session_id, c.funnel_step
# MAGIC )
# MAGIC SELECT
# MAGIC     c.date,
# MAGIC     c.visitor_id,
# MAGIC     c.tour_id,
# MAGIC     c.session_id AS checkout_session_id,
# MAGIC     c.funnel_step,
# MAGIC     c.earliest_event_time,
# MAGIC     nm.session_id IS NOT NULL AS new_matches,
# MAGIC     COALESCE(om.old_matched, 0) = 1 AS old_matches,
# MAGIC     COALESCE(asc_.num_adp_sessions, 0) AS num_adp_sessions,
# MAGIC     COALESCE(oa.overattributed_adp_sessions, 0) AS overattributed_adp_sessions,
# MAGIC     CASE
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC              AND COALESCE(oa.overattributed_adp_sessions, 0) > 0
# MAGIC             THEN 'BOTH_IMPROVED: session-level precision gain'
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC             THEN 'BOTH_SAME: no change at session level'
# MAGIC         WHEN nm.session_id IS NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC             THEN 'OLD_ONLY: cross-session, lost with new approach'
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 0
# MAGIC             THEN 'NEW_ONLY: same session but checkout before ADP view'
# MAGIC         ELSE 'NEITHER: no ADP match in either approach'
# MAGIC     END AS scenario
# MAGIC FROM checkout_sessions c
# MAGIC LEFT JOIN new_match nm
# MAGIC     ON c.date = nm.date
# MAGIC     AND c.visitor_id = nm.visitor_id
# MAGIC     AND c.tour_id = nm.tour_id
# MAGIC     AND c.session_id = nm.session_id
# MAGIC LEFT JOIN old_match om
# MAGIC     ON c.date = om.date
# MAGIC     AND c.visitor_id = om.visitor_id
# MAGIC     AND c.tour_id = om.tour_id
# MAGIC     AND c.session_id = om.checkout_session_id
# MAGIC     AND c.funnel_step = om.funnel_step
# MAGIC LEFT JOIN adp_session_counts asc_
# MAGIC     ON c.date = asc_.date
# MAGIC     AND c.visitor_id = asc_.visitor_id
# MAGIC     AND c.tour_id = asc_.tour_id
# MAGIC LEFT JOIN over_attribution oa
# MAGIC     ON c.date = oa.date
# MAGIC     AND c.visitor_id = oa.visitor_id
# MAGIC     AND c.tour_id = oa.tour_id
# MAGIC     AND c.session_id = oa.checkout_session_id
# MAGIC     AND c.funnel_step = oa.funnel_step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Scenario breakdown by funnel step
# MAGIC
# MAGIC Shows the % distribution across all 5 scenarios for each funnel step.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     scenario,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', visitor_id)) AS unique_visitor_dates,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', visitor_id, '_', CAST(tour_id AS STRING))) AS unique_visitor_date_tours,
# MAGIC     ROUND(100.0 * COUNT(*)
# MAGIC         / SUM(COUNT(*)) OVER (PARTITION BY funnel_step), 2) AS pct_of_funnel_step
# MAGIC FROM checkout_scenario_classification
# MAGIC GROUP BY funnel_step, scenario
# MAGIC ORDER BY funnel_step, scenario

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Same analysis for availability checks

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avail_sessions AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         tour_id,
# MAGIC         session_id,
# MAGIC         MIN(event_time) AS earliest_event_time
# MAGIC     FROM availability_events
# MAGIC     GROUP BY date, visitor_id, tour_id, session_id
# MAGIC ),
# MAGIC new_match AS (
# MAGIC     SELECT DISTINCT date, visitor_id, tour_id, session_id
# MAGIC     FROM adp_views
# MAGIC ),
# MAGIC old_match AS (
# MAGIC     SELECT
# MAGIC         a.date,
# MAGIC         a.visitor_id,
# MAGIC         a.tour_id,
# MAGIC         a.session_id AS avail_session_id,
# MAGIC         MAX(1) AS old_matched
# MAGIC     FROM avail_sessions a
# MAGIC     JOIN adp_views adp
# MAGIC         ON a.date = adp.date
# MAGIC         AND a.visitor_id = adp.visitor_id
# MAGIC         AND a.tour_id = adp.tour_id
# MAGIC         AND a.earliest_event_time >= adp.adp_event_time
# MAGIC     GROUP BY a.date, a.visitor_id, a.tour_id, a.session_id
# MAGIC ),
# MAGIC -- Over-attribution: other ADP sessions that match via old join
# MAGIC -- but don't have their own availability check for this tour
# MAGIC over_attribution AS (
# MAGIC     SELECT
# MAGIC         a.date,
# MAGIC         a.visitor_id,
# MAGIC         a.tour_id,
# MAGIC         a.session_id AS avail_session_id,
# MAGIC         COUNT(DISTINCT adp.session_id) AS overattributed_adp_sessions
# MAGIC     FROM avail_sessions a
# MAGIC     JOIN adp_views adp
# MAGIC         ON a.date = adp.date
# MAGIC         AND a.visitor_id = adp.visitor_id
# MAGIC         AND a.tour_id = adp.tour_id
# MAGIC         AND adp.session_id != a.session_id
# MAGIC         AND a.earliest_event_time >= adp.adp_event_time
# MAGIC     LEFT JOIN avail_sessions a_other
# MAGIC         ON a_other.date = adp.date
# MAGIC         AND a_other.visitor_id = adp.visitor_id
# MAGIC         AND a_other.tour_id = a.tour_id
# MAGIC         AND a_other.session_id = adp.session_id
# MAGIC     WHERE a_other.session_id IS NULL
# MAGIC     GROUP BY a.date, a.visitor_id, a.tour_id, a.session_id
# MAGIC )
# MAGIC SELECT
# MAGIC     'check_availability' AS funnel_step,
# MAGIC     CASE
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC              AND COALESCE(oa.overattributed_adp_sessions, 0) > 0
# MAGIC             THEN 'BOTH_IMPROVED: session-level precision gain'
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC             THEN 'BOTH_SAME: no change at session level'
# MAGIC         WHEN nm.session_id IS NULL AND COALESCE(om.old_matched, 0) = 1
# MAGIC             THEN 'OLD_ONLY: cross-session, lost with new approach'
# MAGIC         WHEN nm.session_id IS NOT NULL AND COALESCE(om.old_matched, 0) = 0
# MAGIC             THEN 'NEW_ONLY: same session but avail before ADP view'
# MAGIC         ELSE 'NEITHER: no ADP match in either approach'
# MAGIC     END AS scenario,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     COUNT(DISTINCT CONCAT(a.date, '_', a.visitor_id)) AS unique_visitor_dates,
# MAGIC     COUNT(DISTINCT CONCAT(a.date, '_', a.visitor_id, '_', CAST(a.tour_id AS STRING))) AS unique_visitor_date_tours,
# MAGIC     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
# MAGIC FROM avail_sessions a
# MAGIC LEFT JOIN new_match nm
# MAGIC     ON a.date = nm.date AND a.visitor_id = nm.visitor_id
# MAGIC     AND a.tour_id = nm.tour_id AND a.session_id = nm.session_id
# MAGIC LEFT JOIN old_match om
# MAGIC     ON a.date = om.date AND a.visitor_id = om.visitor_id
# MAGIC     AND a.tour_id = om.tour_id AND a.session_id = om.avail_session_id
# MAGIC LEFT JOIN over_attribution oa
# MAGIC     ON a.date = oa.date AND a.visitor_id = oa.visitor_id
# MAGIC     AND a.tour_id = oa.tour_id AND a.session_id = oa.avail_session_id
# MAGIC GROUP BY scenario
# MAGIC ORDER BY scenario

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Visitor/date level impact — explains the deviation you observed
# MAGIC
# MAGIC For each funnel step, how many unique visitor-dates:
# MAGIC - **Lost**: all events are `OLD_ONLY` → flag flips true→false
# MAGIC - **Gained**: has `NEW_ONLY` events → flag flips false→true
# MAGIC - **Partial**: mix of `OLD_ONLY` and `BOTH_*` → flag stays true, fewer matched rows

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH visitor_date_scenarios AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         funnel_step,
# MAGIC         MAX(CASE WHEN scenario LIKE 'BOTH_%' THEN 1 ELSE 0 END) AS has_both,
# MAGIC         MAX(CASE WHEN scenario LIKE 'OLD_ONLY:%' THEN 1 ELSE 0 END) AS has_old_only,
# MAGIC         MAX(CASE WHEN scenario LIKE 'NEW_ONLY:%' THEN 1 ELSE 0 END) AS has_new_only,
# MAGIC         MAX(CASE WHEN scenario LIKE 'NEITHER:%' THEN 1 ELSE 0 END) AS has_neither
# MAGIC     FROM checkout_scenario_classification
# MAGIC     GROUP BY date, visitor_id, funnel_step
# MAGIC )
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', visitor_id)) AS total_visitor_dates,
# MAGIC
# MAGIC     COUNT(DISTINCT CASE WHEN has_both = 1 OR has_old_only = 1
# MAGIC         THEN CONCAT(date, '_', visitor_id) END) AS matched_in_production,
# MAGIC
# MAGIC     COUNT(DISTINCT CASE WHEN has_both = 1 OR has_new_only = 1
# MAGIC         THEN CONCAT(date, '_', visitor_id) END) AS matched_in_new,
# MAGIC
# MAGIC     COUNT(DISTINCT CASE WHEN has_old_only = 1 AND has_both = 0 AND has_new_only = 0
# MAGIC         THEN CONCAT(date, '_', visitor_id) END) AS visitor_dates_lost,
# MAGIC
# MAGIC     COUNT(DISTINCT CASE WHEN has_new_only = 1 AND has_both = 0 AND has_old_only = 0
# MAGIC         THEN CONCAT(date, '_', visitor_id) END) AS visitor_dates_gained,
# MAGIC
# MAGIC     COUNT(DISTINCT CASE WHEN has_old_only = 1 AND has_both = 1
# MAGIC         THEN CONCAT(date, '_', visitor_id) END) AS visitor_dates_partial,
# MAGIC
# MAGIC     ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_old_only = 1 AND has_both = 0 AND has_new_only = 0
# MAGIC         THEN CONCAT(date, '_', visitor_id) END)
# MAGIC         / NULLIF(COUNT(DISTINCT CASE WHEN has_both = 1 OR has_old_only = 1
# MAGIC             THEN CONCAT(date, '_', visitor_id) END), 0), 2) AS pct_lost_of_production,
# MAGIC
# MAGIC     ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_new_only = 1 AND has_both = 0 AND has_old_only = 0
# MAGIC         THEN CONCAT(date, '_', visitor_id) END)
# MAGIC         / NULLIF(COUNT(DISTINCT CASE WHEN has_both = 1 OR has_old_only = 1
# MAGIC             THEN CONCAT(date, '_', visitor_id) END), 0), 2) AS pct_gained_vs_production
# MAGIC FROM visitor_date_scenarios
# MAGIC GROUP BY funnel_step
# MAGIC ORDER BY funnel_step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Deep dive into OLD_ONLY — time gap and session patterns
# MAGIC
# MAGIC For events lost with the new approach: how far apart in time was the ADP view
# MAGIC (in another session) from the checkout event? Short gaps suggest session timeouts
# MAGIC are splitting a single user journey.

# COMMAND ----------

display(spark.sql(f"""
WITH old_only AS (
    SELECT DISTINCT
        date,
        visitor_id,
        tour_id,
        checkout_session_id,
        funnel_step,
        earliest_event_time,
        num_adp_sessions
    FROM checkout_scenario_classification
    WHERE scenario LIKE 'OLD_ONLY:%'
),
adp_times AS (
    SELECT
        date,
        user.visitor_id AS visitor_id,
        pageview_properties.tour_ids[0] AS tour_id,
        attribution_session_id AS adp_session_id,
        MAX(event_properties.timestamp) AS latest_adp_time
    FROM production.events.events
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
      AND event_name IN ('ActivityDetailPageRequest', 'ActivityView')
      AND pageview_properties.tour_ids[0] > 0
    GROUP BY date, user.visitor_id, pageview_properties.tour_ids[0], attribution_session_id
),
with_gap AS (
    SELECT
        o.funnel_step,
        o.num_adp_sessions,
        ROUND(
            (UNIX_TIMESTAMP(o.earliest_event_time) - UNIX_TIMESTAMP(a.latest_adp_time)) / 60.0,
            1
        ) AS minutes_gap
    FROM old_only o
    JOIN adp_times a
        ON o.date = a.date
        AND o.visitor_id = a.visitor_id
        AND o.tour_id = a.tour_id
        AND o.checkout_session_id != a.adp_session_id
        AND o.earliest_event_time >= a.latest_adp_time
)
SELECT
    funnel_step,
    COUNT(*) AS total_old_only_events,
    ROUND(PERCENTILE(minutes_gap, 0.10), 1) AS p10_minutes,
    ROUND(PERCENTILE(minutes_gap, 0.25), 1) AS p25_minutes,
    ROUND(PERCENTILE(minutes_gap, 0.50), 1) AS median_minutes,
    ROUND(PERCENTILE(minutes_gap, 0.75), 1) AS p75_minutes,
    ROUND(PERCENTILE(minutes_gap, 0.90), 1) AS p90_minutes,
    ROUND(PERCENTILE(minutes_gap, 0.95), 1) AS p95_minutes,
    SUM(CASE WHEN minutes_gap BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS gap_0_to_5min,
    SUM(CASE WHEN minutes_gap BETWEEN 5 AND 30 THEN 1 ELSE 0 END) AS gap_5_to_30min,
    SUM(CASE WHEN minutes_gap BETWEEN 30 AND 120 THEN 1 ELSE 0 END) AS gap_30min_to_2h,
    SUM(CASE WHEN minutes_gap > 120 THEN 1 ELSE 0 END) AS gap_over_2h
FROM with_gap
GROUP BY funnel_step
ORDER BY funnel_step
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Session pattern analysis for OLD_ONLY
# MAGIC
# MAGIC How many ADP sessions existed when the checkout happened in a different session?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     CASE
# MAGIC         WHEN num_adp_sessions = 1 THEN '1 ADP session -> checkout in different session'
# MAGIC         WHEN num_adp_sessions = 2 THEN '2 ADP sessions -> checkout in yet another session'
# MAGIC         WHEN num_adp_sessions >= 3 THEN '3+ ADP sessions -> checkout in yet another session'
# MAGIC     END AS journey_pattern,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', visitor_id)) AS unique_visitor_dates,
# MAGIC     ROUND(100.0 * COUNT(*)
# MAGIC         / SUM(COUNT(*)) OVER (PARTITION BY funnel_step), 2) AS pct_of_old_only
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'OLD_ONLY:%'
# MAGIC GROUP BY funnel_step,
# MAGIC     CASE
# MAGIC         WHEN num_adp_sessions = 1 THEN '1 ADP session -> checkout in different session'
# MAGIC         WHEN num_adp_sessions = 2 THEN '2 ADP sessions -> checkout in yet another session'
# MAGIC         WHEN num_adp_sessions >= 3 THEN '3+ ADP sessions -> checkout in yet another session'
# MAGIC     END
# MAGIC ORDER BY funnel_step, journey_pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Net impact summary
# MAGIC
# MAGIC Side-by-side: what the new session_id approach **gains** vs **loses**
# MAGIC at the visitor/date level, with all 5 scenarios.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH summary AS (
# MAGIC     SELECT
# MAGIC         funnel_step,
# MAGIC         CASE
# MAGIC             WHEN scenario LIKE 'BOTH_SAME%' THEN 'BOTH_SAME'
# MAGIC             WHEN scenario LIKE 'BOTH_IMPROVED%' THEN 'BOTH_IMPROVED'
# MAGIC             WHEN scenario LIKE 'OLD_ONLY:%' THEN 'OLD_ONLY'
# MAGIC             WHEN scenario LIKE 'NEW_ONLY:%' THEN 'NEW_ONLY'
# MAGIC             WHEN scenario LIKE 'NEITHER:%' THEN 'NEITHER'
# MAGIC         END AS scenario_group,
# MAGIC         COUNT(DISTINCT CONCAT(date, '_', visitor_id)) AS unique_visitor_dates
# MAGIC     FROM checkout_scenario_classification
# MAGIC     GROUP BY funnel_step,
# MAGIC         CASE
# MAGIC             WHEN scenario LIKE 'BOTH_SAME%' THEN 'BOTH_SAME'
# MAGIC             WHEN scenario LIKE 'BOTH_IMPROVED%' THEN 'BOTH_IMPROVED'
# MAGIC             WHEN scenario LIKE 'OLD_ONLY:%' THEN 'OLD_ONLY'
# MAGIC             WHEN scenario LIKE 'NEW_ONLY:%' THEN 'NEW_ONLY'
# MAGIC             WHEN scenario LIKE 'NEITHER:%' THEN 'NEITHER'
# MAGIC         END
# MAGIC )
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     SUM(unique_visitor_dates) AS total_visitor_dates,
# MAGIC     MAX(CASE WHEN scenario_group = 'BOTH_SAME' THEN unique_visitor_dates ELSE 0 END) AS both_same,
# MAGIC     MAX(CASE WHEN scenario_group = 'BOTH_IMPROVED' THEN unique_visitor_dates ELSE 0 END) AS both_improved,
# MAGIC     MAX(CASE WHEN scenario_group = 'OLD_ONLY' THEN unique_visitor_dates ELSE 0 END) AS old_only_lost,
# MAGIC     MAX(CASE WHEN scenario_group = 'NEW_ONLY' THEN unique_visitor_dates ELSE 0 END) AS new_only_gained,
# MAGIC     MAX(CASE WHEN scenario_group = 'NEITHER' THEN unique_visitor_dates ELSE 0 END) AS neither,
# MAGIC     ROUND(100.0 * MAX(CASE WHEN scenario_group = 'BOTH_IMPROVED' THEN unique_visitor_dates ELSE 0 END)
# MAGIC         / NULLIF(SUM(unique_visitor_dates), 0), 2) AS pct_improved,
# MAGIC     ROUND(100.0 * MAX(CASE WHEN scenario_group = 'OLD_ONLY' THEN unique_visitor_dates ELSE 0 END)
# MAGIC         / NULLIF(SUM(unique_visitor_dates), 0), 2) AS pct_lost,
# MAGIC     ROUND(100.0 * MAX(CASE WHEN scenario_group = 'NEW_ONLY' THEN unique_visitor_dates ELSE 0 END)
# MAGIC         / NULLIF(SUM(unique_visitor_dates), 0), 2) AS pct_gained
# MAGIC FROM summary
# MAGIC GROUP BY funnel_step
# MAGIC ORDER BY funnel_step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: BOTH_IMPROVED detail — how many sessions are over-attributed in production?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     overattributed_adp_sessions,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', visitor_id)) AS unique_visitor_dates,
# MAGIC     ROUND(100.0 * COUNT(*)
# MAGIC         / SUM(COUNT(*)) OVER (PARTITION BY funnel_step), 2) AS pct_of_both_improved
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'BOTH_IMPROVED%'
# MAGIC GROUP BY funnel_step, overattributed_adp_sessions
# MAGIC ORDER BY funnel_step, overattributed_adp_sessions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Session-level comparison
# MAGIC
# MAGIC Same structure as Step 5 but at the **session_id** level — how many unique sessions
# MAGIC gain / lose their funnel flags?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH session_scenarios AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         visitor_id,
# MAGIC         checkout_session_id AS session_id,
# MAGIC         funnel_step,
# MAGIC         MAX(CASE WHEN scenario LIKE 'BOTH_%' THEN 1 ELSE 0 END) AS has_both,
# MAGIC         MAX(CASE WHEN scenario LIKE 'OLD_ONLY:%' THEN 1 ELSE 0 END) AS has_old_only,
# MAGIC         MAX(CASE WHEN scenario LIKE 'NEW_ONLY:%' THEN 1 ELSE 0 END) AS has_new_only,
# MAGIC         MAX(CASE WHEN scenario LIKE 'NEITHER:%' THEN 1 ELSE 0 END) AS has_neither
# MAGIC     FROM checkout_scenario_classification
# MAGIC     GROUP BY date, visitor_id, checkout_session_id, funnel_step
# MAGIC )
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     COUNT(*) AS total_sessions,
# MAGIC
# MAGIC     SUM(CASE WHEN has_both = 1 OR has_old_only = 1 THEN 1 ELSE 0 END) AS matched_in_production,
# MAGIC
# MAGIC     SUM(CASE WHEN has_both = 1 OR has_new_only = 1 THEN 1 ELSE 0 END) AS matched_in_new,
# MAGIC
# MAGIC     SUM(CASE WHEN has_old_only = 1 AND has_both = 0 AND has_new_only = 0
# MAGIC         THEN 1 ELSE 0 END) AS sessions_lost,
# MAGIC
# MAGIC     SUM(CASE WHEN has_new_only = 1 AND has_both = 0 AND has_old_only = 0
# MAGIC         THEN 1 ELSE 0 END) AS sessions_gained,
# MAGIC
# MAGIC     SUM(CASE WHEN has_old_only = 1 AND has_both = 1
# MAGIC         THEN 1 ELSE 0 END) AS sessions_partial,
# MAGIC
# MAGIC     ROUND(100.0 * SUM(CASE WHEN has_old_only = 1 AND has_both = 0 AND has_new_only = 0 THEN 1 ELSE 0 END)
# MAGIC         / NULLIF(SUM(CASE WHEN has_both = 1 OR has_old_only = 1 THEN 1 ELSE 0 END), 0), 2) AS pct_sessions_lost,
# MAGIC
# MAGIC     ROUND(100.0 * SUM(CASE WHEN has_new_only = 1 AND has_both = 0 AND has_old_only = 0 THEN 1 ELSE 0 END)
# MAGIC         / NULLIF(SUM(CASE WHEN has_both = 1 OR has_old_only = 1 THEN 1 ELSE 0 END), 0), 2) AS pct_sessions_gained
# MAGIC FROM session_scenarios
# MAGIC GROUP BY funnel_step
# MAGIC ORDER BY funnel_step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Session-level scenario breakdown
# MAGIC
# MAGIC Scenario distribution counted at the session level (vs event level in Step 3).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     scenario,
# MAGIC     COUNT(DISTINCT CONCAT(date, '_', checkout_session_id)) AS unique_sessions,
# MAGIC     ROUND(100.0 * COUNT(DISTINCT CONCAT(date, '_', checkout_session_id))
# MAGIC         / SUM(COUNT(DISTINCT CONCAT(date, '_', checkout_session_id))) OVER (PARTITION BY funnel_step), 2) AS pct_of_funnel_step
# MAGIC FROM checkout_scenario_classification
# MAGIC GROUP BY funnel_step, scenario
# MAGIC ORDER BY funnel_step, scenario

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Sample visitors — BOTH_SAME (no change at any level)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     visitor_id,
# MAGIC     tour_id,
# MAGIC     checkout_session_id,
# MAGIC     funnel_step,
# MAGIC     earliest_event_time,
# MAGIC     num_adp_sessions,
# MAGIC     overattributed_adp_sessions
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'BOTH_SAME%'
# MAGIC   AND funnel_step = 'add_to_cart'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Sample visitors — BOTH_IMPROVED (session-level precision gain)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     visitor_id,
# MAGIC     tour_id,
# MAGIC     checkout_session_id,
# MAGIC     funnel_step,
# MAGIC     earliest_event_time,
# MAGIC     num_adp_sessions,
# MAGIC     overattributed_adp_sessions
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'BOTH_IMPROVED%'
# MAGIC   AND funnel_step = 'add_to_cart'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Sample visitors — OLD_ONLY (lost with new approach)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     visitor_id,
# MAGIC     tour_id,
# MAGIC     checkout_session_id,
# MAGIC     funnel_step,
# MAGIC     earliest_event_time,
# MAGIC     num_adp_sessions,
# MAGIC     overattributed_adp_sessions
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'OLD_ONLY:%'
# MAGIC   AND funnel_step = 'add_to_cart'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15: Sample visitors — NEW_ONLY (gained with new approach)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     visitor_id,
# MAGIC     tour_id,
# MAGIC     checkout_session_id,
# MAGIC     funnel_step,
# MAGIC     earliest_event_time,
# MAGIC     num_adp_sessions,
# MAGIC     overattributed_adp_sessions
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'NEW_ONLY:%'
# MAGIC   AND funnel_step = 'add_to_cart'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16: Sample visitors — NEITHER (no match either way)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     visitor_id,
# MAGIC     tour_id,
# MAGIC     checkout_session_id,
# MAGIC     funnel_step,
# MAGIC     earliest_event_time,
# MAGIC     num_adp_sessions,
# MAGIC     overattributed_adp_sessions
# MAGIC FROM checkout_scenario_classification
# MAGIC WHERE scenario LIKE 'NEITHER:%'
# MAGIC   AND funnel_step = 'add_to_cart'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17: Full event timeline for a sample visitor
# MAGIC
# MAGIC Fill in a visitor_id from any of the sample steps above to see the full journey.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fill in values from the sample steps above:
# MAGIC -- SELECT
# MAGIC --     event_name,
# MAGIC --     attribution_session_id AS session_id,
# MAGIC --     event_properties.timestamp AS event_time,
# MAGIC --     pageview_properties.tour_ids[0] AS tour_id,
# MAGIC --     COALESCE(
# MAGIC --         booking.cart.cart_items.activities.tour_id,
# MAGIC --         pageview_properties.tour_ids,
# MAGIC --         referral_pageview_properties.tour_ids
# MAGIC --     ) AS all_tour_ids
# MAGIC -- FROM production.events.events
# MAGIC -- WHERE date = '<paste_date>'
# MAGIC --   AND user.visitor_id = '<paste_visitor_id>'
# MAGIC --   AND (
# MAGIC --       event_name IN ('ActivityDetailPageRequest', 'ActivityView',
# MAGIC --                      'AddToCartAction', 'CheckoutPageRequest', 'CheckoutView',
# MAGIC --                      'PaymentPageRequest', 'PaymentView', 'CheckTourAvailabilityAction')
# MAGIC --       OR (event_name = 'UISubmit' AND ui.id = 'submit-payment')
# MAGIC --       OR (event_name = 'MobileAppUITap' AND ui.target = 'payment')
# MAGIC --   )
# MAGIC -- ORDER BY event_properties.timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision framework
# MAGIC
# MAGIC | Result | Recommendation |
# MAGIC |---|---|
# MAGIC | `OLD_ONLY` is small (< 1-2%) | Acceptable trade-off — proceed with session_id join |
# MAGIC | `OLD_ONLY` is large, time gaps mostly < 5 min | Session timeouts are splitting journeys — consider hybrid fallback |
# MAGIC | `OLD_ONLY` is large, time gaps are long (30+ min) | True cross-session return visits — session_id join may not fit |
# MAGIC | `BOTH_IMPROVED` is large | Strong signal that session_id adds precision at the session level |
# MAGIC | `BOTH_IMPROVED` >> `OLD_ONLY` | Net positive — session_id join fixes more than it breaks |
# MAGIC | `NEW_ONLY` is non-trivial | Edge case where checkout precedes ADP view — investigate data quality |
# MAGIC
# MAGIC **Hybrid approach**: Join on session_id first; for unmatched checkout events, fall back to the
# MAGIC date-level join with time constraint (closest preceding ADP view).
