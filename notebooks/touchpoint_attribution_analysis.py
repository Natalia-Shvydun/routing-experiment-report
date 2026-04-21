# Databricks notebook source
# MAGIC %md
# MAGIC # Touchpoint Attribution Unification -- Impact Analysis
# MAGIC
# MAGIC **Problem**: Attribution logic is inconsistent between platforms:
# MAGIC
# MAGIC | Platform | Ordering | Window | Anchor |
# MAGIC |---|---|---|---|
# MAGIC | App (platform_id=3) | FIRST (oldest) | 5 days back + 1 day forward | first event timestamp |
# MAGIC | Web (platform_id=1,2) | LAST (most recent) | 24 hours back | session started_at |
# MAGIC
# MAGIC **Goal**: Quantify the impact of unifying to a single "last AT event" logic across
# MAGIC both platforms, across a range of lookback windows (24h to 5 days).
# MAGIC
# MAGIC All intermediate results are written to persistent tables in `testing.analytics`
# MAGIC so that individual cells can be re-run independently without losing earlier results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Config

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-02-01", "Start Date")
dbutils.widgets.text("end_date",   "2026-02-28", "End Date")

start_date = dbutils.widgets.get("start_date")
end_date   = dbutils.widgets.get("end_date")

APP_PLATFORM_ID    = 3
WEB_PLATFORM_IDS   = (1, 2)

APP_LOOKBACK_HOURS    = 120  # 5 days
APP_LOOKFORWARD_HOURS = 24   # 1 day (same-platform only)
WEB_LOOKBACK_HOURS    = 24

MAX_TP_PER_VISITOR_PER_DAY = 100

# Unified scenarios: same LAST logic for BOTH platforms, varying lookback window
# Format: (name, lookback_hours)
UNIFIED_SCENARIOS = [
    ("unified_last_24h",  24),
    ("unified_last_48h",  48),
    ("unified_last_72h",  72),
    ("unified_last_96h",  96),
    ("unified_last_5d",  120),
]

MAX_EVENT_DATE_BUFFER = max(APP_LOOKBACK_HOURS, APP_LOOKFORWARD_HOURS) // 24

# Persistent table names (testing.analytics schema)
TBL_TP_EVENTS         = "testing.analytics.tp_attr_touchpoint_events"
TBL_TP_EVENTS_CAPPED  = "testing.analytics.tp_attr_touchpoint_events_capped"
TBL_APP_SESSIONS      = "testing.analytics.tp_attr_app_sessions"
TBL_WEB_SESSIONS      = "testing.analytics.tp_attr_web_sessions"
TBL_ALL_RESULTS       = "testing.analytics.tp_attr_all_scenario_results"
TBL_SESSIONS_COMBINED = "testing.analytics.tp_attr_all_sessions_combined"
TBL_TP_CHANNEL_LOOKUP = "testing.analytics.tp_attr_tp_channel_lookup"
TBL_WITH_CHANNELS     = "testing.analytics.tp_attr_scenario_with_channels"

print(f"Period           : {start_date} to {end_date}")
print(f"App platform_id  : {APP_PLATFORM_ID}")
print(f"Web platform_ids : {WEB_PLATFORM_IDS}")
print(f"Unified scenarios: {[s[0] for s in UNIFIED_SCENARIOS]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Touchpoint Events (Adaptive Timestamp + 100-Cap)
# MAGIC
# MAGIC Implements `adaptiveTimestamp()` from the Scala pipeline:
# MAGIC - Mobile events (ios/android/traveler_app): `event_properties.timestamp` (client, seconds)
# MAGIC - Non-mobile events: `kafka_timestamp` (server, seconds -- top-level column)
# MAGIC - Fallback: if `kafka_timestamp` IS NULL, use `event_properties.timestamp`
# MAGIC
# MAGIC Then caps to the first 100 touchpoints per (visitor_id, date).
# MAGIC
# MAGIC **Writes**: `testing.analytics.tp_attr_touchpoint_events`,
# MAGIC `testing.analytics.tp_attr_touchpoint_events_capped`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_TP_EVENTS} AS
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
  AND date BETWEEN date_sub('{start_date}', {MAX_EVENT_DATE_BUFFER})
               AND date_add('{end_date}',   {APP_LOOKFORWARD_HOURS // 24})
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_TP_EVENTS_CAPPED} AS
SELECT *
FROM {TBL_TP_EVENTS}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY visitor_id, date
    ORDER BY touchpoint_ts ASC
) < {MAX_TP_PER_VISITOR_PER_DAY}
""")

raw_count    = spark.sql(f"SELECT count(*) AS cnt FROM {TBL_TP_EVENTS}").collect()[0]["cnt"]
capped_count = spark.sql(f"SELECT count(*) AS cnt FROM {TBL_TP_EVENTS_CAPPED}").collect()[0]["cnt"]
print(f"AT events (raw)   : {raw_count:,}")
print(f"AT events (capped): {capped_count:,}  ({raw_count - capped_count:,} removed by cap)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Sessions
# MAGIC
# MAGIC Load app and web sessions from `production.dwh.fact_session`.
# MAGIC App sessions include `first_event_ts` (from `events[0].timestamp`) --
# MAGIC the event-level anchor that mirrors how the Scala pipeline works.
# MAGIC
# MAGIC **Writes**: `testing.analytics.tp_attr_app_sessions`,
# MAGIC `testing.analytics.tp_attr_web_sessions`

# COMMAND ----------

web_platform_ids_sql = ", ".join(str(p) for p in WEB_PLATFORM_IDS)

spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_APP_SESSIONS} AS
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id,
    events[0].event_name AS first_event_name,
    events[0].timestamp  AS first_event_ts
FROM production.dwh.fact_session
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND platform_id = {APP_PLATFORM_ID}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_WEB_SESSIONS} AS
SELECT
    session_id,
    visitor_id,
    started_at,
    touchpoint_id
FROM production.dwh.fact_session
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND platform_id IN ({web_platform_ids_sql})
""")

app_cnt = spark.sql(f"SELECT count(*) AS cnt FROM {TBL_APP_SESSIONS}").collect()[0]["cnt"]
web_cnt = spark.sql(f"SELECT count(*) AS cnt FROM {TBL_WEB_SESSIONS}").collect()[0]["cnt"]
print(f"App sessions : {app_cnt:,}")
print(f"Web sessions : {web_cnt:,}")
print(f"Total        : {app_cnt + web_cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Compute Current Production Touchpoint
# MAGIC
# MAGIC Reproduces the current production logic separately per platform:
# MAGIC
# MAGIC **App**: Self-enrichment (when session starts with an AT event) or FIRST AT event
# MAGIC in the window `[first_event_ts - 5d, first_event_ts + 1d (same-platform only)]`.
# MAGIC
# MAGIC **Web**: LAST AT event in `[started_at - 24h, started_at]`.
# MAGIC
# MAGIC Both are combined and written as scenario `"current"` into
# MAGIC `testing.analytics.tp_attr_all_scenario_results`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_ALL_RESULTS} AS

-- App: current production logic
WITH app_self_enriched AS (
    SELECT
        s.session_id,
        'app'             AS platform,
        'current'         AS scenario,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM {TBL_APP_SESSIONS} s
    JOIN {TBL_TP_EVENTS_CAPPED} t
        ON  t.visitor_id      = s.visitor_id
        AND t.touchpoint_uuid = s.session_id
    WHERE s.first_event_name = 'AttributionTracking'
),
app_window_based AS (
    SELECT
        s.session_id,
        'app'             AS platform,
        'current'         AS scenario,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM {TBL_APP_SESSIONS} s
    LEFT JOIN {TBL_TP_EVENTS_CAPPED} t
        ON  t.visitor_id    = s.visitor_id
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
-- Web: current production logic
web_current AS (
    SELECT
        s.session_id,
        'web'             AS platform,
        'current'         AS scenario,
        t.touchpoint_uuid AS reproduced_touchpoint_id
    FROM {TBL_WEB_SESSIONS} s
    LEFT JOIN {TBL_TP_EVENTS_CAPPED} t
        ON  t.visitor_id    = s.visitor_id
        AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL {WEB_LOOKBACK_HOURS} HOURS
                                AND s.started_at
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.session_id
        ORDER BY t.touchpoint_ts DESC
    ) = 1
)
SELECT * FROM app_self_enriched
UNION ALL
SELECT * FROM app_window_based
UNION ALL
SELECT * FROM web_current
""")

spark.sql(f"OPTIMIZE {TBL_ALL_RESULTS} ZORDER BY (session_id)")

spark.sql(f"""
    SELECT platform, count(*) AS sessions,
           count_if(reproduced_touchpoint_id IS NOT NULL) AS with_touchpoint
    FROM {TBL_ALL_RESULTS}
    GROUP BY platform
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Compute Unified Scenarios
# MAGIC
# MAGIC All unified scenarios apply the same "LAST AT event" logic to BOTH platforms,
# MAGIC using `started_at` as the anchor and varying only the lookback window.
# MAGIC Results are appended to `testing.analytics.tp_attr_all_scenario_results`.

# COMMAND ----------

for scenario_name, lookback_hours in UNIFIED_SCENARIOS:
    spark.sql(f"""
        INSERT INTO {TBL_ALL_RESULTS}
        WITH all_sessions AS (
            SELECT session_id, visitor_id, started_at, 'app' AS platform FROM {TBL_APP_SESSIONS}
            UNION ALL
            SELECT session_id, visitor_id, started_at, 'web' AS platform FROM {TBL_WEB_SESSIONS}
        )
        SELECT
            s.session_id,
            s.platform,
            '{scenario_name}'    AS scenario,
            t.touchpoint_uuid    AS reproduced_touchpoint_id
        FROM all_sessions s
        LEFT JOIN {TBL_TP_EVENTS_CAPPED} t
            ON  t.visitor_id    = s.visitor_id
            AND t.touchpoint_ts BETWEEN s.started_at - INTERVAL {lookback_hours} HOURS
                                    AND s.started_at
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY s.session_id
            ORDER BY t.touchpoint_ts DESC
        ) = 1
    """)
    print(f"Inserted scenario: {scenario_name} ({lookback_hours}h lookback)")

spark.sql(f"OPTIMIZE {TBL_ALL_RESULTS} ZORDER BY (session_id)")

spark.sql(f"""
    SELECT scenario, platform, count(*) AS sessions
    FROM {TBL_ALL_RESULTS}
    GROUP BY scenario, platform
    ORDER BY scenario, platform
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Join with Marketing Channels
# MAGIC
# MAGIC **Optimisation strategy** (was 1.5h before):
# MAGIC
# MAGIC Instead of joining `fact_marketing_touchpoints_attribution_with_cost` (a large table)
# MAGIC twice inside a query over all scenarios × all sessions, we do three cheap steps:
# MAGIC
# MAGIC 1. **`TBL_SESSIONS_COMBINED`** -- resolve `current_channel` *once per session*
# MAGIC    (one scan of the marketing table against unique prod touchpoint IDs).
# MAGIC 2. **`TBL_TP_CHANNEL_LOOKUP`** -- build a tiny table of
# MAGIC    *distinct reproduced touchpoint IDs → channel* (one more scan, far fewer rows).
# MAGIC 3. **`TBL_WITH_CHANNELS`** -- final assembly joins only small local tables;
# MAGIC    the large marketing table is never touched again.
# MAGIC
# MAGIC **Writes**: `testing.analytics.tp_attr_all_sessions_combined`,
# MAGIC `testing.analytics.tp_attr_tp_channel_lookup`,
# MAGIC `testing.analytics.tp_attr_scenario_with_channels`

# COMMAND ----------

# Step 1 -- Sessions with current channel resolved (one join, unique prod touchpoint IDs)
spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_SESSIONS_COMBINED} AS
WITH sessions AS (
    SELECT session_id, visitor_id, started_at, touchpoint_id, 'app' AS platform
    FROM {TBL_APP_SESSIONS}
    UNION ALL
    SELECT session_id, visitor_id, started_at, touchpoint_id, 'web' AS platform
    FROM {TBL_WEB_SESSIONS}
),
unique_prod_tps AS (
    SELECT DISTINCT touchpoint_id
    FROM sessions
    WHERE touchpoint_id IS NOT NULL
),
prod_tp_channels AS (
    SELECT u.touchpoint_id, t.channel
    FROM unique_prod_tps u
    LEFT JOIN production.marketing.fact_marketing_touchpoints_attribution_with_cost t
        ON t.touchpoint_id = u.touchpoint_id
)
SELECT
    s.session_id,
    s.visitor_id,
    s.started_at,
    s.touchpoint_id,
    s.platform,
    c.channel AS current_channel
FROM sessions s
LEFT JOIN prod_tp_channels c
    ON c.touchpoint_id = s.touchpoint_id
""")

prod_tp_count = spark.sql(f"SELECT count(DISTINCT touchpoint_id) AS cnt FROM {TBL_SESSIONS_COMBINED}").collect()[0]["cnt"]
print(f"Unique prod touchpoint IDs resolved: {prod_tp_count:,}")

# COMMAND ----------

# Step 2 -- Channel lookup for all distinct reproduced touchpoint IDs across every scenario
# (far fewer rows than TBL_ALL_RESULTS itself)
spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_TP_CHANNEL_LOOKUP} AS
WITH unique_reproduced_tps AS (
    SELECT DISTINCT reproduced_touchpoint_id
    FROM {TBL_ALL_RESULTS}
    WHERE reproduced_touchpoint_id IS NOT NULL
)
SELECT u.reproduced_touchpoint_id, t.channel
FROM unique_reproduced_tps u
LEFT JOIN production.marketing.fact_marketing_touchpoints_attribution_with_cost t
    ON t.touchpoint_id = u.reproduced_touchpoint_id
""")

reproduced_tp_count = spark.sql(f"SELECT count(*) AS cnt FROM {TBL_TP_CHANNEL_LOOKUP}").collect()[0]["cnt"]
print(f"Unique reproduced touchpoint IDs resolved: {reproduced_tp_count:,}")

# COMMAND ----------

# Step 3 -- Final assembly: join only local tables, no more hits to the marketing table
spark.sql(f"""
CREATE OR REPLACE TABLE {TBL_WITH_CHANNELS} AS
SELECT
    ar.session_id,
    ar.platform,
    ar.scenario,
    s.touchpoint_id              AS prod_touchpoint_id,
    ar.reproduced_touchpoint_id  AS new_touchpoint_id,
    s.current_channel,
    lkp.channel                  AS new_channel
FROM {TBL_ALL_RESULTS} ar
JOIN {TBL_SESSIONS_COMBINED} s
    ON ar.session_id = s.session_id
LEFT JOIN {TBL_TP_CHANNEL_LOOKUP} lkp
    ON lkp.reproduced_touchpoint_id = ar.reproduced_touchpoint_id
""")

spark.sql(f"""
    SELECT scenario, platform, count(*) AS rows
    FROM {TBL_WITH_CHANNELS}
    GROUP BY scenario, platform
    ORDER BY scenario, platform
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Output 1 -- % of Sessions with Changed Touchpoint
# MAGIC
# MAGIC Compares each unified scenario against the `current` scenario (reproduced production logic).
# MAGIC Split by platform to show the impact on app vs web separately.

# COMMAND ----------

spark.sql(f"""
WITH current_tps AS (
    SELECT session_id, platform, reproduced_touchpoint_id AS current_tp_id
    FROM {TBL_ALL_RESULTS}
    WHERE scenario = 'current'
),
unified_tps AS (
    SELECT session_id, platform, scenario, reproduced_touchpoint_id AS new_tp_id
    FROM {TBL_ALL_RESULTS}
    WHERE scenario != 'current'
)
SELECT
    u.scenario,
    u.platform,
    count(*)                                                                   AS total_sessions,
    count_if(c.current_tp_id IS NOT DISTINCT FROM u.new_tp_id)                 AS unchanged,
    count_if(c.current_tp_id IS DISTINCT FROM u.new_tp_id)                     AS total_changed,
    count_if(c.current_tp_id IS NOT NULL AND u.new_tp_id IS NOT NULL
             AND c.current_tp_id != u.new_tp_id)                              AS changed_tp,
    count_if(c.current_tp_id IS NOT NULL AND u.new_tp_id IS NULL)              AS lost_tp,
    count_if(c.current_tp_id IS NULL     AND u.new_tp_id IS NOT NULL)          AS gained_tp,
    round(
        count_if(c.current_tp_id IS DISTINCT FROM u.new_tp_id) / count(*) * 100,
        2
    )                                                                          AS pct_changed
FROM unified_tps u
JOIN current_tps c
    ON u.session_id = c.session_id AND u.platform = c.platform
GROUP BY u.scenario, u.platform
ORDER BY u.scenario, u.platform
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Output 2 -- Channel Transition Matrix
# MAGIC
# MAGIC For each scenario, shows how sessions move between channels
# MAGIC (`current_channel → new_channel`). Diagonal = unchanged.

# COMMAND ----------

for scenario_name, _ in UNIFIED_SCENARIOS:
    for plat in ("app", "web"):
        print(f"\n{'='*60}")
        print(f"  {scenario_name}  |  {plat}")
        print(f"{'='*60}")
        spark.sql(f"""
            SELECT
                COALESCE(c.current_channel, '(no touchpoint)') AS from_channel,
                COALESCE(u.new_channel,     '(no touchpoint)') AS to_channel,
                count(DISTINCT c.session_id)                   AS sessions
            FROM {TBL_WITH_CHANNELS} c
            JOIN {TBL_WITH_CHANNELS} u
                ON  c.session_id = u.session_id
                AND u.scenario   = '{scenario_name}'
                AND u.platform   = '{plat}'
            WHERE c.scenario = 'current'
              AND c.platform  = '{plat}'
            GROUP BY from_channel, to_channel
            ORDER BY sessions DESC
        """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Output 3 -- Channel Attribution Charts
# MAGIC
# MAGIC Visualisations comparing the marketing channel split across scenarios.
# MAGIC
# MAGIC - **9a** Data table: session counts + % shift per channel (app & web)
# MAGIC - **9b** Stacked bar chart: channel share (%) -- current vs all scenarios, per platform
# MAGIC - **9c** Grouped bar chart: absolute sessions per channel -- current vs scenarios (app)
# MAGIC - **9d** Diverging bar chart: net gain / loss per channel (unified - current)
# MAGIC - **9e** Window sweep line chart: % sessions with changed touchpoint vs lookback window

# COMMAND ----------

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── helpers ────────────────────────────────────────────────────────────────────

SCENARIO_LABELS = {
    "current":          "Current",
    "unified_last_24h": "Unified 24h",
    "unified_last_48h": "Unified 48h",
    "unified_last_72h": "Unified 72h",
    "unified_last_96h": "Unified 96h",
    "unified_last_5d":  "Unified 5d",
}
ALL_SCENARIO_NAMES = ["current"] + [s[0] for s in UNIFIED_SCENARIOS]

PLATFORM_CHANNEL_COL = {
    "app": "current_channel",   # for 'current' scenario the channel is in current_channel
    "web": "current_channel",
}
NEW_CHANNEL_COL = "new_channel"

PALETTE = plt.cm.tab20.colors  # 20 distinct colours


def _fetch_channel_shares(platform: str) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by channel with one column per scenario
    containing the % share of sessions attributed to that channel.
    """
    frames = {}
    # current scenario: use current_channel column
    df_cur = spark.sql(f"""
        SELECT
            COALESCE(current_channel, '(no touchpoint)') AS channel,
            count(DISTINCT session_id)                   AS sessions
        FROM {TBL_WITH_CHANNELS}
        WHERE scenario = 'current' AND platform = '{platform}'
        GROUP BY channel
    """).toPandas().set_index("channel")
    frames["current"] = df_cur["sessions"]

    for scen, _ in UNIFIED_SCENARIOS:
        df_s = spark.sql(f"""
            SELECT
                COALESCE(new_channel, '(no touchpoint)') AS channel,
                count(DISTINCT session_id)               AS sessions
            FROM {TBL_WITH_CHANNELS}
            WHERE scenario = '{scen}' AND platform = '{platform}'
            GROUP BY channel
        """).toPandas().set_index("channel")
        frames[scen] = df_s["sessions"]

    combined = pd.DataFrame(frames).fillna(0).astype(int)
    # convert to % share
    shares = combined.div(combined.sum(axis=0), axis=1).mul(100).round(2)
    return combined, shares

# ── 9a: data table ─────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9a: Data table -- sessions and % shift per channel

# COMMAND ----------

for plat in ("app", "web"):
    counts, _ = _fetch_channel_shares(plat)
    counts = counts.sort_values("current", ascending=False)
    counts_reset = counts.reset_index().rename(columns={"index": "channel"})
    for scen in [s[0] for s in UNIFIED_SCENARIOS]:
        counts_reset[f"delta_pct_{scen}"] = round(
            (counts_reset[scen] - counts_reset["current"])
            / counts_reset["current"].replace(0, 1) * 100, 1
        )
    print(f"\n{'='*60}  Platform: {plat.upper()}  {'='*60}")
    display(spark.createDataFrame(counts_reset))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9b: Stacked bar chart -- channel share (%) per scenario

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(20, 7), sharey=False)

for ax, plat in zip(axes, ("app", "web")):
    counts, shares = _fetch_channel_shares(plat)
    shares = shares.sort_values("current", ascending=False)

    # Keep top N channels, group the rest as "Other"
    TOP_N = 10
    top_channels = shares.index[:TOP_N].tolist()
    other_channels = shares.index[TOP_N:].tolist()
    if other_channels:
        other_row = shares.loc[other_channels].sum().rename("Other")
        shares = pd.concat([shares.loc[top_channels], other_row.to_frame().T])
    else:
        shares = shares.loc[top_channels]

    scenarios_ordered = ["current"] + [s[0] for s in UNIFIED_SCENARIOS]
    x = np.arange(len(scenarios_ordered))
    bar_width = 0.65
    bottom = np.zeros(len(scenarios_ordered))

    for i, channel in enumerate(shares.index):
        vals = [shares.loc[channel, s] if s in shares.columns else 0
                for s in scenarios_ordered]
        bars = ax.bar(x, vals, bar_width, bottom=bottom,
                      color=PALETTE[i % len(PALETTE)], label=channel)
        # label segments > 4%
        for bar, val, bot in zip(bars, vals, bottom):
            if val > 4:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bot + val / 2,
                        f"{val:.1f}%", ha="center", va="center",
                        fontsize=7.5, color="white", fontweight="bold")
        bottom = bottom + np.array(vals)

    ax.set_title(f"Channel Share (%) -- {plat.upper()} sessions", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_LABELS[s] for s in scenarios_ordered], rotation=25, ha="right")
    ax.set_ylabel("% of sessions")
    ax.set_ylim(0, 105)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.legend(loc="upper right", fontsize=8, framealpha=0.7,
              bbox_to_anchor=(1.0, 1.0))
    ax.grid(axis="y", alpha=0.25)

plt.suptitle("Marketing Channel Attribution -- Current vs Unified Scenarios",
             fontsize=15, fontweight="bold", y=1.01)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9c: Grouped bar chart -- absolute sessions per channel (app, top channels)

# COMMAND ----------

counts_app, _ = _fetch_channel_shares("app")
counts_app = counts_app.sort_values("current", ascending=False)

TOP_N = 8
top_channels = counts_app.index[:TOP_N].tolist()
counts_top = counts_app.loc[top_channels]

scenarios_ordered = ["current"] + [s[0] for s in UNIFIED_SCENARIOS]
n_scenarios = len(scenarios_ordered)
n_channels  = len(top_channels)

x = np.arange(n_channels)
bar_width = 0.12
offsets = np.linspace(-(n_scenarios - 1) / 2, (n_scenarios - 1) / 2, n_scenarios) * bar_width

fig, ax = plt.subplots(figsize=(18, 6))

for i, scen in enumerate(scenarios_ordered):
    vals = [counts_top.loc[ch, scen] if scen in counts_top.columns else 0
            for ch in top_channels]
    bars = ax.bar(x + offsets[i], vals, bar_width,
                  label=SCENARIO_LABELS[scen],
                  color=PALETTE[i % len(PALETTE)],
                  alpha=0.88)

ax.set_title("App Sessions per Channel -- Current vs Unified Scenarios (absolute)",
             fontsize=13, fontweight="bold")
ax.set_xticks(x)
ax.set_xticklabels(top_channels, rotation=20, ha="right")
ax.set_ylabel("Sessions")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.legend(fontsize=9, framealpha=0.8)
ax.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9d: Diverging bar chart -- net session gain / loss per channel vs current (app)

# COMMAND ----------

counts_app, _ = _fetch_channel_shares("app")
counts_app = counts_app.sort_values("current", ascending=False).head(12)

fig, axes = plt.subplots(1, len(UNIFIED_SCENARIOS),
                         figsize=(5 * len(UNIFIED_SCENARIOS), 6),
                         sharey=True)

for ax, (scen, hours) in zip(axes, UNIFIED_SCENARIOS):
    delta = counts_app[scen] - counts_app["current"]
    colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in delta]
    ax.barh(delta.index, delta.values, color=colors, edgecolor="white", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_title(f"{SCENARIO_LABELS[scen]}\n({hours}h)", fontsize=10, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(axis="x", alpha=0.25)
    # annotate bars
    for i, (val, ch) in enumerate(zip(delta.values, delta.index)):
        if abs(val) > 0:
            ax.text(val + (max(abs(delta.values)) * 0.02 * np.sign(val)),
                    i, f"{val:+,.0f}", va="center", fontsize=7.5,
                    color="#2ecc71" if val >= 0 else "#e74c3c")

axes[0].set_ylabel("Marketing Channel")
fig.suptitle("App -- Net Session Gain / Loss vs Current Attribution (by channel)",
             fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9e: Window sweep -- % sessions with changed touchpoint vs lookback window

# COMMAND ----------

sweep_raw = spark.sql(f"""
    WITH current_tps AS (
        SELECT session_id, platform, reproduced_touchpoint_id AS current_tp_id
        FROM {TBL_ALL_RESULTS}
        WHERE scenario = 'current'
    )
    SELECT
        ar.platform,
        ar.scenario,
        round(
            count_if(c.current_tp_id IS DISTINCT FROM ar.reproduced_touchpoint_id)
            / count(*) * 100, 2
        ) AS pct_changed
    FROM {TBL_ALL_RESULTS} ar
    JOIN current_tps c
        ON ar.session_id = c.session_id AND ar.platform = c.platform
    WHERE ar.scenario != 'current'
    GROUP BY ar.platform, ar.scenario
""").toPandas()

window_map = {name: hours for name, hours in UNIFIED_SCENARIOS}
sweep_raw["window_hours"] = sweep_raw["scenario"].map(window_map)
sweep_raw = sweep_raw.sort_values(["platform", "window_hours"])

fig, ax = plt.subplots(figsize=(11, 5))

platform_styles = {
    "app": {"color": "#2980b9", "marker": "o", "linestyle": "-"},
    "web": {"color": "#e67e22", "marker": "s", "linestyle": "--"},
}
for plat, grp in sweep_raw.groupby("platform"):
    style = platform_styles.get(plat, {})
    ax.plot(grp["window_hours"], grp["pct_changed"],
            label=f"{plat.upper()} sessions",
            linewidth=2.2, **style)
    for _, row in grp.iterrows():
        ax.annotate(
            f'{row["pct_changed"]}%',
            (row["window_hours"], row["pct_changed"]),
            textcoords="offset points",
            xytext=(0, 10 if plat == "app" else -16),
            ha="center", fontsize=9,
            color=style["color"]
        )

ax.set_xlabel("Unified Lookback Window (hours)", fontsize=11)
ax.set_ylabel("% Sessions with Changed Touchpoint", fontsize=11)
ax.set_title("Impact of Unified Attribution -- % Sessions Changing vs Window Size",
             fontsize=13, fontweight="bold")
ax.set_xticks(sweep_raw["window_hours"].unique())
ax.set_xticklabels(
    [f"{h}h\n({h//24}d)" for h in sorted(sweep_raw["window_hours"].unique())]
)
ax.yaxis.set_major_formatter(mticker.PercentFormatter())
ax.legend(fontsize=10)
ax.grid(True, alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Summary
# MAGIC
# MAGIC ### Persistent tables created by this notebook
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `testing.analytics.tp_attr_touchpoint_events` | All AT events with adaptive timestamp |
# MAGIC | `testing.analytics.tp_attr_touchpoint_events_capped` | Same, capped to 100 per visitor/day |
# MAGIC | `testing.analytics.tp_attr_app_sessions` | App sessions with first_event_ts |
# MAGIC | `testing.analytics.tp_attr_web_sessions` | Web sessions |
# MAGIC | `testing.analytics.tp_attr_all_scenario_results` | Reproduced touchpoint per session per scenario |
# MAGIC | `testing.analytics.tp_attr_all_sessions_combined` | App + web sessions combined |
# MAGIC | `testing.analytics.tp_attr_scenario_with_channels` | Full output with marketing channels |
# MAGIC
# MAGIC ### How to interpret results
# MAGIC
# MAGIC **Cell 7 (% changed)**:
# MAGIC - App sessions will always show a higher % changed than web, because the
# MAGIC   current app logic (FIRST in 5d) is fundamentally different from the
# MAGIC   unified logic (LAST in Xh).
# MAGIC - Web sessions change mainly due to window extension beyond 24h.
# MAGIC
# MAGIC **Cell 8 (channel transition matrix)**:
# MAGIC - Off-diagonal cells are channel shifts. Large off-diagonal numbers in paid
# MAGIC   channels warrant discussion with marketing before going live.
# MAGIC
# MAGIC **Cell 9b (window sweep)**:
# MAGIC - Look for the "elbow" where extending the window further barely changes the %.
# MAGIC - If the curve flattens after 48h, a 48h unified window covers most cases.
# MAGIC - If it keeps rising, app users re-engage much later and the longer window is justified.
# MAGIC
# MAGIC ### Decision framework
# MAGIC
# MAGIC | Scenario | Risk | Benefit |
# MAGIC |---|---|---|
# MAGIC | unified_last_24h | Lowest -- matches web today | Some app sessions lose touchpoint |
# MAGIC | unified_last_48h / 72h | Medium | Better app coverage; more recent than current |
# MAGIC | unified_last_5d | Highest channel shift | Full coverage; comparable window to current app |
# MAGIC
# MAGIC ### Next steps
# MAGIC - Share channel transition matrix with marketing team
# MAGIC - Pick window size based on elbow chart
# MAGIC - Re-run on a second month (e.g. January 2026) to validate stability
# MAGIC - Coordinate pipeline update with data engineering
