# Touchpoint Attribution Logic Unification: Web vs App Impact Analysis

**Status**: Draft  
**Author**: Natalia Shvydun  
**Date**: March 2026  
**Stakeholders**: Marketing Analytics, Data Engineering, Marketing

---

## Background

Our session-level touchpoint attribution currently uses different rules depending on the platform a session originates from:

| Platform | Rule | Lookback window | Ordering |
|---|---|---|---|
| Web (desktop, mweb) | Last `AttributionTracking` event before session start | 24 hours | Most recent (LAST) |
| App (iOS, Android) | First `AttributionTracking` event around first app event | 5 days back / 1 day forward | Oldest (FIRST) |

This inconsistency means that the same marketing touchpoint (e.g. a paid search click) can be credited differently depending on whether a user converts on web or app. It creates distortions in channel-level reporting and makes cross-platform comparisons unreliable.

---

## Objective

1. Quantify how many sessions are affected by the current inconsistency.
2. Measure the channel attribution shift that would result from switching to a unified "last touchpoint" logic across both platforms.
3. Identify the optimal lookback window for the unified logic (from 24 hours up to 5 days).

---

## Analysis Scope

- **Period**: February 2026 (full month)
- **Platforms**: App (platform_id = 3) and Web (platform_id = 1, 2)
- **Event type**: `AttributionTracking` only (no `AppOpen`)
- **Source tables**: `production.dwh.fact_session`, `events`, `production.marketing.fact_marketing_touchpoints_attribution_with_cost`

---

## Current Logic (Reproduced Baseline)

The production attribution is reproduced in SQL following the Scala pipeline logic (`CurrentTouchpointEnrichmentProcessor`):

- **Adaptive timestamp**: mobile events use client-side `event_properties.timestamp`; non-mobile events use server-side `kafka_timestamp` (top-level column, already in seconds).
- **100-touchpoint cap**: only the first 100 `AttributionTracking` events per visitor per day are considered.
- **App self-enrichment**: if the first event of an app session is itself an `AttributionTracking` event, it is its own touchpoint.
- **App window**: `[first_event_ts − 5 days, first_event_ts + 1 day]` (forward window applies only to same-platform touchpoints); picks the **oldest** match.
- **Web window**: `[started_at − 24 hours, started_at]`; picks the **most recent** match.

Baseline validation match rates achieved: **~95% for app**, **~97% for web**.

---

## Scenarios Compared

| Scenario | Logic | Window | Ordering | Platforms |
|---|---|---|---|---|
| Current | Production logic (different per platform) | App: −5d/+1d · Web: −24h | Mixed | Per platform |
| Unified 24h | Same logic for both | −24h | LAST | Both |
| Unified 48h | Same logic for both | −48h | LAST | Both |
| Unified 72h | Same logic for both | −72h | LAST | Both |
| Unified 96h | Same logic for both | −96h | LAST | Both |
| Unified 5d | Same logic for both | −120h | LAST | Both |

---

## Results

### % of Sessions with Changed Touchpoint

*[Insert Cell 7 table here]*

| Scenario | Platform | Total sessions | Unchanged | Changed | % changed |
|---|---|---|---|---|---|
| unified_last_24h | app | | | | |
| unified_last_24h | web | | | | |
| unified_last_48h | app | | | | |
| unified_last_48h | web | | | | |
| unified_last_72h | app | | | | |
| unified_last_72h | web | | | | |
| unified_last_96h | app | | | | |
| unified_last_96h | web | | | | |
| unified_last_5d | app | | | | |
| unified_last_5d | web | | | | |

Key observations:

- *[Fill in after running notebook]*

---

### Marketing Channel Attribution Shift — App

#### Channel share (%) — current vs unified scenarios

*[Insert Cell 9b stacked bar chart — app here]*

#### Absolute sessions per channel

*[Insert Cell 9c grouped bar chart — app here]*

#### Net gain / loss per channel vs current

*[Insert Cell 9d diverging bar chart — app here]*

Key observations:

- *[Fill in after running notebook]*

---

### Marketing Channel Attribution Shift — Web

#### Channel share (%) — current vs unified scenarios

*[Insert Cell 9b stacked bar chart — web here]*

Key observations:

- *[Fill in after running notebook]*

---

### Channel Transition Matrix (App — Selected Scenario)

*[Insert Cell 8 channel transition matrix for the recommended scenario here]*

> Rows = attributed channel under **current** logic. Columns = attributed channel under the **unified** scenario. Diagonal cells = sessions where the attribution does not change.

---

### Window Sensitivity

*[Insert Cell 9e window sweep line chart here]*

Key observations:

- *[Describe the elbow point — the window size at which extending further yields minimal additional change — and what it implies for the recommended window choice]*

---

## Recommendation

*[Fill in after reviewing results]*

> Based on the analysis, we recommend unifying to **[unified_last_Xh]** for both platforms because:
>
> - It changes **X%** of app sessions and **Y%** of web sessions.
> - The largest channel shifts are in **[channels]**; the marketing team has been consulted.
> - Extending the window beyond **Xh** provides diminishing returns (see elbow in the sweep chart above).

---

## Risks and Considerations

- Attribution model changes affect historical trend continuity. A **cut-over date** should be agreed and communicated to marketing before deployment.
- Paid channel sessions that lose a touchpoint under the unified logic will be reclassified as organic / direct. This may impact channel-level budget decisions and should be reviewed with the marketing team before going live.
- Re-running the analysis on a second month (e.g. January 2026) is recommended before final sign-off to validate that February results are representative.

---

## Technical Implementation

The pipeline change would be made in `CurrentTouchpointEnrichmentProcessor.scala`:

1. Remove the separate `EVENT_TYPE_APP` / `EVENT_TYPE_OTHER` branching for look-back window sizes.
2. Align both event types to use a single `lookBackWindow` constant with the agreed-upon new value.
3. Remove (or unify) the forward window (`appTouchpointLookForwardWindow`) — currently only applied to app events.
4. Update ordering so both app and web use `LAST` (i.e. replace `appPlatformEventsCurrentTouchpoint` with `otherEventsCurrentTouchpoint`).
5. Coordinate the release with Data Engineering; ensure downstream tables (e.g. `fact_session`, channel dashboards) are refreshed after the change.

---

## Appendix

- Analysis notebook: `notebooks/touchpoint_attribution_analysis.py`
- Baseline verification notebook: `notebooks/touchpoint_baseline_verification.py`
- Intermediate tables: `testing.analytics.tp_attr_*`
- Jira ticket: *[Link]*
- Related Confluence pages: *[Link]*
