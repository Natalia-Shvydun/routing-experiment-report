# PRD: Interactive Routing Experiment Deep Dive Report

**Author:** Natalia Shvydun, Senior Data Analyst — Payments  
**Status:** Draft  
**Last updated:** 2026-04-17

---

## 1. Problem Statement

### Background

We are migrating CIT (customer-initiated transaction) payment card routing from our external orchestrator **Primer** to an **in-house solution** backed by dynamic configs in Statsig. The routing rules themselves are identical — the only change is *where* they are specified. The expected outcome is **zero difference** in attempt-level success rate between control (Primer) and test (in-house).

We are running an A/B experiment (`pay-payment-orchestration-routing-in-house`, started 2026-04-02) to validate this.

### What this replaces

Today, deep dive analysis is performed through **ad-hoc Databricks notebooks** — one per country or per hypothesis. This workflow has several problems:

- **Slow iteration.** Each new cut (e.g. "show me JPMC-currency Visa/MC in the US") requires writing or modifying SQL, re-running a notebook, and waiting for compute.
- **No standardised dimensions.** Analysts slice data by `bin_issuer_country_code`, `currency`, `card_scheme` manually. There is no shared mapping from these fields to the actual routing rule that applied.
- **Hard to share.** Notebooks live in personal Databricks workspaces. Stakeholders (Payments Engineers, EM) cannot self-serve; they request screenshots or schedule calls.
- **Repeated work.** Every experiment restart or date range change means touching dozens of cells across multiple notebooks.

### What we want instead

A single **interactive report** that runs in the browser (localhost), lets the analyst filter by the dimensions that matter, and surfaces the key metrics with statistical significance — all without writing SQL.

---

## 2. Target User

| Role | How they use the report |
|---|---|
| **Payments Data Analyst** (primary) | Deep dive into routing experiment results. Filter by country, currency, routing rule, attempt type. Identify which segments are driving SR differences. |
| **Payments Engineer** | Validate that in-house routing matches Primer behaviour. Spot config discrepancies by inspecting acquirer split per routing rule. |
| **Payments EM / PM** | Review high-level experiment health. Check for statistically significant regressions before ramping. |

All users access the report locally via `localhost` after the analyst generates the data.

---

## 3. Core Features (In Scope)

### 3.1 Routing Rule Classification

Every payment attempt is assigned to exactly one **routing rule** based on the production config. Rules are evaluated top-to-bottom; first match wins.

| Rule name | Type | Conditions | Primary routing |
|---|---|---|---|
| `non-visa-mc-adyen-only` | priority | card network NOT in (VISA, MASTERCARD) | ADYEN, CHECKOUT |
| `adyen-primary-jpm-currency` | priority | issuer country in (AU, CA, CH) AND JPM currency | ADYEN, CHECKOUT, JPMC |
| `adyen-primary` | priority | issuer country in (AU, CA, CH) | ADYEN, CHECKOUT |
| `us-jpm-primary` | volume_split | issuer country = US AND JPM currency | 90% JPMC / 5% ADYEN / 5% CKO |
| `us-no-jpm` | volume_split | issuer country = US | 95% ADYEN / 5% CKO |
| `eu5-jpm-currency` | volume_split | issuer country in (ES, FR, DE, GB, IT) AND JPM currency | 94% CKO / 5% ADYEN / 1% JPMC |
| `eu5` | volume_split | issuer country in (ES, FR, DE, GB, IT) | 95% CKO / 5% ADYEN |
| `latam-plus-jpm-currency` | volume_split | issuer country in (AR, BE, BO, BR, ...) AND JPM currency | 94% CKO / 5% ADYEN / 1% JPMC |
| `latam-plus` | volume_split | issuer country in (AR, BE, BO, BR, ...) | 95% CKO / 5% ADYEN |
| `catch-all-jpm-currency` | volume_split | JPM currency (any country) | 94% ADYEN / 5% CKO / 1% JPMC |
| `catch-all` | volume_split | (none — default) | 95% ADYEN / 5% CKO |

**JPM currencies:** CZK, DKK, EUR, GBP, HKD, JPY, MXN, NOK, NZD, PLN, SEK, SGD, USD, ZAR.

**Implementation:** The data pipeline notebook maps each attempt to a rule using `bin_issuer_country_code`, `currency`, and `payment_method_variant` (card network). The rule name is stored as a column in the exported dataset.

### 3.2 Filters

The report provides four filter dimensions. All filters are multi-select and apply globally to every metric panel.

| Filter | Source field | Default |
|---|---|---|
| **Country** | `bin_issuer_country_code` | All |
| **Currency** | `currency` | All |
| **Routing rule** | Derived `routing_rule` (see 3.1) | All |
| **Attempt type** | Composite — see sub-filters below | CIT, first attempt, all flows |

**Attempt type sub-filters:**

| Sub-filter | Source field | Options |
|---|---|---|
| Payment initiator | `payment_initiator_type` | CIT, MIT |
| Attempt rank | `customer_attempt_rank` | First attempt only (= 1), All attempts |
| Payment flow | `payment_flow` | pay_now, rnpl_auth_0, rnpl_pay_early, rnpl_auto_capture |

### 3.3 Metric Panels

The report body consists of five panels, each displaying control vs test side-by-side.

#### Panel 1 — Acquirer Split (Volume)

| Metric | Definition |
|---|---|
| Attempt count by acquirer | `COUNT(*)` grouped by `payment_processor` (ADYEN, CHECKOUT, JPMC, NULL) per variant |
| Acquirer share % | Each acquirer's share of total attempts within the variant |

**Visualisation:** Stacked bar chart (control vs test) + table with counts and percentages.

**Why it matters:** The routing rules define expected acquirer splits. Any discrepancy between control and test here means the in-house routing is not replicating Primer's behaviour. This is the primary diagnostic for routing correctness.

#### Panel 2 — Attempt Success Rate by Acquirer

| Metric | Definition |
|---|---|
| Overall SR | `SUM(is_customer_attempt_successful) / COUNT(*)` per variant |
| SR by acquirer | Same, grouped by `payment_processor` |
| SR delta | Test SR minus Control SR, per acquirer |

**Visualisation:** Grouped bar chart + table with SR, delta, and significance flag.

#### Panel 3 — Fraud Performance

| Metric | Definition |
|---|---|
| Forter result distribution | Share of ACCEPT, THREE_DS, THREE_DS_EXEMPTION, REFUSE, NULL per variant |
| REFUSE rate | `SUM(fraud_pre_auth_result = 'REFUSE') / COUNT(*)` |
| Sent-to-issuer rate | `SUM(sent_to_issuer) / COUNT(*)` |

**Visualisation:** Side-by-side bar chart of Forter result shares + table with counts, rates, and significance.

**Filter note:** When `payment_initiator_type = 'MIT'` is included, NULL Forter rates will be high by design (MIT has no Forter evaluation). The report should display a warning banner when MIT is included in the filter.

#### Panel 4 — 3DS Performance

| Metric | Definition |
|---|---|
| 3DS trigger rate | Share of attempts with `fraud_pre_auth_result = 'THREE_DS'` |
| Challenge rate | `SUM(challenge_issued = true) / COUNT(*)` among 3DS-triggered attempts |
| 3DS pass rate | `SUM(is_three_ds_passed) / COUNT(*)` among 3DS-triggered attempts |
| SR — challenged vs frictionless | SR split by `challenge_issued` (true vs false) within THREE_DS attempts |

**Visualisation:** KPI cards for rates + grouped bar chart for challenged vs frictionless SR.

#### Panel 5 — Statistical Significance

| Metric | Definition |
|---|---|
| Z-test for proportions | Two-proportion z-test comparing test vs control for each rate metric (SR, REFUSE rate, 3DS rate, challenge rate) |
| p-value | Two-tailed p-value |
| Significance flag | Highlighted when p < 0.05 |

**Implementation:** Computed in the frontend (JavaScript) from the pre-aggregated counts. Every delta shown in Panels 1–4 carries a significance indicator (green = not significant / no concern, red = significant at p < 0.05).

**Display:** A dedicated summary table listing all metrics, their control/test values, delta, z-score, p-value, and a pass/fail indicator. This provides a single-glance view of experiment health.

### 3.4 Header / Context Bar

Always visible at the top of the report:

- Experiment name and ID
- Date range (assignment start, data as-of date)
- Total attempts in current filter selection (control / test)
- Active filters summary

---

## 4. Data Architecture

### 4.1 Pipeline Overview

```
Databricks Notebook          JSON file             Vite + React App
┌─────────────────┐      ┌──────────────┐      ┌──────────────────┐
│ 1. Query         │      │              │      │                  │
│    assignments   │─────>│  data.json   │─────>│  Load JSON       │
│ 2. Join with     │      │              │      │  Apply filters   │
│    fact_payment  │      │  (one file,  │      │  Aggregate       │
│ 3. Classify      │      │   ~5-20 MB)  │      │  Render charts   │
│    routing rule  │      │              │      │  Compute z-tests │
│ 4. Export JSON   │      └──────────────┘      └──────────────────┘
└─────────────────┘
```

### 4.2 Data Pipeline (Databricks Notebook)

**Input tables:**

| Table | Purpose |
|---|---|
| `production.external_statsig.exposures` | Experiment variant assignment |
| `production.dwh.dim_customer` + `fact_customer_to_visitor` | Filter out internal/reseller traffic |
| `production.payments.fact_payment_attempt` | Payment attempt metrics |

**Assignment logic:**

```sql
WITH filtered_customers AS (
  SELECT visitor_id
  FROM production.dwh.dim_customer c
  JOIN production.dwh.fact_customer_to_visitor ctv USING (customer_id_anon)
  WHERE c.is_filtered_ticket_reseller_partner_or_internal = 1
),
assignment AS (
  SELECT
    e.group_name,
    e.user_id AS visitor_id,
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{ASSIGNMENT_START}'
    AND experiment_id = '{EXPERIMENT_ID}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
)
```

**Parameters:**

- `EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"`
- `ASSIGNMENT_START = "2026-04-02"`

**Routing rule classification (pseudo-code, evaluated top-to-bottom):**

```
IF card_network NOT IN (VISA, MASTERCARD)         → non-visa-mc-adyen-only
ELIF issuer_country IN (AU, CA, CH) AND jpm_curr  → adyen-primary-jpm-currency
ELIF issuer_country IN (AU, CA, CH)               → adyen-primary
ELIF issuer_country = US AND jpm_curr             → us-jpm-primary
ELIF issuer_country = US                          → us-no-jpm
ELIF issuer_country IN (ES,FR,DE,GB,IT) AND jpm   → eu5-jpm-currency
ELIF issuer_country IN (ES,FR,DE,GB,IT)           → eu5
ELIF issuer_country IN (AR,BE,...,SI) AND jpm      → latam-plus-jpm-currency
ELIF issuer_country IN (AR,BE,...,SI)              → latam-plus
ELIF jpm_curr                                     → catch-all-jpm-currency
ELSE                                              → catch-all
```

**Output columns per row (attempt-level):**

| Column | Source |
|---|---|
| `group_name` | Variant (control / test) |
| `routing_rule` | Derived — see above |
| `bin_issuer_country_code` | `fact_payment_attempt` |
| `currency` | `fact_payment_attempt` |
| `payment_processor` | `fact_payment_attempt` |
| `payment_method_variant` | `fact_payment_attempt` |
| `payment_initiator_type` | `fact_payment_attempt` |
| `customer_attempt_rank` | `fact_payment_attempt` |
| `payment_flow` | `fact_payment_attempt` |
| `is_customer_attempt_successful` | `fact_payment_attempt` |
| `fraud_pre_auth_result` | `fact_payment_attempt` |
| `sent_to_issuer` | `fact_payment_attempt` |
| `challenge_issued` | `fact_payment_attempt` |
| `is_three_ds_passed` | `fact_payment_attempt` |
| `three_ds_status` | `fact_payment_attempt` |
| `error_code` | `fact_payment_attempt` |
| `payment_attempt_date` | `DATE(payment_attempt_timestamp)` |

**Export format:** JSON array of objects. One file. Expected size: 5–20 MB for a multi-week experiment period (roughly 200K–1M rows with ~17 columns each).

### 4.3 Frontend Stack

| Component | Technology |
|---|---|
| Build tool | Vite (already configured) |
| UI framework | React 18 (already installed) |
| Charts | Recharts (already installed) |
| Stat tests | Custom JS utility (z-test for two proportions) |
| Styling | CSS modules or inline styles (no additional dependency) |

**Data flow in the frontend:**

1. On load, fetch `data.json` from the `/public` directory.
2. Parse into an in-memory array.
3. Apply active filters (country, currency, routing rule, attempt type).
4. Aggregate filtered data into the five metric panels.
5. Compute z-tests on the aggregated counts.
6. Render charts and tables with significance flags.

All aggregation and statistical computation happens client-side. No backend server is needed beyond Vite's dev server.

---

## 5. Out of Scope

| Item | Rationale |
|---|---|
| **Booking-level conversion** (cart SR, cancellation rate) | This report focuses on attempt-level routing correctness. Booking metrics are downstream and not directly affected by routing rule execution. |
| **Financial impact** (GMV, interchange cost) | Important but requires additional data sources (cost tables, FX rates). Can be a follow-up. |
| **APMs** (iDEAL, BLIK, Pix, etc.) | The routing experiment covers payment card CIT only. APMs have separate orchestration. |
| **Live Databricks queries** | Pre-computed JSON is sufficient for experiment analysis. Live queries add auth complexity with no clear benefit for periodic deep dives. |
| **Automated alerting / CI** | The report is analyst-driven. Automated regression detection could be a future enhancement. |
| **Multi-experiment support** | This report is purpose-built for `pay-payment-orchestration-routing-in-house`. Generalisation is not a goal for v1. |
| **Deployment to shared hosting** | v1 runs on localhost only. Sharing happens via the analyst distributing the JSON + built app. |

---

## 6. Open Questions

| # | Question | Impact | Proposed default |
|---|---|---|---|
| 1 | **Should we include `payment_method` beyond `payment_card`?** The experiment scope is CIT payment card, but Apple Pay / Google Pay also go through card routing. | Affects data volume and filter options. | Include `payment_card`, `apple_pay`, `google_pay` (all card-network methods). Exclude pure APMs. |
| 2 | **Date range filter — should the user be able to change it in the UI?** Currently the notebook hardcodes `ASSIGNMENT_START`. | If yes, the JSON would need to contain all dates and the frontend applies the date filter. If no, the analyst re-runs the notebook with new dates. | Include a date range filter in the UI. The notebook exports data from assignment start onward; the frontend allows narrowing. |
| 3 | **How to handle NULL `payment_processor`?** Some attempts never reach a processor (Forter REFUSE, routing gap). | These attempts are valid for fraud analysis but not for acquirer split analysis. | Include them in fraud/3DS panels. Show as "No processor" in acquirer panels. |
| 4 | **Minimum sample size for significance?** Small segments (e.g. a single country with 20 attempts) produce noisy z-tests. | Could mislead users. | Display z-test results only for segments with >= 100 attempts per variant. Show "insufficient data" otherwise. |
| 5 | **Should the report show daily trends?** The current spec is aggregate-only (sum over full date range). | Daily trends help spot transient issues (e.g. a config change mid-experiment). | Include a collapsible "Daily trends" section showing SR and acquirer share over time. |
