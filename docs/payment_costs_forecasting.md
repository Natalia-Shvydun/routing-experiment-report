# Payment Costs Forecasting — Technical Specification

## 1. Purpose

Payment processing costs (interchange fees, scheme fees, acquirer fees) take time to fully settle. For recently checked-out shopping carts, the actual cost data is incomplete. This forecasting model **estimates the final payment costs for carts whose costs have not yet matured**, allowing Finance to produce accurate cost reports without waiting for full settlement.

The model uses a **factor-based approach**: it computes a historical cost-to-GMV ratio (the "factor") from carts whose costs are already mature, then applies that factor to immature carts to predict their final costs.

### Key design principles

- **No data leakage**: only information available at the evaluation date is used.
- **Cart-level output**: one forecasted cost per `shopping_cart_id`.
- **Mature carts pass through**: carts whose costs have already matured use their actual costs; only immature carts get a forecasted amount.
- **Configurable**: all parameters (maturity windows, factor columns, thresholds) are centralised in `ForecastingConfig`.

---

## 2. Input Data

### 2.1 Snapshot table

**Table**: `testing.analytics.payment_costs_data` (configurable via `config.snapshot_table`)

One row per `shopping_cart_id`. This is the **only input table** used in production. It must be a point-in-time snapshot reflecting costs as they were observed on the snapshot date.

**Required columns**:

| Column | Type | Description |
|--------|------|-------------|
| `shopping_cart_id` | string | Unique cart identifier |
| `date_of_checkout` | date | When the customer checked out |
| `gmv` | double | Gross Merchandise Value in EUR |
| `total_payment_costs` | double | Total payment costs in EUR (negative values = costs) |
| `is_rnpl` | boolean | Whether this is a Reserve Now Pay Later booking |
| `last_date_of_travel` | date | Last travel date (nullable; RNPL active anchor) |
| `last_date_of_cancelation` | date | Cancellation date (nullable; RNPL cancelled anchor) |
| `payment_method` | string | e.g. `payment_card`, `apple_pay`, `paypal`, `klarna`, `adyen_ideal` |
| `payment_method_variant` | string | Card network for card methods: `VISA`, `MASTERCARD`, `MC`, `AMEX`, `JCB`, `DISCOVER` |
| `payment_processor` | string | PSP: `ADYEN`, `CHECKOUT`, `JPMC`, `PAYPAL`, `KLARNA`, `DLOCAL` |
| `currency` | string | 3-letter ISO currency code |
| `country_code` | string | 2-letter ISO country code |
| `country_group` | string | Regional grouping (e.g. `DACH`, `Nordics`, `APAC`, `ROW`) |

---

## 3. Module Architecture

```
scripts/payment_costs_forecasting/
├── __init__.py          # Public API exports
├── config.py            # ForecastingConfig dataclass — all parameters
├── bucketing.py         # Dimension normalisation & derived columns
├── factors.py           # Historical cost factor computation
├── forecast.py          # Immature cart detection, factor application, output
└── validation.py        # Quality evaluation & backtesting framework
```

### Dependency flow

```
config.py
    ↓
bucketing.py  ←  config
    ↓
factors.py    ←  bucketing, config
    ↓
forecast.py   ←  bucketing, config, factors (lazy import)
    ↓
validation.py ←  all of the above
```

---

## 4. Pipeline Steps (in execution order)

### Step 1 — Bucketing (`bucketing.apply_bucketing`)

Normalises raw dimension values and creates derived columns used for factor grouping. This runs before every other step.

**Transformations applied**:

1. **Processor normalisation**: Case variants (`Adyen` → `ADYEN`, `checkout` → `CHECKOUT`) are standardised. Values of `_UNKNOWN`, `Unknown`, `unknown`, or empty string are converted to `NULL` — these carts will fall back to the average across processors.

2. **Variant normalisation**: `MC` → `MASTERCARD`.

3. **`payment_method_detail`** (derived):
   - For card methods (`payment_card`, `apple_pay`, `google_pay`): uses `payment_method_variant` (i.e. the card network — VISA, MASTERCARD, etc.)
   - For all other methods: uses `payment_method` directly (e.g. `paypal`, `klarna`, `adyen_ideal`, `adyen_mobilepay`)

4. **`rnpl_segment`** (derived):
   - `is_rnpl = false` → `non_rnpl`
   - `is_rnpl = true` → `rnpl`

5. **`country_bucket`** (derived):
   - Top 20 countries by cart volume keep their `country_code` (e.g. `US`, `GB`, `DE`, `FR`)
   - All remaining countries fall back to their `country_group` (e.g. `APAC`, `ROW`, `Nordics`)

6. **NULL handling**: NULLs in `payment_method_detail`, `currency`, and `country_bucket` are filled with the sentinel value `_UNKNOWN`. This ensures they don't accidentally match in joins but instead fall through the fallback hierarchy. `payment_processor` NULLs are left as-is (NULLs naturally don't match in Spark joins).

### Step 2 — Factor Computation (`factors.compute_factors`)

Computes the historical cost-to-GMV ratio for each combination of factor columns, using only carts whose costs have fully matured.

**Factor formula**:

```
cost_factor = SUM(total_payment_costs) / SUM(gmv)
```

This is computed per unique combination of the 5 factor columns.

**Maturity logic** — three independent segments with different anchors:

| Segment | Filter | Anchor column | Maturity (days) |
|---------|--------|---------------|-----------------|
| Non-RNPL | `rnpl_segment = 'non_rnpl'` | `date_of_checkout` | 14 |
| RNPL active | `rnpl_segment = 'rnpl'` AND `last_date_of_cancelation IS NULL` | `last_date_of_travel` | 7 |
| RNPL cancelled | `rnpl_segment = 'rnpl'` AND `last_date_of_cancelation IS NOT NULL` | `last_date_of_cancelation` | 7 |

**Mature window** (for a given `as_of_date`):

```
window_start = as_of_date − maturity_days − window_days
maturity_cutoff = as_of_date − maturity_days

Eligible carts: anchor_column BETWEEN window_start AND maturity_cutoff
```

With the defaults (`maturity_days=14`, `window_days=14` for non-RNPL), on `as_of_date = 2026-02-16`, the mature window is carts with checkout between **2026-01-19** and **2026-02-02** (14 days of data, at least 14 days old so costs have settled).

**RNPL blending**: RNPL active and RNPL cancelled mature carts are **unioned before grouping**. This means the resulting factor naturally reflects the blended cost rate of active and cancelled RNPL bookings in proportion to their actual mix.

**Quality filters on computed factors**:

- `segment_volume >= 100` (min_volume_for_factor) — segments with fewer carts are dropped as unreliable
- `cost_factor < 0` — positive factors indicate anomalous reversals and are excluded
- No `_UNKNOWN` or `NULL` values in any factor column — these would create spurious factor rows

**Output**: one row per unique factor combination, with columns:
`window_end_date`, `rnpl_segment`, `payment_method_detail`, `currency`, `payment_processor`, `country_bucket`, `cost_factor`, `segment_volume`, `total_cost`, `total_gmv`

### Step 3 — Immature Cart Identification (`forecast.get_immature_carts`)

Identifies carts whose costs have **not yet matured** as of the evaluation date. These are the carts that need a forecast.

**Logic per segment**:

| Segment | Immature condition |
|---------|-------------------|
| Non-RNPL | `date_of_checkout > as_of_date − 14 days` AND `date_of_checkout <= as_of_date` |
| RNPL active (not cancelled) | `last_date_of_travel IS NULL` OR `last_date_of_travel > as_of_date − 7 days`; checkout `<= as_of_date` |
| RNPL cancelled (at as_of_date) | `last_date_of_cancelation <= as_of_date` AND `last_date_of_cancelation > as_of_date − 7 days` |

RNPL carts where `last_date_of_cancelation > as_of_date` are treated as active (the cancellation hasn't happened yet from the model's perspective).

### Step 4 — Factor Application with Hierarchical Fallback (`forecast.apply_factors_with_fallback`)

Each immature cart is assigned a cost factor by joining to the factor table. If no exact match exists, the model falls back through progressively broader groupings.

**Factor columns** (ordered by importance — last column is dropped first):

```
1. rnpl_segment            (most important — never dropped except at global level)
2. payment_method_detail
3. currency
4. payment_processor
5. country_bucket           (least important — dropped first)
```

**Fallback hierarchy**:

| Level | Join columns | Label |
|-------|-------------|-------|
| 5 (exact) | All 5 columns | `exact_match` |
| 4 | rnpl_segment, payment_method_detail, currency, payment_processor | `fallback_4` |
| 3 | rnpl_segment, payment_method_detail, currency | `fallback_3` |
| 2 | rnpl_segment, payment_method_detail | `fallback_2` |
| 1 | rnpl_segment | `fallback_1` |
| 0 | (none) | `global_fallback` |

**Fallback factor aggregation**: At each fallback level, the factor is computed as a **GMV-weighted average** of all matching exact-match factors:

```
fallback_factor = SUM(total_cost) / SUM(total_gmv)
```

This ensures that high-volume segments (which are more representative) contribute proportionally more to the fallback factor than small niche segments.

**Factor application**:

```
forecasted_cost = gmv × cost_factor
```

The `source` column records which fallback level was used, enabling quality analysis.

**NULL / _UNKNOWN handling in fallback**:

- A cart with `_UNKNOWN` in `currency` will not match at any level that includes `currency` (because factors exclude `_UNKNOWN` rows). It falls through to the first level without `currency` (level 2).
- A cart with `NULL` in `payment_processor` behaves the same way — NULLs don't match in Spark equi-joins, so it skips levels 5 and 4 and matches at level 3.

### Step 5 — Final Output Assembly (`forecast.forecast`)

The complete output combines:
1. **Immature carts**: forecasted amount from Step 4, `source = 'exact_match'` or `'fallback_N'`
2. **Mature non-RNPL carts**: actual `total_payment_costs`, `source = 'actual'`
3. **Mature RNPL active carts**: actual costs, `source = 'actual'`
4. **Mature RNPL cancelled carts**: actual costs, `source = 'actual'`

**Output columns**:

| Column | Description |
|--------|-------------|
| `shopping_cart_id` | Cart identifier |
| `date_of_checkout` | Checkout date |
| `amount` | Forecasted cost (immature) or actual cost (mature) |
| `source` | `actual`, `exact_match`, `fallback_4`, ..., `global_fallback` |
| `gmv` | Gross Merchandise Value |
| `rnpl_segment` | `non_rnpl` / `rnpl` |
| `payment_method_detail` | Derived payment method |
| `currency` | Currency code |
| `payment_processor` | Processor name (nullable) |
| `country_bucket` | Country or country group |
| `country_code` | Original country code (passthrough) |
| `country_group` | Regional grouping (passthrough) |

---

## 5. Configuration Parameters

All parameters are defined in `ForecastingConfig` and can be overridden via Databricks widgets at runtime.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `factor_columns` | `[rnpl_segment, payment_method_detail, currency, payment_processor, country_bucket]` | Columns for factor grouping, ordered by importance |
| `non_rnpl_maturity_days` | `14` | Days after checkout for non-RNPL costs to mature |
| `rnpl_maturity_days` | `7` | Days after travel for active RNPL costs to mature |
| `rnpl_cancelled_maturity_days` | `7` | Days after cancellation for cancelled RNPL costs to mature |
| `window_days` | `14` | Rolling window of mature data for factor computation |
| `min_volume_for_factor` | `100` | Minimum carts in a segment to produce a factor |
| `country_top_n` | `20` | Individual countries kept in country_bucket |
| `deviation_threshold` | `0.03` (3%) | Max acceptable forecast deviation per dimension |
| `min_carts_for_validation` | `100` | Segments below this are excluded from pass/fail checks |

---

## 6. Validation Framework

### 6.1 Quality evaluation (`validation.evaluate_quality`)

Compares forecasted costs to actual costs (after both have matured) across multiple dimensions:

- `rnpl_segment`
- `country_group`, `country_bucket`
- `payment_processor`, `payment_method_detail`
- `currency`
- `weekly` (virtual — derived from `date_trunc('week', date_of_checkout)`)

For each dimension value:

```
deviation_pct = |forecast_total − actual_total| / |actual_total|
pass_flag = deviation_pct <= 3%  (for segments with >= 100 carts)
```

### 6.2 Backtest (`validation.backtest`)

Runs the full pipeline for a list of historical evaluation dates and collects quality metrics across all dates. Used in the analysis notebooks to validate model accuracy over a 6-month period.

**Important**: actuals used in backtesting are filtered via `_filter_mature_actuals` to ensure only carts whose costs have truly matured by the snapshot date are included. This prevents comparing forecasts against incomplete cost data.

---

## 7. RNPL (Reserve Now Pay Later) — Detailed Logic

RNPL bookings have fundamentally different cost timing compared to regular bookings:

- **Regular (non-RNPL)**: Payment is captured at checkout. Costs start settling immediately. Mature after **14 days post-checkout**.
- **RNPL active**: Payment is captured closer to the travel date. Costs mature after **7 days post-travel**.
- **RNPL cancelled**: Refund is processed at cancellation. Costs mature after **7 days post-cancellation**.

The model uses a **single blended `rnpl` segment** for factor computation (active + cancelled unioned together), but uses **different anchor dates** for determining maturity:

```
┌─────────────────────────────────────────────────────────────────┐
│                     RNPL Cart Lifecycle                         │
│                                                                 │
│  Checkout ──── ... ──── Travel ──── +7d ──── MATURE             │
│     │                                                           │
│     └── Cancellation ──── +7d ──── MATURE                       │
│                                                                 │
│  Factor = blended rate from both active + cancelled mature carts │
└─────────────────────────────────────────────────────────────────┘
```

---

## 8. Worked Example

**Scenario**: `as_of_date = 2026-02-16`, non-RNPL cart checked out on 2026-02-10.

1. **Is it immature?** Checkout (Feb 10) > cutoff (Feb 16 − 14 = Feb 2). Yes → immature.

2. **Factor computation window**: carts with checkout between `2026-01-19` and `2026-02-02` (these are 14–28 days old, costs fully settled).

3. **Factor lookup**: The cart has attributes `(non_rnpl, VISA, EUR, CHECKOUT, DE)`. The model looks for a factor with these exact 5 values.
   - If found (segment_volume >= 100): use `cost_factor = -0.0115` (1.15% of GMV). Label: `exact_match`.
   - If not found: drop `country_bucket` → look for `(non_rnpl, VISA, EUR, CHECKOUT)`. This factor is the GMV-weighted average across all countries for that combo. Label: `fallback_4`.
   - Continue dropping columns until a match is found.

4. **Forecast**: `amount = gmv × cost_factor`. If GMV = €500 and factor = -0.0115, then `amount = -€5.75`.

---

## 9. Production Considerations

### 9.1 Execution frequency

The model should run **daily**. Each run:
1. Loads the latest snapshot table
2. Computes factors from mature carts (as of today)
3. Identifies immature carts (as of today)
4. Applies factors and produces the output table

### 9.2 Output table

The production output should be written to a Delta table (currently `testing.analytics.forecasted_costs`). Each run **overwrites** the table with the complete set of carts (both mature actuals and immature forecasts).

### 9.3 Idempotency

The pipeline is fully deterministic for a given snapshot table and `as_of_date`. Re-running with the same inputs produces identical output.

### 9.4 Performance

- The snapshot table is cached after bucketing (`bucketed.cache()`)
- Factor computation involves a groupBy/agg on the mature window (~2 weeks of data)
- Fallback application involves 6 sequential left joins (one per level), which could be optimised if needed by broadcasting the small factor table
- Typical runtime: ~5–10 minutes on a standard Databricks cluster for ~2M carts

### 9.5 Monitoring

After each production run, `evaluate_quality` should be executed to check forecast deviation. The quality metrics should be persisted to `testing.analytics.quality_metrics` and monitored for:

- Overall deviation exceeding 3%
- Any major dimension (with 100+ carts) exceeding 3% deviation
- Sudden changes in the share of carts using fallback levels (indicating data quality issues)

### 9.6 Dependencies

- **PySpark** (Databricks runtime)
- **Python 3.10+** (for `list[str]` type hints)
- No external Python packages beyond PySpark

### 9.7 Known limitations

1. **Estimated acquirer fees**: Some PSP fees (Adyen invoice deductions, CKO tiered pricing adjustments, JPMC monthly card network fees) are invoiced monthly and distributed across transactions retroactively. These arrive irregularly and can't be accurately predicted by the maturity-based model. A separate estimated cost forecasting layer is under development.

2. **Rapidly changing cost rates**: Payment methods whose cost rate is shifting quickly (e.g. iDEAL, MobilePay) may show temporary bias because the 14-day factor window uses slightly stale historical data.

3. **Small / new segments**: Payment methods or countries with fewer than 100 carts in the factor window fall back to broader groupings, which may not accurately represent their specific cost profile.

---

## 10. File Reference

| File | Purpose | Key functions |
|------|---------|---------------|
| `config.py` | All configurable parameters | `ForecastingConfig`, `from_widgets()` |
| `bucketing.py` | Normalisation and derived columns | `apply_bucketing()`, `compute_top_n()` |
| `factors.py` | Cost factor computation from mature data | `compute_factors()`, `_filter_mature()`, `_group_into_factors()` |
| `forecast.py` | Immature cart detection and factor application | `forecast()`, `get_immature_carts()`, `apply_factors_with_fallback()` |
| `validation.py` | Quality checks and backtesting | `evaluate_quality()`, `backtest()`, `_filter_mature_actuals()` |
