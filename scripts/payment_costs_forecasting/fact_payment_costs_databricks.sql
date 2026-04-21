-- ============================================================
-- PAYMENT COSTS FORECASTING — Databricks SQL
-- ============================================================
-- Replaces the old 2-dimension (country_group × is_rnpl) model
-- with a 5-factor hierarchical fallback model.
--
-- Factor columns (ordered by importance, last dropped first):
--   1. rnpl_segment           (non_rnpl / rnpl)
--   2. payment_method_detail  (card network or payment method)
--   3. currency               (ISO 4217)
--   4. payment_processor      (PSP — nullable, skips levels if NULL)
--   5. country_bucket         (top-N country codes, rest → country_group)
--
-- Output: one row per booking_id, matching the downstream contract
--   (booking_id, shopping_cart_id, billing_id, date_of_checkout, amount, source)
--
-- Run in Databricks as:
--   CREATE OR REPLACE TABLE <catalog>.<schema>.fact_payment_costs AS ( ... )
-- ============================================================


-- ─── Tunable parameters ───
-- Adjust these values directly before running.
-- maturity_non_rnpl_days  = 14
-- maturity_rnpl_days      = 7
-- maturity_rnpl_cancel_days = 7
-- window_days             = 14
-- min_volume_for_factor   = 100
-- country_top_n           = 20


WITH

-- ============================================================
-- STEP 1: CART-LEVEL SNAPSHOT
-- One row per shopping_cart_id with aggregated costs, GMV,
-- and all payment attributes needed for factor grouping.
-- ============================================================
cart_snapshot AS (
    SELECT
        CAST(s.shopping_cart_id AS BIGINT)   AS shopping_cart_id,
        s.billing_id,
        COALESCE(s.is_rnpl, FALSE)           AS is_rnpl,
        MIN(b.date_of_checkout)              AS date_of_checkout,
        MAX(b.date_of_travel)                AS last_date_of_travel,
        MAX(CASE WHEN b.status_id = 2
                 THEN b.date_of_cancelation
            END)                             AS last_date_of_cancelation,
        SUM(COALESCE(pc.gmv, b.gmv))         AS total_gmv,
        -- costs are stored as positive fees; negate to follow "costs = negative" convention
        SUM(COALESCE(-pc.total_fee_amount_eur_splitted_by_gmv, 0))
                                             AS total_payment_costs,
        -- TODO: verify these columns exist on fact_shopping_cart (or join the
        -- relevant payments table that carries the successful payment attempt attributes)
        MAX(s.payment_method)                AS payment_method,
        MAX(s.payment_method_variant)        AS payment_method_variant,
        MAX(s.payment_processor)             AS payment_processor,
        MAX(s.currency)                      AS currency,
        MAX(c.country_code)                  AS country_code,
        MAX(c.country_group)                 AS country_group

    FROM <catalog>.<schema>.fact_shopping_cart s
    INNER JOIN <catalog>.<schema>.fact_booking b
        ON b.shopping_cart_id = s.shopping_cart_id
    LEFT JOIN <catalog>.<schema>.agg_booking_payment_costs pc
        ON pc.booking_id = b.booking_id
    LEFT JOIN <catalog>.<schema>.dim_country c
        ON b.customer_country_id = c.country_id
    WHERE
        b.status_id IN (1, 2)
        AND b.date_of_checkout >= '2024-01-01'
    GROUP BY 1, 2, 3
),


-- ============================================================
-- STEP 2: BUCKETING
-- Normalise dimensions & derive the 5 factor columns.
-- ============================================================

-- 2a. Top 20 countries by cart volume (all carts, not just mature)
top_countries AS (
    SELECT country_code
    FROM cart_snapshot
    WHERE country_code IS NOT NULL
    GROUP BY country_code
    ORDER BY COUNT(*) DESC
    LIMIT 20
),

-- 2b. Bucketed snapshot
bucketed_carts AS (
    SELECT
        cs.shopping_cart_id,
        cs.billing_id,
        cs.date_of_checkout,
        cs.last_date_of_travel,
        cs.last_date_of_cancelation,
        cs.total_gmv,
        cs.total_payment_costs,
        cs.is_rnpl,
        cs.country_code,
        cs.country_group,

        -- ── Factor column 1: rnpl_segment ──
        CASE WHEN cs.is_rnpl THEN 'rnpl' ELSE 'non_rnpl' END
            AS rnpl_segment,

        -- ── Factor column 2: payment_method_detail ──
        -- For card-based methods use the card network; for everything else use the method name.
        COALESCE(
            CASE
                WHEN cs.payment_method IN ('payment_card', 'apple_pay', 'google_pay')
                     AND cs.payment_method_variant IS NOT NULL
                THEN CASE
                         WHEN UPPER(cs.payment_method_variant) = 'MC' THEN 'MASTERCARD'
                         ELSE UPPER(cs.payment_method_variant)
                     END
                ELSE cs.payment_method
            END,
            '_UNKNOWN'
        ) AS payment_method_detail,

        -- ── Factor column 3: currency ──
        COALESCE(cs.currency, '_UNKNOWN') AS currency,

        -- ── Factor column 4: payment_processor ──
        -- _UNKNOWN / Unknown / empty → NULL so carts skip processor-dependent
        -- factor levels and fall back to the GMV-weighted average across processors.
        CASE
            WHEN UPPER(COALESCE(cs.payment_processor, ''))
                 IN ('', '_UNKNOWN', 'UNKNOWN')
            THEN NULL
            ELSE UPPER(cs.payment_processor)
        END AS payment_processor,

        -- ── Factor column 5: country_bucket ──
        COALESCE(
            CASE
                WHEN tc.country_code IS NOT NULL THEN cs.country_code
                ELSE cs.country_group
            END,
            '_UNKNOWN'
        ) AS country_bucket

    FROM cart_snapshot cs
    LEFT JOIN top_countries tc
        ON cs.country_code = tc.country_code
),


-- ============================================================
-- STEP 3: MATURE CARTS
-- Three segments, each with its own anchor column and maturity
-- window. Unioned together for joint factor computation.
-- ============================================================
mature_carts AS (
    -- Non-RNPL: anchor = date_of_checkout, mature after 14 days
    SELECT * FROM bucketed_carts
    WHERE rnpl_segment   = 'non_rnpl'
      AND date_of_checkout BETWEEN
              DATE_SUB(CURRENT_DATE(), 28)   -- 14 maturity + 14 window
              AND DATE_SUB(CURRENT_DATE(), 14)
      AND total_payment_costs != 0
      AND total_gmv > 0

    UNION ALL

    -- RNPL active (not cancelled): anchor = last_date_of_travel, mature after 7 days
    SELECT * FROM bucketed_carts
    WHERE rnpl_segment            = 'rnpl'
      AND last_date_of_cancelation IS NULL
      AND last_date_of_travel IS NOT NULL
      AND last_date_of_travel BETWEEN
              DATE_SUB(CURRENT_DATE(), 21)   -- 7 maturity + 14 window
              AND DATE_SUB(CURRENT_DATE(), 7)
      AND total_payment_costs != 0
      AND total_gmv > 0

    UNION ALL

    -- RNPL cancelled: anchor = last_date_of_cancelation, mature after 7 days
    SELECT * FROM bucketed_carts
    WHERE rnpl_segment            = 'rnpl'
      AND last_date_of_cancelation IS NOT NULL
      AND last_date_of_cancelation BETWEEN
              DATE_SUB(CURRENT_DATE(), 21)   -- 7 maturity + 14 window
              AND DATE_SUB(CURRENT_DATE(), 7)
      AND total_payment_costs != 0
      AND total_gmv > 0
),


-- ============================================================
-- STEP 4: FACTOR COMPUTATION
-- Level 5 (exact match) → aggregated fallback levels 4..0.
-- All levels use GMV-weighted average  =  SUM(cost) / SUM(gmv)
-- so high-volume segments contribute proportionally.
-- ============================================================

-- Level 5 — exact match on all 5 factor columns
factors_exact AS (
    SELECT
        rnpl_segment,
        payment_method_detail,
        currency,
        payment_processor,
        country_bucket,
        SUM(total_payment_costs)                           AS total_cost,
        SUM(total_gmv)                                     AS total_gmv,
        COUNT(*)                                           AS segment_volume,
        SUM(total_payment_costs) / SUM(total_gmv)          AS cost_factor
    FROM mature_carts
    GROUP BY 1, 2, 3, 4, 5
    HAVING
        COUNT(*) >= 100
        AND SUM(total_payment_costs) / SUM(total_gmv) < 0
        AND payment_method_detail != '_UNKNOWN'
        AND currency              != '_UNKNOWN'
        AND payment_processor     IS NOT NULL
        AND country_bucket        != '_UNKNOWN'
),

-- Level 4 — drop country_bucket
factors_level_4 AS (
    SELECT
        rnpl_segment, payment_method_detail, currency, payment_processor,
        SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
    GROUP BY 1, 2, 3, 4
),

-- Level 3 — drop payment_processor
factors_level_3 AS (
    SELECT
        rnpl_segment, payment_method_detail, currency,
        SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
    GROUP BY 1, 2, 3
),

-- Level 2 — drop currency
factors_level_2 AS (
    SELECT
        rnpl_segment, payment_method_detail,
        SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
    GROUP BY 1, 2
),

-- Level 1 — rnpl_segment only
factors_level_1 AS (
    SELECT
        rnpl_segment,
        SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
    GROUP BY 1
),

-- Level 0 — global fallback (single scalar)
factors_global AS (
    SELECT SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
),


-- ============================================================
-- STEP 5: IMMATURE CARTS
-- Carts whose costs have NOT yet fully settled.
-- UNION (not UNION ALL) guarantees deduplication.
-- ============================================================
immature_cart_ids AS (
    -- Non-RNPL: checkout within last 14 days
    SELECT shopping_cart_id FROM bucketed_carts
    WHERE rnpl_segment   = 'non_rnpl'
      AND date_of_checkout  > DATE_SUB(CURRENT_DATE(), 14)
      AND date_of_checkout <= CURRENT_DATE()

    UNION

    -- RNPL active: travel hasn't happened or within last 7 days
    SELECT shopping_cart_id FROM bucketed_carts
    WHERE rnpl_segment = 'rnpl'
      AND (   last_date_of_cancelation IS NULL
           OR CAST(last_date_of_cancelation AS DATE) > CURRENT_DATE())
      AND (   last_date_of_travel IS NULL
           OR last_date_of_travel > DATE_SUB(CURRENT_DATE(), 7))
      AND date_of_checkout <= CURRENT_DATE()

    UNION

    -- RNPL cancelled: cancellation within last 7 days
    SELECT shopping_cart_id FROM bucketed_carts
    WHERE rnpl_segment = 'rnpl'
      AND last_date_of_cancelation IS NOT NULL
      AND CAST(last_date_of_cancelation AS DATE) <= CURRENT_DATE()
      AND last_date_of_cancelation > DATE_SUB(CURRENT_DATE(), 7)
),


-- ============================================================
-- STEP 6: HIERARCHICAL FALLBACK
-- Each immature cart is LEFT-JOINed to every factor level.
-- COALESCE picks the most specific (= highest level) match.
-- ============================================================
immature_with_factors AS (
    SELECT
        bc.shopping_cart_id,
        COALESCE(
            f5.cost_factor,
            f4.cost_factor,
            f3.cost_factor,
            f2.cost_factor,
            f1.cost_factor,
            fg.cost_factor,
            0
        ) AS cost_factor,
        CASE
            WHEN f5.cost_factor IS NOT NULL THEN 'exact_match'
            WHEN f4.cost_factor IS NOT NULL THEN 'fallback_4'
            WHEN f3.cost_factor IS NOT NULL THEN 'fallback_3'
            WHEN f2.cost_factor IS NOT NULL THEN 'fallback_2'
            WHEN f1.cost_factor IS NOT NULL THEN 'fallback_1'
            WHEN fg.cost_factor IS NOT NULL THEN 'global_fallback'
            ELSE 'no_factor'
        END AS fallback_level

    FROM bucketed_carts bc
    INNER JOIN immature_cart_ids ic
        ON bc.shopping_cart_id = ic.shopping_cart_id

    LEFT JOIN factors_exact f5
        ON  bc.rnpl_segment          = f5.rnpl_segment
        AND bc.payment_method_detail = f5.payment_method_detail
        AND bc.currency              = f5.currency
        AND bc.payment_processor     = f5.payment_processor
        AND bc.country_bucket        = f5.country_bucket

    LEFT JOIN factors_level_4 f4
        ON  bc.rnpl_segment          = f4.rnpl_segment
        AND bc.payment_method_detail = f4.payment_method_detail
        AND bc.currency              = f4.currency
        AND bc.payment_processor     = f4.payment_processor

    LEFT JOIN factors_level_3 f3
        ON  bc.rnpl_segment          = f3.rnpl_segment
        AND bc.payment_method_detail = f3.payment_method_detail
        AND bc.currency              = f3.currency

    LEFT JOIN factors_level_2 f2
        ON  bc.rnpl_segment          = f2.rnpl_segment
        AND bc.payment_method_detail = f2.payment_method_detail

    LEFT JOIN factors_level_1 f1
        ON  bc.rnpl_segment = f1.rnpl_segment

    LEFT JOIN factors_global fg
        ON 1 = 1
),


-- ============================================================
-- STEP 7: BOOKING-LEVEL OUTPUT
-- ============================================================

-- 7a. Actuals — bookings in MATURE carts (pass-through)
actual_payment_costs AS (
    SELECT
        pc.booking_id,
        CAST(pc.shopping_cart_id AS BIGINT) AS shopping_cart_id,
        s.billing_id,
        b.date_of_checkout,
        -pc.total_fee_amount_eur_splitted_by_gmv AS amount,
        'Actuals' AS source
    FROM <catalog>.<schema>.agg_booking_payment_costs pc
    INNER JOIN <catalog>.<schema>.fact_booking b
        ON b.booking_id = pc.booking_id
    INNER JOIN <catalog>.<schema>.fact_shopping_cart s
        ON s.shopping_cart_id = pc.shopping_cart_id
    WHERE
        b.status_id IN (1, 2)
        AND b.date_of_checkout >= '2024-01-01'
        AND NOT EXISTS (
            SELECT 1 FROM immature_cart_ids ic
            WHERE ic.shopping_cart_id = CAST(pc.shopping_cart_id AS BIGINT)
        )
),

-- 7b. Forecasts — bookings in IMMATURE carts
-- Each booking gets:  amount = booking_gmv × cart_factor
forecasted_payment_costs AS (
    SELECT
        b.booking_id,
        CAST(b.shopping_cart_id AS BIGINT) AS shopping_cart_id,
        s.billing_id,
        b.date_of_checkout,
        ROUND(COALESCE(b.gmv * iwf.cost_factor, 0), 2) AS amount,
        'Forecasted' AS source
    FROM immature_with_factors iwf
    INNER JOIN <catalog>.<schema>.fact_booking b
        ON b.shopping_cart_id = iwf.shopping_cart_id
    INNER JOIN <catalog>.<schema>.fact_shopping_cart s
        ON s.shopping_cart_id = iwf.shopping_cart_id
    WHERE b.status_id IN (1, 2)
),

-- 7c. Historical — pre-2024 costs (unchanged from old model)
historical_payment_costs AS (
    SELECT
        hpc.booking_id,
        CAST(hpc.shopping_cart_id AS BIGINT) AS shopping_cart_id,
        hpc.billing_id,
        hpc.date_of_checkout,
        hpc.amount,
        'Actuals' AS source
    FROM <catalog>.<schema>.fact_payment_cost_historical hpc
    WHERE
        hpc.date_of_checkout < '2024-01-01'
        AND NOT EXISTS (
            SELECT 1 FROM actual_payment_costs apc
            WHERE apc.booking_id = hpc.booking_id
        )
)


-- ============================================================
-- FINAL OUTPUT
-- ============================================================
SELECT booking_id, shopping_cart_id, billing_id, date_of_checkout, amount, source
FROM actual_payment_costs

UNION ALL

SELECT booking_id, shopping_cart_id, billing_id, date_of_checkout, amount, source
FROM forecasted_payment_costs

UNION ALL

SELECT booking_id, shopping_cart_id, billing_id, date_of_checkout, amount, source
FROM historical_payment_costs
