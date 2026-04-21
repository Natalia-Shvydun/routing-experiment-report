-- ============================================================
-- PAYMENT COSTS FORECASTING
-- ============================================================
-- 5-factor hierarchical fallback model.
-- Reads from testing.analytics.payment_costs_data (one row per shopping_cart_id).
--
-- Factor columns (ordered by importance, last dropped first):
--   1. rnpl_segment           (non_rnpl / rnpl)
--   2. payment_method_detail  (card network or payment method)
--   3. currency               (ISO 4217)
--   4. payment_processor      (PSP — NULL skips processor-dependent levels)
--   5. country_bucket         (top-N country codes, rest → country_group)
--
-- Output: one row per shopping_cart_id
--   (shopping_cart_id, date_of_checkout, gmv, amount, source, fallback_level,
--    rnpl_segment, payment_method_detail, currency, payment_processor, country_bucket)
-- ============================================================


-- ─── Tunable parameters (adjust inline) ───
-- non_rnpl_maturity_days     = 14
-- rnpl_maturity_days         = 7
-- rnpl_cancel_maturity_days  = 7
-- window_days                = 14
-- min_volume_for_factor      = 100
-- country_top_n              = 20


WITH

-- ============================================================
-- STEP 1: RAW SNAPSHOT
-- One row per shopping_cart_id from the pre-built snapshot table.
-- ============================================================
raw_snapshot AS (
    SELECT
        shopping_cart_id,
        date_of_checkout,
        gmv,
        total_payment_costs,
        COALESCE(is_rnpl, FALSE) AS is_rnpl,
        last_date_of_travel,
        last_date_of_cancelation,
        payment_method,
        payment_method_variant,
        payment_processor,
        currency,
        country_code,
        country_group
    FROM testing.analytics.payment_costs_data
),


-- ============================================================
-- STEP 2: BUCKETING
-- Normalise dimensions & derive the 5 factor columns.
-- ============================================================

-- 2a. Top N countries by cart volume
top_countries AS (
    SELECT country_code
    FROM raw_snapshot
    WHERE country_code IS NOT NULL
    GROUP BY country_code
    ORDER BY COUNT(*) DESC
    LIMIT 20
),

-- 2b. Bucketed snapshot
bucketed AS (
    SELECT
        s.*,

        -- Factor 1: rnpl_segment
        CASE WHEN s.is_rnpl THEN 'rnpl' ELSE 'non_rnpl' END
            AS rnpl_segment,

        -- Factor 2: payment_method_detail
        COALESCE(
            CASE
                WHEN s.payment_method IN ('payment_card', 'apple_pay', 'google_pay')
                     AND s.payment_method_variant IS NOT NULL
                THEN CASE
                         WHEN UPPER(s.payment_method_variant) = 'MC' THEN 'MASTERCARD'
                         ELSE UPPER(s.payment_method_variant)
                     END
                ELSE s.payment_method
            END,
            '_UNKNOWN'
        ) AS payment_method_detail,

        -- Factor 3: currency (fill NULLs)
        COALESCE(s.currency, '_UNKNOWN') AS currency_bucket,

        -- Factor 4: payment_processor (NULL = skip processor-dependent levels)
        CASE
            WHEN UPPER(COALESCE(s.payment_processor, ''))
                 IN ('', '_UNKNOWN', 'UNKNOWN')
            THEN NULL
            ELSE UPPER(s.payment_processor)
        END AS processor_bucket,

        -- Factor 5: country_bucket
        COALESCE(
            CASE
                WHEN tc.country_code IS NOT NULL THEN s.country_code
                ELSE s.country_group
            END,
            '_UNKNOWN'
        ) AS country_bucket

    FROM raw_snapshot s
    LEFT JOIN top_countries tc
        ON s.country_code = tc.country_code
),


-- ============================================================
-- STEP 3: MATURE CARTS
-- Three segments, each with its own anchor and maturity window.
-- ============================================================
mature_carts AS (
    -- Non-RNPL: anchor = date_of_checkout, maturity = 14 days, window = 14 days
    SELECT * FROM bucketed
    WHERE rnpl_segment = 'non_rnpl'
      AND date_of_checkout BETWEEN
              DATE_SUB(CURRENT_DATE(), 28)    -- maturity + window
              AND DATE_SUB(CURRENT_DATE(), 14) -- maturity
      AND total_payment_costs != 0
      AND gmv > 0

    UNION ALL

    -- RNPL active (not cancelled): anchor = last_date_of_travel
    SELECT * FROM bucketed
    WHERE rnpl_segment            = 'rnpl'
      AND last_date_of_cancelation IS NULL
      AND last_date_of_travel IS NOT NULL
      AND last_date_of_travel BETWEEN
              DATE_SUB(CURRENT_DATE(), 21)    -- 7 + 14
              AND DATE_SUB(CURRENT_DATE(), 7)
      AND total_payment_costs != 0
      AND gmv > 0

    UNION ALL

    -- RNPL cancelled: anchor = last_date_of_cancelation
    SELECT * FROM bucketed
    WHERE rnpl_segment            = 'rnpl'
      AND last_date_of_cancelation IS NOT NULL
      AND last_date_of_cancelation BETWEEN
              DATE_SUB(CURRENT_DATE(), 21)    -- 7 + 14
              AND DATE_SUB(CURRENT_DATE(), 7)
      AND total_payment_costs != 0
      AND gmv > 0
),


-- ============================================================
-- STEP 4: FACTOR COMPUTATION
-- Level 5 (exact match) → aggregated fallback levels 4..0.
-- All levels use GMV-weighted average = SUM(cost) / SUM(gmv).
-- ============================================================

-- Level 5 — exact match on all 5 factor columns
factors_exact AS (
    SELECT
        rnpl_segment,
        payment_method_detail,
        currency_bucket,
        processor_bucket,
        country_bucket,
        SUM(total_payment_costs)                           AS total_cost,
        SUM(gmv)                                           AS total_gmv,
        COUNT(*)                                           AS segment_volume,
        SUM(total_payment_costs) / SUM(gmv)                AS cost_factor
    FROM mature_carts
    GROUP BY 1, 2, 3, 4, 5
    HAVING
        COUNT(*) >= 100
        AND SUM(total_payment_costs) / SUM(gmv) < 0
        AND payment_method_detail != '_UNKNOWN'
        AND currency_bucket       != '_UNKNOWN'
        AND processor_bucket      IS NOT NULL
        AND country_bucket        != '_UNKNOWN'
),

-- Level 4 — drop country_bucket
factors_level_4 AS (
    SELECT
        rnpl_segment, payment_method_detail, currency_bucket, processor_bucket,
        SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
    GROUP BY 1, 2, 3, 4
),

-- Level 3 — drop payment_processor
factors_level_3 AS (
    SELECT
        rnpl_segment, payment_method_detail, currency_bucket,
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

-- Level 0 — global fallback
factors_global AS (
    SELECT SUM(total_cost) / SUM(total_gmv) AS cost_factor
    FROM factors_exact
),


-- ============================================================
-- STEP 5: IMMATURE CARTS
-- Carts whose costs have NOT yet fully settled.
-- ============================================================
immature_cart_ids AS (
    -- Non-RNPL: checkout too recent
    SELECT shopping_cart_id FROM bucketed
    WHERE rnpl_segment   = 'non_rnpl'
      AND date_of_checkout  > DATE_SUB(CURRENT_DATE(), 14)
      AND date_of_checkout <= CURRENT_DATE()

    UNION

    -- RNPL active: travel hasn't happened or too recent
    SELECT shopping_cart_id FROM bucketed
    WHERE rnpl_segment = 'rnpl'
      AND (   last_date_of_cancelation IS NULL
           OR CAST(last_date_of_cancelation AS DATE) > CURRENT_DATE())
      AND (   last_date_of_travel IS NULL
           OR last_date_of_travel > DATE_SUB(CURRENT_DATE(), 7))
      AND date_of_checkout <= CURRENT_DATE()

    UNION

    -- RNPL cancelled: cancellation too recent
    SELECT shopping_cart_id FROM bucketed
    WHERE rnpl_segment = 'rnpl'
      AND last_date_of_cancelation IS NOT NULL
      AND CAST(last_date_of_cancelation AS DATE) <= CURRENT_DATE()
      AND last_date_of_cancelation > DATE_SUB(CURRENT_DATE(), 7)
),


-- ============================================================
-- STEP 6: HIERARCHICAL FALLBACK
-- Each immature cart LEFT-JOINs to every factor level.
-- COALESCE picks the most specific match.
-- ============================================================
immature_with_factors AS (
    SELECT
        bc.shopping_cart_id,
        bc.date_of_checkout,
        bc.gmv,
        bc.rnpl_segment,
        bc.payment_method_detail,
        bc.currency_bucket,
        bc.processor_bucket,
        bc.country_bucket,
        bc.country_code,
        bc.country_group,
        bc.payment_method,
        bc.payment_processor,
        bc.is_rnpl,
        bc.last_date_of_cancelation,

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

    FROM bucketed bc
    INNER JOIN immature_cart_ids ic
        ON bc.shopping_cart_id = ic.shopping_cart_id

    LEFT JOIN factors_exact f5
        ON  bc.rnpl_segment          = f5.rnpl_segment
        AND bc.payment_method_detail = f5.payment_method_detail
        AND bc.currency_bucket       = f5.currency_bucket
        AND bc.processor_bucket      = f5.processor_bucket
        AND bc.country_bucket        = f5.country_bucket

    LEFT JOIN factors_level_4 f4
        ON  bc.rnpl_segment          = f4.rnpl_segment
        AND bc.payment_method_detail = f4.payment_method_detail
        AND bc.currency_bucket       = f4.currency_bucket
        AND bc.processor_bucket      = f4.processor_bucket

    LEFT JOIN factors_level_3 f3
        ON  bc.rnpl_segment          = f3.rnpl_segment
        AND bc.payment_method_detail = f3.payment_method_detail
        AND bc.currency_bucket       = f3.currency_bucket

    LEFT JOIN factors_level_2 f2
        ON  bc.rnpl_segment          = f2.rnpl_segment
        AND bc.payment_method_detail = f2.payment_method_detail

    LEFT JOIN factors_level_1 f1
        ON  bc.rnpl_segment = f1.rnpl_segment

    LEFT JOIN factors_global fg
        ON 1 = 1
),


-- ============================================================
-- STEP 7: CART-LEVEL OUTPUT
-- Actuals (mature) + Forecasts (immature)
-- ============================================================

-- 7a. Actuals — mature carts pass through their actual costs
actual_costs AS (
    SELECT
        bc.shopping_cart_id,
        bc.date_of_checkout,
        bc.gmv,
        bc.total_payment_costs                AS amount,
        'actual'                              AS source,
        CAST(NULL AS STRING)                  AS fallback_level,
        bc.rnpl_segment,
        bc.payment_method_detail,
        bc.currency_bucket,
        bc.processor_bucket,
        bc.country_bucket,
        bc.country_code,
        bc.country_group,
        bc.payment_method,
        bc.payment_processor,
        bc.is_rnpl,
        bc.last_date_of_cancelation
    FROM bucketed bc
    WHERE NOT EXISTS (
        SELECT 1 FROM immature_cart_ids ic
        WHERE ic.shopping_cart_id = bc.shopping_cart_id
    )
),

-- 7b. Forecasts — immature carts get estimated costs
forecasted_costs AS (
    SELECT
        shopping_cart_id,
        date_of_checkout,
        gmv,
        ROUND(gmv * cost_factor, 2)           AS amount,
        'forecasted'                          AS source,
        fallback_level,
        rnpl_segment,
        payment_method_detail,
        currency_bucket,
        processor_bucket,
        country_bucket,
        country_code,
        country_group,
        payment_method,
        payment_processor,
        is_rnpl,
        last_date_of_cancelation
    FROM immature_with_factors
)


-- ============================================================
-- FINAL OUTPUT
-- ============================================================
SELECT
    shopping_cart_id,
    date_of_checkout,
    gmv,
    amount,
    source,
    fallback_level,
    rnpl_segment,
    payment_method_detail,
    currency_bucket,
    processor_bucket,
    country_bucket,
    country_code,
    country_group,
    payment_method,
    payment_processor,
    is_rnpl,
    last_date_of_cancelation
FROM actual_costs

UNION ALL

SELECT
    shopping_cart_id,
    date_of_checkout,
    gmv,
    amount,
    source,
    fallback_level,
    rnpl_segment,
    payment_method_detail,
    currency_bucket,
    processor_bucket,
    country_bucket,
    country_code,
    country_group,
    payment_method,
    payment_processor,
    is_rnpl,
    last_date_of_cancelation
FROM forecasted_costs
