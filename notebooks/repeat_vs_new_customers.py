# Databricks notebook source
# MAGIC %md
# MAGIC # Repeat vs New Customer Analysis
# MAGIC
# MAGIC Evaluates for the last 7 days what percentage of customers are **repeat**
# MAGIC (booked at least once in the prior 365 days) vs **new** (no booking in the prior 365 days).
# MAGIC
# MAGIC **Source**: `production.dwh.fact_booking`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Schema Inspection
# MAGIC
# MAGIC Uncomment and run to inspect `fact_booking` columns if needed.

# COMMAND ----------

# display(spark.sql("DESCRIBE production.dwh.fact_booking"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Repeat vs New — Last 7 Days

# COMMAND ----------

result = spark.sql("""
WITH params AS (
    SELECT
        current_date()                    AS today,
        date_sub(current_date(), 7)       AS period_start,
        date_sub(current_date(), 1)       AS period_end,
        date_sub(current_date(), 365 + 7) AS history_start
),

recent_bookings AS (
    SELECT DISTINCT
        b.customer_id
    FROM production.dwh.fact_booking b
    CROSS JOIN params p
    WHERE b.date BETWEEN p.period_start AND p.period_end
      AND b.status = 'CONFIRMED'
),

prior_bookings AS (
    SELECT DISTINCT
        b.customer_id
    FROM production.dwh.fact_booking b
    CROSS JOIN params p
    WHERE b.date BETWEEN p.history_start AND date_sub(p.period_start, 1)
      AND b.status = 'CONFIRMED'
),

classified AS (
    SELECT
        r.customer_id,
        CASE
            WHEN p.customer_id IS NOT NULL THEN 'repeat'
            ELSE 'new'
        END AS customer_type
    FROM recent_bookings r
    LEFT JOIN prior_bookings p ON r.customer_id = p.customer_id
)

SELECT
    customer_type,
    count(*)                                          AS customers,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct
FROM classified
GROUP BY customer_type
ORDER BY customer_type
""")

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Daily Breakdown

# COMMAND ----------

daily = spark.sql("""
WITH params AS (
    SELECT
        date_sub(current_date(), 7)       AS period_start,
        date_sub(current_date(), 1)       AS period_end,
        date_sub(current_date(), 365 + 7) AS history_start
),

daily_customers AS (
    SELECT
        b.date          AS booking_date,
        b.customer_id
    FROM production.dwh.fact_booking b
    CROSS JOIN params p
    WHERE b.date BETWEEN p.period_start AND p.period_end
      AND b.status = 'CONFIRMED'
    GROUP BY b.date, b.customer_id
),

prior_bookings AS (
    SELECT DISTINCT
        b.customer_id
    FROM production.dwh.fact_booking b
    CROSS JOIN params p
    WHERE b.date BETWEEN p.history_start AND date_sub(p.period_start, 1)
      AND b.status = 'CONFIRMED'
),

classified AS (
    SELECT
        d.booking_date,
        d.customer_id,
        CASE
            WHEN p.customer_id IS NOT NULL THEN 'repeat'
            ELSE 'new'
        END AS customer_type
    FROM daily_customers d
    LEFT JOIN prior_bookings p ON d.customer_id = p.customer_id
)

SELECT
    booking_date,
    customer_type,
    count(*)                                                                AS customers,
    round(count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY booking_date), 2) AS pct
FROM classified
GROUP BY booking_date, customer_type
ORDER BY booking_date, customer_type
""")

daily.display()
