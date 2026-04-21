# Databricks notebook source
# MAGIC %md
# MAGIC # US Matched Pairs — Control (success) vs Test (failure)
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (restarted 2026-04-02)
# MAGIC
# MAGIC Find pairs of attempts that match on **14 parameters**
# MAGIC (processor, card scheme, currency, currency type, fraud result,
# MAGIC challenge outcome, attempt rank, payment flow, attempt type,
# MAGIC platform, payment terms, customer country, GMV bucket)
# MAGIC but diverge on the outcome: **Control succeeded, Test failed**.
# MAGIC
# MAGIC These pairs are ideal for investigation in Primer — if everything is
# MAGIC the same except the variant, the routing/fraud config difference is
# MAGIC the only explanation.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Setup & data extraction (US customer attempts)
# MAGIC 2. Build matched pairs
# MAGIC 3. Distribution of matched pairs by segment
# MAGIC 4. Sample pairs for Primer
# MAGIC 5. Exportable temp view

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Data Extraction

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START = "2026-04-02"

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Extract US customer attempts

# COMMAND ----------

df_us = spark.sql(f"""
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
SELECT
  a.group_name,
  a.visitor_id,
  p.payment_provider_reference,
  p.customer_system_attempt_reference,
  p.payment_flow,
  p.payment_method_variant,
  p.currency,
  p.payment_processor,
  p.payment_attempt_status,
  p.payment_terms,
  p.attempt_type,
  p.platform,
  p.challenge_issued,
  p.response_code,
  p.fraud_pre_auth_result,
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.customer_country_code,
  p.gmv_eur_bucket,
  p.is_customer_attempt_successful,
  p.is_successful,
  p.error_code,
  p.error_type,
  p.error_decline_type,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
  AND p.bin_issuer_country_code = 'US'
""")

df_us = df_us.withColumn(
    "currency_type",
    F.when(F.col("currency").isin(JPM_CURRENCIES), "jpm_currency")
     .otherwise("non_jpm_currency")
).withColumn(
    "card_scheme",
    F.when(F.lower(F.col("payment_method_variant")).contains("amex"), "amex")
     .when(F.lower(F.col("payment_method_variant")).isin("visa", "mastercard"), "visa_mc")
     .otherwise("other")
)

df_cust = df_us.filter(F.col("system_attempt_rank") == 1)
df_cust.cache()

total = df_cust.count()
print(f"Total US customer attempts (system_attempt_rank=1): {total:,}")

by_group = df_cust.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Build Matched Pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Match control (success) with test (failure) on identical parameters

# COMMAND ----------

ctrl_success = (
    df_cust
    .filter(F.col("group_name") == g_a)
    .filter(F.col("is_customer_attempt_successful") == 1)
    .select(
        F.col("payment_provider_reference").alias("ctrl_ref"),
        F.col("customer_system_attempt_reference").alias("ctrl_attempt_ref"),
        F.col("payment_attempt_timestamp").alias("ctrl_ts"),
        "payment_processor",
        "payment_method_variant",
        "card_scheme",
        "currency",
        "currency_type",
        "fraud_pre_auth_result",
        F.col("challenge_issued").cast("boolean").alias("challenged"),
        "customer_attempt_rank",
        "payment_flow",
        "attempt_type",
        "platform",
        "payment_terms",
        "customer_country_code",
        "gmv_eur_bucket",
        F.col("response_code").alias("ctrl_response_code"),
    )
)

test_failure = (
    df_cust
    .filter(F.col("group_name") == g_b)
    .filter(F.col("is_customer_attempt_successful") == 0)
    .select(
        F.col("payment_provider_reference").alias("test_ref"),
        F.col("customer_system_attempt_reference").alias("test_attempt_ref"),
        F.col("payment_attempt_timestamp").alias("test_ts"),
        "payment_processor",
        "payment_method_variant",
        "card_scheme",
        "currency",
        "currency_type",
        "fraud_pre_auth_result",
        F.col("challenge_issued").cast("boolean").alias("challenged"),
        "customer_attempt_rank",
        "payment_flow",
        "attempt_type",
        "platform",
        "payment_terms",
        "customer_country_code",
        "gmv_eur_bucket",
        F.col("error_code").alias("test_error_code"),
        F.col("error_type").alias("test_error_type"),
        F.col("error_decline_type").alias("test_error_decline_type"),
        F.col("response_code").alias("test_response_code"),
    )
)

match_cols = [
    "payment_processor",
    "payment_method_variant",
    "card_scheme",
    "currency",
    "currency_type",
    "fraud_pre_auth_result",
    "challenged",
    "customer_attempt_rank",
    "payment_flow",
    "attempt_type",
    "platform",
    "payment_terms",
    "customer_country_code",
    "gmv_eur_bucket",
]

matched = ctrl_success.join(test_failure, on=match_cols, how="inner")

matched = matched.withColumn(
    "ts_diff_hours",
    F.abs(F.unix_timestamp("ctrl_ts") - F.unix_timestamp("test_ts")) / 3600.0
)

matched_close = matched.filter(F.col("ts_diff_hours") <= 48)

w = Window.partitionBy("test_ref").orderBy("ts_diff_hours")
best_match = matched_close.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

w2 = Window.partitionBy("ctrl_ref").orderBy("ts_diff_hours")
best_match = best_match.withColumn("_rn", F.row_number().over(w2)).filter(F.col("_rn") == 1).drop("_rn")

best_match = best_match.orderBy("ts_diff_hours")
best_match.cache()

n_pairs = best_match.count()
print(f"Matched pairs found (within 48h, 1:1): {n_pairs:,}")

ctrl_total = df_cust.filter(F.col("group_name") == g_a).filter(F.col("is_customer_attempt_successful") == 1).count()
test_fail_total = df_cust.filter(F.col("group_name") == g_b).filter(F.col("is_customer_attempt_successful") == 0).count()
print(f"\nPool sizes:  Ctrl successes: {ctrl_total:,}   Test failures: {test_fail_total:,}")
print(f"Match rate:  {n_pairs / test_fail_total:.1%} of test failures matched")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Distribution of Matched Pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Matched pairs by segment

# COMMAND ----------

pair_dist = (
    best_match
    .groupBy("payment_processor", "card_scheme", "currency_type", "fraud_pre_auth_result")
    .agg(F.count("*").alias("pairs"))
    .orderBy(F.desc("pairs"))
    .toPandas()
)

print(f"\n{'=' * 100}")
print(f"Matched pairs by segment (processor × scheme × currency_type × fraud_result)")
print(f"{'=' * 100}")
print(f"{'Processor':<22} {'Scheme':<10} {'CurrType':<16} {'FraudResult':<22} {'Pairs':>6}")
print("-" * 80)
for _, r in pair_dist.head(30).iterrows():
    print(f"{str(r['payment_processor']):<22} {str(r['card_scheme']):<10} "
          f"{str(r['currency_type']):<16} {str(r['fraud_pre_auth_result']):<22} {r['pairs']:>6,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Top error codes in matched test failures

# COMMAND ----------

err_dist = (
    best_match
    .groupBy("test_error_code")
    .agg(F.count("*").alias("pairs"))
    .orderBy(F.desc("pairs"))
    .toPandas()
)

print(f"\n{'=' * 60}")
print(f"Error codes in matched test failures")
print(f"{'=' * 60}")
print(f"{'Error Code':<40} {'Pairs':>8} {'Share':>8}")
print("-" * 60)
for _, r in err_dist.head(15).iterrows():
    print(f"{str(r['test_error_code']):<40} {r['pairs']:>8,} {r['pairs']/n_pairs:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Matched pairs by processor

# COMMAND ----------

proc_dist = (
    best_match
    .groupBy("payment_processor")
    .agg(F.count("*").alias("pairs"))
    .orderBy(F.desc("pairs"))
    .toPandas()
)

print(f"\n{'=' * 50}")
print(f"Matched pairs by processor")
print(f"{'=' * 50}")
print(f"{'Processor':<30} {'Pairs':>8} {'Share':>8}")
print("-" * 50)
for _, r in proc_dist.iterrows():
    print(f"{str(r['payment_processor']):<30} {r['pairs']:>8,} {r['pairs']/n_pairs:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Matched pairs by fraud result

# COMMAND ----------

fraud_dist = (
    best_match
    .groupBy("fraud_pre_auth_result")
    .agg(F.count("*").alias("pairs"))
    .orderBy(F.desc("pairs"))
    .toPandas()
)

print(f"\n{'=' * 50}")
print(f"Matched pairs by fraud_pre_auth_result")
print(f"{'=' * 50}")
print(f"{'Fraud Result':<30} {'Pairs':>8} {'Share':>8}")
print("-" * 50)
for _, r in fraud_dist.iterrows():
    print(f"{str(r['fraud_pre_auth_result']):<30} {r['pairs']:>8,} {r['pairs']/n_pairs:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Time proximity distribution

# COMMAND ----------

ts_stats = best_match.select("ts_diff_hours").toPandas()
print(f"Time difference between matched pairs (hours):")
print(f"  Median : {ts_stats['ts_diff_hours'].median():.1f}h")
print(f"  Mean   : {ts_stats['ts_diff_hours'].mean():.1f}h")
print(f"  P25    : {ts_stats['ts_diff_hours'].quantile(0.25):.1f}h")
print(f"  P75    : {ts_stats['ts_diff_hours'].quantile(0.75):.1f}h")
print(f"  < 1h   : {(ts_stats['ts_diff_hours'] < 1).sum():,} pairs")
print(f"  < 6h   : {(ts_stats['ts_diff_hours'] < 6).sum():,} pairs")
print(f"  < 24h  : {(ts_stats['ts_diff_hours'] < 24).sum():,} pairs")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Sample Pairs for Primer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Closest-matched pairs (all segments)

# COMMAND ----------

sample_pairs = (
    best_match
    .select(
        "ctrl_ref", "test_ref",
        "payment_processor", "card_scheme", "payment_method_variant",
        "currency", "currency_type", "fraud_pre_auth_result", "challenged",
        "customer_attempt_rank", "attempt_type", "payment_flow",
        "platform", "payment_terms", "customer_country_code", "gmv_eur_bucket",
        "ctrl_ts", "test_ts", "ts_diff_hours",
        "ctrl_response_code", "test_error_code", "test_error_type",
        "test_error_decline_type", "test_response_code",
    )
    .orderBy("ts_diff_hours")
)

print(f"Closest-matched pairs (sorted by time proximity):")
print(f"Use ctrl_ref and test_ref to look up both attempts side-by-side in Primer.\n")
display(sample_pairs.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Pairs where test failed with SUSPECTED_FRAUD

# COMMAND ----------

sf_pairs = (
    best_match
    .filter(F.col("test_error_code") == "SUSPECTED_FRAUD")
    .select(
        "ctrl_ref", "test_ref",
        "payment_processor", "card_scheme", "payment_method_variant",
        "currency", "currency_type", "fraud_pre_auth_result", "challenged",
        "customer_attempt_rank", "attempt_type", "platform",
        "payment_terms", "gmv_eur_bucket",
        "ctrl_ts", "test_ts", "ts_diff_hours",
        "ctrl_response_code", "test_error_code", "test_response_code",
    )
    .orderBy("ts_diff_hours")
)

cnt = sf_pairs.count()
print(f"Pairs where test failed with SUSPECTED_FRAUD: {cnt:,}")
if cnt > 0:
    display(sf_pairs.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Pairs where test failed with INSUFFICIENT_FUNDS

# COMMAND ----------

if_pairs = (
    best_match
    .filter(F.col("test_error_code") == "INSUFFICIENT_FUNDS")
    .select(
        "ctrl_ref", "test_ref",
        "payment_processor", "card_scheme", "payment_method_variant",
        "currency", "currency_type", "fraud_pre_auth_result", "challenged",
        "customer_attempt_rank", "attempt_type", "platform",
        "payment_terms", "gmv_eur_bucket",
        "ctrl_ts", "test_ts", "ts_diff_hours",
        "ctrl_response_code", "test_error_code", "test_response_code",
    )
    .orderBy("ts_diff_hours")
)

cnt = if_pairs.count()
print(f"Pairs where test failed with INSUFFICIENT_FUNDS: {cnt:,}")
if cnt > 0:
    display(if_pairs.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Pairs where fraud_pre_auth_result is REFUSE

# COMMAND ----------

refuse_pairs = (
    best_match
    .filter(F.col("fraud_pre_auth_result") == "REFUSE")
    .select(
        "ctrl_ref", "test_ref",
        "payment_processor", "card_scheme", "payment_method_variant",
        "currency", "currency_type", "fraud_pre_auth_result", "challenged",
        "customer_attempt_rank", "attempt_type", "platform",
        "payment_terms", "gmv_eur_bucket",
        "ctrl_ts", "test_ts", "ts_diff_hours",
        "ctrl_response_code", "test_error_code", "test_response_code",
    )
    .orderBy("ts_diff_hours")
)

cnt = refuse_pairs.count()
print(f"Pairs where fraud_pre_auth_result = REFUSE: {cnt:,}")
if cnt > 0:
    display(refuse_pairs.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Exportable Temp View

# COMMAND ----------

best_match.createOrReplaceTempView("us_matched_pairs")
print(f"Created temp view: us_matched_pairs ({n_pairs:,} rows)")
print(f"""
Matched on (14 columns):
  payment_processor, payment_method_variant, card_scheme
  currency, currency_type, fraud_pre_auth_result, challenged
  customer_attempt_rank, payment_flow, attempt_type
  platform, payment_terms, customer_country_code, gmv_eur_bucket

Additional columns:
  ctrl_ref, ctrl_attempt_ref, ctrl_ts, ctrl_response_code
  test_ref, test_attempt_ref, test_ts
  test_error_code, test_error_type, test_error_decline_type, test_response_code
  ts_diff_hours

Example queries:
  -- Closest pairs overall
  SELECT * FROM us_matched_pairs WHERE ts_diff_hours < 1 ORDER BY ts_diff_hours LIMIT 20

  -- REFUSE pairs for a specific processor
  SELECT * FROM us_matched_pairs
  WHERE fraud_pre_auth_result = 'REFUSE' AND payment_processor ILIKE '%jpmc%'
  ORDER BY ts_diff_hours LIMIT 20

  -- SUSPECTED_FRAUD error in test
  SELECT * FROM us_matched_pairs
  WHERE test_error_code = 'SUSPECTED_FRAUD'
  ORDER BY ts_diff_hours LIMIT 20

  -- Same GMV bucket, same platform, amex
  SELECT * FROM us_matched_pairs
  WHERE card_scheme = 'amex' ORDER BY ts_diff_hours LIMIT 20
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of US matched pairs investigation*
