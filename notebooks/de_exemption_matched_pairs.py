# Databricks notebook source
# MAGIC %md
# MAGIC # DE Exemption Matched Pairs Investigation
# MAGIC
# MAGIC **Goal:** Find matching transaction pairs where:
# MAGIC - Control got `THREE_DS_EXEMPTION` (frictionless, high SR)
# MAGIC - Test got `THREE_DS` (triggered 3DS, lower SR)
# MAGIC
# MAGIC **Segment:** DE, EU5 + JPM currency, pay_now, CIT, payment_card
# MAGIC
# MAGIC We match on transaction characteristics (amount, card network, currency, etc.)
# MAGIC to find comparable cases for manual investigation in Primer.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql import Window as W

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START = "2026-04-02"

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 1 — Extract raw transactions with payload details

# COMMAND ----------

df_raw = spark.sql(f"""
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
  p.bin_issuer_country_code,
  p.currency,
  p.payment_processor,
  p.payment_method_variant,
  p.payment_initiator_type,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.payment_flow,
  p.is_customer_attempt_successful,
  p.fraud_pre_auth_result,
  p.challenge_issued,
  p.is_three_ds_passed,
  p.sent_to_issuer,
  p.payment_attempt_timestamp,
  p.amount
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_method = 'payment_card'
  AND p.bin_issuer_country_code = 'DE'
  AND p.currency IN ({','.join(f"'{c}'" for c in JPM_CURRENCIES)})
  AND p.payment_flow = 'pay_now'
  AND p.payment_initiator_type = 'CIT'
  AND p.system_attempt_rank = 1
""")

print(f"Raw DE segment rows: {df_raw.count():,}")
df_raw.groupBy("group_name", "fraud_pre_auth_result").count().orderBy("group_name", "fraud_pre_auth_result").show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 2 — Split into control-exemption vs test-3DS

# COMMAND ----------

ctrl_exempt = df_raw.filter(
    (F.col("group_name") == "Control") &
    (F.col("fraud_pre_auth_result") == "THREE_DS_EXEMPTION")
).alias("ctrl")

test_3ds = df_raw.filter(
    (F.col("group_name") == "Test") &
    (F.col("fraud_pre_auth_result") == "THREE_DS")
).alias("test")

print(f"Control THREE_DS_EXEMPTION: {ctrl_exempt.count():,}")
print(f"Test THREE_DS:              {test_3ds.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 3 — Find matched pairs
# MAGIC
# MAGIC Match on: card network, currency, same date, similar amount bucket.
# MAGIC Take one pair per control transaction to avoid duplicates.

# COMMAND ----------

ctrl_prep = ctrl_exempt.withColumn(
    "amount_bucket", (F.col("amount") / 1000).cast("int") * 1000
).withColumn(
    "attempt_date", F.col("payment_attempt_timestamp").cast("date")
).withColumn(
    "attempt_hour", F.hour("payment_attempt_timestamp")
)

test_prep = test_3ds.withColumn(
    "amount_bucket", (F.col("amount") / 1000).cast("int") * 1000
).withColumn(
    "attempt_date", F.col("payment_attempt_timestamp").cast("date")
).withColumn(
    "attempt_hour", F.hour("payment_attempt_timestamp")
)

MATCH_COLS = ["payment_method_variant", "currency", "attempt_date", "amount_bucket"]

pairs = ctrl_prep.alias("c").join(
    test_prep.alias("t"),
    on=[F.col(f"c.{col}") == F.col(f"t.{col}") for col in MATCH_COLS],
    how="inner"
).select(
    F.col("c.payment_provider_reference").alias("ctrl_ref"),
    F.col("t.payment_provider_reference").alias("test_ref"),
    F.col("c.visitor_id").alias("ctrl_visitor"),
    F.col("t.visitor_id").alias("test_visitor"),
    F.col("c.payment_method_variant").alias("card_network"),
    F.col("c.currency"),
    F.col("c.attempt_date"),
    F.col("c.amount").alias("ctrl_amount"),
    F.col("t.amount").alias("test_amount"),
    F.col("c.payment_processor").alias("ctrl_processor"),
    F.col("t.payment_processor").alias("test_processor"),
    F.col("c.fraud_pre_auth_result").alias("ctrl_fraud"),
    F.col("t.fraud_pre_auth_result").alias("test_fraud"),
    F.col("c.challenge_issued").alias("ctrl_challenge"),
    F.col("t.challenge_issued").alias("test_challenge"),
    F.col("c.is_customer_attempt_successful").alias("ctrl_success"),
    F.col("t.is_customer_attempt_successful").alias("test_success"),
    F.col("c.is_three_ds_passed").alias("ctrl_3ds_passed"),
    F.col("t.is_three_ds_passed").alias("test_3ds_passed"),
    F.col("c.payment_attempt_timestamp").alias("ctrl_timestamp"),
    F.col("t.payment_attempt_timestamp").alias("test_timestamp"),
    F.col("c.customer_attempt_rank").alias("ctrl_cust_rank"),
    F.col("t.customer_attempt_rank").alias("test_cust_rank"),
).withColumn(
    "_rn", F.row_number().over(
        W.partitionBy("ctrl_ref").orderBy(
            F.abs(F.col("ctrl_amount") - F.col("test_amount"))
        )
    )
).filter(F.col("_rn") == 1).drop("_rn")

pair_count = pairs.count()
print(f"Matched pairs: {pair_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 4 — Pair analysis summary

# COMMAND ----------

print("=== MATCHED PAIRS: Ctrl=EXEMPTION vs Test=THREE_DS ===\n")

print("--- Test outcome within these pairs ---")
pairs.groupBy("test_success").count().show()
pairs.groupBy("test_3ds_passed").count().show()
pairs.groupBy("test_challenge").count().show()
pairs.groupBy("test_processor").count().orderBy("count", ascending=False).show()

print("--- Control outcome within these pairs ---")
pairs.groupBy("ctrl_success").count().show()
pairs.groupBy("ctrl_processor").count().orderBy("count", ascending=False).show()

print("--- Processor routing: ctrl vs test ---")
pairs.groupBy("ctrl_processor", "test_processor").count().orderBy("count", ascending=False).show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 5 — Sample pairs for manual inspection
# MAGIC
# MAGIC Focus on the most interesting cases:
# MAGIC 1. Control succeeded (EXEMPTION) but Test failed (THREE_DS)
# MAGIC 2. Same processor — to isolate the exemption effect from routing

# COMMAND ----------

interesting = pairs.filter(
    (F.col("ctrl_success") == 1) & (F.col("test_success") == 0)
)
print(f"Pairs where ctrl succeeded + test failed: {interesting.count():,}")

interesting_same_proc = pairs.filter(
    (F.col("ctrl_success") == 1) &
    (F.col("test_success") == 0) &
    (F.col("ctrl_processor") == F.col("test_processor"))
)
print(f"  ... same processor: {interesting_same_proc.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 30 — Ctrl succeeded (EXEMPTION) but Test failed (THREE_DS)

# COMMAND ----------

display(
    interesting.orderBy("attempt_date", "card_network").limit(30).select(
        "ctrl_ref", "test_ref",
        "card_network", "currency", "attempt_date",
        "ctrl_amount", "test_amount",
        "ctrl_processor", "test_processor",
        "ctrl_fraud", "test_fraud",
        "ctrl_challenge", "test_challenge",
        "ctrl_success", "test_success",
        "test_3ds_passed",
        "ctrl_cust_rank", "test_cust_rank",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 30 — Same processor, Ctrl succeeded but Test failed

# COMMAND ----------

display(
    interesting_same_proc.orderBy("attempt_date", "card_network").limit(30).select(
        "ctrl_ref", "test_ref",
        "card_network", "currency", "attempt_date",
        "ctrl_amount", "test_amount",
        "ctrl_processor", "test_processor",
        "ctrl_fraud", "test_fraud",
        "ctrl_challenge", "test_challenge",
        "ctrl_success", "test_success",
        "test_3ds_passed",
        "ctrl_cust_rank", "test_cust_rank",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 30 — All pairs (representative sample)

# COMMAND ----------

display(
    pairs.orderBy(F.rand()).limit(30).select(
        "ctrl_ref", "test_ref",
        "card_network", "currency", "attempt_date",
        "ctrl_amount", "test_amount",
        "ctrl_processor", "test_processor",
        "ctrl_fraud", "test_fraud",
        "ctrl_challenge", "test_challenge",
        "ctrl_success", "test_success",
        "test_3ds_passed",
        "ctrl_cust_rank", "test_cust_rank",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 6 — Detailed payload comparison for top pairs
# MAGIC
# MAGIC For the most interesting pairs, fetch the raw notification payloads
# MAGIC to compare what Forter received / decided differently.

# COMMAND ----------

top_refs = [row.ctrl_ref for row in interesting.limit(10).select("ctrl_ref").collect()] + \
           [row.test_ref for row in interesting.limit(10).select("test_ref").collect()]

if top_refs:
    ref_list = ",".join(f"'{r}'" for r in top_refs if r)
    notifications = spark.sql(f"""
    WITH ranked AS (
      SELECT
        n.request_payload:payment.id::string AS payment_id,
        n.request_payload,
        n.dbz_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY n.request_payload:payment.id::string
          ORDER BY n.dbz_timestamp DESC
        ) AS rn
      FROM production.db_mirror_dbz.payment__payment_provider_notification n
      WHERE n.request_payload:payment.id::string IN ({ref_list})
    )
    SELECT payment_id, request_payload
    FROM ranked WHERE rn = 1
    """)

    print(f"Fetched {notifications.count()} notification payloads for top pairs")
    display(notifications)
else:
    print("No interesting pairs found to fetch payloads for")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 7 — Summary statistics

# COMMAND ----------

print("=== SUMMARY ===\n")

total_pairs = pairs.count()
ctrl_won = pairs.filter((F.col("ctrl_success") == 1) & (F.col("test_success") == 0)).count()
test_won = pairs.filter((F.col("ctrl_success") == 0) & (F.col("test_success") == 1)).count()
both_ok = pairs.filter((F.col("ctrl_success") == 1) & (F.col("test_success") == 1)).count()
both_fail = pairs.filter((F.col("ctrl_success") == 0) & (F.col("test_success") == 0)).count()

print(f"Total matched pairs:                     {total_pairs:,}")
print(f"Both succeeded:                          {both_ok:,} ({both_ok/total_pairs*100:.1f}%)")
print(f"Ctrl succeeded, Test failed (EXEMP>3DS): {ctrl_won:,} ({ctrl_won/total_pairs*100:.1f}%)")
print(f"Test succeeded, Ctrl failed:             {test_won:,} ({test_won/total_pairs*100:.1f}%)")
print(f"Both failed:                             {both_fail:,} ({both_fail/total_pairs*100:.1f}%)")
print()
print("--- Test 3DS outcome in 'ctrl won' pairs ---")
interesting.groupBy("test_3ds_passed", "test_challenge").count().orderBy("count", ascending=False).show()
print("--- Processor routing in 'ctrl won' pairs ---")
interesting.groupBy("ctrl_processor", "test_processor").count().orderBy("count", ascending=False).show(20, False)
