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
  p.amount,
  p.bin_account_funding_type,
  p.bin_product_usage_type,
  p.bin_network,
  p.bin_issuer_name,
  p.high_risk
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

MATCH_COLS = [
    "payment_method_variant",
    "currency",
    "attempt_date",
    "amount_bucket",
    "bin_account_funding_type",
    "high_risk",
]

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
    F.col("c.bin_account_funding_type").alias("account_funding_type"),
    F.col("c.bin_product_usage_type").alias("ctrl_product_usage"),
    F.col("t.bin_product_usage_type").alias("test_product_usage"),
    F.col("c.bin_issuer_name").alias("ctrl_issuer"),
    F.col("t.bin_issuer_name").alias("test_issuer"),
    F.col("c.high_risk").alias("high_risk"),
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
print(f"Matched pairs (strict — same card type + risk flag): {pair_count:,}")

if pair_count == 0:
    print("\nFallback: relaxing match to exclude high_risk...")
    MATCH_COLS_RELAXED = ["payment_method_variant", "currency", "attempt_date", "amount_bucket", "bin_account_funding_type"]
    pairs = ctrl_prep.alias("c").join(
        test_prep.alias("t"),
        on=[F.col(f"c.{col}") == F.col(f"t.{col}") for col in MATCH_COLS_RELAXED],
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
        F.col("c.bin_account_funding_type").alias("account_funding_type"),
        F.col("c.bin_product_usage_type").alias("ctrl_product_usage"),
        F.col("t.bin_product_usage_type").alias("test_product_usage"),
        F.col("c.bin_issuer_name").alias("ctrl_issuer"),
        F.col("t.bin_issuer_name").alias("test_issuer"),
        F.col("c.high_risk").alias("ctrl_high_risk"),
        F.col("t.high_risk").alias("test_high_risk"),
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
    print(f"Relaxed matched pairs: {pairs.count():,}")

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
# MAGIC ## Cell 4b — Card characteristics analysis
# MAGIC
# MAGIC Analyze whether Forter's 3DS decision correlates with card-level parameters.
# MAGIC These pairs are matched on card network, funding type, risk flag, amount bucket, date —
# MAGIC so any remaining difference is the orchestrator's behavior, not the card profile.

# COMMAND ----------

print("=== CARD CHARACTERISTICS IN MATCHED PAIRS ===\n")

print("--- By account funding type ---")
pairs.groupBy("account_funding_type").agg(
    F.count("*").alias("pairs"),
    F.avg("ctrl_success").alias("ctrl_sr"),
    F.avg("test_success").alias("test_sr"),
    (F.avg("test_success") - F.avg("ctrl_success")).alias("sr_delta"),
).orderBy("pairs", ascending=False).show(10, False)

print("--- By high_risk flag ---")
if "high_risk" in pairs.columns:
    pairs.groupBy("high_risk").agg(
        F.count("*").alias("pairs"),
        F.avg("ctrl_success").alias("ctrl_sr"),
        F.avg("test_success").alias("test_sr"),
        (F.avg("test_success") - F.avg("ctrl_success")).alias("sr_delta"),
    ).orderBy("pairs", ascending=False).show(10, False)
elif "ctrl_high_risk" in pairs.columns:
    pairs.groupBy("ctrl_high_risk", "test_high_risk").agg(
        F.count("*").alias("pairs"),
        F.avg("ctrl_success").alias("ctrl_sr"),
        F.avg("test_success").alias("test_sr"),
        (F.avg("test_success") - F.avg("ctrl_success")).alias("sr_delta"),
    ).orderBy("pairs", ascending=False).show(10, False)

print("--- By card network + account funding type ---")
pairs.groupBy("card_network", "account_funding_type").agg(
    F.count("*").alias("pairs"),
    F.avg("ctrl_success").alias("ctrl_sr"),
    F.avg("test_success").alias("test_sr"),
    (F.avg("test_success") - F.avg("ctrl_success")).alias("sr_delta"),
).orderBy("pairs", ascending=False).show(20, False)

print("--- BIN overlap: same BIN6 in ctrl vs test ---")
same_issuer = pairs.filter(F.col("ctrl_issuer") == F.col("test_issuer"))
diff_issuer = pairs.filter(F.col("ctrl_issuer") != F.col("test_issuer"))
print(f"Same issuer:      {same_issuer.count():,} pairs, ctrl SR={same_issuer.select(F.avg('ctrl_success')).first()[0]:.3f}, test SR={same_issuer.select(F.avg('test_success')).first()[0]:.3f}")
print(f"Different issuer: {diff_issuer.count():,} pairs, ctrl SR={diff_issuer.select(F.avg('ctrl_success')).first()[0]:.3f}, test SR={diff_issuer.select(F.avg('test_success')).first()[0]:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 4c — Forter decision analysis (full population, not just pairs)
# MAGIC
# MAGIC Compare exemption rates controlling for card characteristics to test the
# MAGIC hypothesis that Forter gives different 3DS recommendations for similar cards
# MAGIC depending on the orchestrator path.

# COMMAND ----------

print("=== FORTER EXEMPTION RATE BY CARD CHARACTERISTICS (full segment) ===\n")

exempt_analysis = df_raw.withColumn(
    "is_exempted", (F.col("fraud_pre_auth_result") == "THREE_DS_EXEMPTION").cast("int")
).withColumn(
    "is_3ds", (F.col("fraud_pre_auth_result") == "THREE_DS").cast("int")
)

print("--- Exemption rate by group × account_funding_type ---")
exempt_analysis.groupBy("group_name", "bin_account_funding_type").agg(
    F.count("*").alias("attempts"),
    F.avg("is_exempted").alias("exemption_rate"),
    F.avg("is_3ds").alias("three_ds_rate"),
    F.avg("is_customer_attempt_successful").alias("sr"),
).orderBy("bin_account_funding_type", "group_name").show(20, False)

print("--- Exemption rate by group × high_risk ---")
exempt_analysis.groupBy("group_name", "high_risk").agg(
    F.count("*").alias("attempts"),
    F.avg("is_exempted").alias("exemption_rate"),
    F.avg("is_3ds").alias("three_ds_rate"),
    F.avg("is_customer_attempt_successful").alias("sr"),
).orderBy("high_risk", "group_name").show(10, False)

print("--- Exemption rate by group × card_network × high_risk ---")
exempt_analysis.groupBy("group_name", "payment_method_variant", "high_risk").agg(
    F.count("*").alias("attempts"),
    F.avg("is_exempted").alias("exemption_rate"),
    F.avg("is_3ds").alias("three_ds_rate"),
    F.avg("is_customer_attempt_successful").alias("sr"),
).orderBy("payment_method_variant", "high_risk", "group_name").show(20, False)

print("--- Exemption rate by group × bin_product_usage_type ---")
exempt_analysis.groupBy("group_name", "bin_product_usage_type").agg(
    F.count("*").alias("attempts"),
    F.avg("is_exempted").alias("exemption_rate"),
    F.avg("is_3ds").alias("three_ds_rate"),
    F.avg("is_customer_attempt_successful").alias("sr"),
).orderBy("bin_product_usage_type", "group_name").show(20, False)

print("--- Top BIN6 with largest exemption rate difference ---")
bin_diff = exempt_analysis.groupBy("bin_issuer_name", "group_name").agg(
    F.count("*").alias("attempts"),
    F.avg("is_exempted").alias("exemption_rate"),
)
from pyspark.sql.functions import first
bin_pivot = bin_diff.groupBy("bin_issuer_name").pivot("group_name").agg(
    F.first("attempts").alias("att"),
    F.first("exemption_rate").alias("exempt_rate"),
).filter(
    (F.col("Control_att") >= 20) & (F.col("Test_att") >= 20)
).withColumn(
    "exempt_delta", F.col("Test_exempt_rate") - F.col("Control_exempt_rate")
).orderBy(F.abs(F.col("exempt_delta")).desc())
print("Top 20 issuers with largest exemption rate delta (min 20 att each):")
bin_pivot.show(20, False)

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

DISPLAY_COLS = [
    "ctrl_ref", "test_ref",
    "card_network", "currency", "attempt_date",
    "ctrl_amount", "test_amount",
    "account_funding_type", "ctrl_issuer", "test_issuer",
    "ctrl_processor", "test_processor",
    "ctrl_fraud", "test_fraud",
    "ctrl_challenge", "test_challenge",
    "ctrl_success", "test_success",
    "test_3ds_passed",
    "ctrl_cust_rank", "test_cust_rank",
]
# Add high_risk column(s) depending on strict vs relaxed match
if "high_risk" in pairs.columns:
    DISPLAY_COLS.insert(8, "high_risk")
else:
    DISPLAY_COLS.insert(8, "ctrl_high_risk")
    DISPLAY_COLS.insert(9, "test_high_risk")

display(
    interesting.orderBy("attempt_date", "card_network").limit(30).select(
        *[c for c in DISPLAY_COLS if c in interesting.columns]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 30 — Same processor, Ctrl succeeded but Test failed

# COMMAND ----------

display(
    interesting_same_proc.orderBy("attempt_date", "card_network").limit(30).select(
        *[c for c in DISPLAY_COLS if c in interesting_same_proc.columns]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 30 — All pairs (representative sample)

# COMMAND ----------

display(
    pairs.orderBy(F.rand()).limit(30).select(
        *[c for c in DISPLAY_COLS if c in pairs.columns]
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
