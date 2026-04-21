# Databricks notebook source
# MAGIC %md
# MAGIC # US Payload-Level Matched Pairs — ACCEPT (ctrl) vs THREE_DS (test)
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house`
# MAGIC
# MAGIC **Date:** April 2–7, 2026
# MAGIC
# MAGIC **Goal:** Join experiment data with `payment__payment_provider_notification`
# MAGIC via `request_payload:payment.id` to compare actual **payload request**
# MAGIC parameters. Find pairs where:
# MAGIC - **Control:** fraud pre-auth = ACCEPT, successful
# MAGIC - **Test:** fraud pre-auth = THREE_DS, failed
# MAGIC
# MAGIC **Matching on 13 parameters:**
# MAGIC - Payload: card network, issuer country, funding type, currency,
# MAGIC   amount bucket, platform, processor, customRoutingFlag, skipRisk, high_risk
# MAGIC - Fact: payment_flow, customer_attempt_rank, attempt_type
# MAGIC
# MAGIC **Comparing OUTPUT fields side-by-side:** fraud result, fraud source,
# MAGIC 3DS outcome, processorMerchantId, errors, payment_terms, usage_type.
# MAGIC
# MAGIC **Sections:**
# MAGIC | # | Section |
# MAGIC |---|---|
# MAGIC | 1 | Setup & schema exploration |
# MAGIC | 2 | Extract experiment data (US, April 5) |
# MAGIC | 3 | Join with notification table & extract payload fields |
# MAGIC | 4 | Build payload-matched pairs |
# MAGIC | 5 | Pair analysis — distribution, routing/fraud comparison, errors |
# MAGIC | 6 | Deep payload comparison for closest pairs |
# MAGIC | 7 | Exportable temp views |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
TARGET_DATE_START = "2026-04-02"
TARGET_DATE_END   = "2026-04-07"

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

NOTIF_TABLE = "production.db_mirror_dbz.payment__payment_provider_notification"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Schema Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Notification table schema

# COMMAND ----------

df_notif_raw = spark.table(NOTIF_TABLE)
df_notif_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Sample request_payload — verify structure

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            id,
            request_payload:payment.id::string            AS payment_id,
            request_payload:payment.amount::int            AS amount,
            request_payload:payment.currencyCode::string   AS currency,
            request_payload:payment.processor.name::string AS processor,
            request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string AS fraud_result,
            request_payload:payment.paymentMethod.paymentMethodData.network::string     AS card_network,
            request_payload:payment.paymentMethod.paymentMethodData.binData.issuerCountryCode::string AS issuer_country,
            request_payload:payment.paymentMethod.paymentMethodData.binData.accountFundingType::string AS funding_type,
            request_payload:payment.metadata.platform::string      AS platform,
            request_payload:payment.metadata.customRoutingFlag::string     AS routing_flag
        FROM {NOTIF_TABLE}
        WHERE dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
        LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Extract Experiment Data (US, April 5)

# COMMAND ----------

df_exp = spark.sql(f"""
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
  p.payment_method_variant,
  p.currency,
  p.payment_processor,
  p.payment_attempt_status,
  p.payment_flow,
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
  p.payment_initiated,
  p.sent_to_issuer,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
  AND p.bin_issuer_country_code = 'US'
  AND p.system_attempt_rank = 1
""")

df_exp.cache()

total = df_exp.count()
print(f"US customer attempts {TARGET_DATE_START} to {TARGET_DATE_END}: {total:,}")

by_group = df_exp.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Fraud result split for this day

# COMMAND ----------

fraud_split = df_exp.groupBy("group_name", "fraud_pre_auth_result").count().orderBy("group_name", "fraud_pre_auth_result").toPandas()
grp_totals = fraud_split.groupby("group_name")["count"].sum().to_dict()

print(f"\n{'Group':<15} {'Fraud Result':<25} {'Count':>8} {'Share':>8}")
print("-" * 60)
for _, r in fraud_split.iterrows():
    print(f"{r['group_name']:<15} {str(r['fraud_pre_auth_result']):<25} "
          f"{r['count']:>8,} {r['count']/grp_totals.get(r['group_name'],1):>8.2%}")

ctrl_accept = df_exp.filter(
    (F.col("group_name") == g_a) &
    (F.col("fraud_pre_auth_result") == "ACCEPT") &
    (F.col("is_customer_attempt_successful") == 1)
).count()

test_tds_fail = df_exp.filter(
    (F.col("group_name") == g_b) &
    (F.col("fraud_pre_auth_result") == "THREE_DS") &
    (F.col("is_customer_attempt_successful") == 0)
).count()

print(f"\nTarget pools:")
print(f"  Control ACCEPT + successful: {ctrl_accept:,}")
print(f"  Test THREE_DS + failed:      {test_tds_fail:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Join with Notification Table & Extract Payload Fields
# MAGIC
# MAGIC Join on `payment_provider_reference = request_payload:payment.id`
# MAGIC and extract key fields from the JSON payload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Register experiment data & join

# COMMAND ----------

df_exp.createOrReplaceTempView("us_exp_data")

df_joined = spark.sql(f"""
SELECT
  e.*,

  -- ── Payload MATCH fields (INPUT — transaction characteristics) ──
  n.request_payload:payment.amount::int                                                    AS pl_amount,
  n.request_payload:payment.currencyCode::string                                           AS pl_currency,
  n.request_payload:payment.order.countryCode::string                                      AS pl_order_country,
  n.request_payload:payment.paymentMethod.paymentMethodData.network::string                AS pl_card_network,
  n.request_payload:payment.paymentMethod.paymentMethodData.first6Digits::string           AS pl_bin6,
  n.request_payload:payment.paymentMethod.paymentMethodData.binData.issuerCountryCode::string AS pl_issuer_country,
  n.request_payload:payment.paymentMethod.paymentMethodData.binData.accountFundingType::string AS pl_funding_type,
  n.request_payload:payment.paymentMethod.paymentMethodData.binData.productUsageType::string  AS pl_usage_type,
  n.request_payload:payment.metadata.platform::string                                      AS pl_platform,
  n.request_payload:payment.processor.name::string                                         AS pl_processor,
  n.request_payload:payment.metadata.customRoutingFlag::string                             AS pl_routing_flag,
  n.request_payload:payment.metadata.skipRisk::string                                      AS pl_skip_risk,
  n.request_payload:payment.metadata.high_risk::string                                     AS pl_high_risk,

  -- Amount bucket for matching (amount is in minor units, e.g. 15800 = €158)
  CASE
    WHEN n.request_payload:payment.amount::int < 2000  THEN 'under_20'
    WHEN n.request_payload:payment.amount::int < 5000  THEN '20_to_50'
    WHEN n.request_payload:payment.amount::int < 10000 THEN '50_to_100'
    WHEN n.request_payload:payment.amount::int < 25000 THEN '100_to_250'
    WHEN n.request_payload:payment.amount::int < 50000 THEN '250_to_500'
    ELSE '500_plus'
  END AS pl_amount_bucket,

  -- ── Payload COMPARE fields (OUTPUT — fraud decisions) ──
  n.request_payload:payment.processor.processorMerchantId::string                          AS pl_processor_mid,
  n.request_payload:payment.riskData.fraudChecks.source::string                            AS pl_fraud_source,
  n.request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string            AS pl_fraud_result,
  n.request_payload:payment.paymentMethod.threeDSecureAuthentication.responseCode::string   AS pl_3ds_response,
  n.request_payload:payment.paymentMethod.threeDSecureAuthentication.challengeIssued::string AS pl_3ds_challenge,
  n.request_payload:payment.paymentMethod.threeDSecureAuthentication.protocolVersion::string AS pl_3ds_version,
  n.request_payload:payment.status::string                                                 AS pl_payment_status,

  -- Experiment variant from payload metadata (sanity check)
  n.request_payload:payment.metadata.experiments.pay_payment_orchestration_routing_in_house::string AS pl_exp_variant

FROM us_exp_data e
JOIN {NOTIF_TABLE} n
  ON e.payment_provider_reference = n.request_payload:payment.id::string
  AND n.dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.payment_provider_reference
  ORDER BY n.dbz_timestamp DESC
) = 1
""")

df_joined.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Join stats

# COMMAND ----------

joined_count = df_joined.count()
print(f"Experiment attempts matched to notifications: {joined_count:,}")
print(f"Original experiment attempts: {total:,}")
print(f"Match rate: {joined_count / total:.1%}" if total > 0 else "N/A")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Sanity check — payload variant vs assignment variant

# COMMAND ----------

variant_check = (
    df_joined
    .groupBy("group_name", "pl_exp_variant")
    .count()
    .orderBy("group_name", "pl_exp_variant")
    .toPandas()
)

print(f"{'Group (assignment)':<20} {'Variant (payload)':<20} {'Count':>8}")
print("-" * 52)
for _, r in variant_check.iterrows():
    print(f"{r['group_name']:<20} {str(r['pl_exp_variant']):<20} {r['count']:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Sample joined rows — check payload fields

# COMMAND ----------

display(
    df_joined
    .select(
        "group_name", "payment_provider_reference",
        "pl_card_network", "pl_issuer_country", "pl_funding_type",
        "pl_usage_type", "pl_currency", "pl_amount", "pl_amount_bucket",
        "pl_platform", "pl_order_country",
        "pl_processor", "pl_fraud_result", "pl_fraud_source",
        "pl_routing_flag", "pl_skip_risk", "pl_high_risk",
        "pl_3ds_response", "pl_3ds_challenge",
    )
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Build Payload-Matched Pairs
# MAGIC
# MAGIC Match on **INPUT** parameters from the payload (transaction characteristics).
# MAGIC Do **NOT** match on routing/fraud outcome fields — those are what we compare.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Define payload match columns & build pools

# COMMAND ----------

PAYLOAD_MATCH_COLS = [
    # Payload fields
    "pl_card_network",
    "pl_issuer_country",
    "pl_funding_type",
    "pl_currency",
    "pl_amount_bucket",
    "pl_platform",
    "pl_processor",
    "pl_routing_flag",
    "pl_high_risk",
    # Fact fields
    "payment_flow",
    "payment_terms",
    "customer_attempt_rank",
    "attempt_type",
]

ctrl_pool = (
    df_joined
    .filter(
        (F.col("group_name") == g_a) &
        (F.col("fraud_pre_auth_result") == "ACCEPT") &
        (F.col("is_customer_attempt_successful") == 1)
    )
    .select(
        *PAYLOAD_MATCH_COLS,
        F.col("payment_provider_reference").alias("ctrl_ref"),
        F.col("customer_system_attempt_reference").alias("ctrl_attempt_ref"),
        F.col("payment_attempt_timestamp").alias("ctrl_ts"),
        F.col("fraud_pre_auth_result").alias("ctrl_fact_fraud"),
        F.col("response_code").alias("ctrl_response_code"),
        F.col("payment_attempt_status").alias("ctrl_status"),
        F.col("challenge_issued").cast("boolean").alias("ctrl_challenged"),
        # payload OUTPUT fields for comparison
        F.col("pl_skip_risk").alias("ctrl_pl_skip_risk"),
        F.col("pl_usage_type").alias("ctrl_pl_usage_type"),
        F.col("pl_order_country").alias("ctrl_pl_order_country"),
        F.col("pl_processor_mid").alias("ctrl_pl_mid"),
        F.col("pl_fraud_result").alias("ctrl_pl_fraud"),
        F.col("pl_fraud_source").alias("ctrl_pl_fraud_source"),
        F.col("pl_3ds_response").alias("ctrl_pl_3ds_response"),
        F.col("pl_3ds_challenge").alias("ctrl_pl_3ds_challenge"),
        F.col("pl_3ds_version").alias("ctrl_pl_3ds_version"),
        F.col("pl_payment_status").alias("ctrl_pl_payment_status"),
        F.col("pl_exp_variant").alias("ctrl_pl_variant"),
    )
)

test_pool = (
    df_joined
    .filter(
        (F.col("group_name") == g_b) &
        (F.col("fraud_pre_auth_result") == "THREE_DS") &
        (F.col("is_customer_attempt_successful") == 0)
    )
    .select(
        *PAYLOAD_MATCH_COLS,
        F.col("payment_provider_reference").alias("test_ref"),
        F.col("customer_system_attempt_reference").alias("test_attempt_ref"),
        F.col("payment_attempt_timestamp").alias("test_ts"),
        F.col("fraud_pre_auth_result").alias("test_fact_fraud"),
        F.col("error_code").alias("test_error_code"),
        F.col("error_type").alias("test_error_type"),
        F.col("error_decline_type").alias("test_error_decline_type"),
        F.col("response_code").alias("test_response_code"),
        F.col("payment_attempt_status").alias("test_status"),
        F.col("challenge_issued").cast("boolean").alias("test_challenged"),
        # payload OUTPUT fields for comparison
        F.col("pl_skip_risk").alias("test_pl_skip_risk"),
        F.col("pl_usage_type").alias("test_pl_usage_type"),
        F.col("pl_order_country").alias("test_pl_order_country"),
        F.col("pl_processor_mid").alias("test_pl_mid"),
        F.col("pl_fraud_result").alias("test_pl_fraud"),
        F.col("pl_fraud_source").alias("test_pl_fraud_source"),
        F.col("pl_3ds_response").alias("test_pl_3ds_response"),
        F.col("pl_3ds_challenge").alias("test_pl_3ds_challenge"),
        F.col("pl_3ds_version").alias("test_pl_3ds_version"),
        F.col("pl_payment_status").alias("test_pl_payment_status"),
        F.col("pl_exp_variant").alias("test_pl_variant"),
    )
)

ctrl_n = ctrl_pool.count()
test_n = test_pool.count()
print(f"Control pool (ACCEPT + success):  {ctrl_n:,}")
print(f"Test pool (THREE_DS + failed):    {test_n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Diagnostics — which columns block matching?
# MAGIC
# MAGIC For each match column, show:
# MAGIC 1. Value distribution in ctrl vs test pools (do they overlap?)
# MAGIC 2. How many raw join matches if that column is dropped

# COMMAND ----------

print("=" * 80)
print("DIAGNOSTIC: Value overlap per match column")
print("=" * 80)

for col in PAYLOAD_MATCH_COLS:
    ctrl_vals = set(ctrl_pool.select(col).distinct().toPandas()[col].astype(str).tolist())
    test_vals = set(test_pool.select(col).distinct().toPandas()[col].astype(str).tolist())
    overlap = ctrl_vals & test_vals
    ctrl_only = ctrl_vals - test_vals
    test_only = test_vals - ctrl_vals

    print(f"\n--- {col} ---")
    print(f"  Ctrl distinct: {len(ctrl_vals)}  |  Test distinct: {len(test_vals)}  |  Overlap: {len(overlap)}")
    if len(overlap) == 0:
        print(f"  *** NO OVERLAP — this column blocks ALL matches ***")
        print(f"  Ctrl values: {sorted(ctrl_vals)[:15]}")
        print(f"  Test values: {sorted(test_vals)[:15]}")
    elif len(overlap) <= 10:
        print(f"  Overlapping values: {sorted(overlap)}")
        if ctrl_only and len(ctrl_only) <= 5:
            print(f"  Ctrl-only: {sorted(ctrl_only)}")
        if test_only and len(test_only) <= 5:
            print(f"  Test-only: {sorted(test_only)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Drop-one-column test — matches when each column is removed

# COMMAND ----------

print("=" * 80)
print("DIAGNOSTIC: Matches when dropping one column at a time")
print("=" * 80)
print(f"{'Dropped Column':<30} {'Raw Matches':>12}")
print("-" * 45)

for drop_col in PAYLOAD_MATCH_COLS:
    reduced_cols = [c for c in PAYLOAD_MATCH_COLS if c != drop_col]
    try:
        n = ctrl_pool.join(test_pool, on=reduced_cols, how="inner").count()
    except Exception:
        n = -1
    marker = " <<<" if n > 0 else ""
    print(f"{drop_col:<30} {n:>12,}{marker}")

all_match = ctrl_pool.join(test_pool, on=PAYLOAD_MATCH_COLS, how="inner").count()
print(f"\n{'ALL columns':<30} {all_match:>12,}")

if all_match == 0:
    print("\n--- Trying progressively fewer columns ---")
    for keep_n in range(len(PAYLOAD_MATCH_COLS) - 1, 0, -1):
        subset = PAYLOAD_MATCH_COLS[:keep_n]
        n = ctrl_pool.join(test_pool, on=subset, how="inner").count()
        if n > 0:
            print(f"  First matches with {keep_n} columns: {n:,}")
            print(f"  Columns kept: {subset}")
            print(f"  Column that broke it: {PAYLOAD_MATCH_COLS[keep_n]}")
            break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Match pairs on payload INPUT fields

# COMMAND ----------

matched = ctrl_pool.join(test_pool, on=PAYLOAD_MATCH_COLS, how="inner")

matched = matched.withColumn(
    "ts_diff_hours",
    F.abs(F.unix_timestamp("ctrl_ts") - F.unix_timestamp("test_ts")) / 3600.0
)

w = Window.partitionBy("test_ref").orderBy("ts_diff_hours")
best = matched.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

w2 = Window.partitionBy("ctrl_ref").orderBy("ts_diff_hours")
best = best.withColumn("_rn", F.row_number().over(w2)).filter(F.col("_rn") == 1).drop("_rn")

best = best.orderBy("ts_diff_hours")
best.cache()

n_pairs = best.count()
print(f"\nMatched pairs (1:1, ACCEPT-success vs THREE_DS-fail): {n_pairs:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Fallback — relaxed matching if no strict pairs found
# MAGIC
# MAGIC Try progressively relaxed matching, then a no-constraint closest-time pairing.

# COMMAND ----------

if n_pairs == 0:
    print("No strict pairs found. Trying relaxed matching...\n")

    RELAXED_LEVELS = [
        ("processor + network + currency",
         ["pl_processor", "pl_card_network", "pl_currency"]),
        ("processor + network",
         ["pl_processor", "pl_card_network"]),
        ("processor only",
         ["pl_processor"]),
        ("network only",
         ["pl_card_network"]),
    ]

    for label, cols in RELAXED_LEVELS:
        try:
            n_raw = ctrl_pool.join(test_pool, on=cols, how="inner").count()
        except Exception:
            n_raw = 0
        print(f"  {label:<40} → {n_raw:>8,} raw matches")

        if n_raw > 0 and n_pairs == 0:
            relaxed_matched = ctrl_pool.join(test_pool, on=cols, how="inner")
            relaxed_matched = relaxed_matched.withColumn(
                "ts_diff_hours",
                F.abs(F.unix_timestamp("ctrl_ts") - F.unix_timestamp("test_ts")) / 3600.0
            )
            w_r = Window.partitionBy("test_ref").orderBy("ts_diff_hours")
            best = relaxed_matched.withColumn("_rn", F.row_number().over(w_r)).filter(F.col("_rn") == 1).drop("_rn")
            w_r2 = Window.partitionBy("ctrl_ref").orderBy("ts_diff_hours")
            best = best.withColumn("_rn", F.row_number().over(w_r2)).filter(F.col("_rn") == 1).drop("_rn")
            best = best.orderBy("ts_diff_hours")
            best.cache()
            n_pairs = best.count()
            print(f"  >>> Using '{label}' — {n_pairs:,} 1:1 pairs")
            break

if n_pairs == 0:
    print("\nStill no pairs. Using NO-CONSTRAINT closest-time pairing...")
    ctrl_flat = ctrl_pool.withColumn("_side", F.lit("ctrl"))
    test_flat = test_pool.withColumn("_side", F.lit("test"))
    no_constraint = ctrl_pool.crossJoin(
        test_pool.limit(200)
    ).withColumn(
        "ts_diff_hours",
        F.abs(F.unix_timestamp("ctrl_ts") - F.unix_timestamp("test_ts")) / 3600.0
    )
    w_nc = Window.partitionBy("test_ref").orderBy("ts_diff_hours")
    best = no_constraint.withColumn("_rn", F.row_number().over(w_nc)).filter(F.col("_rn") == 1).drop("_rn")
    w_nc2 = Window.partitionBy("ctrl_ref").orderBy("ts_diff_hours")
    best = best.withColumn("_rn", F.row_number().over(w_nc2)).filter(F.col("_rn") == 1).drop("_rn")
    best = best.orderBy("ts_diff_hours")
    best.cache()
    n_pairs = best.count()
    print(f"  No-constraint pairs: {n_pairs:,}")

print(f"\nFinal pair count for analysis: {n_pairs:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Pair Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Distribution of pairs by matched INPUT fields

# COMMAND ----------

if n_pairs > 0:
    pair_dist = (
        best
        .groupBy("pl_processor", "pl_card_network", "pl_funding_type", "pl_currency", "pl_amount_bucket")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )

    print(f"\n{'=' * 90}")
    print(f"Matched pairs by processor × network × funding × currency × amount")
    print(f"{'=' * 90}")
    print(f"{'Processor':<18} {'Network':<12} {'Funding':<10} {'Currency':<10} {'Amount':<12} {'Pairs':>6}")
    print("-" * 72)
    for _, r in pair_dist.head(25).iterrows():
        print(f"{str(r['pl_processor']):<18} {str(r['pl_card_network']):<12} "
              f"{str(r['pl_funding_type']):<10} {str(r['pl_currency']):<10} "
              f"{str(r['pl_amount_bucket']):<12} {r['pairs']:>6,}")

    print(f"\n--- By routing flag ---")
    flag_dist = best.groupBy("pl_routing_flag").agg(F.count("*").alias("pairs")).orderBy(F.desc("pairs")).toPandas()
    for _, r in flag_dist.iterrows():
        print(f"  {str(r['pl_routing_flag']):<25} {r['pairs']:>6,} ({r['pairs']/n_pairs:.1%})")

    print(f"\n--- By payment_flow × attempt_type ---")
    flow_dist = best.groupBy("payment_flow", "attempt_type").agg(F.count("*").alias("pairs")).orderBy(F.desc("pairs")).toPandas()
    for _, r in flow_dist.iterrows():
        print(f"  {str(r['payment_flow']):<20} {str(r['attempt_type']):<15} {r['pairs']:>6,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Non-matched field comparison (skipRisk, usage_type, order_country)

# COMMAND ----------

if n_pairs > 0:
    skip_compare = (
        best
        .groupBy("ctrl_pl_skip_risk", "test_pl_skip_risk")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )
    print(f"\n{'=' * 60}")
    print(f"skipRisk: Control vs Test")
    print(f"{'=' * 60}")
    print(f"{'Ctrl':<25} {'Test':<25} {'Pairs':>8}")
    print("-" * 60)
    for _, r in skip_compare.iterrows():
        print(f"{str(r['ctrl_pl_skip_risk']):<25} {str(r['test_pl_skip_risk']):<25} {r['pairs']:>8,}")

    usage_compare = (
        best
        .groupBy("ctrl_pl_usage_type", "test_pl_usage_type")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )
    print(f"\n{'=' * 60}")
    print(f"usage_type: Control vs Test")
    print(f"{'=' * 60}")
    for _, r in usage_compare.iterrows():
        print(f"{str(r['ctrl_pl_usage_type']):<25} {str(r['test_pl_usage_type']):<25} {r['pairs']:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Fraud source comparison

# COMMAND ----------

if n_pairs > 0:
    src_compare = (
        best
        .groupBy("ctrl_pl_fraud_source", "test_pl_fraud_source")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )

    print(f"\n{'=' * 70}")
    print(f"Fraud source: Control vs Test (skipRisk & high_risk are matched)")
    print(f"{'=' * 70}")
    print(f"{'Ctrl Source':<25} {'Test Source':<25} {'Pairs':>8} {'Share':>8}")
    print("-" * 70)
    for _, r in src_compare.iterrows():
        print(f"{str(r['ctrl_pl_fraud_source']):<25} {str(r['test_pl_fraud_source']):<25} "
              f"{r['pairs']:>8,} {r['pairs']/n_pairs:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Error codes in THREE_DS test failures

# COMMAND ----------

if n_pairs > 0:
    err_dist = (
        best
        .groupBy("test_error_code", "test_error_type", "test_error_decline_type")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )

    print(f"\n{'=' * 80}")
    print(f"Error codes in matched THREE_DS test failures")
    print(f"{'=' * 80}")
    print(f"{'Error Code':<30} {'Error Type':<18} {'Decline Type':<18} {'Pairs':>6} {'Share':>7}")
    print("-" * 83)
    for _, r in err_dist.head(20).iterrows():
        print(f"{str(r['test_error_code']):<30} {str(r['test_error_type']):<18} "
              f"{str(r['test_error_decline_type']):<18} {r['pairs']:>6,} {r['pairs']/n_pairs:>7.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 3DS outcome in test failures

# COMMAND ----------

if n_pairs > 0:
    tds_outcome = (
        best
        .groupBy("test_pl_3ds_response", "test_pl_3ds_challenge", "test_pl_3ds_version")
        .agg(F.count("*").alias("pairs"))
        .orderBy(F.desc("pairs"))
        .toPandas()
    )

    print(f"\n{'=' * 80}")
    print(f"3DS outcome in matched test failures")
    print(f"{'=' * 80}")
    print(f"{'3DS Response':<22} {'Challenge':<12} {'Version':<10} {'Pairs':>8} {'Share':>8}")
    print("-" * 64)
    for _, r in tds_outcome.iterrows():
        print(f"{str(r['test_pl_3ds_response']):<22} {str(r['test_pl_3ds_challenge']):<12} "
              f"{str(r['test_pl_3ds_version']):<10} {r['pairs']:>8,} {r['pairs']/n_pairs:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Time proximity stats

# COMMAND ----------

if n_pairs > 0:
    ts_stats = best.select("ts_diff_hours").toPandas()
    print(f"Time difference between matched pairs (hours):")
    print(f"  Median : {ts_stats['ts_diff_hours'].median():.1f}h")
    print(f"  Mean   : {ts_stats['ts_diff_hours'].mean():.1f}h")
    print(f"  < 1h   : {(ts_stats['ts_diff_hours'] < 1).sum():,} pairs")
    print(f"  < 3h   : {(ts_stats['ts_diff_hours'] < 3).sum():,} pairs")
    print(f"  < 6h   : {(ts_stats['ts_diff_hours'] < 6).sum():,} pairs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 Sample pairs — full side-by-side comparison

# COMMAND ----------

if n_pairs > 0:
    display(
        best
        .select(
            "ctrl_ref", "test_ref",
            # Matched fields (all same between ctrl & test)
            "pl_processor", "pl_card_network", "pl_funding_type",
            "pl_currency", "pl_amount_bucket", "pl_routing_flag",
            "pl_high_risk",
            "ctrl_pl_skip_risk", "test_pl_skip_risk",
            "payment_flow", "attempt_type", "customer_attempt_rank",
            # Compare: fraud decision (differs)
            "ctrl_pl_fraud", "test_pl_fraud",
            "ctrl_pl_fraud_source", "test_pl_fraud_source",
            # Compare: merchant ID
            "ctrl_pl_mid", "test_pl_mid",
            # Compare: 3DS outcome
            "test_pl_3ds_response", "test_pl_3ds_challenge",
            # Compare: error
            "test_error_code", "test_response_code",
            # Time
            "ctrl_ts", "test_ts", "ts_diff_hours",
        )
        .orderBy("ts_diff_hours")
        .limit(50)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Deep Payload Comparison for Closest Pairs
# MAGIC
# MAGIC For the closest matched pairs, retrieve the full `request_payload`
# MAGIC from the notification table and display them side by side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Full payload comparison for the 10 closest pairs

# COMMAND ----------

if n_pairs > 0:
    top_pairs = best.orderBy("ts_diff_hours").limit(10).toPandas()

    for idx, pair in top_pairs.iterrows():
        print(f"\n{'=' * 120}")
        print(f"PAIR {idx+1}  |  ts_diff = {pair['ts_diff_hours']:.2f}h")
        print(f"  Matched: processor={pair['pl_processor']}  network={pair['pl_card_network']}  "
              f"issuer={pair['pl_issuer_country']}  funding={pair['pl_funding_type']}  "
              f"currency={pair['pl_currency']}  amount={pair['pl_amount_bucket']}")
        print(f"           routingFlag={pair['pl_routing_flag']}  skipRisk={pair['pl_skip_risk']}  "
              f"highRisk={pair['pl_high_risk']}  flow={pair['payment_flow']}  "
              f"attempt_type={pair['attempt_type']}  rank={pair['customer_attempt_rank']}")
        print(f"  CTRL: ref={pair['ctrl_ref']}  fraud={pair['ctrl_pl_fraud']}  "
              f"fraudSrc={pair['ctrl_pl_fraud_source']}  mid={pair['ctrl_pl_mid']}")
        print(f"  TEST: ref={pair['test_ref']}  fraud={pair['test_pl_fraud']}  "
              f"fraudSrc={pair['test_pl_fraud_source']}  mid={pair['test_pl_mid']}  "
              f"3ds={pair['test_pl_3ds_response']}  error={pair['test_error_code']}")
        print(f"{'=' * 120}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Raw request_payload JSON for a closest pair
# MAGIC
# MAGIC Retrieve the full JSON payloads for the single closest pair to
# MAGIC enable detailed field-by-field comparison.

# COMMAND ----------

if n_pairs > 0:
    top1 = best.orderBy("ts_diff_hours").limit(1).toPandas()
    ctrl_ref_val = top1.iloc[0]["ctrl_ref"]
    test_ref_val = top1.iloc[0]["test_ref"]

    print(f"Closest pair:")
    print(f"  Control ref: {ctrl_ref_val}")
    print(f"  Test ref:    {test_ref_val}")

    print(f"\n{'─' * 60}")
    print(f"CONTROL — full request_payload:")
    print(f"{'─' * 60}")
    display(
        spark.sql(f"""
            SELECT request_payload
            FROM {NOTIF_TABLE}
            WHERE request_payload:payment.id::string = '{ctrl_ref_val}'
              AND dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
            ORDER BY dbz_timestamp DESC
            LIMIT 1
        """)
    )

    print(f"\n{'─' * 60}")
    print(f"TEST — full request_payload:")
    print(f"{'─' * 60}")
    display(
        spark.sql(f"""
            SELECT request_payload
            FROM {NOTIF_TABLE}
            WHERE request_payload:payment.id::string = '{test_ref_val}'
              AND dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
            ORDER BY dbz_timestamp DESC
            LIMIT 1
        """)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Key metadata fields side-by-side for top 5 pairs

# COMMAND ----------

if n_pairs > 0:
    top5_refs = best.orderBy("ts_diff_hours").limit(5).select("ctrl_ref", "test_ref").toPandas()

    all_refs = list(top5_refs["ctrl_ref"]) + list(top5_refs["test_ref"])
    refs_str = ", ".join([f"'{r}'" for r in all_refs])

    metadata_df = spark.sql(f"""
        SELECT
            request_payload:payment.id::string                                           AS payment_id,
            request_payload:payment.processor.name::string                               AS processor,
            request_payload:payment.processor.processorMerchantId::string                AS merchant_id,
            request_payload:payment.riskData.fraudChecks.source::string                  AS fraud_source,
            request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string  AS fraud_result,
            request_payload:payment.metadata.customRoutingFlag::string                   AS routing_flag,
            request_payload:payment.metadata.skipRisk::string                            AS skip_risk,
            request_payload:payment.metadata.high_risk::string                           AS high_risk,
            request_payload:payment.metadata.customer_verified::string                   AS customer_verified,
            request_payload:payment.paymentMethod.threeDSecureAuthentication.responseCode::string AS tds_response,
            request_payload:payment.paymentMethod.threeDSecureAuthentication.challengeIssued::string AS tds_challenge,
            request_payload:payment.paymentMethod.paymentMethodData.network::string      AS card_network,
            request_payload:payment.paymentMethod.paymentMethodData.binData.issuerCountryCode::string AS issuer_country,
            request_payload:payment.paymentMethod.paymentMethodData.binData.accountFundingType::string AS funding_type,
            request_payload:payment.amount::int                                          AS amount,
            request_payload:payment.currencyCode::string                                 AS currency,
            request_payload:payment.status::string                                       AS payment_status
        FROM {NOTIF_TABLE}
        WHERE request_payload:payment.id::string IN ({refs_str})
          AND dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY request_payload:payment.id::string
            ORDER BY dbz_timestamp DESC
        ) = 1
    """)

    display(metadata_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Exportable Temp Views

# COMMAND ----------

if n_pairs > 0:
    best.createOrReplaceTempView("us_payload_pairs_accept_vs_threeds")
    print(f"Created temp view: us_payload_pairs_accept_vs_threeds ({n_pairs:,} rows)")

    ctrl_refs = best.select(F.col("ctrl_ref").alias("payment_provider_reference"))
    test_refs = best.select(F.col("test_ref").alias("payment_provider_reference"))
    all_refs = ctrl_refs.union(test_refs)
    all_refs.createOrReplaceTempView("paired_references")
    print(f"Created temp view: paired_references ({all_refs.count():,} refs)")

    print(f"""
Use these views for ad-hoc exploration:

  -- All paired references with payload metadata
  SELECT
      pr.payment_provider_reference,
      n.request_payload:payment.processor.name::string AS processor,
      n.request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string AS fraud_result,
      n.request_payload:payment.metadata.customRoutingFlag::string AS routing_flag,
      n.request_payload
  FROM paired_references pr
  JOIN {NOTIF_TABLE} n
    ON pr.payment_provider_reference = n.request_payload:payment.id::string
    AND n.dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'

  -- Browse matched pairs by processor & card network
  SELECT *
  FROM us_payload_pairs_accept_vs_threeds
  WHERE pl_processor = 'CHECKOUT'
    AND pl_card_network = 'VISA'
  ORDER BY ts_diff_hours

  -- Pairs where fraud source differs
  SELECT *
  FROM us_payload_pairs_accept_vs_threeds
  WHERE ctrl_pl_fraud_source != test_pl_fraud_source
  ORDER BY ts_diff_hours

  -- Pairs by routing flag
  SELECT pl_routing_flag, pl_processor, COUNT(*) AS pairs
  FROM us_payload_pairs_accept_vs_threeds
  GROUP BY 1, 2
  ORDER BY pairs DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Explore `payment__payment_provider_notification` schema & verify `request_payload` fields |
# MAGIC | 2 | Extract US experiment data for April 2–7 (customer attempts only) |
# MAGIC | 3 | Join via `payment_provider_reference = request_payload:payment.id`, extract payload match & compare fields |
# MAGIC | 4 | Build 1:1 matched pairs on **13 parameters** — 10 payload (incl. processor, routing flag, risk flags) + 3 fact (flow, rank, attempt_type) |
# MAGIC | 5 | Pair analysis: fraud source diff, non-matched fields, errors, 3DS outcome, time proximity |
# MAGIC | 6 | Full `request_payload` JSON for closest pairs — enables field-by-field diff |
# MAGIC | 7 | Temp views for ad-hoc SQL exploration |
# MAGIC
# MAGIC **Key questions this answers:**
# MAGIC - With **same processor, routing flag, risk flags, card, amount, flow** —
# MAGIC   why does control get ACCEPT and test get THREE_DS?
# MAGIC - Does the **fraud source** differ between matched pairs?
# MAGIC - What **3DS outcome** do test failures get (challenge? auth failure?)?
# MAGIC - What **error codes** cause the THREE_DS test failures?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of US payload-level matched pairs analysis*
