# Databricks notebook source
# MAGIC %md
# MAGIC # US — Investigation of `payment.metadata.field` between Control vs Test
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house`
# MAGIC
# MAGIC **Date:** April 2–7, 2026
# MAGIC
# MAGIC **Question:** The payload field `request_payload:payment.metadata.field`
# MAGIC was observed as `"value"` in control but missing/different in test during
# MAGIC pair comparison. Is this a genuine control-vs-test difference, or is it
# MAGIC driven by another correlated field (processor, routing flag, etc.)?
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Overall distribution of `metadata.field` by variant
# MAGIC 2. Cross-tabs with processor, routing flag, card network, etc.
# MAGIC 3. Check if `metadata.field` is predictable from other fields
# MAGIC
# MAGIC **Sections:**
# MAGIC | # | Section |
# MAGIC |---|---|
# MAGIC | 1 | Setup & data extraction |
# MAGIC | 2 | Overall `metadata.field` distribution by variant |
# MAGIC | 3 | `metadata.field` × processor |
# MAGIC | 4 | `metadata.field` × routing flag |
# MAGIC | 5 | `metadata.field` × fraud result |
# MAGIC | 6 | `metadata.field` × card network / funding type |
# MAGIC | 7 | `metadata.field` × skipRisk / high_risk |
# MAGIC | 8 | Multivariate: controlling for processor + routing flag |
# MAGIC | 9 | Success rate by `metadata.field` value |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
TARGET_DATE_START = "2026-04-02"
TARGET_DATE_END   = "2026-04-07"

NOTIF_TABLE = "production.db_mirror_dbz.payment__payment_provider_notification"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Data Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Extract experiment data & join with notification payload

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
  p.payment_provider_reference,
  p.payment_processor,
  p.payment_flow,
  p.payment_terms,
  p.attempt_type,
  p.platform,
  p.fraud_pre_auth_result,
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.customer_country_code,
  p.gmv_eur_bucket,
  p.currency,
  p.payment_method_variant,
  p.is_customer_attempt_successful,
  p.error_code,
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
print(f"US customer attempts: {df_exp.count():,}")
df_exp.groupBy("group_name").count().orderBy("group_name").show()

groups = sorted(df_exp.select("group_name").distinct().toPandas()["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Join with notification table — extract metadata.field and other key fields

# COMMAND ----------

df_exp.createOrReplaceTempView("us_exp_field")

df = spark.sql(f"""
SELECT
  e.*,
  n.request_payload:payment.metadata.field::string                                         AS md_field,
  n.request_payload:payment.processor.name::string                                         AS pl_processor,
  n.request_payload:payment.metadata.customRoutingFlag::string                             AS pl_routing_flag,
  n.request_payload:payment.metadata.skipRisk::string                                      AS pl_skip_risk,
  n.request_payload:payment.metadata.high_risk::string                                     AS pl_high_risk,
  n.request_payload:payment.metadata.attemptAuthentication::string                         AS pl_attempt_auth,
  n.request_payload:payment.paymentMethod.paymentMethodData.network::string                AS pl_card_network,
  n.request_payload:payment.paymentMethod.paymentMethodData.binData.accountFundingType::string AS pl_funding_type,
  n.request_payload:payment.paymentMethod.paymentMethodData.binData.issuerCountryCode::string AS pl_issuer_country,
  n.request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string            AS pl_fraud_result,
  n.request_payload:payment.riskData.fraudChecks.source::string                            AS pl_fraud_source,
  n.request_payload:payment.status::string                                                 AS pl_payment_status
FROM us_exp_field e
JOIN {NOTIF_TABLE} n
  ON e.payment_provider_reference = n.request_payload:payment.id::string
  AND n.dbz_timestamp::date BETWEEN '{TARGET_DATE_START}' AND '{TARGET_DATE_END}'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.payment_provider_reference
  ORDER BY n.dbz_timestamp DESC
) = 1
""")

df.cache()
joined_count = df.count()
print(f"Joined rows: {joined_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Overall `metadata.field` Distribution by Variant

# COMMAND ----------

field_dist = df.groupBy("group_name", "md_field").count().orderBy("group_name", "md_field").toPandas()
grp_totals = field_dist.groupby("group_name")["count"].sum().to_dict()

print(f"\n{'=' * 70}")
print(f"metadata.field distribution by variant")
print(f"{'=' * 70}")
print(f"{'Group':<15} {'metadata.field':<25} {'Count':>8} {'Share':>8}")
print("-" * 60)
for _, r in field_dist.iterrows():
    total_g = grp_totals.get(r['group_name'], 1)
    print(f"{r['group_name']:<15} {str(r['md_field']):<25} "
          f"{r['count']:>8,} {r['count']/total_g:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null vs non-null breakdown

# COMMAND ----------

df_with_flag = df.withColumn(
    "has_field",
    F.when(F.col("md_field").isNotNull(), "has_value").otherwise("null/missing")
)

has_field_dist = df_with_flag.groupBy("group_name", "has_field").count().orderBy("group_name", "has_field").toPandas()
grp_totals2 = has_field_dist.groupby("group_name")["count"].sum().to_dict()

print(f"\n{'Group':<15} {'has_field':<15} {'Count':>8} {'Share':>8}")
print("-" * 50)
for _, r in has_field_dist.iterrows():
    print(f"{r['group_name']:<15} {r['has_field']:<15} "
          f"{r['count']:>8,} {r['count']/grp_totals2.get(r['group_name'],1):>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — `metadata.field` × Processor
# MAGIC
# MAGIC Is `metadata.field` driven by which processor is used?

# COMMAND ----------

proc_field = (
    df.groupBy("group_name", "pl_processor", "md_field")
    .count()
    .orderBy("group_name", "pl_processor", F.desc("count"))
    .toPandas()
)

proc_totals = df.groupBy("group_name", "pl_processor").count().toPandas()
proc_totals_dict = {(r['group_name'], r['pl_processor']): r['count'] for _, r in proc_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × processor")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Processor':<22} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 72)
for _, r in proc_field.iterrows():
    key = (r['group_name'], r['pl_processor'])
    total_p = proc_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<22} {str(r['md_field']):<20} "
          f"{r['count']:>8,} {r['count']/total_p:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — `metadata.field` × Routing Flag

# COMMAND ----------

flag_field = (
    df.groupBy("group_name", "pl_routing_flag", "md_field")
    .count()
    .orderBy("group_name", "pl_routing_flag", F.desc("count"))
    .toPandas()
)

flag_totals = df.groupBy("group_name", "pl_routing_flag").count().toPandas()
flag_totals_dict = {(r['group_name'], r['pl_routing_flag']): r['count'] for _, r in flag_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × routing flag")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'RoutingFlag':<22} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 72)
for _, r in flag_field.iterrows():
    key = (r['group_name'], r['pl_routing_flag'])
    total_f = flag_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_routing_flag']):<22} {str(r['md_field']):<20} "
          f"{r['count']:>8,} {r['count']/total_f:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — `metadata.field` × Fraud Result

# COMMAND ----------

fraud_field = (
    df.groupBy("group_name", "pl_fraud_result", "md_field")
    .count()
    .orderBy("group_name", "pl_fraud_result", F.desc("count"))
    .toPandas()
)

fraud_totals = df.groupBy("group_name", "pl_fraud_result").count().toPandas()
fraud_totals_dict = {(r['group_name'], r['pl_fraud_result']): r['count'] for _, r in fraud_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × fraud result")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'FraudResult':<22} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 72)
for _, r in fraud_field.iterrows():
    key = (r['group_name'], r['pl_fraud_result'])
    total_fr = fraud_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_fraud_result']):<22} {str(r['md_field']):<20} "
          f"{r['count']:>8,} {r['count']/total_fr:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — `metadata.field` × Card Network / Funding Type

# COMMAND ----------

net_field = (
    df.groupBy("group_name", "pl_card_network", "md_field")
    .count()
    .orderBy("group_name", "pl_card_network", F.desc("count"))
    .toPandas()
)

net_totals = df.groupBy("group_name", "pl_card_network").count().toPandas()
net_totals_dict = {(r['group_name'], r['pl_card_network']): r['count'] for _, r in net_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × card network")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Network':<15} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 65)
for _, r in net_field.iterrows():
    key = (r['group_name'], r['pl_card_network'])
    total_n = net_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_card_network']):<15} {str(r['md_field']):<20} "
          f"{r['count']:>8,} {r['count']/total_n:>8.2%}")

# COMMAND ----------

fund_field = (
    df.groupBy("group_name", "pl_funding_type", "md_field")
    .count()
    .orderBy("group_name", "pl_funding_type", F.desc("count"))
    .toPandas()
)

fund_totals = df.groupBy("group_name", "pl_funding_type").count().toPandas()
fund_totals_dict = {(r['group_name'], r['pl_funding_type']): r['count'] for _, r in fund_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × funding type")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'FundingType':<15} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 65)
for _, r in fund_field.iterrows():
    key = (r['group_name'], r['pl_funding_type'])
    total_ft = fund_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_funding_type']):<15} {str(r['md_field']):<20} "
          f"{r['count']:>8,} {r['count']/total_ft:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — `metadata.field` × skipRisk / high_risk

# COMMAND ----------

risk_field = (
    df.groupBy("group_name", "pl_skip_risk", "pl_high_risk", "md_field")
    .count()
    .orderBy("group_name", "pl_skip_risk", "pl_high_risk", F.desc("count"))
    .toPandas()
)

risk_totals = df.groupBy("group_name", "pl_skip_risk", "pl_high_risk").count().toPandas()
risk_totals_dict = {
    (r['group_name'], r['pl_skip_risk'], r['pl_high_risk']): r['count']
    for _, r in risk_totals.iterrows()
}

print(f"\n{'=' * 90}")
print(f"metadata.field by variant × skipRisk × high_risk")
print(f"{'=' * 90}")
print(f"{'Group':<10} {'skipRisk':<12} {'highRisk':<12} {'field':<20} {'Count':>8} {'Share':>8}")
print("-" * 74)
for _, r in risk_field.iterrows():
    key = (r['group_name'], r['pl_skip_risk'], r['pl_high_risk'])
    total_rk = risk_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_skip_risk']):<12} {str(r['pl_high_risk']):<12} "
          f"{str(r['md_field']):<20} {r['count']:>8,} {r['count']/total_rk:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Multivariate: Controlling for Processor + Routing Flag
# MAGIC
# MAGIC The key question: within the **same processor × routing flag** combination,
# MAGIC does control still have `metadata.field = "value"` more than test?
# MAGIC If yes → it's a genuine variant difference.
# MAGIC If no → it was just driven by processor/routing split.

# COMMAND ----------

multi = (
    df.groupBy("group_name", "pl_processor", "pl_routing_flag", "md_field")
    .count()
    .orderBy("group_name", "pl_processor", "pl_routing_flag", F.desc("count"))
    .toPandas()
)

multi_totals = (
    df.groupBy("group_name", "pl_processor", "pl_routing_flag")
    .count()
    .toPandas()
)
multi_totals_dict = {
    (r['group_name'], r['pl_processor'], r['pl_routing_flag']): r['count']
    for _, r in multi_totals.iterrows()
}

print(f"\n{'=' * 100}")
print(f"metadata.field by variant × processor × routing flag")
print(f"{'=' * 100}")
print(f"{'Group':<10} {'Processor':<18} {'RoutingFlag':<18} {'field':<18} {'Count':>8} {'Share':>8}")
print("-" * 84)
for _, r in multi.iterrows():
    key = (r['group_name'], r['pl_processor'], r['pl_routing_flag'])
    total_m = multi_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<18} {str(r['pl_routing_flag']):<18} "
          f"{str(r['md_field']):<18} {r['count']:>8,} {r['count']/total_m:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Same test — also controlling for skipRisk

# COMMAND ----------

multi2 = (
    df.groupBy("group_name", "pl_processor", "pl_routing_flag", "pl_skip_risk", "md_field")
    .count()
    .orderBy("group_name", "pl_processor", "pl_routing_flag", "pl_skip_risk", F.desc("count"))
    .toPandas()
)

multi2_totals = (
    df.groupBy("group_name", "pl_processor", "pl_routing_flag", "pl_skip_risk")
    .count()
    .toPandas()
)
multi2_totals_dict = {
    (r['group_name'], r['pl_processor'], r['pl_routing_flag'], r['pl_skip_risk']): r['count']
    for _, r in multi2_totals.iterrows()
}

print(f"\n{'=' * 110}")
print(f"metadata.field by variant × processor × routing flag × skipRisk")
print(f"{'=' * 110}")
print(f"{'Group':<10} {'Processor':<18} {'Flag':<15} {'skipRisk':<12} {'field':<18} {'Count':>8} {'Share':>8}")
print("-" * 93)
for _, r in multi2.iterrows():
    key = (r['group_name'], r['pl_processor'], r['pl_routing_flag'], r['pl_skip_risk'])
    total_m2 = multi2_totals_dict.get(key, 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<18} {str(r['pl_routing_flag']):<15} "
          f"{str(r['pl_skip_risk']):<12} {str(r['md_field']):<18} "
          f"{r['count']:>8,} {r['count']/total_m2:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — Success Rate by `metadata.field` Value
# MAGIC
# MAGIC Does having `metadata.field = "value"` correlate with success rate?

# COMMAND ----------

sr_by_field = (
    df.groupBy("group_name", "md_field")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("group_name", "md_field")
    .toPandas()
)

print(f"\n{'=' * 70}")
print(f"Success rate by variant × metadata.field")
print(f"{'=' * 70}")
print(f"{'Group':<15} {'field':<20} {'Attempts':>10} {'SR':>8}")
print("-" * 57)
for _, r in sr_by_field.iterrows():
    sr_str = f"{r['sr']:.2%}" if pd.notna(r['sr']) else "N/A"
    print(f"{r['group_name']:<15} {str(r['md_field']):<20} {r['attempts']:>10,} {sr_str:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 SR by `metadata.field` × processor

# COMMAND ----------

sr_proc = (
    df.groupBy("group_name", "pl_processor", "md_field")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("group_name", "pl_processor", "md_field")
    .toPandas()
)

print(f"\n{'=' * 80}")
print(f"SR by variant × processor × metadata.field")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Processor':<18} {'field':<18} {'Attempts':>10} {'SR':>8}")
print("-" * 68)
for _, r in sr_proc.iterrows():
    sr_str = f"{r['sr']:.2%}" if pd.notna(r['sr']) else "N/A"
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<18} {str(r['md_field']):<18} "
          f"{r['attempts']:>10,} {sr_str:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 SR by `metadata.field` × fraud result

# COMMAND ----------

sr_fraud = (
    df.groupBy("group_name", "pl_fraud_result", "md_field")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("group_name", "pl_fraud_result", "md_field")
    .toPandas()
)

print(f"\n{'=' * 80}")
print(f"SR by variant × fraud result × metadata.field")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'FraudResult':<22} {'field':<18} {'Attempts':>10} {'SR':>8}")
print("-" * 72)
for _, r in sr_fraud.iterrows():
    sr_str = f"{r['sr']:.2%}" if pd.notna(r['sr']) else "N/A"
    print(f"{r['group_name']:<10} {str(r['pl_fraud_result']):<22} {str(r['md_field']):<18} "
          f"{r['attempts']:>10,} {sr_str:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Extract US experiment data, join with notification payload |
# MAGIC | 2 | Overall `metadata.field` distribution: control vs test |
# MAGIC | 3 | Cross-tab with processor — is `field` processor-specific? |
# MAGIC | 4 | Cross-tab with routing flag — is `field` routing-specific? |
# MAGIC | 5 | Cross-tab with fraud result — does `field` correlate with ACCEPT/THREE_DS? |
# MAGIC | 6 | Cross-tab with card network & funding type |
# MAGIC | 7 | Cross-tab with skipRisk / high_risk |
# MAGIC | 8 | **Multivariate control**: within same processor × routing flag (× skipRisk), does variant still predict `field`? |
# MAGIC | 9 | Success rate by `metadata.field` value (overall, by processor, by fraud result) |
# MAGIC
# MAGIC **Interpretation guide:**
# MAGIC - If `metadata.field = "value"` appears **only for certain processors/routing flags**
# MAGIC   and those processors differ between ctrl/test → the `field` difference is
# MAGIC   a side effect of routing, not a root cause.
# MAGIC - If within the **same processor × routing flag**, control still has `"value"`
# MAGIC   and test doesn't → it's a genuine config difference between variants.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of metadata.field investigation*
