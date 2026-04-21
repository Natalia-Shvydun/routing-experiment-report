# Databricks notebook source
# MAGIC %md
# MAGIC # Routing Experiment — Report Data Export
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (started April 2, 2026)
# MAGIC
# MAGIC **Purpose:** Extract payment attempt data, classify each attempt into a routing rule,
# MAGIC pre-aggregate into a dimensional cube, and export as JSON for the interactive report.
# MAGIC
# MAGIC **Output:** `report_data.json` — a JSON file with `{"metadata": {...}, "data": [...]}`.
# MAGIC Copy this file to `public/data.json` in the frontend project.
# MAGIC
# MAGIC | # | Cell |
# MAGIC |---|---|
# MAGIC | 1 | Setup & constants |
# MAGIC | 2 | SQL extraction |
# MAGIC | 3 | Routing rule classification & derived columns |
# MAGIC | 4 | Pre-aggregation |
# MAGIC | 5 | Metadata |
# MAGIC | 6 | JSON export |

# COMMAND ----------

import json
import datetime
from pyspark.sql import functions as F

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START = "2026-04-02"

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

ADYEN_PRIMARY_COUNTRIES = ["AU", "CA", "CH"]
US_COUNTRIES = ["US"]
EU5_COUNTRIES = ["ES", "FR", "DE", "GB", "IT"]
LATAM_PLUS_COUNTRIES = [
    "AR", "BE", "BO", "BR", "BY", "CL", "CO", "EC", "GY", "MX",
    "PE", "PY", "QA", "RS", "SK", "SR", "UY", "VE", "SI",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 2 — SQL Extraction

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
  p.sent_to_issuer,
  p.challenge_issued,
  p.is_three_ds_passed,
  p.payment_attempt_timestamp,
  p.payment_provider_reference
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_method = 'payment_card'
""")

print(f"Raw rows extracted: {df_raw.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 3 — Routing Rule Classification & Derived Columns

# COMMAND ----------

jpm_currency_condition = F.col("currency").isin(JPM_CURRENCIES)
card_network_upper = F.upper(F.col("payment_method_variant"))

df = df_raw.withColumn(
    "routing_rule",
    F.when(
        ~card_network_upper.isin("VISA", "MASTERCARD"),
        F.lit("non-visa-mc-adyen-only")
    ).when(
        F.col("bin_issuer_country_code").isin(ADYEN_PRIMARY_COUNTRIES) & jpm_currency_condition,
        F.lit("adyen-primary-jpm-currency")
    ).when(
        F.col("bin_issuer_country_code").isin(ADYEN_PRIMARY_COUNTRIES),
        F.lit("adyen-primary")
    ).when(
        F.col("bin_issuer_country_code").isin(US_COUNTRIES) & jpm_currency_condition,
        F.lit("us-jpm-primary")
    ).when(
        F.col("bin_issuer_country_code").isin(US_COUNTRIES),
        F.lit("us-no-jpm")
    ).when(
        F.col("bin_issuer_country_code").isin(EU5_COUNTRIES) & jpm_currency_condition,
        F.lit("eu5-jpm-currency")
    ).when(
        F.col("bin_issuer_country_code").isin(EU5_COUNTRIES),
        F.lit("eu5")
    ).when(
        F.col("bin_issuer_country_code").isin(LATAM_PLUS_COUNTRIES) & jpm_currency_condition,
        F.lit("latam-plus-jpm-currency")
    ).when(
        F.col("bin_issuer_country_code").isin(LATAM_PLUS_COUNTRIES),
        F.lit("latam-plus")
    ).when(
        jpm_currency_condition,
        F.lit("catch-all-jpm-currency")
    ).otherwise(
        F.lit("catch-all")
    )
).withColumn(
    "is_first_attempt",
    (F.col("customer_attempt_rank") == 1).cast("boolean")
).withColumn(
    "payment_attempt_date",
    F.col("payment_attempt_timestamp").cast("date").cast("string")
).withColumn(
    "fraud_pre_auth_result",
    F.coalesce(F.col("fraud_pre_auth_result"), F.lit("NULL"))
).withColumn(
    "challenge_issued",
    F.coalesce(F.col("challenge_issued").cast("string"), F.lit("null"))
).withColumn(
    "payment_processor",
    F.coalesce(F.col("payment_processor"), F.lit("none"))
)

df.cache()
print(f"Classified rows: {df.count():,}")

df.groupBy("routing_rule").count().orderBy("count", ascending=False).show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 4 — Pre-Aggregation

# COMMAND ----------

DIMENSION_COLS = [
    "group_name",
    "routing_rule",
    "bin_issuer_country_code",
    "currency",
    "payment_processor",
    "payment_method_variant",
    "payment_initiator_type",
    "is_first_attempt",
    "system_attempt_rank",
    "payment_flow",
    "fraud_pre_auth_result",
    "challenge_issued",
    "payment_attempt_date",
]

cube = df.groupBy(DIMENSION_COLS).agg(
    F.count("*").alias("attempts"),
    F.sum("is_customer_attempt_successful").cast("long").alias("successful"),
    F.sum("sent_to_issuer").cast("long").alias("sent_to_issuer"),
    F.sum("is_three_ds_passed").cast("long").alias("three_ds_passed"),
)

cube.cache()
print(f"Cube rows: {cube.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 5 — Metadata

# COMMAND ----------

metadata = {
    "experiment_name": "Payment Orchestration Routing — In-House vs Primer",
    "experiment_id": EXPERIMENT_ID,
    "assignment_start": ASSIGNMENT_START,
    "data_as_of": datetime.date.today().isoformat(),
    "dimensions": {},
}

filter_dimensions = [
    "bin_issuer_country_code",
    "currency",
    "routing_rule",
    "payment_initiator_type",
    "payment_flow",
    "challenge_issued",
    "system_attempt_rank",
]

for dim in filter_dimensions:
    values = sorted(
        [row[dim] for row in cube.select(dim).distinct().collect() if row[dim] is not None]
    )
    metadata["dimensions"][dim] = values

date_range = cube.agg(
    F.min("payment_attempt_date").alias("min_date"),
    F.max("payment_attempt_date").alias("max_date"),
).collect()[0]
metadata["date_range"] = {"min": date_range["min_date"], "max": date_range["max_date"]}

total_attempts = cube.agg(F.sum("attempts")).collect()[0][0]
metadata["total_attempts"] = int(total_attempts)

visitor_sr = df.groupBy("group_name").agg(
    F.countDistinct("visitor_id").alias("visitors"),
    F.countDistinct(
        F.when(F.col("is_customer_attempt_successful") == 1, F.col("visitor_id"))
    ).alias("successful_visitors"),
).collect()
metadata["visitor_sr"] = {
    row["group_name"]: {
        "visitors": int(row["visitors"]),
        "successful_visitors": int(row["successful_visitors"]),
    }
    for row in visitor_sr
}

print(json.dumps(metadata, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 5b — Sample References
# MAGIC
# MAGIC Collect example `payment_provider_reference` values per key dimension
# MAGIC so analysts can manually verify in Primer / acquirer portals.

# COMMAND ----------

from pyspark.sql import Window as W

SAMPLE_N = 3
sample_dims = ["group_name", "routing_rule", "payment_processor", "bin_issuer_country_code"]

samples_df = df.filter(
    F.col("payment_provider_reference").isNotNull()
).select(
    *sample_dims,
    "payment_provider_reference",
    "is_customer_attempt_successful",
    "fraud_pre_auth_result",
    "challenge_issued",
).withColumn(
    "_rn",
    F.row_number().over(
        W.partitionBy(*sample_dims).orderBy(F.rand())
    )
).filter(F.col("_rn") <= SAMPLE_N).drop("_rn")

samples_list = [row.asDict() for row in samples_df.collect()]
print(f"Collected {len(samples_list)} sample references")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cell 6 — JSON Export
# MAGIC
# MAGIC Export cube to JSON. Download the resulting file and copy it to `public/data.json`
# MAGIC in the frontend project directory.

# COMMAND ----------

cube_pdf = cube.toPandas()

for col in ["attempts", "successful", "sent_to_issuer", "three_ds_passed", "system_attempt_rank"]:
    cube_pdf[col] = cube_pdf[col].astype(int)
cube_pdf["is_first_attempt"] = cube_pdf["is_first_attempt"].astype(bool)

data_records = cube_pdf.to_dict(orient="records")

output = {
    "metadata": metadata,
    "data": data_records,
    "samples": samples_list,
}

json_str = json.dumps(output, separators=(",", ":"))

local_path = "/tmp/report_data.json"
with open(local_path, "w") as f:
    f.write(json_str)

dbfs_path = "dbfs:/tmp/report_data.json"
dbutils.fs.put(dbfs_path.replace("dbfs:", ""), json_str, overwrite=True)

file_size_mb = round(len(json_str) / (1024 * 1024), 2)
print(f"Exported {len(data_records):,} cube rows ({file_size_mb} MB)")
print(f"  Local:  {local_path}")
print(f"  DBFS:   {dbfs_path}")
print(f"\nAutomatic export:  ./scripts/export_data.sh")
print(f"Manual:            databricks fs cp {dbfs_path} public/data.json --overwrite")
