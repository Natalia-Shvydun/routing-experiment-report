# Databricks notebook source
# MAGIC %md
# MAGIC # Market-Level Root Cause Analysis
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house`
# MAGIC
# MAGIC **Goal:** Identify all underperforming markets, diagnose whether the drop is caused by
# MAGIC routing differences (provider split) or fraud/3DS context changes, and provide
# MAGIC 3 example `payment_provider_reference` values per issue for manual verification.
# MAGIC
# MAGIC **Filters:** `pay_now` + `rnpl_pay_early`, all attempts, CIT, `payment_card` only.

# COMMAND ----------

import json
import math
from collections import defaultdict
from pyspark.sql import functions as F

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START = "2026-04-02"

JPM_CURRENCIES = [
    "CZK","DKK","EUR","GBP","HKD","JPY",
    "MXN","NOK","NZD","PLN","SEK","SGD","USD","ZAR",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Data Extraction
# MAGIC Pull experiment assignments + fact_payment_attempt (payment_card, pay_now/rnpl_pay_early).

# COMMAND ----------

raw = spark.sql(f"""
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
    p.payment_flow,
    p.payment_initiator_type,
    p.payment_method,
    p.payment_method_variant,
    p.payment_processor,
    p.fraud_pre_auth_result,
    p.challenge_issued,
    p.is_customer_attempt_successful,
    p.system_attempt_rank,
    p.high_risk,
    p.bin_network,
    p.bin_issuer_name,
    p.bin_account_funding_type,
    p.amount,
    p.amount_eur,
    DATE(p.payment_attempt_created) AS attempt_date
FROM production.payments.fact_payment_attempt p
JOIN assignment a ON p.visitor_id = a.visitor_id
WHERE DATE(p.payment_attempt_created) >= '{PAYMENT_START}'
  AND p.payment_method = 'payment_card'
  AND p.payment_flow IN ('pay_now', 'rnpl_pay_early')
""")

raw.cache()
print(f"Total rows: {raw.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Identify Underperforming Markets

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def z_test(s1, n1, s2, n2):
    if n1 == 0 or n2 == 0:
        return 0, 1, False
    p1, p2 = s1/n1, s2/n2
    delta = p2 - p1
    pooled = (s1+s2)/(n1+n2)
    if pooled in (0, 1):
        return delta, 1, False
    se = math.sqrt(pooled*(1 - pooled)*(1/n1 + 1/n2))
    z = delta / se
    p = 2 * (1 - 0.5*(1 + math.erf(abs(z)/math.sqrt(2))))
    return delta, p, p < 0.05

country_stats = (
    raw
    .groupBy("bin_issuer_country_code", "group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successful"),
    )
    .toPandas()
)

results = []
for cc, grp in country_stats.groupby("bin_issuer_country_code"):
    ctrl = grp[grp["group_name"].str.lower() == "control"]
    test = grp[grp["group_name"].str.lower() == "test"]
    if ctrl.empty or test.empty:
        continue
    ca, cs = int(ctrl["attempts"].sum()), int(ctrl["successful"].sum())
    ta, ts = int(test["attempts"].sum()), int(test["successful"].sum())
    if ca < 100 or ta < 100:
        continue
    delta, p, sig = z_test(cs, ca, ts, ta)
    results.append({
        "country": cc,
        "ctrl_att": ca, "ctrl_sr": round(cs/ca, 4),
        "test_att": ta, "test_sr": round(ts/ta, 4),
        "delta_pp": round(delta*100, 2),
        "p_value": round(p, 6),
        "sig": sig,
    })

import pandas as pd
results_df = pd.DataFrame(results).sort_values("delta_pp")
print("=== SIGNIFICANTLY UNDERPERFORMING MARKETS ===")
sig_neg = results_df[(results_df["sig"]) & (results_df["delta_pp"] < 0)]
display(spark.createDataFrame(sig_neg))

underperforming = list(sig_neg["country"])
print(f"\n{len(underperforming)} markets: {underperforming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Root Cause Diagnosis per Market
# MAGIC For each underperforming market, drill down into:
# MAGIC 1. Provider split (routing differences)
# MAGIC 2. Provider performance within same routing
# MAGIC 3. Fraud pre-auth result distribution (ACCEPT vs THREE_DS vs REFUSE)
# MAGIC 4. 3DS challenge rate & frictionless SR
# MAGIC 5. processor=none share (routing failures)

# COMMAND ----------

raw_pd = raw.filter(
    F.col("bin_issuer_country_code").isin(underperforming)
).toPandas()

raw_pd["group"] = raw_pd["group_name"].str.lower()
raw_pd["success"] = raw_pd["is_customer_attempt_successful"].astype(int)

def analyze_dimension(df, cc, dim_col, label):
    cc_df = df[df["bin_issuer_country_code"] == cc]
    c_tot = len(cc_df[cc_df["group"] == "control"])
    t_tot = len(cc_df[cc_df["group"] == "test"])
    rows = []
    for val, vdf in cc_df.groupby(dim_col):
        ctrl = vdf[vdf["group"] == "control"]
        test = vdf[vdf["group"] == "test"]
        ca, cs = len(ctrl), int(ctrl["success"].sum())
        ta, ts = len(test), int(test["success"].sum())
        sr_delta, sr_p, sr_sig = z_test(cs, ca, ts, ta)
        c_share = ca/c_tot if c_tot else 0
        t_share = ta/t_tot if t_tot else 0
        share_delta, share_p, share_sig = z_test(
            round(c_share*c_tot), c_tot, round(t_share*t_tot), t_tot
        )
        rows.append({
            "country": cc, "dimension": label, "value": str(val),
            "ctrl_att": ca, "ctrl_share": round(c_share, 3),
            "ctrl_sr": round(cs/ca, 4) if ca else 0,
            "test_att": ta, "test_share": round(t_share, 3),
            "test_sr": round(ts/ta, 4) if ta else 0,
            "share_delta_pp": round(share_delta*100, 2),
            "share_sig": share_sig,
            "sr_delta_pp": round(sr_delta*100, 2),
            "sr_sig": sr_sig,
        })
    return rows

all_rows = []
for cc in underperforming:
    all_rows += analyze_dimension(raw_pd, cc, "payment_processor", "Provider")
    all_rows += analyze_dimension(raw_pd, cc, "fraud_pre_auth_result", "Fraud Pre-Auth")
    all_rows += analyze_dimension(raw_pd, cc, "challenge_issued", "3DS Challenge")

diagnosis_df = pd.DataFrame(all_rows)
display(spark.createDataFrame(diagnosis_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Classify Root Cause per Market

# COMMAND ----------

root_causes = []
for cc in underperforming:
    cc_diag = diagnosis_df[diagnosis_df["country"] == cc]
    causes = []

    # Check processor=none share increase
    none_row = cc_diag[(cc_diag["dimension"] == "Provider") & (cc_diag["value"] == "none")]
    if not none_row.empty:
        nr = none_row.iloc[0]
        if nr["share_sig"] and nr["share_delta_pp"] > 0:
            causes.append(f"Routing failure: processor=none share +{nr['share_delta_pp']:.1f}pp (sig)")
        elif nr["share_delta_pp"] > 2:
            causes.append(f"Routing failure: processor=none share +{nr['share_delta_pp']:.1f}pp (not sig but elevated)")

    # Check REFUSE share increase
    refuse_row = cc_diag[(cc_diag["dimension"] == "Fraud Pre-Auth") & (cc_diag["value"] == "REFUSE")]
    if not refuse_row.empty:
        rr = refuse_row.iloc[0]
        if rr["share_sig"] and rr["share_delta_pp"] > 0:
            causes.append(f"Fraud context: REFUSE share +{rr['share_delta_pp']:.1f}pp (sig)")

    # Check THREE_DS share increase
    tds_row = cc_diag[(cc_diag["dimension"] == "Fraud Pre-Auth") & (cc_diag["value"] == "THREE_DS")]
    if not tds_row.empty:
        tr = tds_row.iloc[0]
        if tr["share_sig"] and tr["share_delta_pp"] > 0:
            causes.append(f"Fraud context: THREE_DS share +{tr['share_delta_pp']:.1f}pp (sig)")
        if tr["sr_sig"] and tr["sr_delta_pp"] < 0:
            causes.append(f"3DS performance: THREE_DS SR {tr['sr_delta_pp']:+.1f}pp (sig)")

    # Check ACCEPT share decrease
    acc_row = cc_diag[(cc_diag["dimension"] == "Fraud Pre-Auth") & (cc_diag["value"] == "ACCEPT")]
    if not acc_row.empty:
        ar = acc_row.iloc[0]
        if ar["share_sig"] and ar["share_delta_pp"] < 0:
            causes.append(f"Fraud context: ACCEPT share {ar['share_delta_pp']:+.1f}pp (sig)")
        if ar["sr_sig"] and ar["sr_delta_pp"] < 0:
            causes.append(f"Provider performance: ACCEPT SR {ar['sr_delta_pp']:+.1f}pp (sig)")

    # Check frictionless SR drop
    fric_row = cc_diag[(cc_diag["dimension"] == "3DS Challenge") & (cc_diag["value"].isin(["false", "False"]))]
    if not fric_row.empty:
        fr = fric_row.iloc[0]
        if fr["sr_sig"] and fr["sr_delta_pp"] < 0:
            causes.append(f"3DS: frictionless SR {fr['sr_delta_pp']:+.1f}pp (sig)")
        if fr["share_sig"] and fr["share_delta_pp"] > 0:
            causes.append(f"3DS: frictionless share +{fr['share_delta_pp']:.1f}pp (sig)")

    # Check provider performance drops
    for _, row in cc_diag[(cc_diag["dimension"] == "Provider") & (cc_diag["sr_sig"]) & (cc_diag["sr_delta_pp"] < -1)].iterrows():
        if row["value"] != "none":
            causes.append(f"Provider perf: {row['value']} SR {row['sr_delta_pp']:+.1f}pp (sig)")

    # Classify
    has_routing = any("Routing" in c or "split" in c.lower() for c in causes)
    has_fraud_3ds = any("Fraud" in c or "3DS" in c or "3ds" in c or "frictionless" in c for c in causes)
    has_provider_perf = any("Provider perf" in c for c in causes)

    if has_routing and has_fraud_3ds:
        category = "MIXED: Routing + Fraud/3DS"
    elif has_routing:
        category = "ROUTING"
    elif has_fraud_3ds:
        category = "FRAUD/3DS CONTEXT"
    elif has_provider_perf:
        category = "PROVIDER PERFORMANCE"
    else:
        category = "UNCLEAR (small volume or borderline)"

    # None share numbers
    c_n = none_row.iloc[0]["ctrl_share"] if not none_row.empty else 0
    t_n = none_row.iloc[0]["test_share"] if not none_row.empty else 0

    overall = results_df[results_df["country"] == cc].iloc[0]
    root_causes.append({
        "country": cc,
        "ctrl_att": int(overall["ctrl_att"]),
        "test_att": int(overall["test_att"]),
        "ctrl_sr": f"{overall['ctrl_sr']:.2%}",
        "test_sr": f"{overall['test_sr']:.2%}",
        "delta_pp": f"{overall['delta_pp']:+.2f}pp",
        "p_value": round(overall["p_value"], 6),
        "none_share_ctrl": f"{c_n:.1%}",
        "none_share_test": f"{t_n:.1%}",
        "category": category,
        "root_causes": " | ".join(causes) if causes else "No single dominant driver",
    })

rc_df = pd.DataFrame(root_causes)
display(spark.createDataFrame(rc_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Sample References for Verification
# MAGIC For each underperforming market, find 3 examples of:
# MAGIC - Test FAILED transactions (with details)
# MAGIC - Control SUCCESSFUL transactions (for comparison)

# COMMAND ----------

from pyspark.sql.window import Window

examples_raw = raw.filter(
    F.col("bin_issuer_country_code").isin(underperforming)
).select(
    "bin_issuer_country_code", "group_name",
    "payment_provider_reference", "payment_processor",
    "fraud_pre_auth_result", "challenge_issued",
    "is_customer_attempt_successful", "bin_network",
    "high_risk", "amount_eur", "currency",
)

w_fail = Window.partitionBy("bin_issuer_country_code").orderBy(F.rand(42))
test_failed = (
    examples_raw
    .filter(
        (F.lower(F.col("group_name")) == "test") &
        (F.col("is_customer_attempt_successful") == False)
    )
    .withColumn("rn", F.row_number().over(w_fail))
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

w_succ = Window.partitionBy("bin_issuer_country_code").orderBy(F.rand(43))
ctrl_success = (
    examples_raw
    .filter(
        (F.lower(F.col("group_name")) == "control") &
        (F.col("is_customer_attempt_successful") == True)
    )
    .withColumn("rn", F.row_number().over(w_succ))
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

examples = test_failed.union(ctrl_success).orderBy("bin_issuer_country_code", "group_name")

print("=== EXAMPLE TRANSACTIONS FOR VERIFICATION ===")
display(examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Processor=none Deep Dive
# MAGIC Where processor=none is significantly elevated, look at what fraud results these
# MAGIC attempts had to understand if they were refused by Forter before routing.

# COMMAND ----------

none_deep = (
    raw
    .filter(
        (F.col("bin_issuer_country_code").isin(underperforming)) &
        (F.col("payment_processor") == "none")
    )
    .groupBy("bin_issuer_country_code", "group_name", "fraud_pre_auth_result")
    .agg(F.count("*").alias("attempts"))
    .orderBy("bin_issuer_country_code", "group_name", F.desc("attempts"))
)

display(none_deep)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — 3DS Challenge Rate by Provider per Market
# MAGIC Check if certain providers trigger more 3DS in test.

# COMMAND ----------

tds_by_provider = (
    raw
    .filter(F.col("bin_issuer_country_code").isin(underperforming))
    .groupBy("bin_issuer_country_code", "group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.when(F.col("challenge_issued") == True, 1).otherwise(0)).alias("challenged"),
        F.sum(F.when(F.col("challenge_issued") == False, 1).otherwise(0)).alias("frictionless"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successful"),
    )
    .withColumn("challenge_rate", F.round(F.col("challenged") / F.col("attempts"), 4))
    .withColumn("sr", F.round(F.col("successful") / F.col("attempts"), 4))
    .orderBy("bin_issuer_country_code", "payment_processor", "group_name")
)

display(tds_by_provider)
