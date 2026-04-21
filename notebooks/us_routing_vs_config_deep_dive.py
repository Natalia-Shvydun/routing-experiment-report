# Databricks notebook source
# MAGIC %md
# MAGIC # US Deep Dive — Routing vs Config Effect on SR
# MAGIC
# MAGIC **Experiment:** `pay-payment-orchestration-routing-in-house` (restarted April 2)
# MAGIC
# MAGIC **Key observation:** Test has lower SR, more THREE_DS / less ACCEPT from Forter.
# MAGIC
# MAGIC **Goal:** Disentangle two possible causes:
# MAGIC 1. **Routing effect** — different provider split drives lower SR
# MAGIC 2. **Config/context effect** — same provider, but different Forter/3DS decisions
# MAGIC    (lower context sent, different workflow settings, different attemptAuthentication)
# MAGIC
# MAGIC **Routing dimensions:** country (US fixed here) × JPM/non-JPM currency × card scheme
# MAGIC (amex / visa_mc / other)
# MAGIC
# MAGIC **Sections:**
# MAGIC | # | Section |
# MAGIC |---|---|
# MAGIC | 1 | Setup & data extraction |
# MAGIC | 2 | Overall SR comparison |
# MAGIC | 3 | Fraud result shift — ACCEPT vs THREE_DS vs REFUSE vs THREE_DS_EXEMPTION |
# MAGIC | 4 | SR within each fraud result — is SR lower even with same fraud decision? |
# MAGIC | 5 | Routing segment analysis — provider split by currency_type × card_scheme |
# MAGIC | 6 | Provider-level SR — same provider, different outcome? |
# MAGIC | 7 | Decomposition — how much SR gap is routing vs config? |
# MAGIC | 8 | Fraud result by provider — which provider drives the THREE_DS shift? |
# MAGIC | 9 | 3DS analysis — challenge rate, 3DS SR, frictionless vs challenged |
# MAGIC | 10 | Payload context — metadata fields that differ between variants |
# MAGIC | 11 | Daily trends |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from scipy.stats import norm

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-04-02"
PAYMENT_START    = "2026-04-02"

JPM_CURRENCIES = [
    "CZK", "DKK", "EUR", "GBP", "HKD", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "USD", "ZAR",
]

NOTIF_TABLE = "production.db_mirror_dbz.payment__payment_provider_notification"

def two_proportion_z_test(s1, n1, s2, n2):
    if n1 == 0 or n2 == 0:
        return None, None
    p1, p2 = s1 / n1, s2 / n2
    p_pool = (s1 + s2) / (n1 + n2)
    if p_pool == 0 or p_pool == 1:
        return p1 - p2, None
    se = np.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
    z = (p1 - p2) / se
    pval = 2 * (1 - norm.cdf(abs(z)))
    return p1 - p2, pval

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Setup & Data Extraction

# COMMAND ----------

df = spark.sql(f"""
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
  AND p.payment_attempt_timestamp >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
  AND p.bin_issuer_country_code = 'US'
  AND p.system_attempt_rank = 1
""")

df = df.withColumn(
    "card_scheme",
    F.when(F.lower(F.col("payment_method_variant")).contains("amex"), "amex")
     .when(F.lower(F.col("payment_method_variant")).isin("visa", "mastercard"), "visa_mc")
     .otherwise("other")
).withColumn(
    "currency_type",
    F.when(F.col("currency").isin(JPM_CURRENCIES), "jpm_currency")
     .otherwise("non_jpm_currency")
).withColumn(
    "routing_segment",
    F.concat_ws(" | ", F.col("currency_type"), F.col("card_scheme"))
).withColumn(
    "challenged",
    F.when(F.col("challenge_issued").cast("boolean") == True, 1).otherwise(0)
).withColumn(
    "pay_date",
    F.col("payment_attempt_timestamp").cast("date")
)

df.cache()

total = df.count()
print(f"US customer attempts (from {PAYMENT_START}): {total:,}")
df.groupBy("group_name").count().orderBy("group_name").show()

groups = sorted(df.select("group_name").distinct().toPandas()["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)
print(f"Control: {g_a}  |  Test: {g_b}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Overall SR Comparison

# COMMAND ----------

overall = (
    df.groupBy("group_name")
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("successes"),
        F.avg(F.col("is_customer_attempt_successful").cast("int")).alias("sr"),
    )
    .orderBy("group_name")
    .toPandas()
)

print(f"\n{'=' * 60}")
print(f"Overall SR — US customer attempts")
print(f"{'=' * 60}")
print(f"{'Group':<15} {'Attempts':>10} {'Successes':>10} {'SR':>8}")
print("-" * 47)
for _, r in overall.iterrows():
    print(f"{r['group_name']:<15} {r['attempts']:>10,} {r['successes']:>10,} {r['sr']:>8.2%}")

r_a = overall[overall["group_name"] == g_a].iloc[0]
r_b = overall[overall["group_name"] == g_b].iloc[0]
diff, pval = two_proportion_z_test(
    int(r_b["successes"]), int(r_b["attempts"]),
    int(r_a["successes"]), int(r_a["attempts"]))
pval_str = f"{pval:.4f}" if pval is not None else "N/A"
print(f"\nSR diff (test - ctrl): {diff:+.3%}  |  p-value: {pval_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Fraud Result Shift

# COMMAND ----------

FRAUD_CATS = ["ACCEPT", "THREE_DS", "REFUSE", "THREE_DS_EXEMPTION"]

fraud = (
    df.groupBy("group_name", "fraud_pre_auth_result")
    .agg(F.count("*").alias("n"))
    .orderBy("group_name", "fraud_pre_auth_result")
    .toPandas()
)
fraud_totals = fraud.groupby("group_name")["n"].sum().to_dict()

print(f"\n{'=' * 75}")
print(f"Fraud pre-auth result distribution — Control vs Test")
print(f"{'=' * 75}")
print(f"{'Fraud Result':<25} {'Ctrl N':>8} {'Ctrl %':>8} {'Test N':>8} {'Test %':>8} {'Δ pp':>8} {'p-val':>8}")
print("-" * 77)

for cat in FRAUD_CATS:
    ctrl_row = fraud[(fraud["group_name"] == g_a) & (fraud["fraud_pre_auth_result"] == cat)]
    test_row = fraud[(fraud["group_name"] == g_b) & (fraud["fraud_pre_auth_result"] == cat)]
    c_n = int(ctrl_row["n"].iloc[0]) if len(ctrl_row) else 0
    t_n = int(test_row["n"].iloc[0]) if len(test_row) else 0
    c_tot = fraud_totals.get(g_a, 1)
    t_tot = fraud_totals.get(g_b, 1)
    diff_pp, pv = two_proportion_z_test(t_n, t_tot, c_n, c_tot)
    pv_s = f"{pv:.4f}" if pv is not None else "N/A"
    diff_s = f"{diff_pp:+.2%}" if diff_pp is not None else "N/A"
    print(f"{cat:<25} {c_n:>8,} {c_n/c_tot:>8.2%} {t_n:>8,} {t_n/t_tot:>8.2%} {diff_s:>8} {pv_s:>8}")

other_cats = fraud[~fraud["fraud_pre_auth_result"].isin(FRAUD_CATS)]
if len(other_cats):
    print(f"\nOther fraud results:")
    for _, r in other_cats.iterrows():
        tot = fraud_totals.get(r['group_name'], 1)
        print(f"  {r['group_name']}: {r['fraud_pre_auth_result']} = {r['n']:,} ({r['n']/tot:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — SR Within Each Fraud Result
# MAGIC
# MAGIC If SR is lower even within the same fraud result category,
# MAGIC the problem is not just the fraud decision shift.

# COMMAND ----------

sr_within = (
    df.groupBy("group_name", "fraud_pre_auth_result")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .orderBy("fraud_pre_auth_result", "group_name")
    .toPandas()
)

print(f"\n{'=' * 80}")
print(f"SR within each fraud result — Control vs Test")
print(f"{'=' * 80}")
print(f"{'Fraud Result':<25} {'Group':<10} {'N':>8} {'SR':>8}")
print("-" * 55)
for cat in FRAUD_CATS:
    for g in [g_a, g_b]:
        row = sr_within[(sr_within["group_name"] == g) & (sr_within["fraud_pre_auth_result"] == cat)]
        if len(row):
            r = row.iloc[0]
            print(f"{cat:<25} {g:<10} {r['n']:>8,} {r['sr']:>8.2%}")
    ctrl_r = sr_within[(sr_within["group_name"] == g_a) & (sr_within["fraud_pre_auth_result"] == cat)]
    test_r = sr_within[(sr_within["group_name"] == g_b) & (sr_within["fraud_pre_auth_result"] == cat)]
    if len(ctrl_r) and len(test_r):
        d, p = two_proportion_z_test(
            int(test_r.iloc[0]["s"]), int(test_r.iloc[0]["n"]),
            int(ctrl_r.iloc[0]["s"]), int(ctrl_r.iloc[0]["n"]))
        ps = f"{p:.4f}" if p is not None else "N/A"
        ds = f"{d:+.3%}" if d is not None else "N/A"
        print(f"  {'→ Δ SR (test-ctrl):':<35} {ds}  p={ps}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Routing Segment Analysis
# MAGIC
# MAGIC Routing is done on **currency_type** (JPM / non-JPM) × **card_scheme**
# MAGIC (amex / visa_mc / other). For each routing segment, compare provider split.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Provider split by routing segment

# COMMAND ----------

seg_prov = (
    df.groupBy("group_name", "routing_segment", "payment_processor")
    .agg(F.count("*").alias("n"))
    .orderBy("routing_segment", "group_name", F.desc("n"))
    .toPandas()
)

seg_totals = (
    df.groupBy("group_name", "routing_segment")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
seg_totals_dict = {(r["group_name"], r["routing_segment"]): r["total"] for _, r in seg_totals.iterrows()}

segments = sorted(seg_prov["routing_segment"].unique())

for seg in segments:
    print(f"\n{'=' * 80}")
    print(f"Routing segment: {seg}")
    print(f"{'=' * 80}")
    seg_data = seg_prov[seg_prov["routing_segment"] == seg]

    print(f"{'Group':<10} {'Processor':<25} {'N':>8} {'Share':>8}")
    print("-" * 55)
    for g in [g_a, g_b]:
        g_data = seg_data[seg_data["group_name"] == g].sort_values("n", ascending=False)
        tot = seg_totals_dict.get((g, seg), 1)
        for _, r in g_data.iterrows():
            print(f"{g:<10} {str(r['payment_processor']):<25} {r['n']:>8,} {r['n']/tot:>8.1%}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 SR by routing segment

# COMMAND ----------

seg_sr = (
    df.groupBy("group_name", "routing_segment")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .orderBy("routing_segment", "group_name")
    .toPandas()
)

print(f"\n{'=' * 75}")
print(f"SR by routing segment — Control vs Test")
print(f"{'=' * 75}")
print(f"{'Segment':<35} {'Ctrl N':>8} {'Ctrl SR':>8} {'Test N':>8} {'Test SR':>8} {'Δ SR':>8} {'p-val':>8}")
print("-" * 87)
for seg in segments:
    ctrl_r = seg_sr[(seg_sr["group_name"] == g_a) & (seg_sr["routing_segment"] == seg)]
    test_r = seg_sr[(seg_sr["group_name"] == g_b) & (seg_sr["routing_segment"] == seg)]
    if len(ctrl_r) and len(test_r):
        c = ctrl_r.iloc[0]
        t = test_r.iloc[0]
        d, p = two_proportion_z_test(int(t["s"]), int(t["n"]), int(c["s"]), int(c["n"]))
        ps = f"{p:.4f}" if p is not None else "N/A"
        ds = f"{d:+.3%}" if d is not None else "N/A"
        print(f"{seg:<35} {int(c['n']):>8,} {c['sr']:>8.2%} {int(t['n']):>8,} {t['sr']:>8.2%} {ds:>8} {ps:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Fraud result shift by routing segment

# COMMAND ----------

seg_fraud = (
    df.groupBy("group_name", "routing_segment", "fraud_pre_auth_result")
    .agg(F.count("*").alias("n"))
    .orderBy("routing_segment", "fraud_pre_auth_result", "group_name")
    .toPandas()
)

for seg in segments:
    print(f"\n{'=' * 80}")
    print(f"Fraud shift: {seg}")
    print(f"{'=' * 80}")
    seg_d = seg_fraud[seg_fraud["routing_segment"] == seg]
    seg_t = {g: seg_d[seg_d["group_name"] == g]["n"].sum() for g in [g_a, g_b]}

    print(f"{'Fraud Result':<25} {'Ctrl N':>8} {'Ctrl %':>8} {'Test N':>8} {'Test %':>8} {'Δ pp':>8}")
    print("-" * 69)
    for cat in FRAUD_CATS:
        c_row = seg_d[(seg_d["group_name"] == g_a) & (seg_d["fraud_pre_auth_result"] == cat)]
        t_row = seg_d[(seg_d["group_name"] == g_b) & (seg_d["fraud_pre_auth_result"] == cat)]
        cn = int(c_row["n"].iloc[0]) if len(c_row) else 0
        tn = int(t_row["n"].iloc[0]) if len(t_row) else 0
        ct = seg_t.get(g_a, 1)
        tt = seg_t.get(g_b, 1)
        delta = tn/tt - cn/ct if ct > 0 and tt > 0 else 0
        print(f"{cat:<25} {cn:>8,} {cn/ct:>8.2%} {tn:>8,} {tn/tt:>8.2%} {delta:>+8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Provider-Level SR
# MAGIC
# MAGIC Within the **same provider**, is SR different between control and test?
# MAGIC If yes → it's not just routing, there's a config/context difference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 SR by provider (overall)

# COMMAND ----------

prov_sr = (
    df.groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

processors = sorted(prov_sr["payment_processor"].dropna().unique())

print(f"\n{'=' * 85}")
print(f"SR by provider — Control vs Test")
print(f"{'=' * 85}")
print(f"{'Processor':<25} {'Ctrl N':>8} {'Ctrl SR':>8} {'Test N':>8} {'Test SR':>8} {'Δ SR':>8} {'p-val':>8}")
print("-" * 79)
for proc in processors:
    ctrl_r = prov_sr[(prov_sr["group_name"] == g_a) & (prov_sr["payment_processor"] == proc)]
    test_r = prov_sr[(prov_sr["group_name"] == g_b) & (prov_sr["payment_processor"] == proc)]
    cn = int(ctrl_r.iloc[0]["n"]) if len(ctrl_r) else 0
    cs = int(ctrl_r.iloc[0]["s"]) if len(ctrl_r) else 0
    tn = int(test_r.iloc[0]["n"]) if len(test_r) else 0
    ts = int(test_r.iloc[0]["s"]) if len(test_r) else 0
    c_sr = cs/cn if cn else 0
    t_sr = ts/tn if tn else 0
    d, p = two_proportion_z_test(ts, tn, cs, cn) if cn > 0 and tn > 0 else (None, None)
    ds = f"{d:+.3%}" if d is not None else "N/A"
    ps = f"{p:.4f}" if p is not None else "N/A"
    print(f"{str(proc):<25} {cn:>8,} {c_sr:>8.2%} {tn:>8,} {t_sr:>8.2%} {ds:>8} {ps:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 SR by provider × routing segment

# COMMAND ----------

prov_seg_sr = (
    df.groupBy("group_name", "routing_segment", "payment_processor")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .orderBy("routing_segment", "payment_processor", "group_name")
    .toPandas()
)

for seg in segments:
    seg_d = prov_seg_sr[prov_seg_sr["routing_segment"] == seg]
    seg_procs = sorted(seg_d["payment_processor"].dropna().unique())
    if not len(seg_procs):
        continue
    print(f"\n{'=' * 90}")
    print(f"SR by provider — {seg}")
    print(f"{'=' * 90}")
    print(f"{'Processor':<25} {'Ctrl N':>8} {'Ctrl SR':>8} {'Test N':>8} {'Test SR':>8} {'Δ SR':>8} {'p-val':>8}")
    print("-" * 79)
    for proc in seg_procs:
        ctrl_r = seg_d[(seg_d["group_name"] == g_a) & (seg_d["payment_processor"] == proc)]
        test_r = seg_d[(seg_d["group_name"] == g_b) & (seg_d["payment_processor"] == proc)]
        cn = int(ctrl_r.iloc[0]["n"]) if len(ctrl_r) else 0
        cs = int(ctrl_r.iloc[0]["s"]) if len(ctrl_r) else 0
        tn = int(test_r.iloc[0]["n"]) if len(test_r) else 0
        ts_val = int(test_r.iloc[0]["s"]) if len(test_r) else 0
        c_sr = cs/cn if cn else 0
        t_sr = ts_val/tn if tn else 0
        d, p = two_proportion_z_test(ts_val, tn, cs, cn) if cn > 0 and tn > 0 else (None, None)
        ds = f"{d:+.3%}" if d is not None else "N/A"
        ps = f"{p:.4f}" if p is not None else "N/A"
        print(f"{str(proc):<25} {cn:>8,} {c_sr:>8.2%} {tn:>8,} {t_sr:>8.2%} {ds:>8} {ps:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Decomposition: Routing Effect vs Config Effect
# MAGIC
# MAGIC Decompose the overall SR gap into:
# MAGIC - **Routing effect**: SR gap explained by different provider shares
# MAGIC   (test sends more to lower-SR providers)
# MAGIC - **Within-provider effect**: SR gap within the same provider
# MAGIC   (same provider, different fraud/3DS config → lower SR)
# MAGIC
# MAGIC Method: Oaxaca-Blinder-style decomposition per routing segment.

# COMMAND ----------

decomp_data = (
    df.groupBy("group_name", "routing_segment", "payment_processor")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .toPandas()
)

print(f"\n{'=' * 90}")
print(f"SR Gap Decomposition by Routing Segment")
print(f"{'=' * 90}")
print(f"{'Segment':<35} {'Total Δ SR':>10} {'Routing':>10} {'Within-Prov':>12} {'Routing %':>10}")
print("-" * 81)

total_routing_effect = 0
total_within_effect = 0

for seg in segments:
    seg_d = decomp_data[decomp_data["routing_segment"] == seg]
    ctrl_d = seg_d[seg_d["group_name"] == g_a]
    test_d = seg_d[seg_d["group_name"] == g_b]

    ctrl_total = ctrl_d["n"].sum()
    test_total = test_d["n"].sum()
    if ctrl_total == 0 or test_total == 0:
        continue

    ctrl_sr = ctrl_d["s"].sum() / ctrl_total
    test_sr = test_d["s"].sum() / test_total
    total_gap = test_sr - ctrl_sr

    all_procs = set(ctrl_d["payment_processor"].tolist()) | set(test_d["payment_processor"].tolist())

    routing_effect = 0
    within_effect = 0

    for proc in all_procs:
        c_row = ctrl_d[ctrl_d["payment_processor"] == proc]
        t_row = test_d[test_d["payment_processor"] == proc]

        c_n = int(c_row["n"].iloc[0]) if len(c_row) else 0
        c_s = int(c_row["s"].iloc[0]) if len(c_row) else 0
        t_n = int(t_row["n"].iloc[0]) if len(t_row) else 0
        t_s = int(t_row["s"].iloc[0]) if len(t_row) else 0

        c_share = c_n / ctrl_total if ctrl_total else 0
        t_share = t_n / test_total if test_total else 0

        c_proc_sr = c_s / c_n if c_n else 0
        t_proc_sr = t_s / t_n if t_n else 0
        avg_sr = (c_s + t_s) / (c_n + t_n) if (c_n + t_n) else 0

        routing_effect += (t_share - c_share) * avg_sr
        within_effect += c_share * (t_proc_sr - c_proc_sr) if c_n > 0 and t_n > 0 else 0

    routing_pct = abs(routing_effect) / abs(total_gap) * 100 if abs(total_gap) > 1e-8 else 0

    total_routing_effect += routing_effect * (ctrl_total + test_total)
    total_within_effect += within_effect * (ctrl_total + test_total)

    print(f"{seg:<35} {total_gap:>+10.3%} {routing_effect:>+10.3%} {within_effect:>+12.3%} {routing_pct:>9.0f}%")

overall_gap = float(r_b["sr"]) - float(r_a["sr"])
denom = total_routing_effect + total_within_effect
if abs(denom) > 1e-8:
    norm_routing = total_routing_effect / denom * overall_gap
    norm_within = total_within_effect / denom * overall_gap
    routing_share = abs(norm_routing) / abs(overall_gap) * 100 if abs(overall_gap) > 1e-8 else 0
    print(f"\n{'OVERALL':<35} {overall_gap:>+10.3%} {norm_routing:>+10.3%} {norm_within:>+12.3%} {routing_share:>9.0f}%")
    print(f"\n  Interpretation:")
    print(f"  - ~{routing_share:.0f}% of the SR gap is explained by different provider routing")
    print(f"  - ~{100-routing_share:.0f}% is within-provider (config/context/fraud decisions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Fraud Result by Provider
# MAGIC
# MAGIC Which provider is driving the THREE_DS shift in test?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Fraud result distribution by provider

# COMMAND ----------

prov_fraud = (
    df.groupBy("group_name", "payment_processor", "fraud_pre_auth_result")
    .agg(F.count("*").alias("n"))
    .orderBy("payment_processor", "fraud_pre_auth_result", "group_name")
    .toPandas()
)

prov_totals = (
    df.groupBy("group_name", "payment_processor")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
prov_totals_dict = {(r["group_name"], r["payment_processor"]): r["total"] for _, r in prov_totals.iterrows()}

for proc in processors:
    proc_d = prov_fraud[prov_fraud["payment_processor"] == proc]
    if proc_d["n"].sum() < 50:
        continue
    print(f"\n{'=' * 80}")
    print(f"Fraud result: {proc}")
    print(f"{'=' * 80}")
    print(f"{'Fraud Result':<25} {'Ctrl N':>8} {'Ctrl %':>8} {'Test N':>8} {'Test %':>8} {'Δ pp':>8}")
    print("-" * 69)
    for cat in FRAUD_CATS:
        c_row = proc_d[(proc_d["group_name"] == g_a) & (proc_d["fraud_pre_auth_result"] == cat)]
        t_row = proc_d[(proc_d["group_name"] == g_b) & (proc_d["fraud_pre_auth_result"] == cat)]
        cn = int(c_row["n"].iloc[0]) if len(c_row) else 0
        tn = int(t_row["n"].iloc[0]) if len(t_row) else 0
        ct = prov_totals_dict.get((g_a, proc), 1)
        tt = prov_totals_dict.get((g_b, proc), 1)
        delta = tn/tt - cn/ct if ct > 0 and tt > 0 else 0
        print(f"{cat:<25} {cn:>8,} {cn/ct:>8.2%} {tn:>8,} {tn/tt:>8.2%} {delta:>+8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Fraud result by provider × routing segment (heatmap data)

# COMMAND ----------

prov_seg_fraud = (
    df.groupBy("group_name", "routing_segment", "payment_processor", "fraud_pre_auth_result")
    .agg(F.count("*").alias("n"))
    .toPandas()
)

prov_seg_totals = (
    df.groupBy("group_name", "routing_segment", "payment_processor")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
pst_dict = {
    (r["group_name"], r["routing_segment"], r["payment_processor"]): r["total"]
    for _, r in prov_seg_totals.iterrows()
}

for seg in segments:
    seg_d = prov_seg_fraud[prov_seg_fraud["routing_segment"] == seg]
    seg_procs = sorted(seg_d["payment_processor"].dropna().unique())
    if not seg_procs:
        continue
    print(f"\n{'=' * 90}")
    print(f"THREE_DS share by provider — {seg}")
    print(f"{'=' * 90}")
    print(f"{'Processor':<25} {'Ctrl 3DS%':>10} {'Test 3DS%':>10} {'Δ pp':>8}  {'Ctrl ACCEPT%':>12} {'Test ACCEPT%':>12}")
    print("-" * 81)
    for proc in seg_procs:
        for cat, label in [("THREE_DS", "tds"), ("ACCEPT", "acc")]:
            pass
        c_tot = pst_dict.get((g_a, seg, proc), 0)
        t_tot = pst_dict.get((g_b, seg, proc), 0)
        c_tds = seg_d[(seg_d["group_name"]==g_a) & (seg_d["payment_processor"]==proc) & (seg_d["fraud_pre_auth_result"]=="THREE_DS")]
        t_tds = seg_d[(seg_d["group_name"]==g_b) & (seg_d["payment_processor"]==proc) & (seg_d["fraud_pre_auth_result"]=="THREE_DS")]
        c_acc = seg_d[(seg_d["group_name"]==g_a) & (seg_d["payment_processor"]==proc) & (seg_d["fraud_pre_auth_result"]=="ACCEPT")]
        t_acc = seg_d[(seg_d["group_name"]==g_b) & (seg_d["payment_processor"]==proc) & (seg_d["fraud_pre_auth_result"]=="ACCEPT")]
        c_tds_n = int(c_tds["n"].iloc[0]) if len(c_tds) else 0
        t_tds_n = int(t_tds["n"].iloc[0]) if len(t_tds) else 0
        c_acc_n = int(c_acc["n"].iloc[0]) if len(c_acc) else 0
        t_acc_n = int(t_acc["n"].iloc[0]) if len(t_acc) else 0
        c_tds_pct = c_tds_n / c_tot if c_tot else 0
        t_tds_pct = t_tds_n / t_tot if t_tot else 0
        c_acc_pct = c_acc_n / c_tot if c_tot else 0
        t_acc_pct = t_acc_n / t_tot if t_tot else 0
        d = t_tds_pct - c_tds_pct
        if c_tot + t_tot >= 20:
            print(f"{str(proc):<25} {c_tds_pct:>10.1%} {t_tds_pct:>10.1%} {d:>+8.2%}  {c_acc_pct:>12.1%} {t_acc_pct:>12.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 9 — 3DS Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Challenge rate & 3DS success rate

# COMMAND ----------

tds_attempts = df.filter(F.col("fraud_pre_auth_result") == "THREE_DS")

tds_stats = (
    tds_attempts.groupBy("group_name")
    .agg(
        F.count("*").alias("tds_attempts"),
        F.sum("challenged").alias("challenged"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("tds_successes"),
    )
    .withColumn("challenge_rate", F.col("challenged") / F.col("tds_attempts"))
    .withColumn("tds_sr", F.col("tds_successes") / F.col("tds_attempts"))
    .orderBy("group_name")
    .toPandas()
)

print(f"\n{'=' * 70}")
print(f"3DS attempts — challenge rate & SR")
print(f"{'=' * 70}")
print(f"{'Group':<10} {'3DS N':>8} {'Challenged':>10} {'Chall%':>8} {'3DS SR':>8}")
print("-" * 48)
for _, r in tds_stats.iterrows():
    ch_pct = f"{r['challenge_rate']:.1%}" if pd.notna(r['challenge_rate']) else "N/A"
    sr_str = f"{r['tds_sr']:.2%}" if pd.notna(r['tds_sr']) else "N/A"
    print(f"{r['group_name']:<10} {r['tds_attempts']:>8,} {int(r['challenged']):>10,} {ch_pct:>8} {sr_str:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 3DS SR: challenged vs frictionless

# COMMAND ----------

tds_split = (
    tds_attempts.groupBy("group_name", "challenged")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .orderBy("challenged", "group_name")
    .toPandas()
)

print(f"\n{'=' * 65}")
print(f"3DS SR — Challenged vs Frictionless")
print(f"{'=' * 65}")
print(f"{'Challenged':<12} {'Group':<10} {'N':>8} {'SR':>8}")
print("-" * 42)
for ch in [0, 1]:
    label = "Challenged" if ch == 1 else "Frictionless"
    for g in [g_a, g_b]:
        row = tds_split[(tds_split["challenged"] == ch) & (tds_split["group_name"] == g)]
        if len(row):
            r = row.iloc[0]
            print(f"{label:<12} {g:<10} {r['n']:>8,} {r['sr']:>8.2%}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 3DS SR by provider

# COMMAND ----------

tds_prov = (
    tds_attempts.groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.col("is_customer_attempt_successful").cast("int")).alias("s"),
        F.sum("challenged").alias("ch"),
    )
    .withColumn("sr", F.col("s") / F.col("n"))
    .withColumn("ch_rate", F.col("ch") / F.col("n"))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

print(f"\n{'=' * 85}")
print(f"3DS: SR & challenge rate by provider")
print(f"{'=' * 85}")
print(f"{'Processor':<25} {'Group':<10} {'3DS N':>8} {'3DS SR':>8} {'Chall%':>8}")
print("-" * 63)
for proc in processors:
    for g in [g_a, g_b]:
        row = tds_prov[(tds_prov["payment_processor"] == proc) & (tds_prov["group_name"] == g)]
        if len(row) and row.iloc[0]["n"] >= 5:
            r = row.iloc[0]
            ch_s = f"{r['ch_rate']:.1%}" if pd.notna(r['ch_rate']) else "N/A"
            print(f"{str(proc):<25} {g:<10} {r['n']:>8,} {r['sr']:>8.2%} {ch_s:>8}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 10 — Payload Context Investigation
# MAGIC
# MAGIC Join with notification table to check if key metadata fields differ.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Join & extract payload fields

# COMMAND ----------

df.createOrReplaceTempView("us_routing_exp")

df_pl = spark.sql(f"""
SELECT
  e.group_name,
  e.routing_segment,
  e.payment_processor,
  e.fraud_pre_auth_result,
  e.is_customer_attempt_successful,
  e.card_scheme,
  e.currency_type,
  n.request_payload:payment.metadata.field::string                                         AS pl_field,
  n.request_payload:payment.metadata.customRoutingFlag::string                             AS pl_routing_flag,
  n.request_payload:payment.metadata.skipRisk::string                                      AS pl_skip_risk,
  n.request_payload:payment.metadata.high_risk::string                                     AS pl_high_risk,
  n.request_payload:payment.metadata.attemptAuthentication::string                         AS pl_attempt_auth,
  n.request_payload:payment.processor.name::string                                         AS pl_processor,
  n.request_payload:payment.processor.processorMerchantId::string                          AS pl_mid,
  n.request_payload:payment.riskData.fraudChecks.source::string                            AS pl_fraud_source,
  n.request_payload:payment.riskData.fraudChecks.preAuthorizationResult::string            AS pl_fraud_result
FROM us_routing_exp e
JOIN {NOTIF_TABLE} n
  ON e.payment_provider_reference = n.request_payload:payment.id::string
  AND n.dbz_timestamp::date >= '{PAYMENT_START}'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.payment_provider_reference
  ORDER BY n.dbz_timestamp DESC
) = 1
""")

df_pl.cache()
print(f"Joined with notification: {df_pl.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.2 Key metadata fields — control vs test distribution

# COMMAND ----------

for field_name, col_name in [
    ("metadata.field", "pl_field"),
    ("customRoutingFlag", "pl_routing_flag"),
    ("skipRisk", "pl_skip_risk"),
    ("high_risk", "pl_high_risk"),
    ("attemptAuthentication", "pl_attempt_auth"),
    ("processor.name (payload)", "pl_processor"),
    ("processorMerchantId", "pl_mid"),
    ("fraudChecks.source", "pl_fraud_source"),
]:
    dist = (
        df_pl.groupBy("group_name", col_name)
        .agg(F.count("*").alias("n"))
        .orderBy("group_name", F.desc("n"))
        .toPandas()
    )
    totals = dist.groupby("group_name")["n"].sum().to_dict()

    print(f"\n{'=' * 65}")
    print(f"{field_name}")
    print(f"{'=' * 65}")
    print(f"{'Group':<10} {'Value':<30} {'N':>8} {'Share':>8}")
    print("-" * 60)
    for _, r in dist.iterrows():
        t = totals.get(r['group_name'], 1)
        print(f"{r['group_name']:<10} {str(r[col_name]):<30} {r['n']:>8,} {r['n']/t:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.3 metadata.field by processor (controlling for routing)

# COMMAND ----------

field_proc = (
    df_pl.groupBy("group_name", "pl_processor", "pl_field")
    .agg(F.count("*").alias("n"))
    .orderBy("pl_processor", "group_name", F.desc("n"))
    .toPandas()
)
fp_totals = (
    df_pl.groupBy("group_name", "pl_processor")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
fp_dict = {(r["group_name"], r["pl_processor"]): r["total"] for _, r in fp_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"metadata.field by variant × processor")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Processor':<22} {'field':<22} {'N':>8} {'Share':>8}")
print("-" * 74)
for _, r in field_proc.iterrows():
    t = fp_dict.get((r["group_name"], r["pl_processor"]), 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<22} {str(r['pl_field']):<22} "
          f"{r['n']:>8,} {r['n']/t:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.4 attemptAuthentication by processor — key for Forter behavior

# COMMAND ----------

auth_proc = (
    df_pl.groupBy("group_name", "pl_processor", "pl_attempt_auth")
    .agg(F.count("*").alias("n"))
    .orderBy("pl_processor", "group_name", F.desc("n"))
    .toPandas()
)
ap_totals = (
    df_pl.groupBy("group_name", "pl_processor")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
ap_dict = {(r["group_name"], r["pl_processor"]): r["total"] for _, r in ap_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"attemptAuthentication by variant × processor")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Processor':<22} {'attemptAuth':<22} {'N':>8} {'Share':>8}")
print("-" * 74)
for _, r in auth_proc.iterrows():
    t = ap_dict.get((r["group_name"], r["pl_processor"]), 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<22} {str(r['pl_attempt_auth']):<22} "
          f"{r['n']:>8,} {r['n']/t:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.5 skipRisk by processor

# COMMAND ----------

skip_proc = (
    df_pl.groupBy("group_name", "pl_processor", "pl_skip_risk")
    .agg(F.count("*").alias("n"))
    .orderBy("pl_processor", "group_name", F.desc("n"))
    .toPandas()
)
sp_totals = (
    df_pl.groupBy("group_name", "pl_processor")
    .agg(F.count("*").alias("total"))
    .toPandas()
)
sp_dict = {(r["group_name"], r["pl_processor"]): r["total"] for _, r in sp_totals.iterrows()}

print(f"\n{'=' * 80}")
print(f"skipRisk by variant × processor")
print(f"{'=' * 80}")
print(f"{'Group':<10} {'Processor':<22} {'skipRisk':<22} {'N':>8} {'Share':>8}")
print("-" * 74)
for _, r in skip_proc.iterrows():
    t = sp_dict.get((r["group_name"], r["pl_processor"]), 1)
    print(f"{r['group_name']:<10} {str(r['pl_processor']):<22} {str(r['pl_skip_risk']):<22} "
          f"{r['n']:>8,} {r['n']/t:>8.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 11 — Daily Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.1 Daily SR by variant

# COMMAND ----------

import matplotlib.pyplot as plt

daily_sr = (
    df.groupBy("group_name", "pay_date")
    .agg(
        F.count("*").alias("n"),
        F.avg(F.col("is_customer_attempt_successful").cast("int")).alias("sr"),
    )
    .orderBy("pay_date", "group_name")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
for g in [g_a, g_b]:
    gd = daily_sr[daily_sr["group_name"] == g].sort_values("pay_date")
    label = f"{g} (ctrl)" if g == g_a else f"{g} (test)"
    ax.plot(gd["pay_date"], gd["sr"], marker="o", label=label)
ax.set_ylabel("SR")
ax.set_title("Daily SR — US Customer Attempts")
ax.legend()
ax.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.2 Daily THREE_DS share by variant

# COMMAND ----------

daily_fraud = (
    df.groupBy("group_name", "pay_date")
    .agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("fraud_pre_auth_result") == "THREE_DS", 1).otherwise(0)).alias("tds"),
        F.sum(F.when(F.col("fraud_pre_auth_result") == "ACCEPT", 1).otherwise(0)).alias("accept"),
    )
    .withColumn("tds_share", F.col("tds") / F.col("n"))
    .withColumn("accept_share", F.col("accept") / F.col("n"))
    .orderBy("pay_date", "group_name")
    .toPandas()
)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))
for g in [g_a, g_b]:
    gd = daily_fraud[daily_fraud["group_name"] == g].sort_values("pay_date")
    label = f"{g} (ctrl)" if g == g_a else f"{g} (test)"
    ax1.plot(gd["pay_date"], gd["tds_share"], marker="o", label=label)
    ax2.plot(gd["pay_date"], gd["accept_share"], marker="o", label=label)

ax1.set_ylabel("THREE_DS share")
ax1.set_title("Daily THREE_DS Share")
ax1.legend()
ax1.grid(True, alpha=0.3)
ax2.set_ylabel("ACCEPT share")
ax2.set_title("Daily ACCEPT Share")
ax2.legend()
ax2.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.3 Daily SR by routing segment

# COMMAND ----------

daily_seg = (
    df.groupBy("group_name", "pay_date", "routing_segment")
    .agg(
        F.count("*").alias("n"),
        F.avg(F.col("is_customer_attempt_successful").cast("int")).alias("sr"),
    )
    .orderBy("pay_date", "routing_segment", "group_name")
    .toPandas()
)

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
axes = axes.flatten()
for i, seg in enumerate(segments[:6]):
    ax = axes[i]
    for g in [g_a, g_b]:
        gd = daily_seg[(daily_seg["group_name"] == g) & (daily_seg["routing_segment"] == seg)].sort_values("pay_date")
        label = f"{g} (ctrl)" if g == g_a else f"{g} (test)"
        ax.plot(gd["pay_date"], gd["sr"], marker="o", markersize=4, label=label)
    ax.set_title(seg, fontsize=10)
    ax.set_ylabel("SR")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='x', rotation=45)

for j in range(len(segments), len(axes)):
    axes[j].set_visible(False)
plt.suptitle("Daily SR by Routing Segment", fontsize=14)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | What it answers |
# MAGIC |---|---|
# MAGIC | 2 | How big is the overall SR gap? |
# MAGIC | 3 | Is the fraud result distribution different (more THREE_DS, less ACCEPT)? |
# MAGIC | 4 | **Within same fraud result**, is SR still lower? (→ config issue beyond fraud shift) |
# MAGIC | 5 | Does provider split differ by routing segment? (→ routing effect) |
# MAGIC | 6 | **Within same provider**, is SR lower? (→ config effect, not routing) |
# MAGIC | 7 | **Decomposition**: X% routing effect vs Y% within-provider effect |
# MAGIC | 8 | Which provider drives the THREE_DS shift? |
# MAGIC | 9 | 3DS challenge rate & SR — is 3DS performing worse in test? |
# MAGIC | 10 | Payload metadata differences — `field`, `attemptAuthentication`, `skipRisk`, `customRoutingFlag` |
# MAGIC | 11 | Daily trends — is the gap consistent or changing? |
# MAGIC
# MAGIC **Decision framework:**
# MAGIC - If Section 7 shows **high routing %** → the problem is provider assignment
# MAGIC - If Section 7 shows **high within-provider %** → same provider, different config/fraud decisions
# MAGIC - If Section 10 shows payload fields differ → investigate those specific config differences
# MAGIC - If Section 4 shows SR lower within same fraud result → something beyond fraud shift (e.g. 3DS flow)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of US routing vs config deep dive*
