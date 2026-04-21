# Databricks notebook source
# MAGIC %md
# MAGIC # Provider Split Analysis by Country × Currency × Network
# MAGIC
# MAGIC Identify segment combinations (issuer country, currency, card network) where
# MAGIC the routing split across **Adyen, Checkout, JPMC** differs between experiment
# MAGIC variants. Analyse customer attempts and system attempts separately.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction
# MAGIC 2. Customer-attempt-level provider split (rank = system_attempt_rank 1 per customer attempt)
# MAGIC 3. System-attempt-level provider split (all system attempts)
# MAGIC 4. Segments with largest divergence
# MAGIC 5. Deep dive into top divergent segments

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]
A_COLOR, B_COLOR = COLORS[0], COLORS[1]

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-03-16"
PAYMENT_START = "2026-03-18"
FOCUS_PROVIDERS = ["adyen", "checkout", "jpmc"]

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Data Extraction

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
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.attempt_type,
  p.is_successful,
  p.is_customer_attempt_successful,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
""")
df.cache()

total = df.count()
print(f"Total payment attempts: {total:,}")

by_group = df.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Customer-Attempt-Level Provider Split
# MAGIC
# MAGIC A "customer attempt" is defined by `customer_attempt_rank`. We take the
# MAGIC first system attempt (`system_attempt_rank = 1`) for each customer attempt
# MAGIC to capture the **initial routing decision** — before any system-level
# MAGIC retries or fallbacks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Customer-attempt provider split by country × currency × network

# COMMAND ----------

df_cust = df.filter(F.col("system_attempt_rank") == 1)

cust_split = (
    df_cust
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROVIDERS))
    .groupBy("group_name", "bin_issuer_country_code", "currency", "payment_method_variant", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .toPandas()
)

seg_total_cust = (
    df_cust
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROVIDERS))
    .groupBy("group_name", "bin_issuer_country_code", "currency", "payment_method_variant")
    .agg(F.count("*").alias("seg_total"))
    .toPandas()
)

cust_split = cust_split.merge(
    seg_total_cust,
    on=["group_name", "bin_issuer_country_code", "currency", "payment_method_variant"],
    how="left",
)
cust_split["pct"] = cust_split["attempts"] / cust_split["seg_total"]
cust_split["sr"] = cust_split["successes"] / cust_split["attempts"]

print(f"Customer-attempt rows (system_rank=1, focus providers): {len(cust_split):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Compute divergence score per segment (customer attempts)
# MAGIC
# MAGIC For each segment (country × currency × network), compute the provider share
# MAGIC in each variant. The divergence score is the sum of absolute differences in
# MAGIC provider share across the focus providers.

# COMMAND ----------

MIN_SEGMENT_ATTEMPTS = 30

seg_cols = ["bin_issuer_country_code", "currency", "payment_method_variant"]

cust_pivot = cust_split.pivot_table(
    index=seg_cols + ["payment_processor"],
    columns="group_name",
    values=["pct", "attempts", "sr"],
    aggfunc="first",
)

cust_seg_totals = seg_total_cust.pivot_table(
    index=seg_cols,
    columns="group_name",
    values="seg_total",
    aggfunc="first",
    fill_value=0,
)

divergence_rows = []
for seg_key, seg_group in cust_split.groupby(seg_cols):
    country, currency, network = seg_key

    vol_ok = True
    for g in groups:
        seg_vol = seg_group[seg_group["group_name"] == g]["attempts"].sum()
        if seg_vol < MIN_SEGMENT_ATTEMPTS:
            vol_ok = False
    if not vol_ok:
        continue

    total_divergence = 0
    provider_details = {}
    for prov in FOCUS_PROVIDERS:
        pcts = {}
        vols = {}
        srs = {}
        for g in groups:
            row = seg_group[(seg_group["group_name"] == g) &
                            (seg_group["payment_processor"].str.lower() == prov)]
            pcts[g] = row["pct"].values[0] if not row.empty else 0
            vols[g] = row["attempts"].values[0] if not row.empty else 0
            srs[g] = row["sr"].values[0] if not row.empty else 0

        if len(groups) == 2:
            delta = abs(pcts[groups[0]] - pcts[groups[1]])
            total_divergence += delta
            provider_details[prov] = {
                "pct_A": pcts[groups[0]], "pct_B": pcts[groups[1]],
                "delta_pp": (pcts[groups[1]] - pcts[groups[0]]) * 100,
                "vol_A": vols[groups[0]], "vol_B": vols[groups[1]],
                "sr_A": srs[groups[0]], "sr_B": srs[groups[1]],
            }

    total_vol = seg_group["attempts"].sum()
    rec = {
        "country": country, "currency": currency, "network": network,
        "total_attempts": total_vol,
        "divergence_score": total_divergence * 100,
    }
    for prov, d in provider_details.items():
        for k, v in d.items():
            rec[f"{prov}_{k}"] = v
    divergence_rows.append(rec)

cust_divergence = pd.DataFrame(divergence_rows).sort_values("divergence_score", ascending=False)
print(f"Segments with sufficient volume (≥{MIN_SEGMENT_ATTEMPTS} per variant): {len(cust_divergence):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Top divergent segments — customer attempts

# COMMAND ----------

top_n = 30
top_cust = cust_divergence.head(top_n)

print(f"Top {top_n} most divergent segments — CUSTOMER ATTEMPTS (system_rank=1)")
print(f"{'Country':<8} {'Curr':<6} {'Network':<12} {'Vol':>8} {'Div(pp)':>8}", end="")
for prov in FOCUS_PROVIDERS:
    print(f"  {prov+'_A':>8} {prov+'_B':>8} {'Δ(pp)':>8}", end="")
print()
print("-" * (44 + len(FOCUS_PROVIDERS) * 28))

for _, r in top_cust.iterrows():
    print(f"{r['country']:<8} {r['currency']:<6} {str(r['network']):<12} {r['total_attempts']:>8,.0f} {r['divergence_score']:>8.1f}", end="")
    for prov in FOCUS_PROVIDERS:
        pct_a = r.get(f"{prov}_pct_A", 0)
        pct_b = r.get(f"{prov}_pct_B", 0)
        delta = r.get(f"{prov}_delta_pp", 0)
        print(f"  {pct_a:>8.1%} {pct_b:>8.1%} {delta:>+8.1f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Customer-attempt divergence chart (top 20)

# COMMAND ----------

top_20_cust = cust_divergence.head(20).copy()
top_20_cust["segment"] = top_20_cust.apply(
    lambda r: f"{r['country']}|{r['currency']}|{r['network']}", axis=1,
)

fig, axes = plt.subplots(1, len(FOCUS_PROVIDERS), figsize=(7 * len(FOCUS_PROVIDERS), 8))
if len(FOCUS_PROVIDERS) == 1:
    axes = [axes]

for ax_idx, prov in enumerate(FOCUS_PROVIDERS):
    segments = top_20_cust["segment"].tolist()[::-1]
    y = np.arange(len(segments))
    h = 0.35

    pct_a = top_20_cust[f"{prov}_pct_A"].values[::-1]
    pct_b = top_20_cust[f"{prov}_pct_B"].values[::-1]

    axes[ax_idx].barh(y - h / 2, pct_a, h, label=groups[0] if len(groups) == 2 else "A", color=A_COLOR)
    axes[ax_idx].barh(y + h / 2, pct_b, h, label=groups[1] if len(groups) == 2 else "B", color=B_COLOR)
    axes[ax_idx].set_yticks(y)
    axes[ax_idx].set_yticklabels(segments, fontsize=7)
    axes[ax_idx].set_title(f"{prov} share", fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=8)

fig.suptitle("Customer Attempts — Provider Share for Top 20 Most Divergent Segments",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — System-Attempt-Level Provider Split
# MAGIC
# MAGIC All system attempts (including retries/fallbacks). This shows whether
# MAGIC the routing divergence grows or shrinks when retries are included.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 System-attempt provider split by country × currency × network

# COMMAND ----------

sys_split = (
    df
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROVIDERS))
    .groupBy("group_name", "bin_issuer_country_code", "currency", "payment_method_variant", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .toPandas()
)

seg_total_sys = (
    df
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROVIDERS))
    .groupBy("group_name", "bin_issuer_country_code", "currency", "payment_method_variant")
    .agg(F.count("*").alias("seg_total"))
    .toPandas()
)

sys_split = sys_split.merge(
    seg_total_sys,
    on=["group_name", "bin_issuer_country_code", "currency", "payment_method_variant"],
    how="left",
)
sys_split["pct"] = sys_split["attempts"] / sys_split["seg_total"]
sys_split["sr"] = sys_split["successes"] / sys_split["attempts"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Compute divergence score per segment (system attempts)

# COMMAND ----------

sys_divergence_rows = []
for seg_key, seg_group in sys_split.groupby(seg_cols):
    country, currency, network = seg_key

    vol_ok = True
    for g in groups:
        seg_vol = seg_group[seg_group["group_name"] == g]["attempts"].sum()
        if seg_vol < MIN_SEGMENT_ATTEMPTS:
            vol_ok = False
    if not vol_ok:
        continue

    total_divergence = 0
    provider_details = {}
    for prov in FOCUS_PROVIDERS:
        pcts = {}
        vols = {}
        srs = {}
        for g in groups:
            row = seg_group[(seg_group["group_name"] == g) &
                            (seg_group["payment_processor"].str.lower() == prov)]
            pcts[g] = row["pct"].values[0] if not row.empty else 0
            vols[g] = row["attempts"].values[0] if not row.empty else 0
            srs[g] = row["sr"].values[0] if not row.empty else 0

        if len(groups) == 2:
            delta = abs(pcts[groups[0]] - pcts[groups[1]])
            total_divergence += delta
            provider_details[prov] = {
                "pct_A": pcts[groups[0]], "pct_B": pcts[groups[1]],
                "delta_pp": (pcts[groups[1]] - pcts[groups[0]]) * 100,
                "vol_A": vols[groups[0]], "vol_B": vols[groups[1]],
                "sr_A": srs[groups[0]], "sr_B": srs[groups[1]],
            }

    total_vol = seg_group["attempts"].sum()
    rec = {
        "country": country, "currency": currency, "network": network,
        "total_attempts": total_vol,
        "divergence_score": total_divergence * 100,
    }
    for prov, d in provider_details.items():
        for k, v in d.items():
            rec[f"{prov}_{k}"] = v
    sys_divergence_rows.append(rec)

sys_divergence = pd.DataFrame(sys_divergence_rows).sort_values("divergence_score", ascending=False)
print(f"Segments with sufficient volume (≥{MIN_SEGMENT_ATTEMPTS} per variant): {len(sys_divergence):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Top divergent segments — system attempts

# COMMAND ----------

top_sys = sys_divergence.head(top_n)

print(f"Top {top_n} most divergent segments — ALL SYSTEM ATTEMPTS")
print(f"{'Country':<8} {'Curr':<6} {'Network':<12} {'Vol':>8} {'Div(pp)':>8}", end="")
for prov in FOCUS_PROVIDERS:
    print(f"  {prov+'_A':>8} {prov+'_B':>8} {'Δ(pp)':>8}", end="")
print()
print("-" * (44 + len(FOCUS_PROVIDERS) * 28))

for _, r in top_sys.iterrows():
    print(f"{r['country']:<8} {r['currency']:<6} {str(r['network']):<12} {r['total_attempts']:>8,.0f} {r['divergence_score']:>8.1f}", end="")
    for prov in FOCUS_PROVIDERS:
        pct_a = r.get(f"{prov}_pct_A", 0)
        pct_b = r.get(f"{prov}_pct_B", 0)
        delta = r.get(f"{prov}_delta_pp", 0)
        print(f"  {pct_a:>8.1%} {pct_b:>8.1%} {delta:>+8.1f}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 System-attempt divergence chart (top 20)

# COMMAND ----------

top_20_sys = sys_divergence.head(20).copy()
top_20_sys["segment"] = top_20_sys.apply(
    lambda r: f"{r['country']}|{r['currency']}|{r['network']}", axis=1,
)

fig, axes = plt.subplots(1, len(FOCUS_PROVIDERS), figsize=(7 * len(FOCUS_PROVIDERS), 8))
if len(FOCUS_PROVIDERS) == 1:
    axes = [axes]

for ax_idx, prov in enumerate(FOCUS_PROVIDERS):
    segments = top_20_sys["segment"].tolist()[::-1]
    y = np.arange(len(segments))
    h = 0.35

    pct_a = top_20_sys[f"{prov}_pct_A"].values[::-1]
    pct_b = top_20_sys[f"{prov}_pct_B"].values[::-1]

    axes[ax_idx].barh(y - h / 2, pct_a, h, label=groups[0] if len(groups) == 2 else "A", color=A_COLOR)
    axes[ax_idx].barh(y + h / 2, pct_b, h, label=groups[1] if len(groups) == 2 else "B", color=B_COLOR)
    axes[ax_idx].set_yticks(y)
    axes[ax_idx].set_yticklabels(segments, fontsize=7)
    axes[ax_idx].set_title(f"{prov} share", fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[ax_idx].legend(fontsize=8)

fig.suptitle("System Attempts — Provider Share for Top 20 Most Divergent Segments",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Customer vs System Attempt Comparison
# MAGIC
# MAGIC Does the divergence grow when system retries are included?
# MAGIC This reveals whether the routing difference comes from the initial
# MAGIC decision or from different fallback/retry strategies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Divergence comparison: customer vs system attempts

# COMMAND ----------

if not cust_divergence.empty and not sys_divergence.empty:
    comparison = cust_divergence[["country", "currency", "network", "divergence_score", "total_attempts"]].rename(
        columns={"divergence_score": "cust_div", "total_attempts": "cust_vol"}
    ).merge(
        sys_divergence[["country", "currency", "network", "divergence_score", "total_attempts"]].rename(
            columns={"divergence_score": "sys_div", "total_attempts": "sys_vol"}
        ),
        on=["country", "currency", "network"],
        how="outer",
    )
    comparison["div_change"] = comparison["sys_div"].fillna(0) - comparison["cust_div"].fillna(0)
    comparison = comparison.sort_values("sys_div", ascending=False)

    print("Customer vs System Attempt Divergence (top 25 by system divergence)")
    print(f"{'Country':<8} {'Curr':<6} {'Network':<12} {'CustDiv':>8} {'SysDiv':>8} {'Change':>8} {'CustVol':>9} {'SysVol':>9}")
    print("-" * 75)
    for _, r in comparison.head(25).iterrows():
        cust_d = f"{r['cust_div']:.1f}" if pd.notna(r["cust_div"]) else "N/A"
        sys_d = f"{r['sys_div']:.1f}" if pd.notna(r["sys_div"]) else "N/A"
        change = f"{r['div_change']:+.1f}" if pd.notna(r["div_change"]) else "N/A"
        cust_v = f"{r['cust_vol']:,.0f}" if pd.notna(r["cust_vol"]) else "N/A"
        sys_v = f"{r['sys_vol']:,.0f}" if pd.notna(r["sys_vol"]) else "N/A"
        print(f"{r['country']:<8} {r['currency']:<6} {str(r['network']):<12} {cust_d:>8} {sys_d:>8} {change:>8} {cust_v:>9} {sys_v:>9}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Scatter: customer-attempt divergence vs system-attempt divergence

# COMMAND ----------

if not cust_divergence.empty and not sys_divergence.empty:
    plot_comp = comparison.dropna(subset=["cust_div", "sys_div"])
    plot_comp = plot_comp[plot_comp["sys_vol"] >= 100]

    fig, ax = plt.subplots(figsize=(10, 8))

    sizes = plot_comp["sys_vol"] / plot_comp["sys_vol"].max() * 400
    ax.scatter(plot_comp["cust_div"], plot_comp["sys_div"],
               s=sizes, alpha=0.5, c=plot_comp["div_change"],
               cmap="RdYlGn_r", edgecolors="black", linewidth=0.5)

    max_val = max(plot_comp["cust_div"].max(), plot_comp["sys_div"].max()) * 1.1
    ax.plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="Cust = Sys (no retry effect)")

    for _, r in plot_comp.nlargest(10, "sys_div").iterrows():
        ax.annotate(f"{r['country']}|{r['currency']}|{r['network']}",
                    (r["cust_div"], r["sys_div"]),
                    fontsize=7, ha="center", va="bottom",
                    xytext=(0, 5), textcoords="offset points")

    ax.set_xlabel("Customer-Attempt Divergence (pp)", fontsize=11)
    ax.set_ylabel("System-Attempt Divergence (pp)", fontsize=11)
    ax.set_title("Provider Split Divergence: Customer vs System Attempts\n"
                 "(above diagonal = retries amplify divergence)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — Deep Dive into Top Divergent Segments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Detailed breakdown for top 10 divergent segments (customer attempts)

# COMMAND ----------

for _, seg in cust_divergence.head(10).iterrows():
    country, currency, network = seg["country"], seg["currency"], seg["network"]
    print(f"\n{'=' * 100}")
    print(f"Segment: {country} | {currency} | {network}  (divergence={seg['divergence_score']:.1f}pp, total={seg['total_attempts']:,.0f})")
    print(f"{'=' * 100}")

    print(f"\n  CUSTOMER ATTEMPTS (system_rank=1):")
    print(f"  {'Provider':<15}", end="")
    for g in groups:
        print(f"  {'n_'+g:>8} {'share_'+g:>8} {'sr_'+g:>8}", end="")
    print(f"  {'Δshare':>8}")
    print(f"  {'-' * (15 + len(groups) * 28 + 10)}")
    for prov in FOCUS_PROVIDERS:
        pct_a = seg.get(f"{prov}_pct_A", 0)
        pct_b = seg.get(f"{prov}_pct_B", 0)
        vol_a = seg.get(f"{prov}_vol_A", 0)
        vol_b = seg.get(f"{prov}_vol_B", 0)
        sr_a = seg.get(f"{prov}_sr_A", 0)
        sr_b = seg.get(f"{prov}_sr_B", 0)
        delta = seg.get(f"{prov}_delta_pp", 0)
        print(f"  {prov:<15}", end="")
        vals = [(vol_a, pct_a, sr_a), (vol_b, pct_b, sr_b)]
        for v, p, s in vals:
            print(f"  {v:>8,.0f} {p:>8.1%} {s:>8.1%}", end="")
        print(f"  {delta:>+8.1f}")

    sys_seg = sys_divergence[
        (sys_divergence["country"] == country) &
        (sys_divergence["currency"] == currency) &
        (sys_divergence["network"] == network)
    ]
    if not sys_seg.empty:
        s = sys_seg.iloc[0]
        print(f"\n  ALL SYSTEM ATTEMPTS:")
        print(f"  {'Provider':<15}", end="")
        for g in groups:
            print(f"  {'n_'+g:>8} {'share_'+g:>8} {'sr_'+g:>8}", end="")
        print(f"  {'Δshare':>8}")
        print(f"  {'-' * (15 + len(groups) * 28 + 10)}")
        for prov in FOCUS_PROVIDERS:
            pct_a = s.get(f"{prov}_pct_A", 0)
            pct_b = s.get(f"{prov}_pct_B", 0)
            vol_a = s.get(f"{prov}_vol_A", 0)
            vol_b = s.get(f"{prov}_vol_B", 0)
            sr_a = s.get(f"{prov}_sr_A", 0)
            sr_b = s.get(f"{prov}_sr_B", 0)
            delta = s.get(f"{prov}_delta_pp", 0)
            print(f"  {prov:<15}", end="")
            vals = [(vol_a, pct_a, sr_a), (vol_b, pct_b, sr_b)]
            for v, p, sr in vals:
                print(f"  {v:>8,.0f} {p:>8.1%} {sr:>8.1%}", end="")
            print(f"  {delta:>+8.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Exportable divergence tables

# COMMAND ----------

cust_divergence_spark = spark.createDataFrame(cust_divergence)
cust_divergence_spark.createOrReplaceTempView("provider_split_divergence_customer_attempts")

sys_divergence_spark = spark.createDataFrame(sys_divergence)
sys_divergence_spark.createOrReplaceTempView("provider_split_divergence_system_attempts")

print("Created temp views:")
print(f"  provider_split_divergence_customer_attempts  ({len(cust_divergence):,} rows)")
print(f"  provider_split_divergence_system_attempts    ({len(sys_divergence):,} rows)")
print(f"\nExample queries:")
print(f"  SELECT * FROM provider_split_divergence_customer_attempts ORDER BY divergence_score DESC LIMIT 50")
print(f"  SELECT * FROM provider_split_divergence_system_attempts WHERE country = 'JP' ORDER BY divergence_score DESC")

display(cust_divergence_spark.orderBy(F.desc("divergence_score")).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | Section | What it shows |
# MAGIC |---|---|
# MAGIC | 2.3 | Top segments where customer-attempt routing to Adyen/Checkout/JPMC differs |
# MAGIC | 3.3 | Top segments where system-attempt (incl. retries) routing differs |
# MAGIC | 4.1 | Whether retries amplify or dampen the initial routing divergence |
# MAGIC | 4.2 | Scatter plot — points above diagonal mean retries make the split worse |
# MAGIC | 5.1 | Detailed per-provider breakdown (share + SR) for the most divergent segments |
# MAGIC | 5.2 | Temp views for ad-hoc SQL exploration |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of provider split analysis*
