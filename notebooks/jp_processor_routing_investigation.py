# Databricks notebook source
# MAGIC %md
# MAGIC # JP Processor Routing Investigation
# MAGIC
# MAGIC The experiment analysis revealed that **Checkout** and **JPMC** have a
# MAGIC significantly lower share of attempts in variant B compared to C for
# MAGIC Japan-issued cards. This notebook investigates the root cause:
# MAGIC
# MAGIC **Is B routing fewer attempts to these processors, or are attempts
# MAGIC failing before they reach the processor?**
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data extraction (JP only, with funnel columns)
# MAGIC 2. Processor distribution — volume & share by variant
# MAGIC 3. Pre-processor funnel — initiated / sent-to-issuer rates
# MAGIC 4. Attempt type & system rank analysis — retry/fallback patterns
# MAGIC 5. First customer attempt routing — initial routing decision
# MAGIC 6. Error analysis for Checkout/JPMC in B
# MAGIC 7. Daily trend — processor share over time
# MAGIC 8. Payment references for Primer investigation

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
from scipy.stats import norm

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]
A_COLOR, B_COLOR = COLORS[0], COLORS[1]

EXPERIMENT_ID = "pay-payment-orchestration-routing-in-house"
ASSIGNMENT_START = "2026-03-16"
PAYMENT_START = "2026-03-18"
TARGET_COUNTRY = "JP"
FOCUS_PROCESSORS = ["checkout", "jpmc"]

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

def two_proportion_z_test(n1, s1, n2, s2):
    """Two-proportion z-test. Returns (z_stat, p_value, delta_pp, ci_lower_pp, ci_upper_pp)."""
    if n1 == 0 or n2 == 0 or (s1 + s2) == 0:
        return 0.0, 1.0, 0.0, 0.0, 0.0
    p1, p2 = s1 / n1, s2 / n2
    delta = p1 - p2
    p_pool = (s1 + s2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return 0.0, 1.0, 0.0, 0.0, 0.0
    z = delta / se
    p_val = 2 * (1 - norm.cdf(abs(z)))
    ci_half = 1.96 * se
    return z, p_val, delta * 100, (delta - ci_half) * 100, (delta + ci_half) * 100

def sig_label(p):
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1 — Data Extraction (JP, with funnel columns)

# COMMAND ----------

df_jp = spark.sql(f"""
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
    COALESCE(
      e.user_dimensions:custom.visitorPlatform,
      e.user_dimensions:visitorPlatform,
      e.user_dimensions:platform
    ) AS platform,
    e.timestamp AS assigned_at
  FROM production.external_statsig.exposures e
  LEFT ANTI JOIN filtered_customers fc ON e.user_id = fc.visitor_id
  WHERE DATE(timestamp) >= '{ASSIGNMENT_START}'
    AND experiment_id = '{EXPERIMENT_ID}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.timestamp) = 1
)
SELECT
  a.group_name,
  a.platform,
  a.visitor_id,
  p.payment_provider_reference,
  p.customer_system_attempt_reference,
  p.payment_flow,
  p.payment_method_variant,
  p.currency,
  p.payment_processor,
  p.payment_attempt_status,
  p.challenge_issued,
  p.response_code,
  p.fraud_pre_auth_result,
  p.bin_issuer_country_code,
  p.customer_attempt_rank,
  p.system_attempt_rank,
  p.attempt_type,
  p.is_customer_attempt_successful,
  p.is_shopping_cart_successful,
  p.is_successful,
  p.error_code,
  p.payment_initiated,
  p.sent_to_issuer,
  p.payment_attempt_timestamp
FROM assignment a
JOIN production.payments.fact_payment_attempt p
  ON a.visitor_id = p.visitor_id
  AND p.payment_attempt_timestamp::date >= '{PAYMENT_START}'
  AND p.payment_attempt_timestamp > a.assigned_at - INTERVAL 60 SECONDS
WHERE p.payment_flow IN ('pay_now', 'rnpl_pay_early')
  AND p.payment_method = 'payment_card'
  AND p.bin_issuer_country_code = '{TARGET_COUNTRY}'
""")
df_jp.cache()

total_jp = df_jp.count()
print(f"Total JP payment attempts: {total_jp:,}")

jp_by_group = df_jp.groupBy("group_name").count().orderBy("group_name").toPandas()
for _, r in jp_by_group.iterrows():
    print(f"  {r['group_name']}: {r['count']:,}")

groups = sorted(jp_by_group["group_name"].tolist())
g_a, g_b = (groups[0], groups[1]) if len(groups) == 2 else (None, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2 — Processor Distribution: Volume & Share by Variant

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Processor volume and share

# COMMAND ----------

proc_dist = (
    df_jp
    .groupBy("group_name", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp = proc_dist.groupby("group_name")["attempts"].sum()
proc_dist["pct"] = proc_dist.apply(
    lambda r: r["attempts"] / total_by_grp.get(r["group_name"], 1), axis=1,
)

proc_pivot = proc_dist.pivot_table(
    index="payment_processor", columns="group_name",
    values=["attempts", "pct"], aggfunc="first", fill_value=0,
)

print("JP — Processor Distribution by Variant")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
if len(groups) == 2:
    print(f"  {'Δ share(pp)':>12}", end="")
print()
print("-" * (25 + len(groups) * 22 + 14))

for proc in proc_pivot.index:
    print(f"{str(proc):<25}", end="")
    pcts = []
    for g in groups:
        n = proc_pivot.loc[proc, ("attempts", g)] if ("attempts", g) in proc_pivot.columns else 0
        p = proc_pivot.loc[proc, ("pct", g)] if ("pct", g) in proc_pivot.columns else 0
        pcts.append(p)
        print(f"  {n:>10,.0f} {p:>8.2%}", end="")
    if len(pcts) == 2:
        delta = (pcts[1] - pcts[0]) * 100
        marker = " <<<" if str(proc).lower() in FOCUS_PROCESSORS else ""
        print(f"  {delta:>+12.2f}{marker}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Processor share comparison chart

# COMMAND ----------

all_procs = proc_dist.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(all_procs))
h = 0.35

fig, axes = plt.subplots(1, 2, figsize=(20, max(5, len(all_procs) * 0.6)))

for ax_idx, (col, title) in enumerate([("pct", "Share of JP Attempts"), ("attempts", "Volume")]):
    for i, grp in enumerate(groups):
        grp_data = proc_dist[proc_dist["group_name"] == grp].set_index("payment_processor").reindex(all_procs)
        bars = axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                                 label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                fmt = f"{val:.1%}" if col == "pct" else f"{vol:,.0f}"
                axes[ax_idx].text(val + val * 0.02, y[j] + i * h, fmt, va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(all_procs, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    if col == "pct":
        axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

for proc in FOCUS_PROCESSORS:
    if proc in all_procs:
        idx = all_procs.index(proc)
        for ax in axes:
            ax.axhspan(idx - 0.4, idx + 0.8, alpha=0.1, color=COLORS[3])

axes[0].legend(fontsize=10)
fig.suptitle("JP — Processor Distribution by Variant", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Checkout & JPMC share — statistical test

# COMMAND ----------

if len(groups) == 2:
    for proc_name in FOCUS_PROCESSORS:
        for g in groups:
            subset = proc_dist[(proc_dist["payment_processor"].str.lower() == proc_name) &
                               (proc_dist["group_name"] == g)]
            n_proc = subset["attempts"].sum() if not subset.empty else 0
            n_total = total_by_grp.get(g, 0)
            print(f"  {proc_name:<15} {g}: {n_proc:,} / {n_total:,} = {n_proc/n_total:.2%}" if n_total > 0 else f"  {proc_name} {g}: 0")

        n_a_total = total_by_grp.get(g_a, 0)
        n_b_total = total_by_grp.get(g_b, 0)
        s_a = proc_dist[(proc_dist["payment_processor"].str.lower() == proc_name) &
                        (proc_dist["group_name"] == g_a)]["attempts"].sum()
        s_b = proc_dist[(proc_dist["payment_processor"].str.lower() == proc_name) &
                        (proc_dist["group_name"] == g_b)]["attempts"].sum()

        if n_a_total > 0 and n_b_total > 0:
            z, p, delta_pp, ci_lo, ci_hi = two_proportion_z_test(n_a_total, s_a, n_b_total, s_b)
            print(f"  → {proc_name} share:  {g_a}={s_a/n_a_total:.2%}  {g_b}={s_b/n_b_total:.2%}  "
                  f"Δ={delta_pp:+.2f}pp  95%CI=[{ci_lo:+.2f},{ci_hi:+.2f}]  p={p:.4f}{sig_label(p)}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3 — Pre-Processor Funnel
# MAGIC
# MAGIC Do attempts reach the processor but fail before being sent to the issuer?
# MAGIC Or does the routing decision itself differ?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Funnel rates by processor × variant

# COMMAND ----------

funnel_by_proc = (
    df_jp
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("total_attempts"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("initiation_rate", F.col("initiated") / F.col("total_attempts"))
    .withColumn("sent_to_issuer_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .withColumn("auth_rate", F.when(F.col("sent_to_issuer") > 0, F.col("successes") / F.col("sent_to_issuer")))
    .withColumn("overall_sr", F.col("successes") / F.col("total_attempts"))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

print("JP — Payment Funnel by Processor × Variant")
print(f"{'Processor':<20} {'Group':<20} {'Total':>8} {'Init':>8} {'InitR':>7} {'SentIss':>8} {'SentR':>7} {'Succ':>8} {'AuthR':>7} {'SR':>7}")
print("-" * 115)
for proc in funnel_by_proc["payment_processor"].unique():
    for g in groups:
        row = funnel_by_proc[(funnel_by_proc["payment_processor"] == proc) &
                             (funnel_by_proc["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            init_r = f"{r['initiation_rate']:.1%}" if pd.notna(r["initiation_rate"]) else "N/A"
            sent_r = f"{r['sent_to_issuer_rate']:.1%}" if pd.notna(r["sent_to_issuer_rate"]) else "N/A"
            auth_r = f"{r['auth_rate']:.1%}" if pd.notna(r["auth_rate"]) else "N/A"
            sr = f"{r['overall_sr']:.1%}" if pd.notna(r["overall_sr"]) else "N/A"
            print(f"{str(proc):<20} {g:<20} {r['total_attempts']:>8,.0f} {r['initiated']:>8,.0f} {init_r:>7} "
                  f"{r['sent_to_issuer']:>8,.0f} {sent_r:>7} {r['successes']:>8,.0f} {auth_r:>7} {sr:>7}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Funnel chart for focus processors (Checkout & JPMC)

# COMMAND ----------

focus_funnel = funnel_by_proc[funnel_by_proc["payment_processor"].str.lower().isin(FOCUS_PROCESSORS)]

if not focus_funnel.empty:
    focus_procs = sorted(focus_funnel["payment_processor"].unique())

    fig, axes = plt.subplots(1, len(focus_procs), figsize=(10 * len(focus_procs), 6))
    if len(focus_procs) == 1:
        axes = [axes]

    for ax_idx, proc in enumerate(focus_procs):
        proc_data = focus_funnel[focus_funnel["payment_processor"] == proc].sort_values("group_name")
        funnel_stages = ["total_attempts", "initiated", "sent_to_issuer", "successes"]
        stage_labels = ["Total\nAttempts", "Payment\nInitiated", "Sent to\nIssuer", "Successful"]
        x = np.arange(len(funnel_stages))
        w = 0.35

        for i, grp in enumerate(groups):
            grp_row = proc_data[proc_data["group_name"] == grp]
            if not grp_row.empty:
                vals = [grp_row[s].values[0] for s in funnel_stages]
                bars = axes[ax_idx].bar(x + i * w, vals, w,
                                        label=grp, color=A_COLOR if i == 0 else B_COLOR)
                for j, v in enumerate(vals):
                    axes[ax_idx].text(x[j] + i * w, v + v * 0.02, f"{v:,.0f}",
                                      ha="center", va="bottom", fontsize=8)

        axes[ax_idx].set_xticks(x + w / 2)
        axes[ax_idx].set_xticklabels(stage_labels)
        axes[ax_idx].set_title(f"{proc} — Payment Funnel", fontsize=13, fontweight="bold")
        axes[ax_idx].legend(fontsize=9)

    fig.suptitle("JP — Payment Funnel for Focus Processors", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Funnel conversion rates — Checkout & JPMC vs rest

# COMMAND ----------

funnel_by_proc["is_focus"] = funnel_by_proc["payment_processor"].str.lower().isin(FOCUS_PROCESSORS)

funnel_focus_vs_rest = (
    funnel_by_proc
    .groupby(["group_name", "is_focus"])
    .agg({
        "total_attempts": "sum",
        "initiated": "sum",
        "sent_to_issuer": "sum",
        "successes": "sum",
    })
    .reset_index()
)
funnel_focus_vs_rest["initiation_rate"] = funnel_focus_vs_rest["initiated"] / funnel_focus_vs_rest["total_attempts"]
funnel_focus_vs_rest["sent_rate"] = funnel_focus_vs_rest["sent_to_issuer"] / funnel_focus_vs_rest["initiated"]
funnel_focus_vs_rest["auth_rate"] = funnel_focus_vs_rest["successes"] / funnel_focus_vs_rest["sent_to_issuer"]
funnel_focus_vs_rest["overall_sr"] = funnel_focus_vs_rest["successes"] / funnel_focus_vs_rest["total_attempts"]
funnel_focus_vs_rest["label"] = funnel_focus_vs_rest["is_focus"].map({True: "Checkout+JPMC", False: "Other Processors"})

print("JP — Funnel Rates: Focus Processors vs Rest")
print(f"{'Segment':<20} {'Group':<20} {'n':>8} {'InitR':>7} {'SentR':>7} {'AuthR':>7} {'SR':>7}")
print("-" * 80)
for _, r in funnel_focus_vs_rest.sort_values(["is_focus", "group_name"], ascending=[False, True]).iterrows():
    print(f"{r['label']:<20} {r['group_name']:<20} {r['total_attempts']:>8,.0f} "
          f"{r['initiation_rate']:>7.1%} {r['sent_rate']:>7.1%} {r['auth_rate']:>7.1%} {r['overall_sr']:>7.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Attempt status distribution for Checkout & JPMC — B vs C

# COMMAND ----------

focus_status = (
    df_jp
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROCESSORS))
    .groupBy("group_name", "payment_processor", "payment_attempt_status")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_focus_by_grp_proc = focus_status.groupby(["group_name", "payment_processor"])["cnt"].sum().to_dict()
focus_status["pct"] = focus_status.apply(
    lambda r: r["cnt"] / total_focus_by_grp_proc.get((r["group_name"], r["payment_processor"]), 1), axis=1,
)

for proc in sorted(focus_status["payment_processor"].unique()):
    print(f"\n{'=' * 80}")
    print(f"{proc} — Attempt Status Distribution (JP)")
    print(f"{'=' * 80}")
    subset = focus_status[focus_status["payment_processor"] == proc]
    statuses = subset.groupby("payment_attempt_status")["cnt"].sum().sort_values(ascending=False).index
    print(f"{'Status':<30}", end="")
    for g in groups:
        print(f"  {'cnt_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (30 + len(groups) * 22))
    for s in statuses:
        print(f"{str(s):<30}", end="")
        for g in groups:
            row = subset[(subset["payment_attempt_status"] == s) & (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['cnt'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4 — Attempt Type & System Rank Analysis
# MAGIC
# MAGIC Is B generating different retry/fallback patterns that shift volume
# MAGIC away from Checkout/JPMC?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Attempt type distribution by variant (JP)

# COMMAND ----------

attempt_type_dist = (
    df_jp
    .groupBy("group_name", "attempt_type")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_at = attempt_type_dist.groupby("group_name")["attempts"].sum()
attempt_type_dist["pct"] = attempt_type_dist.apply(
    lambda r: r["attempts"] / total_by_grp_at.get(r["group_name"], 1), axis=1,
)

print("JP — Attempt Type Distribution")
print(f"{'Attempt Type':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
print()
print("-" * (25 + len(groups) * 22))
for at in attempt_type_dist.groupby("attempt_type")["attempts"].sum().sort_values(ascending=False).index:
    print(f"{str(at):<25}", end="")
    for g in groups:
        row = attempt_type_dist[(attempt_type_dist["attempt_type"] == at) &
                                (attempt_type_dist["group_name"] == g)]
        if not row.empty:
            print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}", end="")
        else:
            print(f"  {'0':>10} {'0.00%':>8}", end="")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Processor distribution by attempt type × variant

# COMMAND ----------

proc_by_attempt_type = (
    df_jp
    .groupBy("group_name", "attempt_type", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_at_type = proc_by_attempt_type.groupby(["group_name", "attempt_type"])["attempts"].sum().to_dict()
proc_by_attempt_type["pct"] = proc_by_attempt_type.apply(
    lambda r: r["attempts"] / total_by_grp_at_type.get((r["group_name"], r["attempt_type"]), 1), axis=1,
)

for at in proc_by_attempt_type.groupby("attempt_type")["attempts"].sum().sort_values(ascending=False).head(4).index:
    print(f"\n{'=' * 90}")
    print(f"Attempt Type: {at}")
    print(f"{'=' * 90}")
    subset = proc_by_attempt_type[proc_by_attempt_type["attempt_type"] == at]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                marker = " <<<" if str(proc).lower() in FOCUS_PROCESSORS else ""
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}{marker}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 System attempt rank distribution by variant

# COMMAND ----------

rank_dist = (
    df_jp
    .filter(F.col("system_attempt_rank") <= 5)
    .groupBy("group_name", "system_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .orderBy("system_attempt_rank", "group_name")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 5))
ranks = sorted(rank_dist["system_attempt_rank"].unique())
x = np.arange(len(ranks))
w = 0.35

for i, grp in enumerate(groups):
    grp_data = rank_dist[rank_dist["group_name"] == grp].set_index("system_attempt_rank").reindex(ranks)
    axes[0].bar(x + i * w, grp_data["attempts"].fillna(0), w,
                label=grp, color=A_COLOR if i == 0 else B_COLOR)
    axes[1].bar(x + i * w, grp_data["sr"].fillna(0), w,
                label=grp, color=A_COLOR if i == 0 else B_COLOR)
    for j, (vol, sr) in enumerate(zip(grp_data["attempts"].fillna(0), grp_data["sr"].fillna(0))):
        axes[0].text(x[j] + i * w, vol + vol * 0.02, f"{vol:,.0f}", ha="center", va="bottom", fontsize=7)
        axes[1].text(x[j] + i * w, sr + 0.005, f"{sr:.1%}", ha="center", va="bottom", fontsize=7)

for ax in axes:
    ax.set_xticks(x + w / 2)
    ax.set_xticklabels([str(r) for r in ranks])
    ax.set_xlabel("System Attempt Rank")
    ax.legend(fontsize=9)

axes[0].set_title("Volume by System Attempt Rank", fontweight="bold")
axes[1].set_title("SR by System Attempt Rank", fontweight="bold")
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("JP — System Attempt Rank Analysis", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Processor routing by system attempt rank

# COMMAND ----------

proc_by_rank = (
    df_jp
    .filter(F.col("system_attempt_rank") <= 3)
    .groupBy("group_name", "system_attempt_rank", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_by_grp_rank = proc_by_rank.groupby(["group_name", "system_attempt_rank"])["attempts"].sum().to_dict()
proc_by_rank["pct"] = proc_by_rank.apply(
    lambda r: r["attempts"] / total_by_grp_rank.get((r["group_name"], r["system_attempt_rank"]), 1), axis=1,
)

for rank in sorted(proc_by_rank["system_attempt_rank"].unique()):
    print(f"\n{'=' * 90}")
    print(f"System Attempt Rank = {rank}")
    print(f"{'=' * 90}")
    subset = proc_by_rank[proc_by_rank["system_attempt_rank"] == rank]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                marker = " <<<" if str(proc).lower() in FOCUS_PROCESSORS else ""
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}{marker}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5 — First Customer Attempt Routing
# MAGIC
# MAGIC Isolate the initial routing decision: `customer_attempt_rank = 1` and
# MAGIC `system_attempt_rank = 1`. This shows where each variant routes
# MAGIC the very first attempt, before any retry/fallback logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 First-attempt processor distribution

# COMMAND ----------

first_attempt = (
    df_jp
    .filter((F.col("customer_attempt_rank") == 1) & (F.col("system_attempt_rank") == 1))
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
    )
    .withColumn("sr", F.col("successes") / F.col("attempts"))
    .withColumn("init_rate", F.col("initiated") / F.col("attempts"))
    .withColumn("sent_rate", F.when(F.col("initiated") > 0, F.col("sent_to_issuer") / F.col("initiated")))
    .toPandas()
)

total_first_by_grp = first_attempt.groupby("group_name")["attempts"].sum()
first_attempt["pct_share"] = first_attempt.apply(
    lambda r: r["attempts"] / total_first_by_grp.get(r["group_name"], 1), axis=1,
)

print("JP — First Attempt (rank 1/1) Processor Distribution")
print(f"{'Processor':<25}", end="")
for g in groups:
    print(f"  {'n_'+g:>8} {'share':>7} {'SR':>7} {'InitR':>7} {'SentR':>7}", end="")
print()
print("-" * (25 + len(groups) * 42))
procs_sorted = first_attempt.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
for proc in procs_sorted:
    print(f"{str(proc):<25}", end="")
    for g in groups:
        row = first_attempt[(first_attempt["payment_processor"] == proc) &
                            (first_attempt["group_name"] == g)]
        if not row.empty:
            r = row.iloc[0]
            sent_r = f"{r['sent_rate']:.1%}" if pd.notna(r["sent_rate"]) else "N/A"
            marker = " <<<" if str(proc).lower() in FOCUS_PROCESSORS else ""
            print(f"  {r['attempts']:>8,.0f} {r['pct_share']:>7.1%} {r['sr']:>7.1%} {r['init_rate']:>7.1%} {sent_r:>7}{marker}", end="")
        else:
            print(f"  {'0':>8} {'0.0%':>7} {'N/A':>7} {'N/A':>7} {'N/A':>7}", end="")
    print()

print(f"\nTotal first attempts: ", end="")
for g in groups:
    print(f"  {g}={total_first_by_grp.get(g, 0):,.0f}", end="")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 First-attempt routing chart

# COMMAND ----------

procs_first = first_attempt.groupby("payment_processor")["attempts"].sum().sort_values(ascending=True).index.tolist()
y = np.arange(len(procs_first))
h = 0.35

fig, axes = plt.subplots(1, 2, figsize=(18, max(5, len(procs_first) * 0.6)))

for ax_idx, (col, title) in enumerate([("pct_share", "Share of 1st Attempts"), ("sr", "Success Rate")]):
    for i, grp in enumerate(groups):
        grp_data = first_attempt[first_attempt["group_name"] == grp].set_index("payment_processor").reindex(procs_first)
        bars = axes[ax_idx].barh(y + i * h, grp_data[col].fillna(0), h,
                                 label=grp if ax_idx == 0 else "", color=A_COLOR if i == 0 else B_COLOR)
        for j, (val, vol) in enumerate(zip(grp_data[col].fillna(0), grp_data["attempts"].fillna(0))):
            if vol > 0:
                axes[ax_idx].text(val + 0.003, y[j] + i * h, f"{val:.1%} (n={vol:,.0f})",
                                  va="center", fontsize=7)
    axes[ax_idx].set_yticks(y + h / 2)
    axes[ax_idx].set_yticklabels(procs_first, fontsize=9)
    axes[ax_idx].set_title(title, fontweight="bold", fontsize=12)
    axes[ax_idx].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

for proc in FOCUS_PROCESSORS:
    if proc in procs_first:
        idx = procs_first.index(proc)
        for ax in axes:
            ax.axhspan(idx - 0.4, idx + 0.8, alpha=0.1, color=COLORS[3])

axes[0].legend(fontsize=10)
fig.suptitle("JP — First Attempt Routing (customer_rank=1, system_rank=1)",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 First-attempt routing by payment method variant

# COMMAND ----------

first_by_pmv = (
    df_jp
    .filter((F.col("customer_attempt_rank") == 1) & (F.col("system_attempt_rank") == 1))
    .groupBy("group_name", "payment_method_variant", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

total_first_by_grp_pmv = first_by_pmv.groupby(["group_name", "payment_method_variant"])["attempts"].sum().to_dict()
first_by_pmv["pct"] = first_by_pmv.apply(
    lambda r: r["attempts"] / total_first_by_grp_pmv.get((r["group_name"], r["payment_method_variant"]), 1), axis=1,
)

top_pmvs = first_by_pmv.groupby("payment_method_variant")["attempts"].sum().nlargest(4).index
for pmv in top_pmvs:
    print(f"\n{'=' * 90}")
    print(f"Payment Method Variant: {pmv} — First Attempt Routing")
    print(f"{'=' * 90}")
    subset = first_by_pmv[first_by_pmv["payment_method_variant"] == pmv]
    procs = subset.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index
    print(f"{'Processor':<25}", end="")
    for g in groups:
        print(f"  {'n_'+g:>10} {'pct_'+g:>8}", end="")
    print()
    print("-" * (25 + len(groups) * 22))
    for proc in procs:
        print(f"{str(proc):<25}", end="")
        for g in groups:
            row = subset[(subset["payment_processor"] == proc) & (subset["group_name"] == g)]
            if not row.empty:
                marker = " <<<" if str(proc).lower() in FOCUS_PROCESSORS else ""
                print(f"  {row['attempts'].values[0]:>10,} {row['pct'].values[0]:>8.2%}{marker}", end="")
            else:
                print(f"  {'0':>10} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6 — Error Analysis for Checkout & JPMC in B

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Error codes for Checkout/JPMC failures in B vs C

# COMMAND ----------

focus_errors = (
    df_jp
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROCESSORS))
    .filter(F.col("is_successful") == 0)
    .groupBy("group_name", "payment_processor", "error_code", "response_code")
    .agg(F.count("*").alias("cnt"))
    .toPandas()
)

total_focus_fails = focus_errors.groupby(["group_name", "payment_processor"])["cnt"].sum().to_dict()
focus_errors["pct"] = focus_errors.apply(
    lambda r: r["cnt"] / total_focus_fails.get((r["group_name"], r["payment_processor"]), 1), axis=1,
)

for proc in sorted(focus_errors["payment_processor"].unique()):
    print(f"\n{'=' * 100}")
    print(f"{proc} — Failed Attempts Error Breakdown (JP)")
    print(f"{'=' * 100}")
    subset = focus_errors[focus_errors["payment_processor"] == proc]
    top_combos = (subset.groupby(["error_code", "response_code"])["cnt"].sum()
                  .sort_values(ascending=False).head(15).index)
    print(f"{'Error Code':<35} {'Resp':>10}", end="")
    for g in groups:
        print(f"  {'cnt_'+g:>8} {'pct_'+g:>8}", end="")
    print()
    print("-" * (45 + len(groups) * 20))
    for ec, rc in top_combos:
        print(f"{str(ec):<35} {str(rc):>10}", end="")
        for g in groups:
            row = subset[(subset["error_code"] == ec) &
                         (subset["response_code"] == rc) &
                         (subset["group_name"] == g)]
            if not row.empty:
                print(f"  {row['cnt'].values[0]:>8,} {row['pct'].values[0]:>8.2%}", end="")
            else:
                print(f"  {'0':>8} {'0.00%':>8}", end="")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 3DS challenge handling for Checkout/JPMC in B vs C

# COMMAND ----------

focus_3ds = (
    df_jp
    .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROCESSORS))
    .withColumn("_challenged", F.col("challenge_issued").cast("boolean"))
    .groupBy("group_name", "payment_processor")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("_challenged").cast("int")).alias("challenges"),
        F.sum(F.when(F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("ch_successes"),
        F.sum(F.when(~F.col("_challenged") & (F.col("is_successful") == 1), 1).otherwise(0)).alias("no_ch_successes"),
        F.sum(F.when(~F.col("_challenged"), 1).otherwise(0)).alias("no_ch_total"),
    )
    .withColumn("challenge_rate", F.col("challenges") / F.col("total"))
    .withColumn("challenge_sr", F.when(F.col("challenges") > 0, F.col("ch_successes") / F.col("challenges")))
    .withColumn("no_challenge_sr", F.when(F.col("no_ch_total") > 0, F.col("no_ch_successes") / F.col("no_ch_total")))
    .orderBy("payment_processor", "group_name")
    .toPandas()
)

print("JP — 3DS for Checkout & JPMC")
print(f"{'Processor':<20} {'Group':<20} {'Total':>8} {'ChRate':>8} {'ChSR':>8} {'NoChSR':>8}")
print("-" * 80)
for _, r in focus_3ds.iterrows():
    ch_sr = f"{r['challenge_sr']:.1%}" if pd.notna(r["challenge_sr"]) else "N/A"
    no_ch_sr = f"{r['no_challenge_sr']:.1%}" if pd.notna(r["no_challenge_sr"]) else "N/A"
    print(f"{r['payment_processor']:<20} {r['group_name']:<20} {r['total']:>8,.0f} "
          f"{r['challenge_rate']:>8.1%} {ch_sr:>8} {no_ch_sr:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 7 — Daily Trend

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Daily processor share for Checkout & JPMC

# COMMAND ----------

daily_proc = (
    df_jp
    .withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date", "group_name", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .toPandas()
)

daily_total = daily_proc.groupby(["attempt_date", "group_name"])["attempts"].sum().reset_index()
daily_total.columns = ["attempt_date", "group_name", "total"]
daily_proc = daily_proc.merge(daily_total, on=["attempt_date", "group_name"])
daily_proc["pct"] = daily_proc["attempts"] / daily_proc["total"]

focus_daily = daily_proc[daily_proc["payment_processor"].str.lower().isin(FOCUS_PROCESSORS)]
focus_procs_daily = sorted(focus_daily["payment_processor"].unique())

fig, axes = plt.subplots(len(focus_procs_daily), 2, figsize=(18, 5 * len(focus_procs_daily)))
if len(focus_procs_daily) == 1:
    axes = [axes]

for row_idx, proc in enumerate(focus_procs_daily):
    proc_daily = focus_daily[focus_daily["payment_processor"] == proc]

    for i, (grp, grp_data) in enumerate(proc_daily.groupby("group_name")):
        color = A_COLOR if i == 0 else B_COLOR
        axes[row_idx][0].plot(grp_data["attempt_date"], grp_data["pct"],
                              color=color, linewidth=1.5, label=grp, marker=".", markersize=4)
        axes[row_idx][1].plot(grp_data["attempt_date"], grp_data["attempts"],
                              color=color, linewidth=1.5, label=grp, marker=".", markersize=4)

    axes[row_idx][0].set_title(f"{proc} — Daily Share of JP Attempts", fontsize=12, fontweight="bold")
    axes[row_idx][0].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    axes[row_idx][0].legend(fontsize=9)
    axes[row_idx][1].set_title(f"{proc} — Daily Volume", fontsize=12, fontweight="bold")
    axes[row_idx][1].legend(fontsize=9)

fig.suptitle("JP — Daily Checkout & JPMC Share and Volume", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Daily overall processor share stacked (B vs C)

# COMMAND ----------

for grp in groups:
    grp_daily = daily_proc[daily_proc["group_name"] == grp].copy()
    dates = sorted(grp_daily["attempt_date"].unique())
    procs_order = grp_daily.groupby("payment_processor")["attempts"].sum().sort_values(ascending=False).index.tolist()

    pivot = grp_daily.pivot_table(index="attempt_date", columns="payment_processor",
                                  values="pct", aggfunc="first", fill_value=0)
    pivot = pivot.reindex(columns=procs_order)

    fig, ax = plt.subplots(figsize=(16, 5))
    pivot.plot.area(ax=ax, stacked=True, alpha=0.7,
                    color=COLORS[:len(procs_order)])
    ax.set_title(f"{grp} — Daily Processor Share (JP)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.legend(fontsize=8, loc="upper right", ncol=2)
    ax.set_xlabel("")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 8 — Payment References for Primer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Checkout failed attempts in B — payment references

# COMMAND ----------

if g_b is not None:
    for proc_name in FOCUS_PROCESSORS:
        refs_df = (
            df_jp
            .filter(F.col("group_name") == g_b)
            .filter(F.lower(F.col("payment_processor")) == proc_name)
            .filter(F.col("is_successful") == 0)
            .select(
                "payment_provider_reference",
                "customer_system_attempt_reference",
                "payment_attempt_timestamp",
                "payment_processor",
                "payment_method_variant",
                "payment_attempt_status",
                "error_code",
                "response_code",
                F.col("challenge_issued").cast("boolean").alias("challenged"),
                "fraud_pre_auth_result",
                "customer_attempt_rank",
                "system_attempt_rank",
                "attempt_type",
            )
            .orderBy(F.desc("payment_attempt_timestamp"))
        )
        cnt = refs_df.count()
        print(f"\n{'=' * 80}")
        print(f"{proc_name.upper()} — Failed attempts in {g_b} (JP): {cnt:,}")
        print(f"{'=' * 80}")
        display(refs_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Successful Checkout/JPMC in C vs failed in B — side by side
# MAGIC
# MAGIC Sample references from C (successful) and B (failed) for the same
# MAGIC processor, to compare in Primer what differs.

# COMMAND ----------

if g_a is not None and g_b is not None:
    for proc_name in FOCUS_PROCESSORS:
        print(f"\n{'=' * 100}")
        print(f"{proc_name.upper()} — Side-by-side: Successful in {g_a} vs Failed in {g_b}")
        print(f"{'=' * 100}")

        for label, grp, success_filter in [
            (f"Successful in {g_a}", g_a, F.col("is_successful") == 1),
            (f"Failed in {g_b}", g_b, F.col("is_successful") == 0),
        ]:
            sample = (
                df_jp
                .filter(F.col("group_name") == grp)
                .filter(F.lower(F.col("payment_processor")) == proc_name)
                .filter(success_filter)
                .select(
                    "payment_provider_reference",
                    "payment_processor",
                    "payment_method_variant",
                    "payment_attempt_status",
                    "error_code",
                    "response_code",
                    F.col("challenge_issued").cast("boolean").alias("challenged"),
                    "attempt_type",
                    "system_attempt_rank",
                )
                .limit(15)
                .toPandas()
            )
            total = (
                df_jp
                .filter(F.col("group_name") == grp)
                .filter(F.lower(F.col("payment_processor")) == proc_name)
                .filter(success_filter)
                .count()
            )
            print(f"\n  {label} (total: {total:,}, showing 15):")
            if not sample.empty:
                print(sample.to_string(index=False))
            else:
                print("  (no attempts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 Exportable temp view

# COMMAND ----------

if g_b is not None:
    jp_b_focus_refs = (
        df_jp
        .filter(F.col("group_name") == g_b)
        .filter(F.lower(F.col("payment_processor")).isin(FOCUS_PROCESSORS))
        .select(
            "payment_provider_reference",
            "customer_system_attempt_reference",
            "payment_attempt_timestamp",
            "payment_processor",
            "payment_method_variant",
            "payment_attempt_status",
            "is_successful",
            "error_code",
            "response_code",
            F.col("challenge_issued").cast("boolean").alias("challenged"),
            "fraud_pre_auth_result",
            "attempt_type",
            "customer_attempt_rank",
            "system_attempt_rank",
            "payment_initiated",
            "sent_to_issuer",
        )
        .orderBy("payment_processor", "error_code", F.desc("payment_attempt_timestamp"))
    )

    jp_b_focus_refs.createOrReplaceTempView("jp_b_checkout_jpmc_references")
    ref_count = jp_b_focus_refs.count()

    print(f"Created temp view: jp_b_checkout_jpmc_references")
    print(f"Total rows: {ref_count:,}")
    print(f"\nExample queries:")
    print(f"  SELECT * FROM jp_b_checkout_jpmc_references WHERE payment_processor ILIKE '%checkout%' AND is_successful = 0 LIMIT 50")
    print(f"  SELECT * FROM jp_b_checkout_jpmc_references WHERE payment_processor ILIKE '%jpmc%' AND challenged = true LIMIT 50")
    print(f"  SELECT DISTINCT payment_provider_reference FROM jp_b_checkout_jpmc_references WHERE is_successful = 0")

    display(jp_b_focus_refs.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC **Key questions this notebook answers:**
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 2.1 | How different is the processor share between B and C? |
# MAGIC | 3.1 | Do Checkout/JPMC attempts in B fail before reaching the issuer? |
# MAGIC | 3.4 | What statuses do Checkout/JPMC attempts end up in for each variant? |
# MAGIC | 4.2 | Does B route to different processors for the same attempt type? |
# MAGIC | 4.4 | Does B route system retries differently (rank 2, 3)? |
# MAGIC | 5.1 | For the very first attempt, does B route to different processors than C? |
# MAGIC | 5.3 | Does the routing difference depend on card brand (Visa/MC)? |
# MAGIC | 6.1 | What are the specific error/response codes for Checkout/JPMC failures in B? |
# MAGIC | 7.1 | Did the processor share shift happen on a specific date or was it always present? |
# MAGIC | 8.1–8.3 | Payment references for Primer investigation |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of JP processor routing investigation*
