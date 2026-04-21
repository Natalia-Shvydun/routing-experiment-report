# Databricks notebook source
# MAGIC %md
# MAGIC # Routing Optimisation Analysis — `fact_payment_attempt`
# MAGIC
# MAGIC Identify segments where routing traffic to a different payment processor
# MAGIC could improve success rates, and quantify the potential uplift.
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. For every combination of routing-relevant dimensions
# MAGIC    (country, currency, payment method, payment terms), compare success
# MAGIC    rates across processors.
# MAGIC 2. Apply statistical significance testing (two-proportion z-test) so
# MAGIC    that conclusions are only drawn from segments with sufficient volume.
# MAGIC 3. Rank the opportunities by estimated uplift (additional successful
# MAGIC    attempts if the segment were routed to the best processor).
# MAGIC
# MAGIC **Key parameters (tunable in the config cell):**
# MAGIC - `MIN_ATTEMPTS_PER_CELL`  — minimum attempts per processor×segment to be
# MAGIC   considered (guards against noisy estimates from low volume).
# MAGIC - `MIN_ATTEMPTS_SEGMENT`   — minimum total attempts in the segment across
# MAGIC   all processors (ignores ultra-long-tail segments).
# MAGIC - `SIGNIFICANCE_LEVEL`     — p-value threshold for the z-test (default 0.05).
# MAGIC - `MIN_SR_DELTA`           — minimum absolute success-rate difference to flag
# MAGIC   (e.g. 0.02 = 2 pp).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

TABLE = "production.payments.fact_payment_attempt"

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

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

# COMMAND ----------

df = (
    spark.table(TABLE)
    .filter(F.col("payment_attempt_timestamp").isNotNull())
    .filter(F.col("payment_processor").isNotNull())
)
df.cache()
total_rows = df.count()
print(f"Total rows: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis parameters

# COMMAND ----------

MIN_ATTEMPTS_PER_CELL = 100
MIN_ATTEMPTS_SEGMENT  = 500
SIGNIFICANCE_LEVEL    = 0.05
MIN_SR_DELTA          = 0.02

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1: Processor Landscape Overview

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Volume & success rate by processor

# COMMAND ----------

proc_overview = (
    df.groupBy("payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
    .withColumn("pct_volume", F.round(F.col("attempts") / total_rows * 100, 2))
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, max(3, len(proc_overview) * 0.5)))
axes[0].barh(proc_overview["payment_processor"], proc_overview["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Processor", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1000:.0f}K"))

axes[1].barh(proc_overview["payment_processor"], proc_overview["success_rate"], color=COLORS[2])
axes[1].set_title("Success Rate by Processor", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("Processor Landscape", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

display(spark.createDataFrame(proc_overview))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Weekly success rate trend by processor

# COMMAND ----------

weekly_proc = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
for i, (proc, grp) in enumerate(weekly_proc.groupby("payment_processor")):
    ax.plot(grp["week"], grp["success_rate"], color=COLORS[i % len(COLORS)],
            linewidth=1.5, label=proc)
ax.set_title("Weekly Success Rate by Processor", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2: Segment-Level Processor Comparison
# MAGIC
# MAGIC For every segment (defined by the routing dimensions below), compute
# MAGIC per-processor success rates and flag opportunities where routing to the
# MAGIC best processor would yield a statistically significant uplift.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Build segment × processor matrix

# COMMAND ----------

segment_dims = ["customer_country_code", "currency", "payment_method", "payment_terms"]

print(f"Routing dimensions used: {segment_dims}")

seg_proc = (
    df.groupBy(*segment_dims, "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
)

seg_totals = (
    seg_proc.groupBy(*segment_dims)
    .agg(
        F.sum("attempts").alias("seg_total_attempts"),
        F.sum("successes").alias("seg_total_successes"),
        F.count("*").alias("num_processors"),
    )
    .withColumn("seg_success_rate", F.col("seg_total_successes") / F.col("seg_total_attempts"))
)

seg_proc_enriched = (
    seg_proc
    .join(seg_totals, on=segment_dims, how="inner")
    .filter(F.col("seg_total_attempts") >= MIN_ATTEMPTS_SEGMENT)
    .filter(F.col("attempts") >= MIN_ATTEMPTS_PER_CELL)
    .filter(F.col("num_processors") >= 2)
)

print(f"Segments with ≥2 qualified processors: {seg_proc_enriched.select(*segment_dims).distinct().count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Find best & worst processor per segment

# COMMAND ----------

w = Window.partitionBy(*segment_dims)

seg_ranked = (
    seg_proc_enriched
    .withColumn("best_sr", F.max("success_rate").over(w))
    .withColumn("worst_sr", F.min("success_rate").over(w))
    .withColumn("sr_rank", F.dense_rank().over(w.orderBy(F.desc("success_rate"))))
    .withColumn("num_processors_in_seg", F.count("*").over(w))
    .withColumn("is_best", F.col("success_rate") == F.col("best_sr"))
    .withColumn("is_worst", F.col("sr_rank") == F.col("num_processors_in_seg"))
)

seg_summary = (
    seg_ranked
    .groupBy(*segment_dims)
    .agg(
        F.sum("attempts").alias("total_attempts"),
        F.max("best_sr").alias("best_sr"),
        F.min("worst_sr").alias("worst_sr"),
        F.max(F.when(F.col("is_best"), F.col("payment_processor"))).alias("best_processor"),
        F.max(F.when(F.col("is_best"), F.col("attempts"))).alias("best_proc_volume"),
        F.max(F.when(F.col("is_worst"), F.col("payment_processor"))).alias("worst_processor"),
        F.first("seg_success_rate").alias("current_blended_sr"),
    )
    .withColumn("sr_gap_pp", F.round((F.col("best_sr") - F.col("worst_sr")) * 100, 2))
    .withColumn("potential_uplift_attempts",
                F.round(F.col("total_attempts") * (F.col("best_sr") - F.col("current_blended_sr")), 0))
    .orderBy(F.desc("potential_uplift_attempts"))
)

display(seg_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Statistical significance testing
# MAGIC
# MAGIC Two-proportion z-test for every segment: compare the best processor
# MAGIC against each other processor. A segment is flagged as an "opportunity"
# MAGIC only if the gap is both practically meaningful (≥ `MIN_SR_DELTA`) and
# MAGIC statistically significant (p < `SIGNIFICANCE_LEVEL`).

# COMMAND ----------

seg_pdf = seg_ranked.toPandas()

from scipy.stats import norm

def two_proportion_z_test(n1, s1, n2, s2):
    """Return (z_stat, p_value) for H0: p1 = p2 vs H1: p1 ≠ p2."""
    p1, p2 = s1 / n1, s2 / n2
    p_pool = (s1 + s2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return 0.0, 1.0
    z = (p1 - p2) / se
    p_val = 2 * (1 - norm.cdf(abs(z)))
    return z, p_val

records = []
for seg_key, seg_group in seg_pdf.groupby(segment_dims):
    if isinstance(seg_key, str):
        seg_key = (seg_key,)
    seg_group = seg_group.sort_values("success_rate", ascending=False)
    best_row = seg_group.iloc[0]
    for _, other_row in seg_group.iloc[1:].iterrows():
        z_stat, p_val = two_proportion_z_test(
            best_row["attempts"], best_row["successes"],
            other_row["attempts"], other_row["successes"],
        )
        sr_delta = best_row["success_rate"] - other_row["success_rate"]
        is_significant = p_val < SIGNIFICANCE_LEVEL and sr_delta >= MIN_SR_DELTA

        rec = {}
        for dim, val in zip(segment_dims, seg_key):
            rec[dim] = val
        rec.update({
            "best_processor":    best_row["payment_processor"],
            "best_sr":           best_row["success_rate"],
            "best_volume":       int(best_row["attempts"]),
            "other_processor":   other_row["payment_processor"],
            "other_sr":          other_row["success_rate"],
            "other_volume":      int(other_row["attempts"]),
            "sr_delta_pp":       round(sr_delta * 100, 2),
            "z_stat":            round(z_stat, 3),
            "p_value":           round(p_val, 6),
            "is_significant":    is_significant,
            "potential_extra_successes": round(sr_delta * other_row["attempts"], 0),
        })
        records.append(rec)

comparisons = pd.DataFrame(records)
print(f"Total pairwise comparisons: {len(comparisons):,}")
print(f"Statistically significant with ≥{MIN_SR_DELTA*100:.0f}pp gap: "
      f"{comparisons['is_significant'].sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Top opportunities — significant gaps ranked by uplift

# COMMAND ----------

opps = comparisons[comparisons["is_significant"]].sort_values(
    "potential_extra_successes", ascending=False
)

display(spark.createDataFrame(opps.head(50)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Opportunity heatmap: success-rate gap by country × processor

# COMMAND ----------

if len(opps) > 0:
    heat_data = (
        opps.groupby(["customer_country_code", "other_processor"])
        .agg(
            total_extra=("potential_extra_successes", "sum"),
            avg_gap_pp=("sr_delta_pp", "mean"),
            segments=("sr_delta_pp", "count"),
        )
        .reset_index()
        .sort_values("total_extra", ascending=False)
    )

    top_countries = heat_data.groupby("customer_country_code")["total_extra"].sum().nlargest(15).index
    heat_filtered = heat_data[heat_data["customer_country_code"].isin(top_countries)]

    if len(heat_filtered) > 0:
        pivot_gap = heat_filtered.pivot_table(
            index="customer_country_code", columns="other_processor",
            values="avg_gap_pp", aggfunc="mean",
        ).fillna(0)

        pivot_gap = pivot_gap.loc[
            pivot_gap.max(axis=1).sort_values(ascending=False).index
        ]

        fig, ax = plt.subplots(figsize=(max(8, pivot_gap.shape[1] * 1.5),
                                        max(5, pivot_gap.shape[0] * 0.45)))
        im = ax.imshow(pivot_gap.values, cmap="YlOrRd", aspect="auto")
        ax.set_xticks(range(pivot_gap.shape[1]))
        ax.set_xticklabels(pivot_gap.columns, rotation=45, ha="right", fontsize=9)
        ax.set_yticks(range(pivot_gap.shape[0]))
        ax.set_yticklabels(pivot_gap.index, fontsize=9)
        for i in range(pivot_gap.shape[0]):
            for j in range(pivot_gap.shape[1]):
                val = pivot_gap.iloc[i, j]
                if val > 0:
                    ax.text(j, i, f"{val:.1f}pp", ha="center", va="center", fontsize=7,
                            color="white" if val > pivot_gap.values.max() * 0.6 else "black")
        plt.colorbar(im, ax=ax, label="Avg SR gap (pp) vs best processor")
        ax.set_title("Avg Success-Rate Gap vs Best Processor\n(Top 15 countries with significant opportunities)",
                     fontsize=13, fontweight="bold")
        fig.tight_layout()
        plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Distribution of success-rate gaps (significant only)

# COMMAND ----------

if len(opps) > 0:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.hist(opps["sr_delta_pp"], bins=30, color=COLORS[0], edgecolor="white", alpha=0.8)
    ax.axvline(opps["sr_delta_pp"].median(), color=COLORS[1], linestyle="--",
               label=f"Median: {opps['sr_delta_pp'].median():.1f}pp")
    ax.set_xlabel("Success-Rate Gap (pp)")
    ax.set_ylabel("Number of segment×processor pairs")
    ax.set_title("Distribution of Significant SR Gaps", fontsize=14, fontweight="bold")
    ax.legend()
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3: Aggregate Uplift Estimation
# MAGIC
# MAGIC If every "significant opportunity" segment were routed entirely to its
# MAGIC best processor, how many additional successes would we expect?
# MAGIC This is an **upper-bound** estimate — in practice gains will be smaller
# MAGIC because the best processor may not scale linearly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Total potential uplift

# COMMAND ----------

if len(opps) > 0:
    total_reroutable = opps["other_volume"].sum()
    total_extra      = opps["potential_extra_successes"].sum()
    total_current    = (opps["other_sr"] * opps["other_volume"]).sum()
    current_sr       = total_current / total_reroutable if total_reroutable > 0 else 0
    potential_sr     = (total_current + total_extra) / total_reroutable if total_reroutable > 0 else 0

    print(f"Reroutable attempts       : {total_reroutable:>12,.0f}")
    print(f"Current successes (those) : {total_current:>12,.0f}  ({current_sr:.2%})")
    print(f"Potential extra successes  : {total_extra:>12,.0f}")
    print(f"Potential new SR (those)   : {potential_sr:>12.2%}  (+{(potential_sr-current_sr)*100:.1f}pp)")
    print(f"% of all attempts affected : {total_reroutable/total_rows*100:.1f}%")
else:
    print("No significant opportunities found with current thresholds.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Uplift by country (top 15)

# COMMAND ----------

if len(opps) > 0:
    uplift_by_country = (
        opps.groupby("customer_country_code")
        .agg(
            reroutable_attempts=("other_volume", "sum"),
            extra_successes=("potential_extra_successes", "sum"),
            pairs=("sr_delta_pp", "count"),
            avg_gap_pp=("sr_delta_pp", "mean"),
        )
        .sort_values("extra_successes", ascending=False)
        .head(15)
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(uplift_by_country["customer_country_code"], uplift_by_country["extra_successes"],
                   color=COLORS[0])
    ax.invert_yaxis()
    ax.set_xlabel("Potential Extra Successes")
    ax.set_title("Top 15 Countries by Routing Optimisation Uplift", fontsize=14, fontweight="bold")
    for bar, gap in zip(bars, uplift_by_country["avg_gap_pp"]):
        ax.text(bar.get_width() + bar.get_width() * 0.02,
                bar.get_y() + bar.get_height() / 2,
                f"avg gap {gap:.1f}pp", va="center", fontsize=8, color="#555")
    fig.tight_layout()
    plt.show()

    display(spark.createDataFrame(uplift_by_country))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Uplift by payment method

# COMMAND ----------

if len(opps) > 0:
    uplift_by_method = (
        opps.groupby("payment_method")
        .agg(
            reroutable_attempts=("other_volume", "sum"),
            extra_successes=("potential_extra_successes", "sum"),
            pairs=("sr_delta_pp", "count"),
            avg_gap_pp=("sr_delta_pp", "mean"),
        )
        .sort_values("extra_successes", ascending=False)
        .head(10)
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.barh(uplift_by_method["payment_method"], uplift_by_method["extra_successes"],
                   color=COLORS[1])
    ax.invert_yaxis()
    ax.set_xlabel("Potential Extra Successes")
    ax.set_title("Top Payment Methods by Routing Optimisation Uplift", fontsize=14, fontweight="bold")
    for bar, gap in zip(bars, uplift_by_method["avg_gap_pp"]):
        ax.text(bar.get_width() + bar.get_width() * 0.02,
                bar.get_y() + bar.get_height() / 2,
                f"avg gap {gap:.1f}pp", va="center", fontsize=8, color="#555")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Uplift by payment terms

# COMMAND ----------

if len(opps) > 0:
    uplift_by_terms = (
        opps.groupby("payment_terms")
        .agg(
            reroutable_attempts=("other_volume", "sum"),
            extra_successes=("potential_extra_successes", "sum"),
            pairs=("sr_delta_pp", "count"),
            avg_gap_pp=("sr_delta_pp", "mean"),
        )
        .sort_values("extra_successes", ascending=False)
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, max(3, len(uplift_by_terms) * 0.6)))
    bars = ax.barh(uplift_by_terms["payment_terms"], uplift_by_terms["extra_successes"],
                   color=COLORS[2])
    ax.invert_yaxis()
    ax.set_xlabel("Potential Extra Successes")
    ax.set_title("Routing Uplift by Payment Terms", fontsize=14, fontweight="bold")
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4: Deep Dives

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Processor success rate by country (top 15 countries)

# COMMAND ----------

top_countries = (
    df.groupBy("customer_country_code").count()
    .orderBy(F.desc("count")).limit(15)
    .select("customer_country_code").toPandas()["customer_country_code"].tolist()
)

proc_country = (
    df.filter(F.col("customer_country_code").isin(top_countries))
    .groupBy("customer_country_code", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
    .filter(F.col("attempts") >= MIN_ATTEMPTS_PER_CELL)
    .toPandas()
)

proc_list = proc_country.groupby("payment_processor")["attempts"].sum().nlargest(6).index.tolist()
plot_data = proc_country[proc_country["payment_processor"].isin(proc_list)]

pivot = plot_data.pivot_table(
    index="customer_country_code", columns="payment_processor",
    values="success_rate", aggfunc="first",
)
pivot = pivot.loc[pivot.mean(axis=1).sort_values(ascending=False).index]

fig, ax = plt.subplots(figsize=(14, max(5, len(pivot) * 0.45)))
x = np.arange(len(pivot))
w = 0.8 / len(pivot.columns)
for i, proc in enumerate(pivot.columns):
    vals = pivot[proc].fillna(0).values
    ax.barh(x + i * w, vals, height=w, color=COLORS[i % len(COLORS)], label=proc)
ax.set_yticks(x + w * len(pivot.columns) / 2)
ax.set_yticklabels(pivot.index, fontsize=9)
ax.invert_yaxis()
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate by Country × Processor (min volume filter applied)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=8, loc="lower right")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Processor success rate by payment method

# COMMAND ----------

proc_method = (
    df.groupBy("payment_method", "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
    .filter(F.col("attempts") >= MIN_ATTEMPTS_PER_CELL)
    .toPandas()
)

top_methods = proc_method.groupby("payment_method")["attempts"].sum().nlargest(10).index
plot_data = proc_method[proc_method["payment_method"].isin(top_methods)]

pivot_m = plot_data.pivot_table(
    index="payment_method", columns="payment_processor",
    values="success_rate", aggfunc="first",
)
pivot_m = pivot_m.loc[pivot_m.mean(axis=1).sort_values(ascending=False).index]

fig, ax = plt.subplots(figsize=(14, max(4, len(pivot_m) * 0.5)))
x = np.arange(len(pivot_m))
w = 0.8 / len(pivot_m.columns)
for i, proc in enumerate(pivot_m.columns):
    vals = pivot_m[proc].fillna(0).values
    ax.barh(x + i * w, vals, height=w, color=COLORS[i % len(COLORS)], label=proc)
ax.set_yticks(x + w * len(pivot_m.columns) / 2)
ax.set_yticklabels(pivot_m.index, fontsize=9)
ax.invert_yaxis()
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate by Payment Method × Processor",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=8, loc="lower right")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Volume coverage — which processors serve which segments?

# COMMAND ----------

coverage = (
    df.groupBy("customer_country_code", "payment_method", "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .filter(F.col("attempts") >= MIN_ATTEMPTS_PER_CELL)
    .groupBy("customer_country_code", "payment_method")
    .agg(
        F.count("*").alias("num_qualified_processors"),
        F.collect_list("payment_processor").alias("processors"),
    )
    .orderBy(F.desc("num_qualified_processors"))
)

cov_summary = (
    coverage.groupBy("num_qualified_processors")
    .count()
    .orderBy("num_qualified_processors")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(8, 4))
ax.bar(cov_summary["num_qualified_processors"].astype(str), cov_summary["count"], color=COLORS[0])
ax.set_xlabel("Number of Processors with Sufficient Volume")
ax.set_ylabel("Number of Country×Method Segments")
ax.set_title("Processor Coverage per Segment\n(How many segments have routing alternatives?)",
             fontsize=13, fontweight="bold")
fig.tight_layout()
plt.show()

single_proc = cov_summary.loc[cov_summary["num_qualified_processors"] == 1, "count"].sum()
multi_proc = cov_summary.loc[cov_summary["num_qualified_processors"] > 1, "count"].sum()
print(f"Segments with only 1 qualified processor : {single_proc:,}  (no routing alternative)")
print(f"Segments with ≥2 qualified processors    : {multi_proc:,}  (routing optimisation possible)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Sample size adequacy — attempts per processor×segment

# COMMAND ----------

cell_volumes = (
    df.groupBy(*segment_dims, "payment_processor")
    .agg(F.count("*").alias("attempts"))
    .select("attempts")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 4))

axes[0].hist(cell_volumes["attempts"], bins=100, color=COLORS[0], edgecolor="white", alpha=0.8)
axes[0].axvline(MIN_ATTEMPTS_PER_CELL, color=COLORS[3], linestyle="--",
                label=f"Threshold: {MIN_ATTEMPTS_PER_CELL}")
axes[0].set_title("Distribution of Cell Volumes (all)", fontweight="bold")
axes[0].set_xlabel("Attempts per processor×segment")
axes[0].set_ylabel("Frequency")
axes[0].legend(fontsize=9)
axes[0].set_xlim(0, cell_volumes["attempts"].quantile(0.95))

above = (cell_volumes["attempts"] >= MIN_ATTEMPTS_PER_CELL).sum()
below = (cell_volumes["attempts"] < MIN_ATTEMPTS_PER_CELL).sum()
axes[1].bar(["Below threshold", "Above threshold"], [below, above],
            color=[COLORS[3], COLORS[2]])
axes[1].set_title("Cells Above vs Below Volume Threshold", fontweight="bold")
for i, v in enumerate([below, above]):
    axes[1].text(i, v + v * 0.02, f"{v:,}\n({v/(above+below)*100:.0f}%)",
                 ha="center", fontsize=10)

fig.suptitle(f"Sample Size Adequacy (threshold = {MIN_ATTEMPTS_PER_CELL})",
             fontsize=13, fontweight="bold", y=1.03)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5: First-Attempt Routing (Rank 1 only)
# MAGIC
# MAGIC The most actionable routing decisions apply to the **first** customer
# MAGIC payment attempt (rank 1). Retries may already use fallback logic. This
# MAGIC section repeats the core analysis on rank-1 attempts only.

# COMMAND ----------

df_r1 = df.filter(F.col("customer_attempt_rank") == 1)
total_r1 = df_r1.count()
print(f"Rank-1 attempts: {total_r1:,}  ({total_r1/total_rows*100:.1f}% of all)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Rank-1 success rate by processor

# COMMAND ----------

r1_proc = (
    df_r1.groupBy("payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, max(3, len(r1_proc) * 0.5)))
axes[0].barh(r1_proc["payment_processor"], r1_proc["attempts"], color=COLORS[0])
axes[0].set_title("Rank-1 Volume by Processor", fontweight="bold")
axes[0].invert_yaxis()
axes[1].barh(r1_proc["payment_processor"], r1_proc["success_rate"], color=COLORS[2])
axes[1].set_title("Rank-1 SR by Processor", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
fig.suptitle("First Attempt (Rank 1) — Processor Performance", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Rank-1 segment opportunities

# COMMAND ----------

seg_proc_r1 = (
    df_r1.groupBy(*segment_dims, "payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.sum("is_successful").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("attempts"))
)

seg_totals_r1 = (
    seg_proc_r1.groupBy(*segment_dims)
    .agg(
        F.sum("attempts").alias("seg_total_attempts"),
        F.sum("successes").alias("seg_total_successes"),
        F.count("*").alias("num_processors"),
    )
    .withColumn("seg_success_rate", F.col("seg_total_successes") / F.col("seg_total_attempts"))
)

seg_proc_r1_enriched = (
    seg_proc_r1
    .join(seg_totals_r1, on=segment_dims, how="inner")
    .filter(F.col("seg_total_attempts") >= MIN_ATTEMPTS_SEGMENT)
    .filter(F.col("attempts") >= MIN_ATTEMPTS_PER_CELL)
    .filter(F.col("num_processors") >= 2)
)

w_r1 = Window.partitionBy(*segment_dims)
seg_ranked_r1 = (
    seg_proc_r1_enriched
    .withColumn("best_sr", F.max("success_rate").over(w_r1))
    .withColumn("is_best", F.col("success_rate") == F.col("best_sr"))
)

seg_r1_pdf = seg_ranked_r1.toPandas()

records_r1 = []
for seg_key, seg_group in seg_r1_pdf.groupby(segment_dims):
    if isinstance(seg_key, str):
        seg_key = (seg_key,)
    seg_group = seg_group.sort_values("success_rate", ascending=False)
    best_row = seg_group.iloc[0]
    for _, other_row in seg_group.iloc[1:].iterrows():
        z_stat, p_val = two_proportion_z_test(
            best_row["attempts"], best_row["successes"],
            other_row["attempts"], other_row["successes"],
        )
        sr_delta = best_row["success_rate"] - other_row["success_rate"]
        is_sig = p_val < SIGNIFICANCE_LEVEL and sr_delta >= MIN_SR_DELTA

        rec = {}
        for dim, val in zip(segment_dims, seg_key):
            rec[dim] = val
        rec.update({
            "best_processor": best_row["payment_processor"],
            "best_sr": best_row["success_rate"],
            "best_volume": int(best_row["attempts"]),
            "other_processor": other_row["payment_processor"],
            "other_sr": other_row["success_rate"],
            "other_volume": int(other_row["attempts"]),
            "sr_delta_pp": round(sr_delta * 100, 2),
            "p_value": round(p_val, 6),
            "is_significant": is_sig,
            "potential_extra_successes": round(sr_delta * other_row["attempts"], 0),
        })
        records_r1.append(rec)

opps_r1 = pd.DataFrame(records_r1)
opps_r1 = opps_r1[opps_r1["is_significant"]].sort_values("potential_extra_successes", ascending=False)

total_extra_r1 = opps_r1["potential_extra_successes"].sum()
total_reroutable_r1 = opps_r1["other_volume"].sum()
print(f"Rank-1 significant opportunities: {len(opps_r1):,}")
print(f"Rank-1 potential extra successes : {total_extra_r1:,.0f}")
print(f"Rank-1 reroutable attempts       : {total_reroutable_r1:,.0f}")

display(spark.createDataFrame(opps_r1.head(30)))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6: Sensitivity Analysis
# MAGIC
# MAGIC How do the results change as we vary the minimum-volume threshold?
# MAGIC This helps us understand the trade-off between statistical confidence
# MAGIC and opportunity coverage.

# COMMAND ----------

thresholds = [30, 50, 100, 200, 500, 1000]
sensitivity = []

for thresh in thresholds:
    filtered = comparisons[
        (comparisons["best_volume"] >= thresh) &
        (comparisons["other_volume"] >= thresh)
    ]
    sig = filtered[filtered["is_significant"]]
    sensitivity.append({
        "min_volume": thresh,
        "total_pairs": len(filtered),
        "significant_pairs": len(sig),
        "extra_successes": sig["potential_extra_successes"].sum(),
        "reroutable_attempts": sig["other_volume"].sum(),
    })

sens_df = pd.DataFrame(sensitivity)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].plot(sens_df["min_volume"], sens_df["significant_pairs"], marker="o",
             color=COLORS[0], linewidth=2)
axes[0].set_xlabel("Minimum Attempts per Cell")
axes[0].set_ylabel("Significant Opportunity Pairs")
axes[0].set_title("Number of Opportunities by Volume Threshold", fontweight="bold")
axes[0].set_xscale("log")

axes[1].plot(sens_df["min_volume"], sens_df["extra_successes"], marker="o",
             color=COLORS[2], linewidth=2)
axes[1].set_xlabel("Minimum Attempts per Cell")
axes[1].set_ylabel("Potential Extra Successes")
axes[1].set_title("Total Uplift by Volume Threshold", fontweight="bold")
axes[1].set_xscale("log")

fig.suptitle("Sensitivity: Volume Threshold vs Opportunity Size",
             fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

display(spark.createDataFrame(sens_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary & Recommendations
# MAGIC
# MAGIC Re-run with adjusted `MIN_ATTEMPTS_PER_CELL`, `MIN_SR_DELTA`, and
# MAGIC `SIGNIFICANCE_LEVEL` to tune the trade-off between confidence and
# MAGIC opportunity coverage.
# MAGIC
# MAGIC **Key outputs to act on:**
# MAGIC - Section 2.4: the ranked list of significant routing opportunities
# MAGIC - Section 3.1: total uplift estimate
# MAGIC - Section 5.2: first-attempt-specific opportunities (most actionable)
# MAGIC - Section 6: sensitivity to volume thresholds

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of routing optimisation analysis*
