# Databricks notebook source
# MAGIC %md
# MAGIC # Descriptive Analysis — `fact_payment_attempt`
# MAGIC
# MAGIC Exploratory analysis of `production.payments.fact_payment_attempt`.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data profiling & basic statistics
# MAGIC 2. Trends by time
# MAGIC 3. Breakdowns by attempt type, rank, payment terms
# MAGIC 4. Success rates — four levels (attempt, customer attempt, cart, billing)
# MAGIC 5. Payment funnel (initiated → sent to issuer → authorized)
# MAGIC 6. Error analysis
# MAGIC 7. Additional insights (retries, payment method/processor, GMV, 3DS, platform)

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

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#004B87", "#FF6B35", "#2CA02C", "#D62728", "#9467BD",
          "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]

def pct_fmt(x, _):
    return f"{x:.0%}" if abs(x) <= 1 else f"{x:.1f}%"

# COMMAND ----------

_FILL_COLS = [
    "payment_processor", "payment_method", "payment_method_variant",
    "customer_country_code", "customer_country", "country_group",
    "currency", "payment_terms", "attempt_type", "payment_flow",
    "platform", "error_type", "error_decline_type", "error_code",
    "payment_attempt_status", "gmv_eur_bucket",
]

df_raw = spark.table(TABLE)
null_ts = df_raw.filter(F.col("payment_attempt_timestamp").isNull()).count()
df = df_raw.filter(F.col("payment_attempt_timestamp").isNotNull())
for _c in _FILL_COLS:
    df = df.withColumn(_c, F.coalesce(F.col(_c), F.lit("Unknown")))
df.cache()
total_rows = df.count()
print(f"Total rows: {total_rows:,}")
if null_ts > 0:
    print(f"Rows excluded (null timestamp): {null_ts:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema & sample

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic statistics

# COMMAND ----------

display(df.describe())

# COMMAND ----------

date_range = df.agg(
    F.min(F.col("payment_attempt_timestamp").cast("date")).alias("min_date"),
    F.max(F.col("payment_attempt_timestamp").cast("date")).alias("max_date"),
    F.countDistinct("shopping_cart_id").alias("distinct_carts"),
    F.countDistinct("billing_id").alias("distinct_billings"),
    F.countDistinct("payment_processor").alias("distinct_processors"),
    F.countDistinct("payment_method").alias("distinct_methods"),
    F.countDistinct("customer_country_code").alias("distinct_countries"),
).collect()[0]
print(f"Date range        : {date_range['min_date']}  →  {date_range['max_date']}")
print(f"Distinct carts    : {date_range['distinct_carts']:,}")
print(f"Distinct billings : {date_range['distinct_billings']:,}")
print(f"Distinct processors: {date_range['distinct_processors']}")
print(f"Distinct methods  : {date_range['distinct_methods']}")
print(f"Distinct countries: {date_range['distinct_countries']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 1: Trends by Time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Daily attempt volume

# COMMAND ----------

daily = (
    df.withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .groupBy("attempt_date")
    .agg(F.count("*").alias("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(daily["attempt_date"], daily["attempts"], color=COLORS[0], linewidth=1)
ax.set_title("Daily Payment Attempt Volume", fontsize=14, fontweight="bold")
ax.set_xlabel("")
ax.set_ylabel("Attempts")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K" if x >= 1000 else f"{x:.0f}"))
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Weekly attempt volume with 4-week moving average

# COMMAND ----------

weekly = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week")
    .agg(F.count("*").alias("attempts"))
    .orderBy("week")
    .toPandas()
)
weekly["ma_4w"] = weekly["attempts"].rolling(4).mean()

fig, ax = plt.subplots(figsize=(14, 4))
ax.bar(weekly["week"], weekly["attempts"], width=5, color=COLORS[0], alpha=0.5, label="Weekly")
ax.plot(weekly["week"], weekly["ma_4w"], color=COLORS[1], linewidth=2, label="4-week MA")
ax.set_title("Weekly Payment Attempt Volume", fontsize=14, fontweight="bold")
ax.set_ylabel("Attempts")
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Daily volume by outcome

# COMMAND ----------

daily_outcome = (
    df.withColumn("attempt_date", F.col("payment_attempt_timestamp").cast("date"))
    .withColumn("outcome", F.when(F.col("is_successful") == 1, "Successful").otherwise("Failed"))
    .groupBy("attempt_date", "outcome")
    .agg(F.count("*").alias("attempts"))
    .orderBy("attempt_date")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 4))
for i, (outcome, grp) in enumerate(daily_outcome.groupby("outcome")):
    ax.plot(grp["attempt_date"], grp["attempts"], color=COLORS[i], linewidth=1, label=outcome)
ax.set_title("Daily Attempt Volume by Outcome", fontsize=14, fontweight="bold")
ax.set_ylabel("Attempts")
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Weekly volume by payment terms

# COMMAND ----------

weekly_terms = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week", "payment_terms")
    .agg(F.count("*").alias("attempts"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 4))
for i, (terms, grp) in enumerate(weekly_terms.groupby("payment_terms")):
    ax.plot(grp["week"], grp["attempts"], color=COLORS[i % len(COLORS)], linewidth=1.5, label=terms)
ax.set_title("Weekly Volume by Payment Terms", fontsize=14, fontweight="bold")
ax.set_ylabel("Attempts")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 2: Breakdowns by Attempt Type, Rank, Payment Terms

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Distribution by attempt type

# COMMAND ----------

type_dist = (
    df.groupBy("attempt_type")
    .agg(
        F.count("*").alias("attempts"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .withColumn("pct_attempts", F.round(F.col("attempts") / total_rows * 100, 2))
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].barh(type_dist["attempt_type"], type_dist["attempts"], color=COLORS[0])
axes[0].set_title("Attempt Count by Type", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(type_dist["attempt_type"], type_dist["carts"], color=COLORS[1])
axes[1].set_title("Distinct Carts by Type", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

fig.tight_layout()
plt.show()

display(spark.createDataFrame(type_dist))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Distribution by customer attempt rank

# COMMAND ----------

rank_dist = (
    df.groupBy("customer_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .withColumn("pct_attempts", F.round(F.col("attempts") / total_rows * 100, 2))
    .orderBy("customer_attempt_rank")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, 5))
display_rank = rank_dist[rank_dist["customer_attempt_rank"] <= 10]
x = np.arange(len(display_rank))
w = 0.35
ax.bar(x - w/2, display_rank["attempts"], w, label="Attempts", color=COLORS[0])
ax.bar(x + w/2, display_rank["carts"], w, label="Distinct Carts", color=COLORS[1])
ax.set_xticks(x)
ax.set_xticklabels(display_rank["customer_attempt_rank"].astype(str))
ax.set_xlabel("Customer Attempt Rank")
ax.set_title("Attempts & Carts by Customer Attempt Rank (≤10)", fontsize=14, fontweight="bold")
ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K" if x >= 1000 else f"{x:.0f}"))
fig.tight_layout()
plt.show()

display(spark.createDataFrame(rank_dist))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Customer attempt rank vs system attempt rank

# COMMAND ----------

rank_compare = (
    df.groupBy("customer_attempt_rank", "system_attempt_rank")
    .agg(F.count("*").alias("attempts"))
    .filter(F.col("customer_attempt_rank") <= 6)
    .filter(F.col("system_attempt_rank") <= 6)
    .toPandas()
)

pivot_ranks = rank_compare.pivot_table(
    index="customer_attempt_rank", columns="system_attempt_rank",
    values="attempts", aggfunc="sum",
).fillna(0)

fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(pivot_ranks.values, cmap="Blues", aspect="auto")
ax.set_xticks(range(pivot_ranks.shape[1]))
ax.set_xticklabels(pivot_ranks.columns.astype(int))
ax.set_yticks(range(pivot_ranks.shape[0]))
ax.set_yticklabels(pivot_ranks.index.astype(int))
ax.set_xlabel("System Attempt Rank")
ax.set_ylabel("Customer Attempt Rank")
for i in range(pivot_ranks.shape[0]):
    for j in range(pivot_ranks.shape[1]):
        val = pivot_ranks.iloc[i, j]
        if val > 0:
            ax.text(j, i, f"{val/1000:.0f}K", ha="center", va="center", fontsize=8,
                    color="white" if val > pivot_ranks.values.max() * 0.5 else "black")
plt.colorbar(im, ax=ax, label="Attempts")
ax.set_title("Customer vs System Attempt Rank", fontsize=14, fontweight="bold")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Distribution by payment terms

# COMMAND ----------

terms_dist = (
    df.groupBy("payment_terms")
    .agg(
        F.count("*").alias("attempts"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .withColumn("pct_attempts", F.round(F.col("attempts") / total_rows * 100, 2))
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

axes[0].pie(terms_dist["attempts"], labels=terms_dist["payment_terms"],
            autopct="%1.1f%%", colors=COLORS[:len(terms_dist)], startangle=90)
axes[0].set_title("Share of Attempts by Payment Terms", fontweight="bold")

axes[1].pie(terms_dist["carts"], labels=terms_dist["payment_terms"],
            autopct="%1.1f%%", colors=COLORS[:len(terms_dist)], startangle=90)
axes[1].set_title("Share of Carts by Payment Terms", fontweight="bold")

fig.tight_layout()
plt.show()

display(spark.createDataFrame(terms_dist))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Attempt type × customer rank cross-tabulation

# COMMAND ----------

cross_tab = (
    df.filter(F.col("customer_attempt_rank") <= 5)
    .groupBy("attempt_type", "customer_attempt_rank")
    .agg(F.count("*").alias("attempts"))
    .orderBy("attempt_type", "customer_attempt_rank")
    .toPandas()
)
pivot = cross_tab.pivot(index="attempt_type", columns="customer_attempt_rank", values="attempts").fillna(0)

fig, ax = plt.subplots(figsize=(12, max(4, len(pivot) * 0.6)))
pivot.plot(kind="barh", stacked=True, ax=ax, color=COLORS[:pivot.shape[1]])
ax.set_title("Attempts: Type × Customer Rank (≤5)", fontsize=14, fontweight="bold")
ax.set_xlabel("Attempts")
ax.legend(title="Rank", fontsize=8, title_fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Attempt type × payment terms cross-tabulation

# COMMAND ----------

cross_tab2 = (
    df.groupBy("attempt_type", "payment_terms")
    .agg(F.count("*").alias("attempts"))
    .orderBy("attempt_type", "payment_terms")
    .toPandas()
)
pivot2 = cross_tab2.pivot(index="attempt_type", columns="payment_terms", values="attempts").fillna(0)

fig, ax = plt.subplots(figsize=(12, max(4, len(pivot2) * 0.6)))
pivot2.plot(kind="barh", stacked=True, ax=ax, color=COLORS[:pivot2.shape[1]])
ax.set_title("Attempts: Type × Payment Terms", fontsize=14, fontweight="bold")
ax.set_xlabel("Attempts")
ax.legend(title="Payment Terms", fontsize=8, title_fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 3: Success Rates
# MAGIC
# MAGIC The table provides four levels of success:
# MAGIC - **`is_successful`** / **`is_payment_attempt_successful`** — this specific attempt was authorized
# MAGIC - **`is_customer_attempt_successful`** — this customer-initiated attempt succeeded
# MAGIC - **`is_shopping_cart_successful`** — the cart eventually had a successful payment
# MAGIC - **`is_billing_successful`** — the billing was successful

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 All success levels — overall

# COMMAND ----------

overall_sr = df.agg(
    F.mean("is_successful").alias("attempt_sr"),
    F.mean("is_payment_attempt_successful").alias("payment_attempt_sr"),
    F.mean("is_customer_attempt_successful").alias("customer_attempt_sr"),
    F.mean("is_shopping_cart_successful").alias("cart_sr"),
    F.mean("is_billing_successful").alias("billing_sr"),
).collect()[0]

labels = ["Attempt\n(is_successful)", "Payment Attempt\n(is_payment_attempt_\nsuccessful)",
          "Customer Attempt\n(is_customer_attempt_\nsuccessful)",
          "Cart\n(is_shopping_cart_\nsuccessful)", "Billing\n(is_billing_\nsuccessful)"]
values = [overall_sr["attempt_sr"], overall_sr["payment_attempt_sr"],
          overall_sr["customer_attempt_sr"], overall_sr["cart_sr"], overall_sr["billing_sr"]]

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.bar(labels, values, color=COLORS[:5])
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Overall Success Rates — All Levels", fontsize=14, fontweight="bold")
for bar, val in zip(bars, values):
    if val is not None:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                f"{val:.2%}", ha="center", va="bottom", fontsize=10, fontweight="bold")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Weekly success rates — all levels

# COMMAND ----------

weekly_sr_all = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_customer_attempt_successful").alias("customer_attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
        F.mean("is_billing_successful").alias("billing_sr"),
    )
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(weekly_sr_all["week"], weekly_sr_all["attempt_sr"], color=COLORS[0], linewidth=1.5, label="Attempt SR")
ax.plot(weekly_sr_all["week"], weekly_sr_all["customer_attempt_sr"], color=COLORS[1], linewidth=1.5, label="Customer Attempt SR")
ax.plot(weekly_sr_all["week"], weekly_sr_all["cart_sr"], color=COLORS[2], linewidth=1.5, label="Cart SR")
ax.plot(weekly_sr_all["week"], weekly_sr_all["billing_sr"], color=COLORS[4], linewidth=1.5, label="Billing SR")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Weekly Success Rates — All Levels", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Weekly attempt volume & attempt success rate

# COMMAND ----------

fig, ax1 = plt.subplots(figsize=(14, 5))
ax2 = ax1.twinx()
ax1.bar(weekly_sr_all["week"], weekly_sr_all["attempts"], width=5, color=COLORS[0], alpha=0.3, label="Attempts")
ax2.plot(weekly_sr_all["week"], weekly_sr_all["attempt_sr"], color=COLORS[1], linewidth=2, label="Attempt SR")
ax1.set_ylabel("Attempts", color=COLORS[0])
ax2.set_ylabel("Success Rate", color=COLORS[1])
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax1.set_title("Weekly Attempt Volume & Success Rate", fontsize=14, fontweight="bold")
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="lower left")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Success rate by attempt type

# COMMAND ----------

sr_by_type = (
    df.groupBy("attempt_type")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, max(3, len(sr_by_type) * 0.6)))
y = np.arange(len(sr_by_type))
h = 0.35
ax.barh(y - h/2, sr_by_type["attempt_sr"], h, label="Attempt SR", color=COLORS[0])
ax.barh(y + h/2, sr_by_type["cart_sr"], h, label="Cart SR", color=COLORS[2])
ax.set_yticks(y)
ax.set_yticklabels(sr_by_type["attempt_type"])
ax.invert_yaxis()
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate by Attempt Type", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
for i, vol in enumerate(sr_by_type["attempts"]):
    ax.text(0.001, i, f"n={vol:,.0f}", va="center", fontsize=7, color="#333")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Success rate by customer attempt rank

# COMMAND ----------

sr_by_rank = (
    df.filter(F.col("customer_attempt_rank") <= 10)
    .groupBy("customer_attempt_rank")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy("customer_attempt_rank")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 5))
x = np.arange(len(sr_by_rank))
w = 0.35
ax.bar(x - w/2, sr_by_rank["attempt_sr"], w, label="Attempt SR", color=COLORS[0])
ax.bar(x + w/2, sr_by_rank["cart_sr"], w, label="Cart SR", color=COLORS[2])
ax.set_xticks(x)
ax.set_xticklabels(sr_by_rank["customer_attempt_rank"].astype(str))
ax.set_xlabel("Customer Attempt Rank")
ax.set_title("Success Rate by Customer Attempt Rank", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
for i, (asr, vol) in enumerate(zip(sr_by_rank["attempt_sr"], sr_by_rank["attempts"])):
    ax.text(i - w/2, asr + 0.005, f"{asr:.1%}", ha="center", va="bottom", fontsize=7)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Success rate by payment terms

# COMMAND ----------

sr_by_terms = (
    df.groupBy("payment_terms")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, max(3, len(sr_by_terms) * 0.6)))
y = np.arange(len(sr_by_terms))
h = 0.35
ax.barh(y - h/2, sr_by_terms["attempt_sr"], h, label="Attempt SR", color=COLORS[0])
ax.barh(y + h/2, sr_by_terms["cart_sr"], h, label="Cart SR", color=COLORS[2])
ax.set_yticks(y)
ax.set_yticklabels(sr_by_terms["payment_terms"])
ax.invert_yaxis()
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate by Payment Terms", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
for i, vol in enumerate(sr_by_terms["attempts"]):
    ax.text(0.001, i, f"n={vol:,.0f}", va="center", fontsize=7, color="#333")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Weekly success rate by payment terms

# COMMAND ----------

weekly_terms_sr = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week", "payment_terms")
    .agg(F.mean("is_successful").alias("attempt_sr"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
for i, (terms, grp) in enumerate(weekly_terms_sr.groupby("payment_terms")):
    ax.plot(grp["week"], grp["attempt_sr"], color=COLORS[i % len(COLORS)], linewidth=1.5, label=terms)
ax.set_title("Weekly Attempt SR by Payment Terms", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 Weekly success rate by customer attempt rank (top 5)

# COMMAND ----------

weekly_rank_sr = (
    df.filter(F.col("customer_attempt_rank") <= 5)
    .withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week", "customer_attempt_rank")
    .agg(F.mean("is_successful").alias("attempt_sr"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
for i, (rank, grp) in enumerate(weekly_rank_sr.groupby("customer_attempt_rank")):
    ax.plot(grp["week"], grp["attempt_sr"], color=COLORS[i % len(COLORS)], linewidth=1.5, label=f"Rank {rank}")
ax.set_title("Weekly Attempt SR by Customer Attempt Rank", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 4: Payment Funnel
# MAGIC
# MAGIC `payment_initiated` → `sent_to_issuer` → `is_successful`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Overall funnel

# COMMAND ----------

funnel = df.agg(
    F.count("*").alias("total_attempts"),
    F.sum("payment_initiated").alias("initiated"),
    F.sum("sent_to_issuer").alias("sent_to_issuer"),
    F.sum("is_successful").alias("successful"),
).collect()[0]

funnel_labels = ["Total Attempts", "Initiated", "Sent to Issuer", "Successful"]
funnel_vals = [funnel["total_attempts"], funnel["initiated"],
               funnel["sent_to_issuer"], funnel["successful"]]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.barh(funnel_labels[::-1], [v for v in funnel_vals[::-1]], color=COLORS[0])
ax.set_title("Payment Attempt Funnel", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1000:.0f}K"))
for bar, val, total in zip(bars, funnel_vals[::-1], [funnel["total_attempts"]] * 4):
    pct = val / total * 100
    ax.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
            f"{val:,.0f} ({pct:.1f}%)", va="center", fontsize=9)
fig.tight_layout()
plt.show()

print(f"Initiation rate         : {funnel['initiated']/funnel['total_attempts']:.2%}")
print(f"Sent-to-issuer rate     : {funnel['sent_to_issuer']/funnel['initiated']:.2%}" if funnel["initiated"] else "N/A")
print(f"Authorization rate      : {funnel['successful']/funnel['sent_to_issuer']:.2%}" if funnel["sent_to_issuer"] else "N/A")
print(f"Overall success rate    : {funnel['successful']/funnel['total_attempts']:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Weekly funnel conversion rates

# COMMAND ----------

weekly_funnel = (
    df.withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week")
    .agg(
        F.count("*").alias("total"),
        F.sum("payment_initiated").alias("initiated"),
        F.sum("sent_to_issuer").alias("sent_to_issuer"),
        F.sum("is_successful").alias("successful"),
    )
    .withColumn("initiation_rate", F.col("initiated") / F.col("total"))
    .withColumn("sent_to_issuer_rate", F.col("sent_to_issuer") / F.col("initiated"))
    .withColumn("auth_rate", F.col("successful") / F.col("sent_to_issuer"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(weekly_funnel["week"], weekly_funnel["initiation_rate"], color=COLORS[0], linewidth=1.5, label="Initiation Rate")
ax.plot(weekly_funnel["week"], weekly_funnel["sent_to_issuer_rate"], color=COLORS[1], linewidth=1.5, label="Sent-to-Issuer Rate")
ax.plot(weekly_funnel["week"], weekly_funnel["auth_rate"], color=COLORS[2], linewidth=1.5, label="Authorization Rate")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Weekly Funnel Conversion Rates", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 5: Error Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Error type breakdown (failed attempts only)

# COMMAND ----------

failed = df.filter(F.col("is_successful") == 0)
failed_count = failed.count()

error_type_dist = (
    failed.groupBy("error_type")
    .agg(F.count("*").alias("attempts"))
    .withColumn("pct", F.round(F.col("attempts") / failed_count * 100, 2))
    .orderBy(F.desc("attempts"))
    .limit(15)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, max(4, len(error_type_dist) * 0.4)))
ax.barh(error_type_dist["error_type"].fillna("(null)"), error_type_dist["attempts"], color=COLORS[3])
ax.invert_yaxis()
ax.set_title(f"Error Type Distribution (failed attempts, n={failed_count:,})", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))
for bar, pct in zip(ax.patches, error_type_dist["pct"]):
    ax.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height()/2,
            f"{pct:.1f}%", va="center", fontsize=8, color="#555")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Error decline type breakdown

# COMMAND ----------

decline_dist = (
    failed.groupBy("error_decline_type")
    .agg(F.count("*").alias("attempts"))
    .withColumn("pct", F.round(F.col("attempts") / failed_count * 100, 2))
    .orderBy(F.desc("attempts"))
    .limit(10)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, max(3, len(decline_dist) * 0.5)))
ax.barh(decline_dist["error_decline_type"].fillna("(null)"), decline_dist["attempts"], color=COLORS[3])
ax.invert_yaxis()
ax.set_title("Error Decline Type Distribution", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Error source breakdown

# COMMAND ----------

error_source = df.filter(F.col("is_successful") == 0).agg(
    F.sum("issuer_declined").alias("issuer_declined"),
    F.sum("gateway_rejected").alias("gateway_rejected"),
    F.sum("application_error").alias("application_error"),
    F.sum("authorization_error").alias("authorization_error"),
).collect()[0]

labels_err = ["Issuer Declined", "Gateway Rejected", "Application Error", "Authorization Error"]
vals_err = [error_source["issuer_declined"], error_source["gateway_rejected"],
            error_source["application_error"], error_source["authorization_error"]]

fig, ax = plt.subplots(figsize=(10, 4))
bars = ax.bar(labels_err, vals_err, color=[COLORS[3], COLORS[1], COLORS[4], COLORS[5]])
ax.set_title("Failed Attempts by Error Source", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))
for bar, val in zip(bars, vals_err):
    if val and val > 0:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + bar.get_height() * 0.01,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Weekly error source trend

# COMMAND ----------

weekly_errors = (
    df.filter(F.col("is_successful") == 0)
    .withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week")
    .agg(
        F.count("*").alias("failed"),
        F.sum("issuer_declined").alias("issuer_declined"),
        F.sum("gateway_rejected").alias("gateway_rejected"),
        F.sum("application_error").alias("application_error"),
    )
    .withColumn("issuer_pct", F.col("issuer_declined") / F.col("failed"))
    .withColumn("gateway_pct", F.col("gateway_rejected") / F.col("failed"))
    .withColumn("app_error_pct", F.col("application_error") / F.col("failed"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(weekly_errors["week"], weekly_errors["issuer_pct"], color=COLORS[3], linewidth=1.5, label="Issuer Declined")
ax.plot(weekly_errors["week"], weekly_errors["gateway_pct"], color=COLORS[1], linewidth=1.5, label="Gateway Rejected")
ax.plot(weekly_errors["week"], weekly_errors["app_error_pct"], color=COLORS[4], linewidth=1.5, label="Application Error")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Weekly Share of Failed Attempts by Error Source", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SECTION 6: Additional Insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Retry analysis — customer attempts per cart

# COMMAND ----------

attempts_per_cart = (
    df.groupBy("shopping_cart_id")
    .agg(F.max("customer_attempt_rank").alias("max_rank"))
    .groupBy("max_rank")
    .agg(F.count("*").alias("num_carts"))
    .orderBy("max_rank")
    .toPandas()
)

total_carts = attempts_per_cart["num_carts"].sum()
attempts_per_cart["pct"] = attempts_per_cart["num_carts"] / total_carts * 100
attempts_per_cart["cum_pct"] = attempts_per_cart["pct"].cumsum()

fig, ax1 = plt.subplots(figsize=(12, 5))
ax2 = ax1.twinx()
display_apc = attempts_per_cart[attempts_per_cart["max_rank"] <= 10].copy()
ax1.bar(display_apc["max_rank"].astype(str), display_apc["num_carts"], color=COLORS[0])
ax2.plot(display_apc["max_rank"].astype(str), display_apc["cum_pct"], color=COLORS[1], marker="o", linewidth=2)
ax1.set_xlabel("Max Customer Attempt Rank per Cart")
ax1.set_ylabel("Number of Carts", color=COLORS[0])
ax2.set_ylabel("Cumulative %", color=COLORS[1])
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
ax1.set_title("Distribution of Customer Attempts per Cart", fontsize=14, fontweight="bold")
fig.tight_layout()
plt.show()

single = attempts_per_cart.loc[attempts_per_cart["max_rank"] == 1, "pct"].values
retry_pct = 100 - (single[0] if len(single) > 0 else 0)
print(f"Carts with single attempt   : {single[0] if len(single) > 0 else 'N/A':.1f}%")
print(f"Carts with retries (rank>1) : {retry_pct:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Retry success — does retrying help?

# COMMAND ----------

cart_retry_success = (
    df.groupBy("shopping_cart_id")
    .agg(
        F.max("customer_attempt_rank").alias("max_rank"),
        F.max("is_shopping_cart_successful").alias("cart_success"),
    )
    .withColumn("attempt_bucket", F.when(F.col("max_rank") == 1, "1")
                .when(F.col("max_rank") == 2, "2")
                .when(F.col("max_rank") == 3, "3")
                .when(F.col("max_rank") <= 5, "4-5")
                .otherwise("6+"))
    .groupBy("attempt_bucket")
    .agg(
        F.count("*").alias("carts"),
        F.sum("cart_success").alias("successful_carts"),
    )
    .withColumn("cart_success_rate", F.col("successful_carts") / F.col("carts"))
    .orderBy("attempt_bucket")
    .toPandas()
)

bucket_order = ["1", "2", "3", "4-5", "6+"]
cart_retry_success["attempt_bucket"] = cart_retry_success["attempt_bucket"].astype("category")
cart_retry_success["attempt_bucket"] = cart_retry_success["attempt_bucket"].cat.set_categories(bucket_order, ordered=True)
cart_retry_success = cart_retry_success.sort_values("attempt_bucket")

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(cart_retry_success["attempt_bucket"].astype(str),
              cart_retry_success["cart_success_rate"], color=COLORS[0])
ax.set_xlabel("Number of Customer Attempts per Cart")
ax.set_title("Cart-Level Success Rate by Number of Attempts", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
for bar, rate, vol in zip(bars, cart_retry_success["cart_success_rate"], cart_retry_success["carts"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
            f"{rate:.1%}\n(n={vol:,.0f})", ha="center", va="bottom", fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Payment method distribution & success rate

# COMMAND ----------

method_stats = (
    df.groupBy("payment_method")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .orderBy(F.desc("attempts"))
    .limit(15)
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

axes[0].barh(method_stats["payment_method"], method_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Payment Method (Top 15)", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(method_stats["payment_method"], method_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Payment Method", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Payment processor distribution & success rate

# COMMAND ----------

proc_stats = (
    df.groupBy("payment_processor")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, max(3, len(proc_stats) * 0.5)))

axes[0].barh(proc_stats["payment_processor"], proc_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Processor", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(proc_stats["payment_processor"], proc_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Processor", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Weekly success rate by payment method (Top 5)

# COMMAND ----------

top5_methods = (
    df.groupBy("payment_method").count()
    .orderBy(F.desc("count")).limit(5)
    .select("payment_method").toPandas()["payment_method"].tolist()
)

weekly_method_sr = (
    df.filter(F.col("payment_method").isin(top5_methods))
    .withColumn("week", F.date_trunc("week", F.col("payment_attempt_timestamp")))
    .groupBy("week", "payment_method")
    .agg(F.mean("is_successful").alias("attempt_sr"))
    .orderBy("week")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(14, 5))
for i, (method, grp) in enumerate(weekly_method_sr.groupby("payment_method")):
    ax.plot(grp["week"], grp["attempt_sr"], color=COLORS[i % len(COLORS)], linewidth=1.5, label=method)
ax.set_title("Weekly Success Rate by Payment Method (Top 5)", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.6 Country analysis — top 15 by volume

# COMMAND ----------

country_stats = (
    df.groupBy("customer_country_code")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .orderBy(F.desc("attempts"))
    .limit(15)
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

axes[0].barh(country_stats["customer_country_code"], country_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Country (Top 15)", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(country_stats["customer_country_code"], country_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Country (Top 15)", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.7 Country group analysis

# COMMAND ----------

cg_stats = (
    df.groupBy("country_group")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, max(3, len(cg_stats) * 0.6)))
y = np.arange(len(cg_stats))
h = 0.35
ax.barh(y - h/2, cg_stats["attempt_sr"], h, label="Attempt SR", color=COLORS[0])
ax.barh(y + h/2, cg_stats["cart_sr"], h, label="Cart SR", color=COLORS[2])
ax.set_yticks(y)
ax.set_yticklabels(cg_stats["country_group"])
ax.invert_yaxis()
ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate by Country Group", fontsize=14, fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.8 Amount (EUR) distribution by GMV bucket

# COMMAND ----------

gmv_stats = df.agg(
    F.mean("amount_eur").alias("mean"),
    F.expr("percentile_approx(amount_eur, 0.25)").alias("p25"),
    F.expr("percentile_approx(amount_eur, 0.50)").alias("p50"),
    F.expr("percentile_approx(amount_eur, 0.75)").alias("p75"),
    F.expr("percentile_approx(amount_eur, 0.95)").alias("p95"),
    F.expr("percentile_approx(amount_eur, 0.99)").alias("p99"),
).collect()[0]
print(f"Amount EUR distribution:")
print(f"  Mean:  {gmv_stats['mean']:>10.2f}")
print(f"  P25:   {gmv_stats['p25']:>10.2f}")
print(f"  P50:   {gmv_stats['p50']:>10.2f}")
print(f"  P75:   {gmv_stats['p75']:>10.2f}")
print(f"  P95:   {gmv_stats['p95']:>10.2f}")
print(f"  P99:   {gmv_stats['p99']:>10.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.9 Success rate by GMV bucket (pre-computed)

# COMMAND ----------

gmv_bucket_sr = (
    df.groupBy("gmv_eur_bucket")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy("gmv_eur_bucket")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 5))
x = np.arange(len(gmv_bucket_sr))
w = 0.35
ax.bar(x - w/2, gmv_bucket_sr["attempt_sr"], w, label="Attempt SR", color=COLORS[0])
ax.bar(x + w/2, gmv_bucket_sr["cart_sr"], w, label="Cart SR", color=COLORS[2])
ax.set_xticks(x)
ax.set_xticklabels(gmv_bucket_sr["gmv_eur_bucket"], rotation=45, ha="right", fontsize=9)
ax.set_xlabel("GMV EUR Bucket")
ax.set_title("Success Rate by GMV Bucket", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.legend()
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.10 RNPL vs non-RNPL comparison

# COMMAND ----------

rnpl_compare = (
    df.withColumn("rnpl_label", F.when(F.col("is_rnpl"), "RNPL").otherwise("Non-RNPL"))
    .groupBy("rnpl_label")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
        F.countDistinct("shopping_cart_id").alias("carts"),
    )
    .toPandas()
)

fig, axes = plt.subplots(1, 3, figsize=(15, 4))

axes[0].bar(rnpl_compare["rnpl_label"], rnpl_compare["attempts"], color=[COLORS[0], COLORS[1]])
axes[0].set_title("Volume", fontweight="bold")
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1000:.0f}K"))

axes[1].bar(rnpl_compare["rnpl_label"], rnpl_compare["attempt_sr"], color=[COLORS[0], COLORS[1]])
axes[1].set_title("Attempt SR", fontweight="bold")
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

axes[2].bar(rnpl_compare["rnpl_label"], rnpl_compare["cart_sr"], color=[COLORS[0], COLORS[1]])
axes[2].set_title("Cart SR", fontweight="bold")
axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.suptitle("RNPL vs Non-RNPL Comparison", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.11 Platform breakdown

# COMMAND ----------

platform_stats = (
    df.groupBy("platform")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
        F.mean("is_shopping_cart_successful").alias("cart_sr"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, max(3, len(platform_stats) * 0.5)))

axes[0].barh(platform_stats["platform"].fillna("(null)"), platform_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Platform", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(platform_stats["platform"].fillna("(null)"), platform_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Platform", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.12 3DS analysis

# COMMAND ----------

three_ds = df.agg(
    F.count("*").alias("total"),
    F.sum(F.col("challenge_issued").cast("int")).alias("challenges"),
    F.sum("is_three_ds_passed").alias("three_ds_passed"),
).collect()[0]

print(f"3DS Challenge issued rate: {three_ds['challenges']/three_ds['total']:.2%}")
print(f"3DS Passed rate (overall): {three_ds['three_ds_passed']/three_ds['total']:.2%}")

# COMMAND ----------

three_ds_sr = (
    df.withColumn("challenge_label",
                  F.when(F.col("challenge_issued"), "Challenge Issued")
                  .otherwise("No Challenge"))
    .groupBy("challenge_label")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
    )
    .toPandas()
)

fig, ax = plt.subplots(figsize=(8, 4))
bars = ax.bar(three_ds_sr["challenge_label"], three_ds_sr["attempt_sr"],
              color=[COLORS[0], COLORS[3]])
ax.yaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
ax.set_title("Success Rate: Challenge Issued vs Not", fontsize=14, fontweight="bold")
for bar, rate, vol in zip(bars, three_ds_sr["attempt_sr"], three_ds_sr["attempts"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
            f"{rate:.1%}\n(n={vol:,.0f})", ha="center", va="bottom", fontsize=10)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.13 Currency distribution — top 10

# COMMAND ----------

currency_stats = (
    df.groupBy("currency")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
    )
    .orderBy(F.desc("attempts"))
    .limit(10)
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].barh(currency_stats["currency"], currency_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Currency (Top 10)", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(currency_stats["currency"], currency_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Currency", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.14 Payment flow breakdown

# COMMAND ----------

flow_stats = (
    df.groupBy("payment_flow")
    .agg(
        F.count("*").alias("attempts"),
        F.mean("is_successful").alias("attempt_sr"),
    )
    .orderBy(F.desc("attempts"))
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, max(3, len(flow_stats) * 0.5)))

axes[0].barh(flow_stats["payment_flow"].fillna("(null)"), flow_stats["attempts"], color=COLORS[0])
axes[0].set_title("Volume by Payment Flow", fontweight="bold")
axes[0].invert_yaxis()
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))

axes[1].barh(flow_stats["payment_flow"].fillna("(null)"), flow_stats["attempt_sr"], color=COLORS[2])
axes[1].set_title("Success Rate by Payment Flow", fontweight="bold")
axes[1].invert_yaxis()
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))

fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary Statistics Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall summary — all dimensions at a glance

# COMMAND ----------

summary_dims = []
for dim_name, dim_col in [
    ("Attempt Type", "attempt_type"),
    ("Customer Attempt Rank", "customer_attempt_rank"),
    ("Payment Terms", "payment_terms"),
    ("Payment Method", "payment_method"),
    ("Processor", "payment_processor"),
    ("Country", "customer_country_code"),
    ("Country Group", "country_group"),
    ("Currency", "currency"),
    ("Platform", "platform"),
    ("Payment Flow", "payment_flow"),
]:
    stats = (
        df.groupBy(dim_col)
        .agg(
            F.count("*").alias("attempts"),
            F.mean("is_successful").alias("attempt_sr"),
            F.mean("is_shopping_cart_successful").alias("cart_sr"),
            F.countDistinct("shopping_cart_id").alias("carts"),
        )
        .withColumn("attempt_sr", F.round("attempt_sr", 4))
        .withColumn("cart_sr", F.round("cart_sr", 4))
        .withColumn("pct_of_total", F.round(F.col("attempts") / total_rows * 100, 2))
        .withColumn("dimension", F.lit(dim_name))
        .withColumnRenamed(dim_col, "dimension_value")
        .select("dimension", "dimension_value", "attempts", "carts",
                "attempt_sr", "cart_sr", "pct_of_total")
        .orderBy(F.desc("attempts"))
    )
    summary_dims.append(stats)

from functools import reduce
summary = reduce(lambda a, b: a.unionByName(b), summary_dims)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *End of analysis*
