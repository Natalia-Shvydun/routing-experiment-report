"""
Forecasting step: identify immature carts, apply cost factors with
hierarchical fallback, produce output matching the downstream contract.

Two segments (via ``rnpl_segment`` column from bucketing):
  1. non_rnpl  → immature if checkout within last N days
  2. rnpl      → smart anchor: cancellation date if already cancelled,
                 travel date otherwise. Both get the same blended factor.

Input is already one row per shopping_cart_id — no aggregation needed.
"""

from datetime import date, timedelta
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .bucketing import apply_bucketing
from .config import ForecastingConfig


def _build_fallback_levels(factor_columns: List[str]) -> List[List[str]]:
    """
    Generate fallback levels by progressively dropping factors from the end
    (least important first, assuming the list is ordered by importance).
    """
    return [factor_columns[:i] for i in range(len(factor_columns), -1, -1)]


def get_immature_carts(
    snapshot: DataFrame,
    as_of_date: date,
    config: ForecastingConfig,
) -> DataFrame:
    """
    Return carts whose costs are not yet mature.

    Non-RNPL: checkout too recent.
    RNPL (not yet cancelled at as_of_date): travel too recent or not yet.
    RNPL (already cancelled at as_of_date): cancellation too recent.

    All RNPL carts share ``rnpl_segment = 'rnpl'`` so they get the same
    blended factor, but the *anchor* used to determine maturity differs.
    """
    # --- Non-RNPL ---
    non_rnpl_cutoff = as_of_date - timedelta(days=config.non_rnpl_maturity_days)
    non_rnpl_immature = (
        snapshot
        .filter(F.col("rnpl_segment") == "non_rnpl")
        .filter(F.col(config.non_rnpl_anchor) > F.lit(non_rnpl_cutoff))
        .filter(F.col(config.non_rnpl_anchor) <= F.lit(as_of_date))
    )

    # --- RNPL: not cancelled (or cancelled after as_of_date — unknown) ---
    rnpl = snapshot.filter(F.col("rnpl_segment") == "rnpl")

    rnpl_cutoff = as_of_date - timedelta(days=config.rnpl_maturity_days)
    rnpl_active_immature = (
        rnpl
        .filter(
            F.col("last_date_of_cancelation").isNull()
            | (F.col("last_date_of_cancelation").cast("date") > F.lit(as_of_date))
        )
        .filter(
            (F.col(config.rnpl_anchor).isNull())
            | (F.col(config.rnpl_anchor) > F.lit(rnpl_cutoff))
        )
        .filter(F.col("date_of_checkout") <= F.lit(as_of_date))
    )

    # --- RNPL: already cancelled before/on as_of_date ---
    rnpl_cancel_cutoff = as_of_date - timedelta(days=config.rnpl_cancelled_maturity_days)
    rnpl_cancelled_immature = (
        rnpl
        .filter(F.col("last_date_of_cancelation").isNotNull())
        .filter(F.col("last_date_of_cancelation").cast("date") <= F.lit(as_of_date))
        .filter(F.col("last_date_of_cancelation") > F.lit(rnpl_cancel_cutoff))
    )

    return (
        non_rnpl_immature
        .unionByName(rnpl_active_immature)
        .unionByName(rnpl_cancelled_immature)
    )


def apply_factors_with_fallback(
    immature: DataFrame,
    factors: DataFrame,
    config: ForecastingConfig,
) -> DataFrame:
    """
    Join immature carts to factors using hierarchical fallback.

    Since ``rnpl_segment`` is in ``factor_columns``, non_rnpl and rnpl
    naturally get distinct factors at every fallback level that includes it.
    """
    fallback_levels = _build_fallback_levels(config.factor_columns)

    result = immature.withColumn(
        "_factor", F.lit(None).cast("double")
    ).withColumn(
        "_source", F.lit(None).cast("string")
    )

    for level_dims in fallback_levels:
        if not level_dims:
            global_factor = factors.agg(
                (F.sum("total_cost") / F.sum("total_gmv")).alias("gf")
            ).collect()
            if global_factor and global_factor[0]["gf"] is not None:
                gf = float(global_factor[0]["gf"])
                result = result.withColumn(
                    "_factor", F.coalesce(F.col("_factor"), F.lit(gf))
                ).withColumn(
                    "_source", F.coalesce(F.col("_source"), F.lit("global_fallback"))
                )
            continue

        level_factors = (
            factors.groupBy(level_dims)
            .agg((F.sum("total_cost") / F.sum("total_gmv")).alias("_lf"))
        )

        source_label = f"fallback_{len(level_dims)}"
        if len(level_dims) == len(config.factor_columns):
            source_label = "exact_match"
        level_factors = level_factors.withColumn("_ls", F.lit(source_label))

        result = (
            result.join(level_factors, level_dims, "left")
            .withColumn("_factor", F.coalesce(F.col("_factor"), F.col("_lf")))
            .withColumn("_source", F.coalesce(F.col("_source"), F.col("_ls")))
            .drop("_lf", "_ls")
        )

    result = (
        result
        .withColumn("amount", F.col("gmv") * F.coalesce(F.col("_factor"), F.lit(0.0)))
        .withColumn("source", F.col("_source"))
        .drop("_factor", "_source")
    )

    return result


def _ensure_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    for col_name in columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df


def forecast(
    snapshot: DataFrame,
    as_of_date: date,
    config: ForecastingConfig,
    factors: DataFrame = None,
) -> DataFrame:
    """
    Full forecast pipeline: immature carts get forecasted costs,
    mature carts pass through actual costs.
    """
    from .factors import compute_factors

    bucketed = apply_bucketing(snapshot, config)
    immature = get_immature_carts(bucketed, as_of_date, config)

    if factors is None:
        factors = compute_factors(bucketed, as_of_date, config)

    result = apply_factors_with_fallback(immature, factors, config)

    # --- Mature carts (actuals) ---
    non_rnpl_cutoff = as_of_date - timedelta(days=config.non_rnpl_maturity_days)
    rnpl_cutoff = as_of_date - timedelta(days=config.rnpl_maturity_days)
    rnpl_cancel_cutoff = as_of_date - timedelta(days=config.rnpl_cancelled_maturity_days)

    mature_non_rnpl = (
        bucketed
        .filter(F.col("rnpl_segment") == "non_rnpl")
        .filter(F.col(config.non_rnpl_anchor) <= F.lit(non_rnpl_cutoff))
        .withColumn("amount", F.col("total_payment_costs"))
        .withColumn("source", F.lit("actual"))
    )

    # RNPL active (not cancelled): mature if travel old enough
    mature_rnpl_active = (
        bucketed
        .filter(F.col("rnpl_segment") == "rnpl")
        .filter(F.col("last_date_of_cancelation").isNull())
        .filter(F.col(config.rnpl_anchor).isNotNull())
        .filter(F.col(config.rnpl_anchor) <= F.lit(rnpl_cutoff))
        .withColumn("amount", F.col("total_payment_costs"))
        .withColumn("source", F.lit("actual"))
    )

    # RNPL cancelled: mature if cancellation old enough
    mature_rnpl_cancelled = (
        bucketed
        .filter(F.col("rnpl_segment") == "rnpl")
        .filter(F.col("last_date_of_cancelation").isNotNull())
        .filter(F.col("last_date_of_cancelation") <= F.lit(rnpl_cancel_cutoff))
        .withColumn("amount", F.col("total_payment_costs"))
        .withColumn("source", F.lit("actual"))
    )

    shared_cols = ["shopping_cart_id", "date_of_checkout", "amount", "source", "gmv"]
    for c in config.factor_columns:
        if c not in shared_cols:
            shared_cols.append(c)
    passthrough_dims = [
        "country_code", "country_group", "payment_method",
        "payment_processor", "is_rnpl", "last_date_of_cancelation",
    ]
    for c in passthrough_dims:
        if c not in shared_cols and c in bucketed.columns:
            shared_cols.append(c)

    all_dfs = [result, mature_non_rnpl, mature_rnpl_active, mature_rnpl_cancelled]
    all_dfs = [_ensure_columns(df, shared_cols) for df in all_dfs]

    output = all_dfs[0].select(shared_cols)
    for df in all_dfs[1:]:
        output = output.unionByName(df.select(shared_cols))

    return output
