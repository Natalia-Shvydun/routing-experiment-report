"""
Factor computation for payment costs forecasting.

Two factor segments (``rnpl_segment``):
  - ``non_rnpl``: anchor = date_of_checkout
  - ``rnpl``:     mixed anchor — travel date for active, cancellation date
                  for cancelled.  Both are unioned before grouping so the
                  resulting factor naturally reflects the active/cancelled mix.

Flow:
  1. Apply bucketing
  2. Filter each sub-group to its mature window
  3. Group by factor_columns → factor = sum(cost) / sum(gmv)
"""

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .bucketing import apply_bucketing
from .config import ForecastingConfig


def _filter_mature(
    df: DataFrame,
    anchor_col: str,
    maturity_days: int,
    window_days: int,
    as_of_date: date,
) -> DataFrame:
    """Return rows whose anchor falls within the mature historical window."""
    window_start = as_of_date - timedelta(days=maturity_days + window_days)
    maturity_cutoff = as_of_date - timedelta(days=maturity_days)

    return (
        df.filter(F.col(anchor_col).isNotNull())
        .filter(F.col(anchor_col).between(F.lit(window_start), F.lit(maturity_cutoff)))
        .filter(F.col("total_payment_costs") != 0)
        .filter(F.col("gmv") > 0)
    )


_UNKNOWN = "_UNKNOWN"


def _group_into_factors(
    df: DataFrame,
    factor_columns: list[str],
    min_volume: int,
    as_of_date: date,
) -> DataFrame:
    """Group mature carts by factor columns and compute cost_factor.

    Filters applied after grouping:
      - segment_volume >= min_volume (too few carts → unreliable)
      - no _UNKNOWN values in factor columns (NULLs should fall through)
      - cost_factor must be negative (positive = anomalous reversal)
    """
    factors = (
        df.groupBy(factor_columns)
        .agg(
            F.sum("total_payment_costs").alias("total_cost"),
            F.sum("gmv").alias("total_gmv"),
            F.count("*").alias("segment_volume"),
        )
        .withColumn("cost_factor", F.col("total_cost") / F.col("total_gmv"))
        .withColumn("window_end_date", F.lit(as_of_date))
        .filter(F.col("segment_volume") >= min_volume)
        .filter(F.col("cost_factor") < 0)
    )

    for col_name in factor_columns:
        factors = factors.filter(
            F.col(col_name).isNotNull() & (F.col(col_name) != F.lit(_UNKNOWN))
        )

    return factors


def _ensure_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for col_name in columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df


def compute_factors(
    snapshot: DataFrame,
    as_of_date: date,
    config: ForecastingConfig,
) -> DataFrame:
    """
    Compute cost factors from the checkout-day snapshot.

    Non-RNPL uses a single anchor (date_of_checkout).
    RNPL unions two sub-populations (active → travel anchor,
    cancelled → cancellation anchor) before grouping, so the factor
    naturally reflects the blended active/cancelled cost rate.

    """
    bucketed = apply_bucketing(snapshot, config)

    output_cols = (
        ["window_end_date"]
        + config.factor_columns
        + ["cost_factor", "segment_volume", "total_cost", "total_gmv"]
    )

    # --- Non-RNPL: single anchor ---
    non_rnpl = bucketed.filter(F.col("rnpl_segment") == "non_rnpl")
    non_rnpl_mature = _filter_mature(
        non_rnpl, config.non_rnpl_anchor,
        config.non_rnpl_maturity_days, config.window_days, as_of_date,
    )
    non_rnpl_factors = _group_into_factors(
        non_rnpl_mature, config.factor_columns,
        config.min_volume_for_factor, as_of_date,
    )

    # --- RNPL: union active-mature + cancelled-mature, then group ---
    rnpl = bucketed.filter(F.col("rnpl_segment") == "rnpl")

    rnpl_active_mature = _filter_mature(
        rnpl.filter(F.col("last_date_of_cancelation").isNull()),
        config.rnpl_anchor,
        config.rnpl_maturity_days, config.window_days, as_of_date,
    )
    rnpl_cancelled_mature = _filter_mature(
        rnpl.filter(F.col("last_date_of_cancelation").isNotNull()),
        config.rnpl_cancelled_anchor,
        config.rnpl_cancelled_maturity_days, config.window_days, as_of_date,
    )
    rnpl_mature = rnpl_active_mature.unionByName(rnpl_cancelled_mature)
    rnpl_factors = _group_into_factors(
        rnpl_mature, config.factor_columns,
        config.min_volume_for_factor, as_of_date,
    )

    # --- Combine ---
    result = _ensure_columns(non_rnpl_factors, output_cols).select(output_cols)
    rnpl_part = _ensure_columns(rnpl_factors, output_cols).select(output_cols)
    return result.unionByName(rnpl_part)
