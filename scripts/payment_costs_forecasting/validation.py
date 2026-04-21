"""
Quality evaluation framework for payment costs forecasting.

Checks forecast deviation against actuals across all configured
validation dimensions (including downstream aggregation dimensions
like country_group, device_platform, and weekly).
"""

from datetime import date, timedelta
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .bucketing import apply_bucketing
from .config import ForecastingConfig
from .factors import compute_factors
from .forecast import get_immature_carts, apply_factors_with_fallback


def compute_deviation_for_dimension(
    forecast_df: DataFrame,
    actuals_df: DataFrame,
    dimension: str,
    config: ForecastingConfig,
    evaluation_date: date,
) -> DataFrame:
    """
    Compare forecast vs actuals for a single dimension.

    Both DataFrames are at shopping_cart_id level.
    forecast_df: has ``amount`` (forecasted cost)
    actuals_df:  has ``total_payment_costs`` (actual mature cost)

    For the virtual ``weekly`` dimension, both DFs must have ``date_of_checkout``.
    """
    if dimension == "weekly":
        fc = forecast_df.withColumn("_dim_val", F.date_trunc("week", F.col("date_of_checkout")).cast("string"))
        ac = actuals_df.withColumn("_dim_val", F.date_trunc("week", F.col("date_of_checkout")).cast("string"))
    else:
        if dimension not in forecast_df.columns or dimension not in actuals_df.columns:
            return None
        fc = forecast_df.withColumn("_dim_val", F.col(dimension).cast("string"))
        ac = actuals_df.withColumn("_dim_val", F.col(dimension).cast("string"))

    joined = (
        fc.alias("f")
        .join(ac.alias("a"), F.col("f.shopping_cart_id") == F.col("a.shopping_cart_id"), "inner")
        .select(
            F.col("f._dim_val").alias("dimension_value"),
            F.col("f.amount").alias("forecasted_cost"),
            F.col("a.total_payment_costs").alias("actual_cost"),
        )
    )

    min_carts = config.min_carts_for_validation

    metrics = (
        joined.groupBy("dimension_value")
        .agg(
            F.sum("forecasted_cost").alias("forecast_total"),
            F.sum("actual_cost").alias("actual_total"),
            F.count("*").alias("cart_count"),
        )
        .withColumn(
            "deviation_pct",
            F.when(
                F.abs(F.col("actual_total")) > 0,
                F.abs(F.col("forecast_total") - F.col("actual_total")) / F.abs(F.col("actual_total")),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "pass_flag",
            F.when(
                F.col("cart_count") < min_carts,
                F.lit(None).cast("boolean"),
            ).when(
                F.col("actual_total").isNull() | (F.col("actual_total") == 0),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("deviation_pct") <= config.deviation_threshold),
        )
        .withColumn("dimension", F.lit(dimension))
        .withColumn("evaluation_date", F.lit(evaluation_date))
    )

    return metrics


def evaluate_quality(
    spark: SparkSession,
    forecast_df: DataFrame,
    actuals_df: DataFrame,
    evaluation_date: date,
    config: ForecastingConfig,
) -> tuple[DataFrame, bool]:
    """
    Compute deviation for all validation dimensions and check against threshold.

    forecast_df: cart-level with ``shopping_cart_id``, ``amount``, ``date_of_checkout``,
                 and all dimension columns.
    actuals_df:  cart-level with ``shopping_cart_id``, ``total_payment_costs``,
                 ``date_of_checkout``, and dimension columns.

    Returns (quality_metrics_df, all_passed).
    """
    all_metrics = []

    for dim in config.validation_dimensions:
        m = compute_deviation_for_dimension(
            forecast_df, actuals_df, dim, config, evaluation_date
        )
        if m is not None:
            all_metrics.append(m)

    # Overall (total) deviation
    overall_row = (
        forecast_df.alias("f")
        .join(actuals_df.alias("a"), F.col("f.shopping_cart_id") == F.col("a.shopping_cart_id"), "inner")
        .agg(
            F.sum("f.amount").alias("forecast_total"),
            F.sum("a.total_payment_costs").alias("actual_total"),
            F.count("*").alias("cart_count"),
        )
    ).collect()[0]

    actual_abs = abs(overall_row["actual_total"] or 0)
    if actual_abs > 0:
        overall_dev = abs((overall_row["forecast_total"] or 0) - (overall_row["actual_total"] or 0)) / actual_abs
    else:
        overall_dev = None

    overall_df = spark.createDataFrame([{
        "dimension_value": "ALL",
        "forecast_total": float(overall_row["forecast_total"] or 0),
        "actual_total": float(overall_row["actual_total"] or 0),
        "deviation_pct": overall_dev,
        "pass_flag": overall_dev <= config.deviation_threshold if overall_dev is not None else None,
        "dimension": "_overall",
        "evaluation_date": evaluation_date,
        "cart_count": int(overall_row["cart_count"] or 0),
    }])
    all_metrics.append(overall_df)

    if not all_metrics:
        empty = spark.createDataFrame(
            [],
            "dimension string, dimension_value string, actual_total double, "
            "forecast_total double, deviation_pct double, pass_flag boolean, "
            "evaluation_date date, cart_count long",
        )
        return empty, False

    quality_metrics = all_metrics[0]
    for m in all_metrics[1:]:
        quality_metrics = quality_metrics.unionByName(m, allowMissingColumns=True)

    failed = quality_metrics.filter(
        (F.abs(F.col("actual_total")) > 0) & (F.col("pass_flag") == False)
    )
    all_passed = failed.count() == 0

    return quality_metrics, all_passed


def _filter_mature_actuals(
    df: DataFrame,
    snapshot_date: date,
    config: ForecastingConfig,
) -> DataFrame:
    """
    Keep only carts whose costs are truly mature as of ``snapshot_date``.

    Uses the *actual* final state (is_rnpl, last_date_of_cancelation,
    last_date_of_travel) to determine maturity — regardless of what
    rnpl_segment was assigned at prediction time.
    """
    non_rnpl_cutoff = snapshot_date - timedelta(days=config.non_rnpl_maturity_days)
    rnpl_active_cutoff = snapshot_date - timedelta(days=config.rnpl_maturity_days)
    rnpl_cancel_cutoff = snapshot_date - timedelta(days=config.rnpl_cancelled_maturity_days)

    return df.filter(
        # non-RNPL: checkout old enough
        (
            (F.col("is_rnpl") == False)
            & (F.col("date_of_checkout").cast("date") <= F.lit(non_rnpl_cutoff))
        )
        |
        # RNPL never cancelled: travel date exists and old enough
        (
            (F.col("is_rnpl") == True)
            & F.col("last_date_of_cancelation").isNull()
            & F.col("last_date_of_travel").isNotNull()
            & (F.col("last_date_of_travel").cast("date") <= F.lit(rnpl_active_cutoff))
        )
        |
        # RNPL cancelled: cancellation old enough
        (
            (F.col("is_rnpl") == True)
            & F.col("last_date_of_cancelation").isNotNull()
            & (F.col("last_date_of_cancelation").cast("date") <= F.lit(rnpl_cancel_cutoff))
        )
    )


def backtest(
    spark: SparkSession,
    snapshot: DataFrame,
    evaluation_dates: List[date],
    config: ForecastingConfig,
    snapshot_date: date = None,
) -> DataFrame:
    """
    Run the forecast for multiple historical "as of" dates and evaluate each.

    ``snapshot_date`` is when costs were last observed (defaults to today).
    Actuals are filtered to only include carts whose costs have fully matured
    by this date, preventing comparison against incomplete cost data.

    Returns combined quality_metrics across all evaluation dates.
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    bucketed = apply_bucketing(snapshot, config)
    all_results = []

    for eval_date in evaluation_dates:
        factors = compute_factors(bucketed, eval_date, config)

        # get_immature_carts uses smart anchor logic per RNPL sub-type
        immature = get_immature_carts(bucketed, eval_date, config)
        forecast_df = apply_factors_with_fallback(immature, factors, config)

        # Build actuals: only carts with mature costs as of snapshot_date
        immature_ids = immature.select("shopping_cart_id").distinct()

        actuals_df = (
            bucketed
            .join(immature_ids, "shopping_cart_id", "inner")
            .filter(F.col("total_payment_costs") != 0)
        )
        actuals_df = _filter_mature_actuals(actuals_df, snapshot_date, config)

        if actuals_df.count() == 0:
            continue

        # Filter forecast to only carts that have mature actuals
        mature_ids = actuals_df.select("shopping_cart_id")
        forecast_df = forecast_df.join(mature_ids, "shopping_cart_id", "inner")

        quality_metrics, _ = evaluate_quality(
            spark, forecast_df, actuals_df, eval_date, config
        )
        all_results.append(quality_metrics)

    if not all_results:
        return spark.createDataFrame(
            [],
            "dimension string, dimension_value string, actual_total double, "
            "forecast_total double, deviation_pct double, pass_flag boolean, "
            "evaluation_date date, cart_count long",
        )

    combined = all_results[0]
    for r in all_results[1:]:
        combined = combined.unionByName(r, allowMissingColumns=True)

    return combined
