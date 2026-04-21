"""
Dimension bucketing for stable cost factors.

Reduces cardinality of long-tail dimensions and creates derived columns
used for factor grouping. Original columns are preserved for passthrough.

Key derived columns:
  - ``rnpl_segment``: non_rnpl / rnpl — used as a factor column so RNPL
    and non-RNPL get separate cost factors.
  - ``payment_method_detail``: uses payment_method_variant for cards/mobile
    wallets (where variant = card network), payment_method for everything else.
  - ``country_bucket``: top N country_codes individually, rest fall back to
    country_group.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import ForecastingConfig

CARD_METHODS = ("payment_card", "apple_pay", "google_pay")

VARIANT_ALIASES = {
    "MC": "MASTERCARD",
}

PROCESSOR_ALIASES = {
    "Adyen": "ADYEN",
    "adyen": "ADYEN",
    "Checkout": "CHECKOUT",
    "checkout": "CHECKOUT",
    "Jpmc": "JPMC",
    "jpmc": "JPMC",
    "Paypal": "PAYPAL",
    "paypal": "PAYPAL",
    "Klarna": "KLARNA",
    "klarna": "KLARNA",
    "Dlocal": "DLOCAL",
    "dlocal": "DLOCAL",
}


def _normalize_column(df: DataFrame, column: str, aliases: dict) -> DataFrame:
    """Apply alias mapping to a column, then uppercase any remaining values."""
    if column not in df.columns or not aliases:
        return df
    expr = F.col(column)
    for old, new in aliases.items():
        expr = F.when(F.col(column) == old, F.lit(new)).otherwise(expr)
    return df.withColumn(column, expr)


def compute_top_n(df: DataFrame, column: str, n: int) -> List[str]:
    """Return the top N values of *column* by cart count."""
    if n <= 0:
        return []
    rows = (
        df.filter(F.col(column).isNotNull())
        .groupBy(column)
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(n)
        .collect()
    )
    return [r[column] for r in rows]


def apply_bucketing(df: DataFrame, config: ForecastingConfig) -> DataFrame:
    """
    Normalize dimension values and add derived/bucketed columns.

    Steps:
      1. Normalize ``payment_processor`` (case-insensitive → uppercase)
      2. Normalize ``payment_method_variant`` (MC → MASTERCARD, etc.)
      3. Derive ``payment_method_detail``
      4. Derive ``country_bucket``

    Original columns are modified in-place for normalization to prevent
    duplicate segments from inconsistent naming.
    """
    # --- Normalize payment_processor ---
    if "payment_processor" in df.columns:
        df = _normalize_column(df, "payment_processor", PROCESSOR_ALIASES)

    # --- Normalize payment_method_variant ---
    if "payment_method_variant" in df.columns:
        df = _normalize_column(df, "payment_method_variant", VARIANT_ALIASES)

    # --- Payment method detail (variant for cards, method for rest) ---
    if "payment_method" in df.columns and "payment_method_detail" not in df.columns:
        df = df.withColumn(
            "payment_method_detail",
            F.when(
                F.col("payment_method").isin(*CARD_METHODS)
                & F.col("payment_method_variant").isNotNull(),
                F.col("payment_method_variant"),
            ).otherwise(F.col("payment_method")),
        )

    # --- RNPL segment (non_rnpl / rnpl) ---
    if "is_rnpl" in df.columns and "rnpl_segment" not in df.columns:
        df = df.withColumn(
            "rnpl_segment",
            F.when(F.col("is_rnpl") == False, F.lit("non_rnpl"))
            .otherwise(F.lit("rnpl")),
        )

    # --- Country bucketing ---
    if config.country_top_n > 0 and "country_code" in df.columns:
        top_countries = compute_top_n(df, "country_code", config.country_top_n)
        df = df.withColumn(
            "country_bucket",
            F.when(
                F.col("country_code").isin(top_countries),
                F.col("country_code"),
            ).otherwise(F.col("country_group")),
        )
    elif "country_group" in df.columns and "country_bucket" not in df.columns:
        df = df.withColumn("country_bucket", F.col("country_group"))

    # --- Normalize _UNKNOWN processor to NULL ---
    # Carts with NULL/unknown processor skip all factor levels that
    # include payment_processor and fall back to the level without it
    # (= GMV-weighted average across processors).
    if "payment_processor" in df.columns:
        df = df.withColumn(
            "payment_processor",
            F.when(
                F.col("payment_processor").isin("_UNKNOWN", "Unknown", "unknown", ""),
                F.lit(None),
            ).otherwise(F.col("payment_processor")),
        )

    # --- Fill NULLs in other factor columns with _UNKNOWN ---
    # NULLs don't match in Spark joins, so carts with NULL in a factor
    # column skip all fallback levels that include it and jump to the
    # first level without it. Using _UNKNOWN makes this explicit.
    # payment_processor is excluded: NULLs fall through naturally.
    _NULL_FILL_COLS = [
        "payment_method_detail", "currency", "country_bucket",
    ]
    for col_name in _NULL_FILL_COLS:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.coalesce(F.col(col_name), F.lit("_UNKNOWN")),
            )

    return df
