"""
Configuration for payment costs forecasting.

All parameters are centralized here. Values marked "from analysis" should
be updated after running the exploratory analysis notebooks (01–04).
Override at runtime via Databricks widgets.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ForecastingConfig:

    # ------------------------------------------------------------------
    # Factor columns — all required for accurate cost prediction
    # ------------------------------------------------------------------
    factor_columns: List[str] = field(default_factory=lambda: [
        "rnpl_segment",
        "payment_method_detail",
        "currency",
        "payment_processor",
        "country_bucket",
    ])
    """Columns used for factor grouping (ordered by importance for fallback).
    rnpl_segment = non_rnpl / rnpl.
    payment_method_detail = variant for cards/wallets, method for rest.
    currency = strong independent differentiator.
    payment_processor = moderate independent value above currency.
    country_bucket = top N individual codes + country_group for rest (most redundant)."""

    # ------------------------------------------------------------------
    # Maturation anchors and cutoffs — three segments
    # ------------------------------------------------------------------
    non_rnpl_anchor: str = "date_of_checkout"
    """Date column used to anchor maturation for non-RNPL carts."""

    rnpl_anchor: str = "last_date_of_travel"
    """Date column used to anchor maturation for active (non-cancelled) RNPL."""

    rnpl_cancelled_anchor: str = "last_date_of_cancelation"
    """Date column used to anchor maturation for cancelled RNPL."""

    non_rnpl_maturity_days: int = 14
    """Days after anchor for non-RNPL costs to be considered mature."""

    rnpl_maturity_days: int = 7
    """Days after travel for active RNPL costs to be considered mature.
    Analysis shows 99.4% of costs settled by day 7 post-travel."""

    rnpl_cancelled_maturity_days: int = 7
    """Days after cancellation for cancelled RNPL costs to be considered
    mature. Refund processing typically settles within a few days."""

    # ------------------------------------------------------------------
    # Factor computation window
    # ------------------------------------------------------------------
    window_days: int = 14
    """Rolling window size (days) of mature data used for factor calculation."""

    min_volume_for_factor: int = 100
    """Minimum cart count in a segment within the factor window to trust
    its factor. Increased from 30 to push volatile small segments to
    broader fallback groups."""

    # ------------------------------------------------------------------
    # Dimension bucketing (from analysis)
    # ------------------------------------------------------------------
    country_top_n: int = 20
    """Keep top N individual country_codes in country_bucket; rest fall
    back to country_group. Set to 0 to always use country_group."""

    # ------------------------------------------------------------------
    # Quality validation
    # ------------------------------------------------------------------
    deviation_threshold: float = 0.03
    """Max allowed |forecast - actual| / actual per dimension value."""

    min_carts_for_validation: int = 100
    """Segments with fewer carts are excluded from pass/fail checks.
    Small segments have noisy deviation that doesn't reflect model quality."""

    validation_dimensions: List[str] = field(default_factory=lambda: [
        "rnpl_segment",
        "country_group",
        "country_bucket",
        "payment_processor",
        "payment_method_detail",
        "currency",
        "weekly",
    ])
    """Dimensions checked during quality evaluation. 'weekly' is a virtual
    dimension computed from date_of_checkout via date_trunc."""

    # ------------------------------------------------------------------
    # Output contract
    # ------------------------------------------------------------------
    output_columns: List[str] = field(default_factory=lambda: [
        "shopping_cart_id",
        "date_of_checkout",
        "amount",
        "source",
    ])
    """Core output columns. Dimension columns are passed through automatically."""

    # ------------------------------------------------------------------
    # Table names
    # ------------------------------------------------------------------
    snapshot_table: str = "testing.analytics.payment_costs_data"
    """Checkout-day snapshot: one row per shopping_cart_id."""

    costs_detail_table: str = "production.payments.int_all_psp_transactions_payment_costs"
    """Transaction-level costs with processing dates (used in analysis only)."""

    factors_table: str = "testing.analytics.payment_cost_factors"
    forecasted_costs_table: str = "testing.analytics.forecasted_costs"
    quality_metrics_table: str = "testing.analytics.quality_metrics"

    @classmethod
    def from_widgets(cls, dbutils) -> "ForecastingConfig":
        """Build config from Databricks notebook widgets."""
        def _get(name: str, default, cast=str):
            try:
                raw = dbutils.widgets.get(name)
                if cast is int:
                    return int(raw)
                if cast is float:
                    return float(raw)
                return raw
            except Exception:
                return default

        factor_cols_raw = _get(
            "factor_columns",
            "rnpl_segment,payment_method_detail,currency,payment_processor,country_bucket",
            str,
        )
        factor_cols = [c.strip() for c in factor_cols_raw.split(",")]

        return cls(
            factor_columns=factor_cols,
            non_rnpl_anchor=_get("non_rnpl_anchor", "date_of_checkout", str),
            rnpl_anchor=_get("rnpl_anchor", "last_date_of_travel", str),
            rnpl_cancelled_anchor=_get("rnpl_cancelled_anchor", "last_date_of_cancelation", str),
            non_rnpl_maturity_days=_get("non_rnpl_maturity_days", 14, int),
            rnpl_maturity_days=_get("rnpl_maturity_days", 7, int),
            rnpl_cancelled_maturity_days=_get("rnpl_cancelled_maturity_days", 7, int),
            window_days=_get("window_days", 14, int),
            min_volume_for_factor=_get("min_volume_for_factor", 100, int),
            country_top_n=_get("country_top_n", 20, int),
            deviation_threshold=_get("deviation_threshold", 0.03, float),
            snapshot_table=_get("snapshot_table", "testing.analytics.payment_costs_data", str),
            factors_table=_get("factors_table", "testing.analytics.payment_cost_factors", str),
            forecasted_costs_table=_get("forecasted_costs_table", "testing.analytics.forecasted_costs", str),
            quality_metrics_table=_get("quality_metrics_table", "testing.analytics.quality_metrics", str),
        )
