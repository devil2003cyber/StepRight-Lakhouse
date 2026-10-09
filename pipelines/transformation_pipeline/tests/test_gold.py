"""
Unit tests — Gold, Part 1 (L16): gold_daily_revenue, gold_product_performance,
gold_customer_360. See L15 for the full plan and approach, and each logic
file's own docstring for the reasoning behind its transformation.

Every fixture here is small and hand-crafted, written directly in the test
that uses it — no external file, no generator. Assertions use
pyspark.testing.utils.assertDataFrameEqual, PySpark's own standard-library
tool for exactly this (Spark 3.5+, no extra install).
"""

from datetime import date, datetime

import pytest
from pyspark.testing.utils import assertDataFrameEqual

from gold_revenue_logic import compute_daily_revenue
from gold_product_logic import compute_product_performance
from gold_customer_logic import compute_customer_360
from gold_funnel_logic import compute_funnel_analysis
from gold_fulfillment_logic import compute_fulfillment_health

# ---------------------------------------------------------------------------
# gold_daily_revenue
# ---------------------------------------------------------------------------

def test_daily_revenue_allocates_discount_proportionally_and_filters_scd2(local_spark):
    categories_df = local_spark.createDataFrame(
        [("CAT1", "Formal"), ("CAT2", "Sports")],
        schema="category_id string, category_name string",
    )
    products_df = local_spark.createDataFrame(
        [("P1", "CAT1"), ("P2", "CAT2")],
        schema="product_id string, category_id string",
    )

    orders_df = local_spark.createDataFrame(
        [
            # Current version of O1 — the one that should actually be used.
            ("O1", "2026-07-10", "CA", 10.0, None),
            # A SUPERSEDED version of the same order, with a deliberately wrong
            # discount, to prove the SCD2 filter excludes it. If this leaked
            # through, the test's expected totals below would be wrong.
            ("O1", "2026-07-10", "CA", 999.0, "2026-07-09T00:00:00"),
            # O2 has no discount at all — proves coalesce(discount_amount, 0).
            ("O2", "2026-07-10", "NY", None, None),
        ],
        schema="order_id string, order_date string, shipping_state string, discount_amount double, __END_AT string",
    )

    order_items_df = local_spark.createDataFrame(
        [
            ("I1", "O1", "P1", 60.0),
            ("I2", "O1", "P2", 40.0),
            ("I3", "O2", "P1", 50.0),
        ],
        schema="order_item_id string, order_id string, product_id string, line_total double",
    )

    result = compute_daily_revenue(order_items_df, orders_df, products_df, categories_df )

    expected = local_spark.createDataFrame(
        [
            (date(2026, 7, 10), "Formal", "CA", 60.0, 6.0, 54.0),
            (date(2026, 7, 10), "Sports", "CA", 40.0, 4.0, 36.0),
            (date(2026, 7, 10), "Formal", "NY", 50.0, 0.0, 50.0),
        ],
        schema="revenue_date date, category_name string, region string, "
        "gross_revenue double, total_discount double, net_revenue double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)


def test_daily_revenue_guards_against_zero_order_subtotal(local_spark):
    # A fully-comped line item: line_total = 0, but a discount is still present
    # on the order. order_subtotal ends up 0 — the guard clause must return 0
    # allocated_discount here, not raise a division-by-zero error.
    categories_df = local_spark.createDataFrame(
        [("CAT1", "Formal")], schema="category_id string, category_name string"
    )
    products_df = local_spark.createDataFrame(
        [("P1", "CAT1")], schema="product_id string, category_id string"
    )
    orders_df = local_spark.createDataFrame(
        [("O1", "2026-07-10", "CA", 5.0, None)],
        schema="order_id string, order_date string, shipping_state string, discount_amount double, __END_AT string",
    )
    order_items_df = local_spark.createDataFrame(
        [("I1", "O1", "P1", 0.0)],
        schema="order_item_id string, order_id string, product_id string, line_total double",
    )

    result = compute_daily_revenue(order_items_df, orders_df, products_df, categories_df)

    expected = local_spark.createDataFrame(
        [(date(2026, 7, 10), "Formal", "CA", 0.0, 0.0, 0.0)],
        schema="revenue_date date, category_name string, region string, "
        "gross_revenue double, total_discount double, net_revenue double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_product_performance
# ---------------------------------------------------------------------------

def test_product_performance_latest_snapshot_and_null_safety(local_spark):
    products_df = local_spark.createDataFrame(
        [
            ("P1", "Product One", "BrandA"),  # has inventory, ends up below threshold
            ("P2", "Product Two", "BrandB"),  # NO inventory rows at all
            ("P3", "Product Three", "BrandC"),  # has inventory, well-stocked
        ],
        schema="product_id string, product_name string, brand string",
    )

    order_items_df = local_spark.createDataFrame(
        [("I1", "O1", "P1", 2, 30.0), ("I2", "O2", "P3", 1, 90.0)],
        schema="order_item_id string, order_id string, product_id string, quantity int, line_total double",
    )

    inventory_df = local_spark.createDataFrame(
        [
            # P1: an OLDER, larger snapshot that must be superseded by the latest.
            ("P1", "WH1", "2026-07-08", 50),
            ("P1", "WH1", "2026-07-10", 5),   # latest for WH1
            ("P1", "WH2", "2026-07-10", 10),  # latest for WH2 → current_stock = 5 + 10 = 15
            # P3: well-stocked, single warehouse.
            ("P3", "WH1", "2026-07-10", 100),
            # P2 intentionally has no rows at all.
        ],
        schema="product_id string, warehouse_id string, snapshot_date string, quantity_available int",
    )

    result = compute_product_performance(order_items_df, products_df, inventory_df, stockout_threshold=20)

    expected = local_spark.createDataFrame(
        [
            ("P1", "Product One", "BrandA", 2, 30.0, 15, True),
            ("P2", "Product Two", "BrandB", None, None, 0, True),
            ("P3", "Product Three", "BrandC", 1, 90.0, 100, False),
        ],
        schema="product_id string, product_name string, brand string, units_sold long, "
        "revenue double, current_stock long, stockout_risk boolean",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_customer_360
# ---------------------------------------------------------------------------

def test_customer_360_is_deterministic_given_a_fixed_as_of_date(local_spark):
    customers_df = local_spark.createDataFrame(
        [
            # Current version — this is the one that should be used.
            ("CU1", "Alex", "Doe", "gold", None),
            # A superseded version, with a DIFFERENT tier, proving the SCD2
            # filter excludes it — if it leaked through, this test could pick
            # up "bronze" instead of "gold" depending on row order.
            ("CU1", "Alex", "Doe", "bronze", "2026-06-01T00:00:00"),
        ],
        schema="customer_id string, first_name string, last_name string, loyalty_tier string, __END_AT string",
    )

    orders_df = local_spark.createDataFrame(
        [
            ("O1", "CU1", 100.0, "2026-06-15", None),
            ("O2", "CU1", 50.0, "2026-07-10", None),  # most recent order
        ],
        schema="order_id string, customer_id string, total_amount double, order_date string, __END_AT string",
    )

    # Fixed, not datetime.now() — the same input always produces the same
    # output, regardless of what day this test actually runs.
    result = compute_customer_360(customers_df, orders_df, as_of_date=date(2026, 7, 20))

    expected = local_spark.createDataFrame(
        [("CU1", "Alex", "Doe", "gold", 150.0, 2, "2026-07-10", 10)],
        schema="customer_id string, first_name string, last_name string, loyalty_tier string, "
        "lifetime_value double, order_count long, last_order_date string, days_since_last_order int",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_funnel_analysis
# ---------------------------------------------------------------------------

def test_funnel_analysis_counts_anonymous_sessions(local_spark):
    # Two sessions on "google": one converts (has a purchase event), one
    # doesn't. One anonymous session (no customer_id at all, which the
    # clickstream schema allows) on "direct" that also converts — proves
    # anonymity has no bearing on whether a session counts.
    clickstream_df = local_spark.createDataFrame(
        [
            ("S1", "google", "product_view"),
            ("S1", "google", "purchase"),
            ("S2", "google", "product_view"),  # never converts
            ("S3", "direct", "purchase"),       # anonymous session, still counts
        ],
        schema="session_id string, referrer string, event_type string",
    )

    result = compute_funnel_analysis(clickstream_df)

    expected = local_spark.createDataFrame(
        [
            ("google", 2, 1, 0.5),
            ("direct", 1, 1, 1.0),
        ],
        schema="referrer string, total_sessions long, converted_sessions long, conversion_rate double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)


# ---------------------------------------------------------------------------
# gold_fulfillment_health
# ---------------------------------------------------------------------------

def test_fulfillment_health_sla_boundary_is_inclusive(local_spark):
    # Two delivered orders in the same day/region: one delivered in EXACTLY
    # 5 days (the SLA boundary itself — must count as within SLA, <=, not <),
    # one delivered in 6 days (must count as a miss). A third order, not yet
    # delivered, must be excluded entirely.
    orders_df = local_spark.createDataFrame(
        [
            ("O1", "delivered", "2026-07-01", "2026-07-06", "CA", None),  # exactly 5 days
            ("O2", "delivered", "2026-07-01", "2026-07-07", "CA", None),  # 6 days — a miss
            ("O3", "pending", "2026-07-01", "2026-07-01", "CA", None),    # not delivered — excluded
        ],
        schema="order_id string, order_status string, order_date string, updated_at string, "
        "shipping_state string, __END_AT string",
    )

    result = compute_fulfillment_health(orders_df, sla_days=5)

    expected = local_spark.createDataFrame(
        [(date(2026, 7, 1), "CA", 2, 1, 0.5)],
        schema="order_date date, region string, delivered_orders long, within_sla_orders long, sla_compliance_rate double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)













