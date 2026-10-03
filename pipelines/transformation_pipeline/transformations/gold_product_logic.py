"""
Gold — gold_product_performance: pure transformation logic, no pipeline dependency.

Deliberately has NO `from pyspark import pipelines` import anywhere in this
file — see gold_revenue_logic.py's docstring for why that's a verified
requirement, not a preference.

silver_inventory is a daily time series, not a current-state table — every
product has one row per warehouse per day it's ever been snapshotted. Summing
it directly would add up stock across every day in history, not just today.
We take the latest snapshot_date per product/warehouse first, using a window
function rather than a driver-side collect(), to stay fully incremental-refresh
friendly.

Null-safety: a brand-new product with no inventory snapshot yet produces
current_stock = NULL after the left join. NULL < threshold evaluates to NULL,
not true — so an at-risk new product would silently show as neither flagged
nor safe. coalesce(current_stock, 0) forces it to read as at-risk, correctly.

stockout_threshold is a parameter, not a hardcoded constant, so a test can
exercise the boundary explicitly rather than being locked to one fixed value.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, sum as _sum, row_number, desc, coalesce, lit


def compute_product_performance(
    order_items_df: DataFrame,
    products_df: DataFrame,
    inventory_df: DataFrame,
    stockout_threshold: int = 20,
) -> DataFrame:
    sales = (
        order_items_df
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("units_sold"),
            _sum("line_total").alias("revenue"),
        )
    )

    latest_per_warehouse = (
        inventory_df
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("product_id", "warehouse_id").orderBy(desc("snapshot_date"))),
        )
        .filter(col("rn") == 1)
    )
    current_stock = (
        latest_per_warehouse
        .groupBy("product_id")
        .agg(_sum("quantity_available").alias("current_stock"))
    )

    return (
        products_df
        .join(sales, "product_id", "left")
        .join(current_stock, "product_id", "left")
        .withColumn("current_stock", coalesce(col("current_stock"), lit(0)))
        .withColumn("stockout_risk", col("current_stock") < stockout_threshold)
        .select("product_id", "product_name", "brand", "units_sold", "revenue", "current_stock", "stockout_risk")
    )
