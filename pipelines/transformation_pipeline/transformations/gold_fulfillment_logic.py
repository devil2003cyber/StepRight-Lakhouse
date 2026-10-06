"""
Gold — gold_fulfillment_health: pure transformation logic, no pipeline dependency.

Deliberately has NO `from pyspark import pipelines` import anywhere in this
file — see gold_revenue_logic.py's docstring for why that's a verified
requirement, not a preference.

Honest scope note, decided before building this: our schema has no
warehouse_id on orders, so there is no clean join from an order to "the
specific inventory that fulfilled it." Rather than fake a precise join that
the schema doesn't actually support, this table is scoped as an order-timing
SLA metric — order date to delivered date, against a defined promise window.
Inventory is deliberately not joined here; correlating regional stockouts with
delayed orders is a separate, aggregate-level analysis, not a per-order join.

updated_at is used as a proxy for the delivery timestamp — per the schema
document, it's "last status change," so for an order currently in delivered
status, updated_at approximates when delivery happened. This is a documented
simplification, not a precise delivery-event timestamp.

sla_days is a parameter, not a hardcoded constant, same reasoning as
stockout_threshold in gold_product_logic.py — a test can exercise the boundary
explicitly (delivered exactly on the SLA day vs. one day over).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as _sum, when, datediff, to_date


def compute_fulfillment_health(orders_df: DataFrame, sla_days: int = 5) -> DataFrame:
    orders_current = orders_df.filter(col("__END_AT").isNull())

    delivered = (
        orders_current
        .filter(col("order_status") == "delivered")
        .withColumn(
            "days_to_deliver",
            datediff(to_date(col("updated_at")), to_date(col("order_date"))),
        )
        .withColumn("within_sla", col("days_to_deliver") <= sla_days)
    )

    return (
        delivered
        .groupBy(
            to_date(col("order_date")).alias("order_date"),
            col("shipping_state").alias("region"),
        )
        .agg(
            count("order_id").alias("delivered_orders"),
            _sum(when(col("within_sla"), 1).otherwise(0)).alias("within_sla_orders"),
        )
        .withColumn("sla_compliance_rate", col("within_sla_orders") / col("delivered_orders"))
    )
