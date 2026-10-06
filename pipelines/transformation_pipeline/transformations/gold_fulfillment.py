"""
Gold — gold_fulfillment_health: pipeline wrapper.

All transformation logic lives in gold_fulfillment_logic.py, unit tested
directly. This file's only job is applying the pipeline decorator and
supplying real table reads.
"""

from pyspark import pipelines as dp

from gold_fulfillment_logic import compute_fulfillment_health


@dp.materialized_view(
    name="gold_fulfillment_health",
    comment="Order fulfillment SLA compliance by day and region — Operations' question from L1. "
    "Order-timing metric only; not joined to inventory (orders has no warehouse_id).",
)
def gold_fulfillment_health():
    return compute_fulfillment_health(spark.read.table("silver_orders"))
