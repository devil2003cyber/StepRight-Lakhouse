"""
Gold — gold_customer_360: pipeline wrapper.

All transformation logic lives in gold_customer_logic.py, unit tested directly. 
This file's only job is applying the pipeline decorator, supplying real
table reads, and supplying the real current date — the one place in this
project current_date() is still allowed to appear, since a real pipeline run
should genuinely use today.
"""

from datetime import timezone, datetime

from pyspark import pipelines as dp

from gold_customer_logic import compute_customer_360


@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency, and a descriptive churn signal — Marketing's question from requirement.",
)
def gold_customer_360():
    return compute_customer_360(
        customers_df=spark.read.table("silver_customers"),
        orders_df=spark.read.table("silver_orders"),
        as_of_date=datetime.now(timezone.utc).date(),
    )
