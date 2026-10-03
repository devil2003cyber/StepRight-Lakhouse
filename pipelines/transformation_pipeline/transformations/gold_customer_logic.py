"""
Gold — gold_customer_360: pure transformation logic, no pipeline dependency.

Deliberately has NO `from pyspark import pipelines` import anywhere in this
file — see gold_revenue_logic.py's docstring for why that's a verified
requirement, not a preference.

Same SCD Type 2 rule as gold_revenue_logic.py — silver_customers and
silver_orders both carry history, so both get filtered to their current row
before anything else happens.

"Churn signal" is kept descriptive, not predictive — days_since_last_order is
a number a stakeholder can read and act on themselves, not a model output
someone has to trust blindly. This project isn't building a churn model.

as_of_date is a required parameter, not a call to current_date() inside this
function. That distinction matters for a reason beyond style: a function that
calls current_date() internally is NOT deterministic — the exact same test
could pass today and fail tomorrow, since "correct" shifts with the calendar.
Injecting "today" as a parameter makes the function's output fully determined
by its inputs, which is the actual definition of something that can be unit
tested at all. The pipeline wrapper (gold_customer.py) supplies the real
current date; a test supplies a fixed one.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, count, max as _max, datediff, lit, to_date


def compute_customer_360(
    customers_df: DataFrame,
    orders_df: DataFrame,
    as_of_date: date,
) -> DataFrame:
    customers_current = customers_df.filter(col("__END_AT").isNull())
    orders_current = orders_df.filter(col("__END_AT").isNull())

    order_summary = (
        orders_current
        .groupBy("customer_id")
        .agg(
            _sum("total_amount").alias("lifetime_value"),
            count("order_id").alias("order_count"),
            _max("order_date").alias("last_order_date"),
        )
    )

    return (
        customers_current
        .join(order_summary, "customer_id", "left")
        .withColumn(
            "days_since_last_order",
            datediff(lit(as_of_date), to_date(col("last_order_date"))),
        )
        .select(
            "customer_id", "first_name", "last_name", "loyalty_tier",
            "lifetime_value", "order_count", "last_order_date", "days_since_last_order",
        )
    )
