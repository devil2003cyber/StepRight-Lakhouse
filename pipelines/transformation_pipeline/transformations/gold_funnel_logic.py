"""
Gold — gold_funnel_analysis: pure transformation logic, no pipeline dependency.

Deliberately has NO `from pyspark import pipelines` import anywhere in this
file — see gold_revenue_logic.py's docstring for why that's a verified
requirement, not a preference.

Attribution happens at the SESSION level, not the customer level. Roughly half
our clickstream events have a null customer_id — an anonymous visitor. If we
attributed conversion only by customer_id, every anonymous session would be
silently excluded from the funnel entirely, undercounting every channel that
brings in browsing traffic before someone logs in or checks out as a guest.
session_id exists on every event, known customer or not, so it's the correct
grain for this table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as _sum, max as _max, when


def compute_funnel_analysis(clickstream_df: DataFrame) -> DataFrame:
    session_outcomes = (
        clickstream_df
        .groupBy("session_id", "referrer")
        .agg(
            _max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("converted")
        )
    )

    return (
        session_outcomes
        .groupBy("referrer")
        .agg(
            count("session_id").alias("total_sessions"),
            _sum("converted").alias("converted_sessions"),
        )
        .withColumn("conversion_rate", col("converted_sessions") / col("total_sessions"))
    )
