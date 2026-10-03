"""
Gold — gold_funnel_analysis: pipeline wrapper.

All transformation logic lives in gold_funnel_logic.py, unit tested directly. 
This file's only job is applying the pipeline decorator and supplying
real table reads.
"""

from pyspark import pipelines as dp

from gold_funnel_logic import compute_funnel_analysis


@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="Conversion rate by channel, including anonymous sessions — Growth's question from L1.",
)
def gold_funnel_analysis():
    return compute_funnel_analysis(spark.read.table("silver_clickstream"))
