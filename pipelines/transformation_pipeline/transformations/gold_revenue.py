"""
Gold — gold_daily_revenue: revenue by day, category, and region.

Answers the VP of Finance's question from L1: trustworthy, current revenue
numbers, not a day-late spreadsheet stitched together by hand.

Revenue definition: gross_revenue, total_discount, and net_revenue are
reported as three separate, clearly labeled columns — not one ambiguous
"total_revenue" that silently picks gross or net. discount_amount lives at the
ORDER grain, not the line-item grain, so net revenue per category requires
proportionally allocating each order's discount across its line items, by each
item's share of that order's subtotal — not just subtracting a flat amount.

silver_orders is SCD Type 2 — filtered to the current row (__END_AT IS NULL)
before anything else, same rule as every Gold read of an SCD Type 2 table.
silver_order_items, silver_products, silver_categories are SCD Type 1 —
already current, no __END_AT column to filter, and silver_order_items itself
is already the quarantine-verified table (line_total already checked in L8).

coalesce(discount_amount, 0): a null discount on a discounted order shouldn't
null out the entire net-revenue calculation for that order's line items.

to_date(col("order_date")): order_date arrives from Silver as a real
TIMESTAMP. This call truncates it down to a date, for "revenue by DAY"
grouping — a normal, permanent part of Gold's job, not a workaround for
anything upstream.
"""

from pyspark import pipelines as dp
from gold_revenue_logic import compute_daily_revenue

@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Revenue by day, category, and region — gross, discount, and net reported "
    "separately. The trustworthy number Finance asked for in L1.",
)
def gold_daily_revenue():    
    return compute_daily_revenue(
        order_items_df=spark.read.table("silver_order_items"),
        orders_df=spark.read.table("silver_orders"),
        products_df=spark.read.table("silver_products"),
        categories_df=spark.read.table("silver_categories"),
    )

