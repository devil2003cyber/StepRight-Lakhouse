from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, to_date, coalesce, lit, when, round as _round

def compute_daily_revenue(
    order_items_df: DataFrame,
    orders_df: DataFrame,
    products_df: DataFrame,
    categories_df: DataFrame,
) -> DataFrame:
    # 1. Filter out historical soft-deleted or updated SCD2 records
    orders_current = orders_df.filter(col("__END_AT").isNull())
    
    # # 2. Derive line_total (quantity * unit_price)
    # items_with_line_total = order_items_df.withColumn(
    #     "line_total", 
    #     col("quantity") * col("unit_price")
    # )
    
    # 3. Calculate order-level subtotal
    order_subtotals = (
        order_items_df
        .groupBy("order_id")
        .agg(_sum("line_total").alias("order_subtotal"))
    )

    # 4. Filter strictly for completed/active sales (excluding pending, cancelled, returned)
    valid_statuses = ["confirmed", "shipped", "delivered"]

    items_with_discount = (
        order_items_df
        .join(orders_current, "order_id")
        .join(order_subtotals, "order_id")
        .filter(col("order_status").isin(valid_statuses))
        .withColumn("order_discount", coalesce(col("discount_amount"), lit(0.0)))
        .withColumn(
            "allocated_discount",
            when(
                col("order_subtotal") > 0, 
                (col("line_total") / col("order_subtotal")) * col("order_discount")
            ).otherwise(lit(0.0)),
        )
    )

    # 5. Aggregate revenue metrics by Day, Category, and Region
    return (
        items_with_discount
        .join(products_df, "product_id")
        .join(categories_df, "category_id")
        .groupBy(
            to_date(col("order_date")).alias("sales_date"),
            col("category_name"),
            col("shipping_state").alias("region"),
        )
        .agg(
            _round(_sum("line_total"), 2).alias("gross_revenue"),
            _round(_sum("allocated_discount"), 2).alias("total_discount"),
            _round(_sum("line_total") - _sum("allocated_discount"), 2).alias("net_revenue"),
        )
        .orderBy("sales_date", "region", "category_name")
    )