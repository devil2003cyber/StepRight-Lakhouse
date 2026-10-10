"""
Bronze quality — the quarantine pattern, applied to all 7 bronze tables.

Taught in depth, in the lecture, on two tables: bronze_orders (CDC) and
bronze_products (file-based). The remaining five follow the exact same shape —
only the rules differ, based on each table's own fields. Code here is complete
for all seven; nothing is left as an exercise.

Why this sits ON TOP of bronze, not inside it: bronze_orders, bronze_products,
and every other bronze table (built in L4/L5) keep their existing contract —
exactly what arrived, untouched. This file reads from those existing bronze
tables and adds quality tagging as a separate step, so ingestion code never has
to know quality rules exist, and quality rules can evolve independently.

Why expect_all, not expect_all_or_drop or expect_all_or_fail: a hard fail halts
the whole pipeline over one bad row. A silent drop loses that row without anyone
noticing — the same mistake as the referential-integrity lesson from L1.
expect_all keeps every row and simply tags it — nothing lost, nothing blocked.

Scope — structural checks only, not business checks: every rule below is
checkable on a single row, in isolation. Business and semantic rules —
deduplication, SCD Type 2 correctness, cross-table referential integrity —
need more context than one row can offer, and belong in Silver (Module 3).

PUBLISHING, corrected after live testing: the *_quality_check tagging table is
private=True on purpose — nothing outside this pipeline needs to see the raw
tagging step. But *_valid and *_quarantined are deliberately NOT private and
NOT plain @dp.view — both private tables and views are pipeline-scoped and
cannot be read from another pipeline, or from Databricks SQL, dashboards, or
alerts. Silver runs as its own separate pipeline (stepright-transformation-
pipeline) starting Module 3, and quarantine monitoring in L19 runs outside any
pipeline entirely — both need *_valid and *_quarantined to be real, published
Unity Catalog tables, readable via spark.read.table("dev.stepright.<name>")
from anywhere, not pipeline-internal objects.

Consumption: Silver reads from the *_valid tables below, never from bronze
directly. The *_quarantined tables are monitored — row-count trending and
spike alerting is built in L19 (DQ as code, Module 5).

Remediation: a quarantined row is never hand-edited in place. A correction
always re-enters as new data through the normal path — a new CDC update event
for CDC sources, a re-exported file for file-based sources, or a manually
curated correction batch for one-off historical fixes with no real source to
correct. See the Locked Decisions Document for the full remediation pattern.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import expr
from bronze_quality_logic import quarantine_rule




ORDERS_RULES={
    "valid_order_id":"after.order_id IS NOT NULL",
    "valid_customer_ref":"after.customer_id IS NOT NULL",
    "valid_total_amount":"after.total_amount IS NULL OR after.total_amount>=0",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(ORDERS_RULES)
def bronze_orders_quality_check():
    return spark.readStream.table("bronze_orders").withColumn(
        "is_quarantined", expr(quarantine_rule(ORDERS_RULES))
    )

@dp.table(comment="Orders that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_orders_valid():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = false")

@dp.table(comment="Orders that failed at least one structural quality check. Published — monitored in L19.")
def bronze_orders_quarantined():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = true")

PRODUCTS_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "valid_retail_price": "retail_price IS NULL OR retail_price > 0",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(PRODUCTS_RULES)
def bronze_products_quality_check():
    return spark.readStream.table("bronze_products").withColumn(
        "is_quarantined", expr(quarantine_rule(PRODUCTS_RULES))
    )
@dp.table(comment="Products that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_products_valid():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = false")


@dp.table(comment="Products that failed at least one structural quality check. Published — monitored in L19.")
def bronze_products_quarantined():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = true")


ORDER_ITEMS_RULES = {
    "valid_order_item_id": "after.order_item_id IS NOT NULL",
    "valid_order_ref": "after.order_id IS NOT NULL",
    "valid_product_ref": "after.product_id IS NOT NULL",
    "valid_quantity": "after.quantity IS NULL OR after.quantity > 0",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(ORDER_ITEMS_RULES)
def bronze_order_items_quality_check():
    return spark.readStream.table("bronze_order_items").withColumn(
        "is_quarantined", expr(quarantine_rule(ORDER_ITEMS_RULES))
    )
@dp.table(comment="Order items that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_order_items_valid():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = false")


@dp.table(comment="Order items that failed at least one structural quality check. Published — monitored in L19.")
def bronze_order_items_quarantined():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = true")



CUSTOMERS_RULES = {
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_email": "after.email IS NOT NULL",
    "valid_loyalty_tier": "after.loyalty_tier IS NULL OR after.loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum')",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CUSTOMERS_RULES)
def bronze_customers_quality_check():
    return spark.readStream.table("bronze_customers").withColumn(
        "is_quarantined", expr(quarantine_rule(CUSTOMERS_RULES))
    )
@dp.table(comment="Customers that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_customers_valid():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = false")


@dp.table(comment="Customers that failed at least one structural quality check")
def bronze_customers_quarantined():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = true")


CATEGORIES_RULES = {
    "valid_category_id": "category_id IS NOT NULL",
    "valid_category_name": "category_name IS NOT NULL",
}
@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CATEGORIES_RULES)
def bronze_categories_quality_check():
    return spark.readStream.table("bronze_categories").withColumn(
        "is_quarantined", expr(quarantine_rule(CATEGORIES_RULES))
    )
@dp.table(comment="Categories that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_categories_valid():  
    return spark.readStream.table("bronze_categories_quality_check").filter("is_quarantined = false")


@dp.table(comment="Categories that failed at least one structural quality check. Published — monitored in L19.")
def bronze_categories_quarantined():
    return spark.readStream.table("bronze_categories_quality_check").filter("is_quarantined = true")


CLICKSTREAM_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CLICKSTREAM_RULES)
def bronze_clickstream_quality_check():
    return spark.readStream.table("bronze_clickstream").withColumn(
        "is_quarantined", expr(quarantine_rule(CLICKSTREAM_RULES))
    )
@dp.table(comment="Clickstream events that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_clickstream_valid():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = false")


@dp.table(comment="Clickstream events that failed at least one structural quality check. Published — monitored in L19.")
def bronze_clickstream_quarantined():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = true")


INVENTORY_RULES = {
    "valid_snapshot_id": "snapshot_id IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_quantity_on_hand": "quantity_on_hand IS NULL OR quantity_on_hand >= 0",
}

@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(INVENTORY_RULES)
def bronze_inventory_quality_check():
    return spark.readStream.table("bronze_inventory").withColumn(
        "is_quarantined", expr(quarantine_rule(INVENTORY_RULES))
    )
@dp.table(comment="Inventory snapshots that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_inventory_valid():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = false")


@dp.table(comment="Inventory snapshots that failed at least one structural quality check. Published — monitored in L19.")
def bronze_inventory_quarantined():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = true")
