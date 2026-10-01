"""
Silver layer — file-based sources: products, categories (snapshot/reference data),
clickstream, inventory (event/timeseries data).

Type casting (L7 gap fix): launch_date arrives from bronze as StringType — cast
to a real date here, once, same principle as event_timestamp and snapshot_date
below (already correct since L8's original build).

Business rules (L7): report-only via expect_all — either as create_streaming_table's
native parameter (products, categories — AUTO CDC targets), or as the @dp.expect_all
decorator stacked on a plain @dp.table (clickstream, inventory — not AUTO CDC
targets, so there's no create_streaming_table call to pass the parameter to).
categories has no business rule in this project's list — not every table needs one.

Primary keys (products, categories): declared via the schema= parameter on
create_streaming_table, not a separate ALTER TABLE step — ALTER TABLE is not
supported on pipeline-managed streaming tables at all (confirmed via a live
error: STREAMING_TABLE_OPERATION_NOT_ALLOWED.INVALID_ALTER). schema= must list
every output column exactly, not just the constraint — if this table's query
changes, this schema string must be updated to match, or table creation fails.
Foreign keys are NOT declared here or anywhere in this project — a schema-
embedded FK requires its parent's PK to already exist, and nothing in the
pipeline's dependency graph tracks that ordering from a schema string alone.
Every foreign key relationship is monitored via anti-join instead (L9 demo,
wired into L16).
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col

from utilities.helpers import bronze_table

# ---------------------------------------------------------------------------
# products — snapshot source, AUTO CDC upsert (SCD Type 1), report-only rule
# ---------------------------------------------------------------------------


PRODUCTS_RULES = {
    "retail_above_cost": "retail_price > cost_price",
}


@dp.view
def products_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_products_valid"))
        .withColumn("launch_date", col("launch_date").cast("date"))
    )


dp.create_streaming_table(
    name="silver_products",
    comment="Products, current state only. Upserted by product_id, latest file wins.",
    expect_all=PRODUCTS_RULES,
    schema="""
        product_id STRING NOT NULL,
        sku STRING,
        product_name STRING,
        brand STRING,
        category_id STRING,
        gender_target STRING,
        size_uk DOUBLE,
        colour STRING,
        material STRING,
        cost_price DOUBLE,
        retail_price DOUBLE,
        is_active BOOLEAN,
        launch_date DATE,
        CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
    """,
)

dp.create_auto_cdc_flow(
    target="silver_products",
    source="products_change_feed",
    keys=["product_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
    except_column_list= ["_ingested_at", "_source_file", "is_quarantined"]
)

# ---------------------------------------------------------------------------
# categories — snapshot source, AUTO CDC upsert (SCD Type 1), no rule in this
# project's list — not every table needs one.
# ---------------------------------------------------------------------------


@dp.view
def categories_change_feed():
    return spark.readStream.table(bronze_table("bronze_categories_valid"))


dp.create_streaming_table(
    name="silver_categories",
    comment="Categories, current state only. Upserted by category_id, latest file wins.",
    schema="""
        category_id STRING NOT NULL,
        category_name STRING,
        parent_category_id STRING,
        is_active BOOLEAN,
        CONSTRAINT pk_silver_categories PRIMARY KEY (category_id)
    """,
)

dp.create_auto_cdc_flow(
    target="silver_categories",
    source="categories_change_feed",
    keys=["category_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
    except_column_list= ["_ingested_at", "_source_file", "is_quarantined"]
)

# ---------------------------------------------------------------------------
# clickstream, inventory — event/timeseries sources, conform + report-only rules
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "purchase_has_order": "NOT (event_type = 'purchase' AND order_id IS NULL)",
    "product_event_has_product": "NOT (event_type IN ('product_view', 'add_to_cart') AND product_id IS NULL)",
}


@dp.table(
    comment="Clickstream events, conformed — real timestamp type. Already an append-only "
    "event log, so no merge logic is needed; every row is naturally distinct.",
)
@dp.expect_all(CLICKSTREAM_RULES)
def silver_clickstream():
    return (
        spark.readStream.table(bronze_table("bronze_clickstream_valid"))
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )

INVENTORY_RULES = {
    "available_matches": "quantity_available = quantity_on_hand - quantity_reserved",
    "reserved_not_exceeding": "quantity_reserved <= quantity_on_hand",
}


@dp.table(
    comment="Inventory snapshots, conformed — real date type. Each day's snapshot is "
    "distinct from the last, not a replacement for it, so no merge logic is needed.",
)
@dp.expect_all(INVENTORY_RULES)
def silver_inventory():
    return (
        spark.readStream.table(bronze_table("bronze_inventory_valid"))
        .withColumn("snapshot_date", col("snapshot_date").cast("date"))
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )
