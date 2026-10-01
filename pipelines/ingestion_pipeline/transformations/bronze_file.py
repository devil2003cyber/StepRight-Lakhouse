
"""
Bronze layer — file-based sources: products, categories, clickstream, inventory.

Same discipline as bronze_cdc.py: Auto Loader, one mechanism regardless of how
often a source actually changes, and an explicitly declared schema, since these
are our own locked, known schemas too — nothing to infer.

No envelope wrapping here. op/before/after exists in bronze_cdc.py because
Debezium's CDC format needs it. These sources are plain file drops — already
flat, already a real record on their own. Wrapping them in an envelope would add
complexity with no purpose.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

from utilities.helpers import get_catalog, landing_path


CATEGORIES_SCHEMA = StructType(
    [
        StructField("category_id", StringType()),
        StructField("category_name", StringType()),
        StructField("parent_category_id", StringType()),
        StructField("is_active", BooleanType()),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("product_name", StringType()),
        StructField("brand", StringType()),
        StructField("category_id", StringType()),
        StructField("gender_target", StringType()),
        StructField("size_uk", DoubleType()),
        StructField("colour", StringType()),
        StructField("material", StringType()),
        StructField("cost_price", DoubleType()),
        StructField("retail_price", DoubleType()),
        StructField("is_active", BooleanType()),
        StructField("launch_date", StringType()),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType()),
        StructField("snapshot_date", StringType()),
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("warehouse_id", StringType()),
        StructField("quantity_on_hand", LongType()),
        StructField("quantity_reserved", LongType()),
        StructField("quantity_available", LongType()),
        StructField("reorder_point", LongType()),
        StructField("days_of_supply", LongType()),
    ]
)

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("product_id", StringType()),
        StructField("page_url", StringType()),
        StructField("referrer", StringType()),
        StructField("device_type", StringType()),
        StructField("search_term", StringType()),
        StructField("order_id", StringType()),
    ]
)


def _read_file_bronze(subfolder: str, schema: StructType, file_format: str):
    """Shared Auto Loader read, with ingestion metadata attached, for one file source."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .schema(schema)
    )
    if file_format == "csv":
        reader = reader.option("header", "true")

    return (
        reader.load(landing_path(subfolder))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="bronze_categories",
    comment="Raw categories reference data, exactly as exported. Low change frequency, same Auto Loader mechanism as every other source.",
)
def bronze_categories():
    return _read_file_bronze("categories", CATEGORIES_SCHEMA, "csv")


@dp.table(
    name="bronze_products",
    comment="Raw product catalog export, exactly as it arrived.",
)
def bronze_products():
    return _read_file_bronze("products", PRODUCTS_SCHEMA, "csv")


@dp.table(
    name="bronze_clickstream",
    comment="Raw storefront clickstream events, append-only, exactly as they arrived.",
)
def bronze_clickstream():
    return _read_file_bronze("clickstream", CLICKSTREAM_SCHEMA, "json")


@dp.table(
    name="bronze_inventory",
    comment="Raw daily inventory snapshots across all warehouses, exactly as exported.",
)
def bronze_inventory():
    return _read_file_bronze("inventory", INVENTORY_SCHEMA, "csv")



