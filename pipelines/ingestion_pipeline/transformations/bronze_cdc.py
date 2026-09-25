"""
Bronze exists to give us a durable, replayable record of exactly what arrived, so
any downstream rebuild never has to re-touch the source systems. Using Variant for 
'before' and 'after' protects Bronze from breaking when source schemas drift.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    VariantType,
)

from utilities.helpers import get_catalog, landing_path


# Single reusable CDC envelope schema across all Bronze tables.
# 'before' and 'after' use VariantType to store flexible JSON payloads seamlessly.
CDC_ENVELOPE_SCHEMA = StructType(
    [
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("before", VariantType()),
        StructField("after", VariantType()),
    ]
)


def _read_cdc_bronze(subfolder: str):
    """Shared Auto Loader read using Variant schema, with ingestion metadata attached."""
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(CDC_ENVELOPE_SCHEMA)
            .load(landing_path(subfolder))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_orders",
    comment="Raw Debezium CDC envelope for orders with Variant payloads. Not flattened yet."
)
def bronze_orders():
    return _read_cdc_bronze("orders_cdc")


@dp.table(
    name="bronze_order_items",
    comment="Raw Debezium CDC envelope for order_items with Variant payloads. Not flattened yet.",
)
def bronze_order_items():
    return _read_cdc_bronze("order_items_cdc")


@dp.table(
    name="bronze_customers",
    comment="Raw Debezium CDC envelope for customers with Variant payloads. Not flattened yet.",
)
def bronze_customers():
    return _read_cdc_bronze("customers_cdc")