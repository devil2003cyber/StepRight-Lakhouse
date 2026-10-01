"""
Shared utilities for the StepRight transformation pipeline (Silver + Gold).
"""

from pyspark.sql import SparkSession


def get_catalog() -> str:
    """
    Returns the target catalog for this pipeline run. Same pattern as the
    ingestion pipeline's helper — reads 'stepright.catalog' from the pipeline's
    Configuration field, defaults to 'dev' for interactive development.
    """
    spark = SparkSession.getActiveSession()
    return spark.conf.get("stepright.catalog", "dev")


def bronze_table(name: str) -> str:
    """
    Fully-qualified reference to a table published by the ingestion pipeline —
    a genuinely separate pipeline from this one.

    Cross-pipeline reads must be fully qualified. This pipeline's own target
    catalog/schema (set in pipeline settings) is only the default resolution
    context for tables THIS pipeline writes — it does not extend to tables
    published by a different pipeline, even one writing to the same catalog.
    Being explicit here also matches this project's rule from Module 2: every
    cross-boundary reference is built through a helper, never left implicit.
    """
    return f"{get_catalog()}.stepright.{name}"
