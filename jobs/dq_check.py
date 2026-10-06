# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### DQ Gate — Between Ingestion and Transformation
# MAGIC Runs as the second task in `stepright-orchestration-job`, after ingestion, before
# MAGIC transformation. This is a JOB-LEVEL health check, not a row-level quality rule —
# MAGIC it asks "is this entire run healthy enough to build on top of," not "should this
# MAGIC one row be trusted." Row-level rules already exist in Bronze and Silver
# MAGIC and never block anything. This gate can, and does, on purpose.
# MAGIC
# MAGIC Two checks:
# MAGIC 1. Every bronze table has at least one row from `run_date` specifically.
# MAGIC 2. No bronze quarantine pair's rate, computed only over `run_date`'s rows, has
# MAGIC    spiked past a coarse threshold (50%). This check only exists to catch
# MAGIC    "something is catastrophically wrong with today's batch," not to replace
# MAGIC    real DQ reporting.
# MAGIC
# MAGIC If either check fails, this notebook raises, the task fails, and — because job
# MAGIC tasks only run if their dependency succeeded by default — transformation never
# MAGIC starts. Nothing silently continues on top of a broken run.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("catalog", "dev", "Target catalog")
# Standalone default (real "today") for interactive testing outside a job run.
# When this notebook runs as part of stepright-orchestration-job, the job
# parameter run_date (default {{job.start_time.iso_date}}) overrides this
# automatically — see job_config_reference.md.
dbutils.widgets.text("run_date", datetime.now(timezone.utc).date().isoformat(), "Run date (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

BRONZE_TABLES = [
    "bronze_orders", "bronze_order_items", "bronze_customers",
    "bronze_products", "bronze_categories", "bronze_clickstream", "bronze_inventory",
]

# All 7 bronze quarantine pairs — the only layer that has actually finished
# executing by the time this gate runs. Every pair carries _ingested_at
# forward from bronze unchanged
QUARANTINE_PAIRS = [
    ("bronze_orders_valid", "bronze_orders_quarantined"),
    ("bronze_order_items_valid", "bronze_order_items_quarantined"),
    ("bronze_customers_valid", "bronze_customers_quarantined"),
    ("bronze_products_valid", "bronze_products_quarantined"),
    ("bronze_categories_valid", "bronze_categories_quarantined"),
    ("bronze_clickstream_valid", "bronze_clickstream_quarantined"),
    ("bronze_inventory_valid", "bronze_inventory_quarantined"),
]

QUARANTINE_RATE_THRESHOLD = 0.5  # coarse circuit breaker, not a precision metric


def date_filter() -> str:
    """Every table this notebook checks is Bronze — _ingested_at is always a
    real timestamp column here."""
    return f"date(_ingested_at) = '{run_date}'"


# COMMAND ----------

failures = []

for table in BRONZE_TABLES:
    count = spark.table(f"{catalog}.stepright.{table}") \
        .filter(date_filter()) \
        .count()
    if count == 0:
        failures.append(f"{table} has zero rows for run_date={run_date} — ingestion may have failed silently.")

for valid_table, quarantined_table in QUARANTINE_PAIRS:
    filter_expr = date_filter()
    valid_count = spark.table(f"{catalog}.stepright.{valid_table}").filter(filter_expr).count()
    quarantined_count = spark.table(f"{catalog}.stepright.{quarantined_table}").filter(filter_expr).count()
    total = valid_count + quarantined_count
    if total > 0:
        rate = quarantined_count / total
        if rate > QUARANTINE_RATE_THRESHOLD:
            failures.append(
                f"{quarantined_table} quarantine rate for run_date={run_date} is {rate:.0%} "
                f"(threshold {QUARANTINE_RATE_THRESHOLD:.0%}) — {quarantined_count} of {total} rows."
            )

# COMMAND ----------

if failures:
    message = f"DQ gate failed for run_date={run_date}:\n" + "\n".join(f"  - {f}" for f in failures)
    print(message)
    raise Exception(message)

print(f"DQ gate passed for run_date={run_date} — {len(BRONZE_TABLES)} bronze tables checked, "
      f"{len(QUARANTINE_PAIRS)} quarantine pairs within threshold.")