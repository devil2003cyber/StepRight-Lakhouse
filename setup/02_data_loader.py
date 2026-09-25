# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### Data Loader
# MAGIC Copies one generated batch from the `dev.stepright.staging` volume into the
# MAGIC `dev.stepright.landing` volume, and verifies row counts per source.
# MAGIC
# MAGIC This notebook stands in for Debezium and the warehouse's nightly export — the real
# MAGIC delivery mechanisms this project doesn't have running in this course. In production,
# MAGIC these sources would land in the volume continuously and automatically.
# MAGIC
# MAGIC **Before running:** generate a batch locally with `data_generator.py`, then manually
# MAGIC upload that batch's folder into `dev.stepright.staging/batch_<n>/` using the Databricks
# MAGIC workspace UI (Catalog Explorer → dev → stepright → staging → Upload).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One-time: the staging volume, separate from landing, only used to receive
# MAGIC -- manually-uploaded batch files before we load them.
# MAGIC CREATE VOLUME IF NOT EXISTS dev.stepright.staging;

# COMMAND ----------

dbutils.widgets.text("batch", "0", "Batch number to load")
batch_num = dbutils.widgets.get("batch")
 
staging_root = f"/Volumes/dev/stepright/staging/batch_{batch_num}"
landing_root = "/Volumes/dev/stepright/landing"
 
print(f"Loading batch_{batch_num} from {staging_root} into {landing_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Copy each source's files from staging into its landing subfolder

# COMMAND ----------

subfolders = [
    "orders_cdc",
    "order_items_cdc",
    "customers_cdc",
    "products",
    "categories",
    "clickstream",
    "inventory",
]
 
def path_exists(path: str) -> bool:
    """
    Standard Databricks pattern for checking a path exists — dbutils.fs has no
    dedicated exists() method, so we ls() it and treat any failure as "missing."
    This is checked BEFORE attempting a copy, rather than relying on dbutils.fs.cp
    to raise a Python-catchable exception on a missing source — that behavior
    isn't reliable across all Databricks runtimes, and silently depending on it
    is exactly the kind of thing that works in testing and breaks later.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False
 
 
for folder in subfolders:
    src = f"{staging_root}/{folder}"
    dst = f"{landing_root}/{folder}"
    if path_exists(src):
        dbutils.fs.cp(src, dst, recurse=True)
        print(f"Copied: {src} -> {dst}")
    else:
        print(f"Skipped {folder} — nothing staged for batch_{batch_num}. "
              f"Expected for products/categories on most incremental batches — "
              f"see data_generator.py for which sources each batch touches.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify row counts per source
# MAGIC This is also our environment health check — if Unity Catalog and the landing volume
# MAGIC are correctly wired, every source below should show a non-zero row count.

# COMMAND ----------

csv_sources = ["products", "categories", "inventory"]
json_sources = ["orders_cdc", "order_items_cdc", "customers_cdc", "clickstream"]
 
print(f"{'source':<20}{'format':<8}{'row_count'}")
print("-" * 45)
 
for folder in csv_sources:
    path = f"{landing_root}/{folder}"
    try:
        count = spark.read.option("header", "true").csv(path).count()
    except Exception:
        count = 0
    print(f"{folder:<20}{'csv':<8}{count}")
 
for folder in json_sources:
    path = f"{landing_root}/{folder}"
    try:
        count = spark.read.json(path).count()
    except Exception:
        count = 0
    print(f"{folder:<20}{'json':<8}{count}")
 

# COMMAND ----------

# MAGIC %md
# MAGIC All seven sources should show a non-zero count above. If any source shows zero,
# MAGIC check that this batch's folder was actually uploaded to
# MAGIC `/Volumes/dev/stepright/staging/batch_<n>/<source>/`, matching the `batch` widget
# MAGIC value above.