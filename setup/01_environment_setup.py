# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### Environment Setup
# MAGIC Creates the `<catalog>` catalog and `<catalog>.stepright` schema, the `landing` volume, and its
# MAGIC seven subfolders.
# MAGIC
# MAGIC Parameterized via a `catalog` widget — this is one-time-per-environment setup, run once against
# MAGIC `dev`, once against `uat`, once against `prod`. Every statement below is idempotent
# MAGIC (`IF NOT EXISTS` / `mkdirs`), so a re-run against an environment that already exists is a safe
# MAGIC no-op, not an error.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog")
catalog = dbutils.widgets.get("catalog")
print(f"Running environment setup for catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC Fixed from the original: every reference below used to be hardcoded to `dev` — this notebook
# MAGIC predates the project's catalog-parameterization convention (`get_catalog()`, locked in
# MAGIC Module 6) and never got updated. Running it unmodified against `uat` or `prod` would have
# MAGIC silently kept touching `dev` instead. `%sql` magic cells can't take a Python f-string
# MAGIC substitution directly, so these are `spark.sql(f"...")` calls instead — same pattern already
# MAGIC used everywhere else parameterization matters in this project.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# COMMAND ----------

display(spark.sql(f"SHOW CATALOGS LIKE '{catalog}'"))

# COMMAND ----------

# Decision 1 (unchanged from original): a dedicated schema for this project, isolated from dev.dbx_course
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.stepright")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE 'stepright'"))

# COMMAND ----------

# Decision 2 (unchanged from original): one landing volume for all five sources, not five separate volumes
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.stepright.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the seven landing subfolders
# MAGIC Three CDC subfolders (orders, order_items, customers) and four file-based subfolders
# MAGIC (products, categories, clickstream, inventory) — one folder per source, inside the single
# MAGIC `landing` volume.

# COMMAND ----------

landing_root = f"/Volumes/{catalog}/stepright/landing"

subfolders = [
    "orders_cdc",
    "order_items_cdc",
    "customers_cdc",
    "products",
    "categories",
    "clickstream",
    "inventory",
]

for folder in subfolders:
    dbutils.fs.mkdirs(f"{landing_root}/{folder}")
    print(f"Created: {landing_root}/{folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify the landing zone

# COMMAND ----------

display(dbutils.fs.ls(landing_root))

# COMMAND ----------

# MAGIC %md
# MAGIC All seven folders should be listed above. On a first run against a new catalog they'll be
# MAGIC empty; on a re-run against an environment that already has data, this step is a safe no-op.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scaffold the GitHub
# MAGIC Setup complete project structure now, using the terminal built into this Git folder.
# MAGIC ```bash
# MAGIC mkdir -p setup
# MAGIC mkdir -p pipelines/ingestion_pipeline/transformations
# MAGIC mkdir -p pipelines/ingestion_pipeline/utilities
# MAGIC mkdir -p pipelines/ingestion_pipeline/tests
# MAGIC mkdir -p pipelines/transformation_pipeline/transformations
# MAGIC mkdir -p pipelines/transformation_pipeline/utilities
# MAGIC mkdir -p pipelines/transformation_pipeline/tests
# MAGIC mkdir -p jobs
# MAGIC mkdir -p bundle/resources
# MAGIC mkdir -p .github/workflows
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ####Placeholder files
# MAGIC Git does not track empty folders. If we commit right now, most of these empty folders will simply disappear from GitHub. So, we place one placeholder file inside each empty folder.
# MAGIC
# MAGIC ```bash
# MAGIC find setup pipelines jobs bundle .github -type d -empty -exec touch {}/.gitkeep \;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ####Let us also create three files we already know we will need.
# MAGIC ```bash
# MAGIC touch README.md
# MAGIC touch requirements.txt
# MAGIC touch data_generator.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ####Commit to GitHub
# MAGIC Make sure you have repo and workflow access to your GitHub PAT
# MAGIC