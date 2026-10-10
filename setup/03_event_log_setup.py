# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### DQ Monitoring — Event Log View Setup
# MAGIC Creates the `event_log_raw` view for each pipeline
# MAGIC **One real constraint:** the `event_log` table-valued function
# MAGIC "cannot be used... to access the event logs of multiple pipelines."
# MAGIC
# MAGIC Two pipelines,
# MAGIC two separate views — `ingestion_event_log_raw` and `transformation_event_log_raw`
# MAGIC
# MAGIC Only the pipeline owner can create this view, and it can't be shared with other
# MAGIC users — run this as whichever identity owns both pipelines.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Target catalog")
dbutils.widgets.text("ingestion_pipeline_id", "7c49cd0f-e118-48bd-ac81-1c43f9e2d7ed", "stepright-ingestion-pipeline ID")
dbutils.widgets.text("transformation_pipeline_id", "e076bb4f-d8b2-4423-9902-16078e1d9a7e", "stepright-transformation-pipeline ID")

catalog = dbutils.widgets.get("catalog")
ingestion_pipeline_id = dbutils.widgets.get("ingestion_pipeline_id")
transformation_pipeline_id = dbutils.widgets.get("transformation_pipeline_id")

if not ingestion_pipeline_id or not transformation_pipeline_id:
    raise ValueError(
        "Both pipeline IDs are required. Find them in each pipeline's Settings panel "
        "in the Lakeflow Pipelines Editor — this notebook won't guess them."
    )

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.ingestion_event_log_raw AS
SELECT * FROM event_log("{ingestion_pipeline_id}")
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.transformation_event_log_raw AS
SELECT * FROM event_log("{transformation_pipeline_id}")
""")

print(f"Created {catalog}.stepright.ingestion_event_log_raw")
print(f"Created {catalog}.stepright.transformation_event_log_raw")