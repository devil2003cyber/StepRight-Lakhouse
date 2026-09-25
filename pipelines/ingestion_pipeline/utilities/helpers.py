"""
Shared utilities for the StepRight ingestion pipeline.
"""
from pyspark.sql import SparkSession
def get_catalog() -> str:
  """
    Returns the target catalog for this pipeline run.

    Reads the 'stepright.catalog' key from the pipeline's Configuration field
    (set per DABs target from Module 6 onward — dev, uat, or prod). Defaults to
    'dev' so the pipeline still works during interactive development, before any
    Configuration value has been set.
  """
  spark = SparkSession.getActiveSession()
  if spark is None:
    return "dev"
  return spark.conf.get("stepright.catalog", "dev")

def landing_path(subfolder: str) -> str:
  """Builds a path into this project's landing volume for a given source subfolder."""
  return f"/Volumes/{get_catalog()}/stepright/landing/{subfolder}"