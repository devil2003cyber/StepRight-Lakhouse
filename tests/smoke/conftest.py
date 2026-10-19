"""
tests/smoke/conftest.py
 
Fixtures for the prod smoke test — deliberately separate from
tests/integration/conftest.py, not a shared/parameterized version of it.
This suite must never touch landing, never trigger full_refresh, and never
assert against fixed-seed-specific values — reusing integration's fixtures
risked inheriting assumptions (reset_uat's landing-clearing, for instance)
that would be actively dangerous pointed at real production data.
"""
 
import json
import subprocess
from dataclasses import dataclass

import pytest
from databricks.sdk import WorkspaceClient
 
BUNDLE_DIR = "../../bundle"
TARGET = "prod"
 
 
@dataclass
class ProdResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str
    dashboard_id: str
    warehouse_id: str
 
 
@pytest.fixture(scope="session")
def prod_resource_ids() -> ProdResourceIds:
    """
    Same mechanism as uat_resource_ids — `databricks bundle summary` is
    the authoritative source for "which resource is prod's," not name or
    tag lookup. Brace-stripping for the CLI's installer-banner-in-stdout
    quirk carried over unchanged (confirmed real, not environment-specific
    to the Databricks Terminal — see tests/integration/conftest.py).
    """
    result = subprocess.run(
        ["databricks", "bundle", "summary", "--target", TARGET, "--output", "json"],
        cwd=BUNDLE_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"`databricks bundle summary --target {TARGET}` failed — is the "
            f"bundle actually deployed to {TARGET}?\n{result.stderr}"
        )
 
    brace_index = result.stdout.find("{")
    if brace_index == -1:
        pytest.fail(
            f"`databricks bundle summary` produced no JSON at all — full stdout:\n{result.stdout}"
        )
    summary = json.loads(result.stdout[brace_index:])
 
    try:
        resources = summary["resources"]
        return ProdResourceIds(
            ingestion_pipeline_id=resources["pipelines"]["stepright_ingestion_pipeline"]["id"],
            transformation_pipeline_id=resources["pipelines"]["stepright_transformation_pipeline"]["id"],
            orchestration_job_id=resources["jobs"]["stepright_orchestration_job"]["id"],
            dashboard_id=resources["dashboards"]["stepright_dq_dashboard"]["id"],
            warehouse_id=summary["variables"]["stepright_warehouse_id"]["value"],
        )
    except KeyError as e:
        pytest.fail(
            f"Unexpected `bundle summary` JSON shape — missing key {e}. "
            f"Full output:\n{json.dumps(summary, indent=2)}"
        )
 
 
@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """
    prod is a genuinely separate workspace from dev/uat (unlike those
    two, which share one host) — this relies on DATABRICKS_HOST/TOKEN
    env vars pointing at the PROD workspace specifically when this suite
    runs. In deploy-prod.yml, that's DATABRICKS_HOST_PROD/TOKEN_PROD.
    Running this suite locally requires deliberately setting those env
    vars to prod's values first — there is no target-based host
    switching here, same as tests/integration's client.
    """
    return WorkspaceClient()
 
 
# Deliberately no reset/cleanup fixture in this file. This suite reads
# only — no landing volume writes, no full_refresh, nothing that mutates
# state. There is nothing here to reset before or after a run.
 