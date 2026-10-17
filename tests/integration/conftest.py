"""
tests/integration/conftest.py
 
Shared fixtures for integration tests against the real, bundle-deployed
`uat` catalog. Per the locked Testing Data Strategy: Setup -> Trigger ->
Validate, via the Databricks SDK.
 
Job/pipeline IDs are resolved via `databricks bundle summary`, not by name
or tag lookup — the bundle deployment is the authoritative source for
"which resource is uat's," same as `terraform output`. This sidesteps the
duplicate-name situation entirely (dev/uat share a workspace and, by
design, identical resource names) rather than trying to disambiguate
after the fact.
"""
 
import json
import subprocess
from dataclasses import dataclass
 
import pytest
from databricks.sdk import WorkspaceClient
 
# Path from tests/integration/ to bundle/ — same ../../ depth convention
# used throughout this project's resource YAML.
BUNDLE_DIR = "../../bundle"
TARGET = "uat"
 
 
@dataclass
class UatResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str
    warehouse_id: str
 
 
@pytest.fixture(scope="session")
def uat_resource_ids() -> UatResourceIds:
    """
    Resolves uat's actual deployed resource IDs via `databricks bundle
    summary`, run once per test session. Fails loudly (not silently) if
    the bundle hasn't been deployed to uat, or if a resource key here no
    longer matches what's in the bundle's resources/*.yml — both are
    real setup problems that should stop the test suite immediately,
    not surface later as a confusing SDK "not found" error.
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
 
    # Running `databricks` from inside the Databricks Terminal reprints
    # an installer banner ("Installing the CLI...", same banner seen
    # throughout every bundle validate/deploy in this project) to stdout
    # on every invocation, ahead of the actual JSON — confirmed via a
    # real JSONDecodeError, not assumed. Slicing to the first '{' strips
    # it without depending on the banner's exact wording.
    # Running `databricks` from inside the Databricks Terminal reprints
    # an installer banner ("Installing the CLI...", same banner seen
    # throughout every bundle validate/deploy in this project) to stdout
    # on every invocation, ahead of the actual JSON — confirmed via a
    # real JSONDecodeError, not assumed. Slicing to the first '{' strips
    # it without depending on the banner's exact wording. This is a
    # superset fix, not an environment-specific workaround: in CI, where
    # a pre-installed CLI likely produces clean JSON with no banner,
    # brace_index is just 0 and this is a no-op.
    brace_index = result.stdout.find("{")
    if brace_index == -1:
        pytest.fail(
            f"`databricks bundle summary` produced no JSON at all — full stdout:\n{result.stdout}"
        )
    summary = json.loads(result.stdout[brace_index:])
 
    # Confirmed against a real `bundle summary` run — resources.pipelines.
    # <key>.id / resources.jobs.<key>.id, plus variables.<name>.value for
    # stepright_warehouse_id. Previously flagged UNVERIFIED; verified.
    try:
        resources = summary["resources"]
        return UatResourceIds(
            ingestion_pipeline_id=resources["pipelines"]["stepright_ingestion_pipeline"]["id"],
            transformation_pipeline_id=resources["pipelines"]["stepright_transformation_pipeline"]["id"],
            orchestration_job_id=resources["jobs"]["stepright_orchestration_job"]["id"],
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
    dev and uat share one workspace host (locked design), so no
    target-specific host/profile switching is needed here — the default
    credential chain (env vars or ~/.databrickscfg profile) is sufficient
    for everything this suite does. Only prod would need a distinct
    client, and prod integration testing isn't in scope for L21.
    """
    return WorkspaceClient()
 
 
@pytest.fixture
def reset_uat(workspace_client: WorkspaceClient):
    """
    Setup step: clears the landing volume's contents before a test runs,
    so a stale file from a previous run can't get reprocessed alongside
    the fresh fixed-seed batch.
 
    Does NOT truncate bronze/silver/gold tables — confirmed, via a real
    error, that this doesn't work the way it might seem to. Two
    independent reasons: (1) Lakeflow materialized views can't be
    TRUNCATEd at all — confirmed via a live EXPECT_TABLE_NOT_VIEW error
    on gold_daily_revenue, and SHOW VIEWS doesn't reliably identify them
    either (it missed this one). (2) Even for genuinely mutable streaming
    tables, Auto Loader's checkpoint (tracking which files have already
    been processed) lives separately from the table itself — truncating
    the table wouldn't make the pipeline reprocess anything on a normal
    incremental run regardless.
 
    The actual reset mechanism is a FULL REFRESH, triggered as part of
    the Trigger step in test_integration.py (`pipeline_params:
    {full_refresh: true}` on the job run_now call, confirmed as a real
    Jobs API field) — that's what resets materialized views AND clears
    streaming checkpoints together, correctly, in one documented
    operation. This fixture's only remaining job is making sure the
    fixed-seed batch is the only thing in landing when that run starts.
 
    Does NOT drop the schema/volume/pipelines/dashboard — those are
    one-time-per-environment (stepright_environment_bootstrap's job),
    not per-test-run.
    """
    catalog = TARGET  # "uat" — catalog and target name happen to match here
 
    # Clear the landing volume's contents (not the folders themselves —
    # 01_environment_setup.py already created those, and pipelines expect
    # them to exist).
    landing_root = f"/Volumes/{catalog}/stepright/landing"
    subfolders = [
        "orders_cdc", "order_items_cdc", "customers_cdc",
        "products", "categories", "clickstream", "inventory",
    ]
    for folder in subfolders:
        dir_path = f"{landing_root}/{folder}"
        for entry in workspace_client.files.list_directory_contents(dir_path):
            workspace_client.files.delete(f"{dir_path}/{entry.name}")
 
    yield
 