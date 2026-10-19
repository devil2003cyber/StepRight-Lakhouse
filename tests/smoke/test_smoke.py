"""
tests/smoke/test_smoke.py
 
Non-destructive prod smoke test. Confirms a deployment is healthy —
resources exist, are reachable, and nothing is in a failed state — without
touching landing, without full_refresh, and without asserting specific row
counts. Real prod data has no fixed-seed guarantee the way tests/integration
does, and a freshly-promoted prod may legitimately have zero rows in every
table until real business data arrives — asserting non-zero here would make
this fail on every clean first deploy, exactly backwards for a health check.
 
HARD assertions: resources exist and are reachable, tables are queryable,
no pipeline's most recent update (if any exist) is in a failed state.
INFORMATIONAL only: actual row counts — logged, never asserted.
"""
 
GOLD_TABLES = [
    "gold_daily_revenue",
    "gold_product_performance",
    "gold_customer_360",
    "gold_funnel_analysis",
    "gold_fulfillment_health",
]
 
 
def test_pipelines_exist_and_are_reachable(workspace_client, prod_resource_ids):
    for pipeline_id in (
        prod_resource_ids.ingestion_pipeline_id,
        prod_resource_ids.transformation_pipeline_id,
    ):
        pipeline = workspace_client.pipelines.get(pipeline_id=pipeline_id)
        assert pipeline is not None, f"Pipeline {pipeline_id} not reachable"
 
 
def test_pipelines_latest_update_did_not_fail(workspace_client, prod_resource_ids):
    """
    Only checks the MOST RECENT update, if one exists. No update history
    yet (a genuinely fresh prod, bootstrap job's own priming run aside) is
    not a failure — it just means nothing has run for real yet.
 
    Confirmed via a real call against uat: list_updates() returns a
    ListUpdatesResponse object with an `.updates` attribute holding the
    actual list — NOT something directly iterable itself. The State enum
    values (COMPLETED, FAILED, CANCELED, etc.) are also confirmed exact.
    """
    for pipeline_id in (
        prod_resource_ids.ingestion_pipeline_id,
        prod_resource_ids.transformation_pipeline_id,
    ):
        response = workspace_client.pipelines.list_updates(
            pipeline_id=pipeline_id, max_results=1
        )
        updates = response.updates or []
        if not updates:
            print(f"Pipeline {pipeline_id}: no update history yet — skipping, not a failure")
            continue
 
        latest = updates[0]
        assert latest.state.value not in ("FAILED", "CANCELED"), (
            f"Pipeline {pipeline_id}'s most recent update is in state "
            f"{latest.state.value}, not healthy"
        )
 
 
def test_orchestration_job_exists_and_is_reachable(workspace_client, prod_resource_ids):
    job = workspace_client.jobs.get(job_id=int(prod_resource_ids.orchestration_job_id))
    assert job is not None, "Orchestration job not reachable"
 
 
def test_dashboard_exists_and_is_reachable(workspace_client, prod_resource_ids):
    """
    Confirmed against a real call: workspace_client.lakeview.get(...) is
    the correct method (LakeviewAPI, not the legacy dashboards/Redash
    API) — verified via help() before trusting it.
    """
    dashboard = workspace_client.lakeview.get(dashboard_id=prod_resource_ids.dashboard_id)
    assert dashboard is not None, "Dashboard not reachable"
 
 
def test_gold_tables_are_queryable(workspace_client, prod_resource_ids):
    """
    HARD: every gold table must be queryable at all — proves the schema
    and tables genuinely exist post-deploy, which is the real point of a
    smoke test. INFORMATIONAL: the actual row count is printed for visual
    inspection in the CI log, never asserted — zero rows is a legitimate,
    healthy state for a freshly-promoted prod.
    """
    for table in GOLD_TABLES:
        result = workspace_client.statement_execution.execute_statement(
            warehouse_id=prod_resource_ids.warehouse_id,
            catalog="prod",
            schema="stepright",
            statement=f"SELECT COUNT(*) FROM {table}",
            wait_timeout="30s",
        )
        assert result.status.state.value == "SUCCEEDED", (
            f"{table} is not queryable: {result.status.error}"
        )
        row_count = int(result.result.data_array[0][0])
        print(f"{table}: {row_count} rows (informational only, not asserted)")
 