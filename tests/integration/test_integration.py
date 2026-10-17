"""
tests/integration/test_integration.py
 
Setup -> Trigger -> Validate against the real, bundle-deployed uat catalog.
Per the locked Testing Data Strategy — uses the dedicated fixed-seed
generator batch, kept separate from dev/uat's regular dataset, so results
are deterministic and assertions can check exact values.
 
Seed/--as-of/scale values and every expected count below were run for
real (data_generator.py is standalone, no Databricks runtime needed) and
verified reproducible — two independent runs, same seed+--as-of, produced
byte-identical output. Not guessed.
 
Still genuinely open: the SDK's run_now().result() wait pattern hasn't
been confirmed against databricks-sdk==0.67.0 specifically, and the
"zero quarantined rows" assumption below is a reasoned expectation (the
generator enforces referential integrity by construction), not something
confirmed by actually running the pipeline — flagged where it matters.
"""
 
import os
import subprocess
 
 
# Exact values from the generator's own docstring example — confirmed
# reproducible via two independent runs producing byte-identical output.
FIXED_SEED = 42
FIXED_AS_OF = "2026-01-01"
GENERATOR_ARGS = [
    "--n-customers", "20", "--n-orders", "50", "--n-products", "15",
    "--n-clickstream", "200", "--n-inventory-days", "2",
]
 
# Real counts, read from an actual run of data_generator.py at the
# settings above — categories/customers/products/orders/clickstream are
# direct loop-count arguments (deterministic by construction, no need to
# even run it); inventory is n_inventory_days * n_products * len(WAREHOUSES)
# = 2 * 15 * 3 = 90, also computable directly. order_items is the one
# genuinely data-dependent count (random items-per-order) and could only
# be known by actually running the generator once — confirmed as 101.
EXPECTED_LANDING_COUNTS = {
    "categories": 6,
    "customers": 20,
    "products": 15,
    "orders": 50,
    "order_items": 101,
    "clickstream": 200,
    "inventory": 90,
}
 
 
def _generate_fixed_seed_batch(output_dir: str) -> str:
    """
    Runs data_generator.py with the confirmed integration-test seed/as-of
    and scale. Returns the actual batch_0 directory path (the generator
    writes to {output_dir}/batch_0/, not output_dir directly).
    """
    result = subprocess.run(
        [
            "python3", "../../data_generator.py",
            "--batch", "0",
            "--seed", str(FIXED_SEED),
            "--as-of", FIXED_AS_OF,
            "--output-dir", output_dir,
            "--state-file", os.path.join(output_dir, "generator_state.json"),
            *GENERATOR_ARGS,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"data_generator.py failed:\n{result.stderr}"
    return os.path.join(output_dir, "batch_0")
 
 
def _upload_batch_to_landing(workspace_client, batch_dir: str, catalog: str):
    """
    Uploads the generated batch's files into uat's landing volume
    subfolders. Subfolder names match exactly between the generator's
    SUBFOLDERS dict and 01_environment_setup.py's landing structure —
    both already use customers_cdc/orders_cdc/order_items_cdc/products/
    categories/clickstream/inventory, so no renaming needed.
    """
    landing_root = f"/Volumes/{catalog}/stepright/landing"
    for folder in os.listdir(batch_dir):
        local_folder = os.path.join(batch_dir, folder)
        if not os.path.isdir(local_folder):
            continue
        for filename in os.listdir(local_folder):
            local_path = os.path.join(local_folder, filename)
            remote_path = f"{landing_root}/{folder}/{filename}"
            with open(local_path, "rb") as f:
                workspace_client.files.upload(remote_path, f, overwrite=True)
 
 
def _table_count(workspace_client, catalog: str, warehouse_id: str, table: str) -> int:
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema="stepright",
        statement=f"SELECT COUNT(*) FROM {table}",
        wait_timeout="30s",
    )
    assert result.status.state.value == "SUCCEEDED", f"Query on {table} failed: {result.status.error}"
    return int(result.result.data_array[0][0])
 
 
def test_orchestration_job_end_to_end(
    workspace_client, uat_resource_ids, reset_uat, tmp_path
):
    """
    Setup -> Trigger -> Validate, against uat, using the fixed-seed batch.
    """
 
    # --- Setup ---
    # reset_uat (fixture) has already cleared landing's contents.
    batch_dir = _generate_fixed_seed_batch(str(tmp_path / "it_batch"))
    _upload_batch_to_landing(workspace_client, batch_dir, catalog="uat")
 
    # --- Trigger ---
    # CONFIRMED (not hypothesized) via two live tests: pipeline_params on
    # jobs.run_now is rejected outright on this job — "Cannot use legacy
    # parameters ... because the job has job parameters configured,"
    # identical error with or without job_parameters={} present. The
    # error's own list of "legacy" fields doesn't even name
    # pipeline_params, but the platform blocks it anyway — a real gap
    # between the error message and actual enforcement.
    #
    # Fix: trigger full_refresh directly on each pipeline via the
    # Pipelines API (a separate code path from jobs.run_now, unaffected
    # by this job's job-level parameters), sequentially — transformation
    # needs ingestion's fresh bronze tables first, same ordering as
    # stepright_environment_bootstrap. THEN trigger the job normally,
    # with no special params — by that point Auto Loader has nothing new
    # to process, so run_ingestion/run_transformation inside the job are
    # harmless no-ops, while dq_check/report get their real trigger.
    #
    # UNVERIFIED — pipelines.start_update's exact method/kwarg names and
    # its own wait-for-completion pattern haven't been confirmed against
    # this SDK version yet, same caveat as run_now().result() below.
    ingestion_update = workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id, full_refresh=True
    )
    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id
    )
 
    transformation_update = workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.transformation_pipeline_id, full_refresh=True
    )
    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.transformation_pipeline_id
    )
 
    run = workspace_client.jobs.run_now(
        job_id=int(uat_resource_ids.orchestration_job_id),
    ).result()
 
    assert run.state.result_state.value == "SUCCESS", (
        f"Orchestration job run did not succeed: "
        f"{run.state.result_state} — {run.state.state_message}"
    )
 
    # --- Validate ---
    catalog = "uat"
 
    # Bronze layer: raw ingestion count should exactly match what was
    # generated — every incoming row must land somewhere (valid OR
    # quarantined), none silently dropped. This "conservation" check
    # doesn't depend on knowing the DQ rules' internal logic, only that
    # rows aren't lost — a solid invariant regardless of pass/fail split.
    conservation_checks = {
        "categories": EXPECTED_LANDING_COUNTS["categories"],
        "customers": EXPECTED_LANDING_COUNTS["customers"],
        "products": EXPECTED_LANDING_COUNTS["products"],
        "orders": EXPECTED_LANDING_COUNTS["orders"],
    }
    for source, expected_total in conservation_checks.items():
        valid = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid")
        quarantined = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined")
        assert valid + quarantined == expected_total, (
            f"bronze_{source}: {valid} valid + {quarantined} quarantined "
            f"= {valid + quarantined}, expected {expected_total} — a row "
            f"was lost somewhere, not just failed validation"
        )
 
    # Reasoned expectation, not confirmed by an actual pipeline run yet:
    # zero quarantined rows. The generator enforces referential integrity
    # by construction (every customer_id/product_id an order references
    # exists within the same batch), so nothing SHOULD trip a
    # referential-integrity DQ rule. But range/null-check rules aren't
    # something this test file has visibility into — if this assertion
    # fails on a real run, that's genuinely useful signal (either a real
    # DQ issue, or a rule this reasoning didn't account for), not
    # necessarily a bug in the test itself.
    for source in conservation_checks:
        quarantined = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined")
        assert quarantined == 0, (
            f"bronze_{source}_quarantined has {quarantined} rows — expected 0 "
            f"given the generator's referential-integrity-by-construction. "
            f"Investigate before assuming this assertion is simply wrong."
        )
 