"""
Real-time DQ alerting — event hook, part of stepright-transformation-pipeline's
own source code.

Same file, same reasoning, as ingestion_pipeline/transformations/dq_alert_hook.py
— see that file's docstring for the full explanation of why this lives in
transformations/, not utilities/, and why it has to be registered separately
in each pipeline at all. Event hooks are pipeline-scoped, the same way
event_log() is pipeline-scoped (L18) — registering a hook in one pipeline
never makes it fire for the other.

This is the piece that was originally missing: without this file,
transformation_pipeline's ~11 report-only Silver rules and its one quarantined
rule (line_total) would generate real failures with zero real-time alerts —
only ever visible on the dashboard, on whatever schedule someone happens to
check it.
"""

from pyspark import pipelines as dp

from utilities.dq_alert_logic import check_expectations_and_alert


@dp.on_event_hook(max_allowable_consecutive_failures=3)
def dq_expectation_alert_hook(event: dict) -> None:
    check_expectations_and_alert(event, pipeline_label="transformation")
