"""
DQ alert logic — shared between both pipelines, duplicated as a parallel copy
in each pipeline's own utilities/ folder, same as helpers.py already is.

Why duplicated, not a single shared file: this project's pipelines are each
self-contained — no cross-pipeline Python imports anywhere, matching why
bronze_table() builds fully-qualified table names for cross-pipeline TABLE
reads instead of one pipeline importing the other's code. A genuinely shared
external module would be a new pattern, inconsistent with how the rest of the
repo is organized. Keeping this file identical in both places is a real,
accepted maintenance cost of that choice — if the alerting logic changes, it
has to change in both copies.

No `from pyspark import pipelines` here, on purpose — this file is plain,
importable logic. The decorator that actually registers a hook lives only in
each pipeline's own transformations/dq_alert_hook.py, which imports this
module and calls check_expectations_and_alert(). Same "logic file, thin
wrapper" split used throughout this project since L15.
"""

FAILURE_RATE_ALERT_THRESHOLD = 25.0  # percent — deliberately coarse, matching
# dq_check.py's own 50% job-level gate philosophy: this is a "something looks
# genuinely wrong" signal, not a precise per-rule alert. Real trend analysis
# happens on the dashboard, not in a hook that has to run fast and not block.


def send_alert(message: str) -> None:
    """
    Stub — replace with a real Slack/PagerDuty/webhook call. Databricks' own
    documented event hook example sends to Slack via a Databricks secret-backed
    token: dbutils.secrets.get(scope=..., key=...) then a requests.post() to
    the Slack webhook URL. Kept as a stub here rather than a fake credential.
    """
    print(f"[DQ ALERT] {message}")


def check_expectations_and_alert(event: dict, pipeline_label: str) -> None:
    """
    pipeline_label distinguishes which pipeline raised the alert — the same
    event shape arrives from either pipeline's event log, so the message
    needs to say which one, or an on-call engineer has to go find out.
    """
    if event.get("event_type") != "flow_progress":
        return

    details = event.get("details", {})
    flow_progress = details.get("flow_progress", {})
    data_quality = flow_progress.get("data_quality", {})
    expectations = data_quality.get("expectations", [])

    for exp in expectations:
        passed = exp.get("passed_records", 0)
        failed = exp.get("failed_records", 0)
        total = passed + failed
        if total == 0:
            continue

        failure_rate = (failed / total) * 100
        if failure_rate > FAILURE_RATE_ALERT_THRESHOLD:
            send_alert(
                f"[{pipeline_label}] Expectation '{exp.get('name')}' on '{exp.get('dataset')}' is "
                f"failing {failure_rate:.1f}% of rows ({failed} of {total}) — "
                f"above the {FAILURE_RATE_ALERT_THRESHOLD}% threshold."
            )
