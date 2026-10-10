"""
Real-time DQ alerting — event hook, part of stepright-ingestion-pipeline's own
source code.

Lives in transformations/, not utilities/ — this is a real technical
requirement, not organizational preference. @dp.on_event_hook is a decorator,
and decorators only take effect when the module containing them actually gets
executed. utilities/ files (like helpers.py, dq_alert_logic.py) work fine as
plain importable code, reached via an explicit import from something that IS
pipeline source — but nothing would ever import THIS file that way, since a
hook doesn't get called, it registers itself as a side effect of its own
module loading. Confirmed by our own finding that the Pipeline UI only
attaches transformations/ as pipeline source, matching the docs' own
requirement that event hooks "must be included as part of the pipeline's
source code."

This wrapper is deliberately thin — all the actual logic (threshold check,
alert formatting) lives in utilities/dq_alert_logic.py, shared in shape (not
in import — see that file's own docstring) with the identical hook in
transformation_pipeline. Registration is per-pipeline and cannot be shared;
the logic underneath doesn't have to be duplicated in spirit, even though the
file itself is duplicated on disk.

Verified constraints, not assumed:
- The hook function takes exactly one argument — a dict representing the event.
  Any return value is ignored.
- Event hooks are NOT part of the pipeline graph — they don't show up as a
  dataset, and they run asynchronously from the actual pipeline update.
- Only triggered for events at maturity_level STABLE. flow_progress (the same
  event type the dashboard queries) is what we key off here — if a future
  Databricks release changes that event's maturity level, this hook simply
  stops firing; it fails silent, not loud, which is a real limitation worth
  knowing, not hidden.
- max_allowable_consecutive_failures disables a hook after N consecutive
  exceptions, so one broken hook can't block others indefinitely — set to a
  real number here, not None, since a leaking Slack webhook token or a network
  blip is exactly the failure mode this guards against.
"""

from pyspark import pipelines as dp

from utilities.dq_alert_logic import check_expectations_and_alert


@dp.on_event_hook(max_allowable_consecutive_failures=3)
def dq_expectation_alert_hook(event: dict) -> None:
    check_expectations_and_alert(event, pipeline_label="ingestion")
