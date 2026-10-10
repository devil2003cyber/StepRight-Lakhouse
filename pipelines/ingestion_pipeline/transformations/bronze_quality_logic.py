"""
Bronze quality — pure logic, no pipeline dependency.

Deliberately has NO `from pyspark import pipelines` import anywhere in this
file — same verified reason as every Gold logic file: a decorator fires at
import time, so this function can't share a file with anything decorated with
@dp.table, or importing it in a plain test process would crash before any
test ever ran. See gold_revenue_logic.py's docstring for the full reasoning,
and L17 for how this was found and fixed for Bronze specifically.
"""


def quarantine_rule(rules: dict) -> str:
    """
    True if ANY rule failed — the row belongs in quarantine.

    An empty rules dict is a real edge case, not a hypothetical one — it would
    previously have produced "NOT()", which is invalid SQL, not "nothing
    fails," the way an empty dict might look like it should behave. Guarded
    explicitly rather than left to fail confusingly deep inside a pipeline run.
    """
    if not rules:
        raise ValueError("quarantine_rule() requires at least one rule — an empty dict would produce invalid SQL (\"NOT()\").")
    return "NOT({0})".format(" AND ".join(rules.values()))
