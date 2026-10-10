"""
Unit tests — Bronze (L17): quarantine_rule(), the one real case in Bronze.
See L15 for why Bronze gets exactly one test and Silver gets none today.

No SparkSession needed for this one, unlike every Gold test — quarantine_rule()
is plain Python string building, not a DataFrame operation. Worth noticing:
not every unit test needs Spark at all, just because the project is built on it.
"""

import pytest

from bronze_quality_logic import quarantine_rule


def test_quarantine_rule_combines_multiple_rules():
    rules = {
        "valid_order_id": "after.order_id IS NOT NULL",
        "valid_total_amount": "after.total_amount IS NULL OR after.total_amount >= 0",
    }

    result = quarantine_rule(rules)

    assert result == "NOT(after.order_id IS NOT NULL AND after.total_amount IS NULL OR after.total_amount >= 0)"


def test_quarantine_rule_rejects_empty_dict():
    # The real edge case: an empty rules dict used to silently produce
    # "NOT()" — invalid SQL, not "nothing fails." Fixed to raise clearly and
    # immediately instead of failing confusingly, deep inside a pipeline run.
    with pytest.raises(ValueError, match="requires at least one rule"):
        quarantine_rule({})





        
