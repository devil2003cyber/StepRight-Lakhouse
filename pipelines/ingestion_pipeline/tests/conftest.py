"""
Shared pytest fixtures for unit tests — ingestion_pipeline.

Separate from transformation_pipeline's conftest.py — pytest fixtures don't
cross pipeline folders, and these are two genuinely separate pipelines with
separate test suites, matching the locked repo structure.

sys.path setup: makes transformations/*_logic.py importable directly by name,
same pattern as transformation_pipeline's conftest.py.
"""

import os
import sys

_TRANSFORMATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "transformations")
sys.path.insert(0, os.path.abspath(_TRANSFORMATIONS_DIR))
