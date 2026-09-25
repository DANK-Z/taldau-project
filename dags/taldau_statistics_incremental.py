"""Airflow entry point; orchestration is shared, historical scope stays frozen."""
from pathlib import Path
import sys

_DAG_DIR = str(Path(__file__).resolve().parent)
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from taldau_elt.orchestration import build_statistics_dag

taldau_statistics_incremental = build_statistics_dag("taldau_statistics_incremental", incremental=True)
