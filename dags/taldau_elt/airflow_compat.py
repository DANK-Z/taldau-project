"""Continuation operator for the supported Apache Airflow 2.9.2 runtime."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowSkipException, DagRunAlreadyExists
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SkipExistingDagRunOperator(TriggerDagRunOperator):
    """Skip a retried deterministic trigger without clearing the existing run."""

    def execute(self, context: Context) -> Any:
        # Guard before the parent can enter its clear/reset path.
        if self.reset_dag_run:
            raise ValueError('Snapshot continuation must not reset an existing DAG run')
        try:
            return super().execute(context)
        except DagRunAlreadyExists as exc:
            raise AirflowSkipException(
                f'Continuation run {self.trigger_run_id} already exists in {self.trigger_dag_id}'
            ) from exc
