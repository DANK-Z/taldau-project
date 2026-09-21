"""Offline DAG/continuation checks; runtime checks require Apache Airflow 2.9.2.

No tasks performing extraction, SQL or publication are executed.
"""
import ast
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dags'))
HAS_AIRFLOW = importlib.util.find_spec('airflow') is not None


class AirflowImportTests(unittest.TestCase):
    def test_no_airflow_3_imports_or_unsupported_trigger_option(self):
        for path in (ROOT / 'dags').rglob('*.py'):
            tree = ast.parse(path.read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    modules = [node.module or '']
                elif isinstance(node, ast.Import):
                    modules = [alias.name for alias in node.names]
                else:
                    modules = []
                for module in modules:
                    self.assertFalse(module.startswith(('airflow.sdk', 'airflow.providers.standard')),
                                     f'{path}:{node.lineno}: {module}')
                if isinstance(node, ast.keyword):
                    self.assertNotEqual(node.arg, 'skip_when_already_exists', str(path))


@unittest.skipUnless(HAS_AIRFLOW, 'Requires the Airflow 2.9.2 runtime; not a mocked DAG parser')
class AirflowRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import airflow
        if airflow.__version__ != '2.9.2':
            raise RuntimeError('Run compatibility checks with Apache Airflow 2.9.2')
        # Fail closed if parsing or these mocked operator checks attempt external I/O.
        for target in ('requests.sessions.Session.request', 'psycopg2.connect',
                       'socket.socket.connect', 'socket.create_connection'):
            guard = patch(target, side_effect=AssertionError('External I/O is forbidden in DAG checks'))
            guard.start()
            cls.addClassCleanup(guard.stop)
        from airflow.models import DagBag
        cls.bag = DagBag(dag_folder=str(ROOT / 'dags'), include_examples=False)
        if cls.bag.import_errors:
            raise AssertionError(cls.bag.import_errors)

    def operator(self):
        from taldau_elt.airflow_compat import SkipExistingDagRunOperator
        return SkipExistingDagRunOperator(
            task_id='continue_snapshot', trigger_dag_id='taldau_inv_fixed_assets_2025',
            trigger_run_id='fixture__wave__deterministic', reset_dag_run=False,
            wait_for_completion=False, conf={'snapshot_id': 'fixture', 'allow_extraction': True})

    def test_retry_skips_existing_run_without_clear_or_second_run(self):
        from airflow.exceptions import AirflowSkipException, DagRunAlreadyExists
        from airflow.utils import timezone
        operator = self.operator()
        run = Mock(run_id=operator.trigger_run_id, logical_date=timezone.utcnow())
        context = {'task_instance': Mock()}
        with patch('airflow.operators.trigger_dagrun.trigger_dag',
                   side_effect=[run, DagRunAlreadyExists(dag_run=run)]) as trigger, \
             patch('airflow.operators.trigger_dagrun.DagModel.get_current') as get_model, \
             patch('airflow.operators.trigger_dagrun.DagBag') as get_bag:
            operator.execute(context)
            with self.assertRaises(AirflowSkipException):
                operator.execute(context)
        self.assertEqual(trigger.call_count, 2)
        self.assertEqual([call.kwargs['run_id'] for call in trigger.call_args_list],
                         [operator.trigger_run_id] * 2)
        get_model.assert_not_called()
        get_bag.assert_not_called()
        run.clear.assert_not_called()

    def test_other_errors_propagate(self):
        with patch('airflow.operators.trigger_dagrun.trigger_dag', side_effect=RuntimeError('fixture failure')):
            with self.assertRaisesRegex(RuntimeError, 'fixture failure'):
                self.operator().execute({'task_instance': Mock()})

    def test_reset_is_rejected_before_trigger(self):
        operator = self.operator()
        operator.reset_dag_run = True
        with patch('airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute') as execute:
            with self.assertRaisesRegex(ValueError, 'must not reset'):
                operator.execute({})
        execute.assert_not_called()

    def test_dag_ids_graph_mapping_and_concurrency(self):
        from airflow.models.mappedoperator import MappedOperator
        from taldau_elt.airflow_compat import SkipExistingDagRunOperator
        self.assertEqual(set(self.bag.dags), {
            'taldau_pipeline', 'taldau_inv_fixed_assets', 'taldau_inv_fixed_assets_2025'})
        dag = self.bag.dags['taldau_inv_fixed_assets_2025']
        self.assertEqual(set(dag.task_ids), {'authorize_snapshot', 'discover', 'plan_wave',
            'load_chunk', 'route_wave', 'continue_snapshot', 'validate_snapshot', 'diagnostics'})
        for upstream, downstream in [('authorize_snapshot', 'discover'), ('discover', 'plan_wave'),
                ('plan_wave', 'load_chunk'), ('load_chunk', 'route_wave'),
                ('route_wave', 'continue_snapshot'), ('route_wave', 'validate_snapshot'),
                ('validate_snapshot', 'diagnostics')]:
            self.assertIn(downstream, dag.get_task(upstream).downstream_task_ids)
        self.assertEqual({task.task_id for task in dag.leaves}, {'continue_snapshot', 'diagnostics'})
        mapped = dag.get_task('load_chunk')
        self.assertIsInstance(mapped, MappedOperator)
        self.assertEqual(mapped.max_active_tis_per_dag, 3)
        self.assertFalse(mapped.do_xcom_push)
        for task_id in ('discover', 'load_chunk'):
            task = dag.get_task(task_id)
            self.assertEqual((task.pool, task.pool_slots), ('taldau_api', 1))
        self.assertEqual(dag.max_active_runs, 1)
        self.assertFalse(dag.params['allow_extraction'])
        continuation = dag.get_task('continue_snapshot')
        self.assertIsInstance(continuation, SkipExistingDagRunOperator)
        self.assertFalse(continuation.reset_dag_run)
        self.assertFalse(continuation.wait_for_completion)
        self.assertIn('next_run_id', continuation.trigger_run_id)

    def test_branch_keeps_deterministic_continuation_id(self):
        dag = self.bag.dags['taldau_inv_fixed_assets_2025']
        route = dag.get_task('route_wave')
        module = sys.modules[route.python_callable.__module__]
        context = {'run_id': 'fixture-parent-run', 'ti': Mock()}
        with patch.object(module, 'connection', return_value=Mock()), \
             patch('taldau_elt.snapshots.wave_outcome', return_value='continue') as outcome, \
             patch('airflow.operators.python.get_current_context', return_value=context):
            self.assertEqual(route.python_callable('fixture', [1]), 'continue_snapshot')
            self.assertEqual(route.python_callable('fixture', [1]), 'continue_snapshot')
            calls = context['ti'].xcom_push.call_args_list
            self.assertEqual(calls[0], calls[1])
            outcome.return_value = 'validate'
            self.assertEqual(route.python_callable('fixture', [1]), 'validate_snapshot')


if __name__ == '__main__':
    unittest.main()
