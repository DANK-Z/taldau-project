"""Offline DAG/continuation checks; runtime checks require Apache Airflow 2.9.2.

No tasks performing extraction, SQL or publication are executed.
"""
import ast
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
from types import ModuleType
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

    def test_production_dags_use_existing_database_connection(self):
        connection_ids = []
        for name in ('taldau_inv_fixed_assets_2025.py', 'taldau_inv_fixed_assets.py',
                     'taldau_pipeline.py'):
            tree = ast.parse((ROOT / 'dags' / name).read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == 'get_connection' and node.args
                        and isinstance(node.args[0], ast.Constant)):
                    connection_ids.append(node.args[0].value)
        self.assertEqual(len(connection_ids), 7)
        self.assertEqual(set(connection_ids), {'digest_target_db'})

    def test_dags_import_from_nested_deployment_with_only_dag_root_on_path(self):
        class Node:
            def __init__(self, *args, **kwargs):
                pass

            def __rshift__(self, other):
                return other

        def task_decorator(function=None, **kwargs):
            def decorate(callable_):
                def invoke(*args, **kwargs):
                    return Node()
                invoke.expand = lambda **mapped: Node()
                return invoke
            return decorate(function) if function is not None else decorate

        task_decorator.branch = task_decorator

        def dag_decorator(**kwargs):
            def decorate(callable_):
                def invoke(*args, **call_kwargs):
                    callable_(*args, **call_kwargs)
                    return Node()
                return invoke
            return decorate

        stubs = {}
        for name in ('airflow', 'airflow.decorators', 'airflow.models', 'airflow.models.param',
                     'airflow.exceptions', 'airflow.operators', 'airflow.operators.trigger_dagrun'):
            stubs[name] = ModuleType(name)
        stubs['airflow.decorators'].dag = dag_decorator
        stubs['airflow.decorators'].task = task_decorator
        stubs['airflow.models.param'].Param = lambda default, **kwargs: default
        stubs['airflow.exceptions'].AirflowSkipException = type('AirflowSkipException', (Exception,), {})
        stubs['airflow.exceptions'].DagRunAlreadyExists = type('DagRunAlreadyExists', (Exception,), {})
        stubs['airflow.operators.trigger_dagrun'].TriggerDagRunOperator = Node
        pendulum = ModuleType('pendulum')
        pendulum.datetime = lambda *args, **kwargs: object()
        stubs['pendulum'] = pendulum

        dag_names = ('taldau_inv_fixed_assets_2025.py', 'taldau_inv_fixed_assets.py',
                     'taldau_pipeline.py')
        original_path = list(sys.path)
        saved_modules = {name: module for name, module in sys.modules.items()
                         if name == 'taldau_elt' or name.startswith('taldau_elt.')}
        with tempfile.TemporaryDirectory() as temporary:
            airflow_root = Path(temporary)
            nested_dags = airflow_root / 'taldau' / 'dags'
            nested_dags.mkdir(parents=True)
            shutil.copytree(ROOT / 'dags' / 'taldau_elt', nested_dags / 'taldau_elt')
            for name in dag_names:
                shutil.copy2(ROOT / 'dags' / name, nested_dags / name)
            try:
                for name in saved_modules:
                    sys.modules.pop(name, None)
                system_paths = []
                for path in original_path:
                    if not path:
                        continue
                    try:
                        Path(path).resolve().relative_to(ROOT)
                    except ValueError:
                        system_paths.append(path)
                # Keep the Python runtime paths, but expose only the outer Airflow DAG root.
                self.assertNotIn(str(nested_dags.resolve()), system_paths)
                sys.path[:] = [str(airflow_root), *system_paths]
                with patch.dict(sys.modules, stubs):
                    for index, name in enumerate(dag_names):
                        module_name = f'nested_dag_{index}'
                        spec = importlib.util.spec_from_file_location(module_name, nested_dags / name)
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        self.assertEqual(sys.path[0], str(nested_dags.resolve()))
            finally:
                sys.path[:] = original_path
                for name in list(sys.modules):
                    if name == 'taldau_elt' or name.startswith('taldau_elt.'):
                        sys.modules.pop(name, None)
                sys.modules.update(saved_modules)


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
