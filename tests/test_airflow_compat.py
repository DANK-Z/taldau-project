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
        for name in ('taldau_elt/orchestration.py',):
            tree = ast.parse((ROOT / 'dags' / name).read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == 'get_connection' and node.args
                        and isinstance(node.args[0], ast.Constant)):
                    connection_ids.append(node.args[0].value)
        self.assertEqual(len(connection_ids), 1)
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
                     'airflow.exceptions', 'airflow.operators', 'airflow.operators.trigger_dagrun',
                     'airflow.timetables', 'airflow.timetables.trigger'):
            stubs[name] = ModuleType(name)
        stubs['airflow.timetables.trigger'].CronTriggerTimetable = Node
        stubs['airflow.decorators'].dag = dag_decorator
        stubs['airflow.decorators'].task = task_decorator
        stubs['airflow.models.param'].Param = lambda default, **kwargs: default
        stubs['airflow.exceptions'].AirflowSkipException = type('AirflowSkipException', (Exception,), {})
        stubs['airflow.exceptions'].DagRunAlreadyExists = type('DagRunAlreadyExists', (Exception,), {})
        stubs['airflow.operators.trigger_dagrun'].TriggerDagRunOperator = Node
        pendulum = ModuleType('pendulum')
        pendulum.datetime = lambda *args, **kwargs: object()
        stubs['pendulum'] = pendulum

        dag_names = ('taldau_statistics_incremental.py', 'taldau_statistics_2023_2026.py')
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

    def test_full_dag_directory_scan_ignores_package_but_entry_points_import_it(self):
        import os
        from airflow.configuration import conf
        from airflow.models import DagBag
        from airflow.utils.file import list_py_file_paths

        dag_root = ROOT / 'dags'
        expected_files = {dag_root / 'taldau_statistics_incremental.py',
                          dag_root / 'taldau_statistics_2023_2026.py'}
        with patch.dict(os.environ, {'AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX': 'regexp'}):
            self.assertEqual(conf.get('core', 'dag_ignore_file_syntax'), 'regexp')
            # Disable safe-mode heuristics: .airflowignore must exclude the package
            # itself, not merely rely on its modules containing no DAG-like text.
            candidates = {Path(path) for path in list_py_file_paths(str(dag_root), safe_mode=False)}
            self.assertEqual(candidates, expected_files)
            bag = DagBag(dag_folder=str(dag_root), include_examples=False, safe_mode=False)

        self.assertEqual(bag.import_errors, {})
        self.assertEqual(set(bag.dags), {'taldau_statistics_incremental', 'taldau_statistics_2023_2026'})
        self.assertEqual({Path(path) for path in bag.file_last_changed}, expected_files)
        for name in ('statistics', 'orchestration'):
            module = importlib.import_module('taldau_elt.' + name)
            self.assertEqual(Path(module.__file__).resolve(), (dag_root / 'taldau_elt' / (name + '.py')).resolve())
            self.assertNotIn(Path(module.__file__), candidates)
        self.assertIsNone(bag.dags['taldau_statistics_2023_2026'].schedule_interval)
        for dag_id, dag in bag.dags.items():
            original = self.bag.dags[dag_id]
            self.assertEqual(dag.task_ids, original.task_ids)
            self.assertEqual(dict(dag.params), dict(original.params))
            self.assertEqual(dag.timetable.serialize(), original.timetable.serialize())

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
                   side_effect=[run, DagRunAlreadyExists(dag_run=run, execution_date=run.logical_date,
                                                       run_id=run.run_id)]) as trigger, \
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

    def test_incremental_graph_schedule_and_legacy_removal(self):
        from airflow.models.mappedoperator import MappedOperator
        from airflow.timetables.trigger import CronTriggerTimetable
        self.assertEqual(set(self.bag.dags), {
            'taldau_statistics_2023_2026', 'taldau_statistics_incremental'})
        dag = self.bag.dags['taldau_statistics_incremental']
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertEqual(str(dag.timezone), 'Asia/Almaty')
        self.assertIsInstance(dag.timetable, CronTriggerTimetable)
        self.assertEqual(dag.timetable.serialize()['expression'], '0 7 20 * *')
        self.assertFalse(dag.params['auto_publish'])
        self.assertIsNone(dag.params['year_start'])
        self.assertIsNone(dag.params['year_end'])
        for task_id in ('discover', 'load_chunk'):
            mapped = dag.get_task(task_id)
            self.assertIsInstance(mapped, MappedOperator)
            self.assertEqual((mapped.pool, mapped.pool_slots, mapped.max_active_tis_per_dag),
                             ('taldau_api', 1, 3))
        self.assertIn('publish_batch', dag.get_task('diagnostics').downstream_task_ids)
        self.assertIn('final_batch_summary', dag.get_task('publish_batch').downstream_task_ids)
        self.assertEqual(dag.get_task('final_batch_summary').trigger_rule, 'all_success')
        continuation = dag.get_task('continue_batch')
        self.assertEqual(continuation.trigger_dag_id, dag.dag_id)
        self.assertIn('authorize_batch', continuation.conf['batch_id'])
        self.assertEqual(continuation.conf['resume_failed'], False)
        self.assertIn('auto_publish', continuation.conf)
        self.assertIn('year_start', continuation.conf)

    def test_historical_scope_remains_explicit(self):
        dag = self.bag.dags['taldau_statistics_2023_2026']
        authorize = dag.get_task('authorize_batch').python_callable
        module = sys.modules[authorize.__module__]
        with patch.object(module, 'connection', return_value=Mock()), \
             patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'history'}) as create, \
             patch('airflow.operators.python.get_current_context', return_value={
                 'params': {'batch_id': 'history', 'allow_extraction': True}}):
            self.assertEqual(authorize(), 'history')
        self.assertEqual(create.call_args.kwargs['year_start'], 2023)
        self.assertEqual(create.call_args.kwargs['year_end'], 2026)
        self.assertNotIn('publish_batch', dag.task_ids)

    def test_diagnostics_full_report_once_and_final_metadata_only(self):
        import os
        import json
        report = {'batch_id': 'fixture', 'state': 'validated', 'indicators_total': 8,
                  'indicators_validated': 7, 'indicators_no_new_periods': 1, 'indicators_failed': 0}
        for dag_id in ('taldau_statistics_incremental', 'taldau_statistics_2023_2026'):
            dag = self.bag.dags[dag_id]
            diagnostics = dag.get_task('diagnostics').python_callable
            final = dag.get_task('final_batch_summary').python_callable
            module = sys.modules[final.__module__]
            with tempfile.TemporaryDirectory() as directory, \
                 patch.dict(os.environ, {'TALDAU_REPORT_DIR': directory}), \
                 patch.object(module, 'connection', return_value=Mock()), \
                 patch('taldau_elt.statistics.batch_summary', return_value=report) as full, \
                 patch('taldau_elt.statistics.batch_state_summary', return_value=report) as light, \
                 patch('taldau_elt.statistics.finish_incremental_batch') as finish:
                diagnostics('fixture')
                full.assert_called_once()
                self.assertEqual(json.loads(next(Path(directory).glob('*.json')).read_text()), report)
                finish.assert_not_called()
                full.side_effect = AssertionError('Full report forbidden in final task')
                final('fixture')
                light.assert_called_once()
                self.assertEqual(finish.call_count, int(dag_id.endswith('incremental')))
                # An inconsistent batch state cannot release ownership, for either DAG.
                finish.reset_mock()
                light.return_value = {**report, 'indicators_validated': 6, 'indicators_failed': 1}
                with self.assertRaisesRegex(ValueError, 'incomplete or failed'):
                    final('fixture')
                finish.assert_not_called()
                light.side_effect = RuntimeError('metadata unavailable')
                with self.assertRaisesRegex(RuntimeError, 'metadata unavailable'):
                    final('fixture')
                finish.assert_not_called()

    def test_incremental_authorization_freezes_year_and_continuation_conf(self):
        import pendulum
        from airflow.utils.types import DagRunType
        from unittest.mock import MagicMock
        dag = self.bag.dags['taldau_statistics_incremental']
        authorize = dag.get_task('authorize_batch').python_callable
        module = sys.modules[authorize.__module__]
        params = dict(dag.params)
        context = {'params': params, 'logical_date': pendulum.datetime(2027, 1, 20, 7, tz='Asia/Almaty'),
                   'run_id': 'scheduled__2027-01-20T02:00:00+00:00', 'ti': Mock(),
                   'dag_run': Mock(conf={}, run_type=DagRunType.SCHEDULED)}
        with patch.object(module, 'connection', return_value=MagicMock()), \
             patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'generated'}) as create, \
             patch('airflow.operators.python.get_current_context', return_value=context):
            self.assertEqual(authorize(), 'generated')
        self.assertEqual((create.call_args.kwargs['year_start'], create.call_args.kwargs['year_end']), (2027, 2027))
        self.assertEqual(create.call_args.kwargs['source_as_of'], '2027-01-20')
        self.assertEqual(create.call_args.kwargs['orchestration_key'], dag.dag_id)
        self.assertTrue(context['ti'].xcom_push.call_args.kwargs['value']['effective_auto_publish'])

        context['ti'].xcom_pull.side_effect = lambda task_ids, key=None: (
            {'year_start': 2027, 'year_end': 2027, 'effective_auto_publish': True} if key == 'scope' else 'generated')
        import copy
        continuation = copy.deepcopy(dag.get_task('continue_batch'))
        continuation.render_template_fields(context)
        self.assertEqual(continuation.conf, {'batch_id': 'generated', 'allow_extraction': True,
            'resume_failed': False, 'year_start': 2027, 'year_end': 2027,
            'auto_publish': True, 'effective_auto_publish': True})

    def test_incremental_resume_keeps_stored_scope_after_year_rollover(self):
        import pendulum
        from airflow.utils.types import DagRunType
        from unittest.mock import MagicMock
        dag = self.bag.dags['taldau_statistics_incremental']
        authorize = dag.get_task('authorize_batch').python_callable
        module = sys.modules[authorize.__module__]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (2027, 2027)
        context = {'params': {**dict(dag.params), 'batch_id': 'existing', 'resume_failed': True},
                   'logical_date': pendulum.datetime(2028, 1, 20, tz='Asia/Almaty'),
                   'run_id': 'manual__resume', 'ti': Mock(),
                   'dag_run': Mock(conf={}, run_type=DagRunType.MANUAL)}
        with patch.object(module, 'connection', return_value=conn), \
             patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'existing'}) as create, \
             patch('airflow.operators.python.get_current_context', return_value=context):
            authorize()
        self.assertEqual((create.call_args.kwargs['year_start'], create.call_args.kwargs['year_end']), (2027, 2027))
        self.assertTrue(create.call_args.kwargs['resume_failed'])

    def test_publication_policy_uses_run_type_and_explicit_conf_not_ui_defaults(self):
        from airflow.utils.types import DagRunType
        from taldau_elt.orchestration import effective_auto_publish
        cases = [(DagRunType.SCHEDULED, {}, True),
                 (DagRunType.MANUAL, {}, False),
                 (DagRunType.MANUAL, {'auto_publish': True}, True),
                 (DagRunType.MANUAL, {'auto_publish': False}, False),
                 (DagRunType.SCHEDULED, {'auto_publish': False}, False),
                 (DagRunType.BACKFILL_JOB, {}, False)]
        for kind, conf, expected in cases:
            # Airflow's ORM can return the enum's persisted string value.
            for persisted in (kind, kind.value):
                with self.subTest(kind=persisted, conf=conf):
                    context = {'dag_run': Mock(run_type=persisted, conf=conf),
                               'params': {'auto_publish': not expected}, 'ti': Mock()}
                    context['ti'].xcom_pull.return_value = None
                    self.assertIs(effective_auto_publish(context), expected)

    def test_continuation_inherits_decision_through_authorization_publish_and_retry(self):
        import copy
        import pendulum
        from airflow.utils.types import DagRunType
        from unittest.mock import MagicMock
        dag = self.bag.dags['taldau_statistics_incremental']
        authorize = dag.get_task('authorize_batch').python_callable
        publish = dag.get_task('publish_batch').python_callable
        module = sys.modules[authorize.__module__]
        for expected in (True, False):
            scope = {'year_start': 2027, 'year_end': 2027, 'effective_auto_publish': expected}
            parent_ti = Mock()
            parent_ti.xcom_pull.side_effect = lambda task_ids, key=None: scope if key == 'scope' else 'original'
            continuation = copy.deepcopy(dag.get_task('continue_batch'))
            continuation.render_template_fields({'ti': parent_ti, 'params': {'auto_publish': not expected}})
            conf = continuation.conf
            self.assertIs(conf['effective_auto_publish'], expected)
            self.assertIs(conf['auto_publish'], expected)
            child_ti = Mock()
            child_ti.xcom_pull.return_value = None
            context = {'dag_run': Mock(run_type=DagRunType.MANUAL, conf=conf),
                       'params': {**dict(dag.params), **conf, 'auto_publish': not expected},
                       'logical_date': pendulum.datetime(2027, 1, 20, tz='Asia/Almaty'),
                       'run_id': 'original__wave__fixture', 'ti': child_ti}
            conn = MagicMock()
            conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (2027, 2027)
            with patch.object(module, 'connection', return_value=conn), \
                 patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'original'}), \
                 patch('airflow.operators.python.get_current_context', return_value=context), \
                 patch('taldau_elt.statistics.publish_batch') as publish_sql:
                # Retry may clear XCom. The durable continuation conf retains the decision.
                for _ in range(2):
                    child_ti.xcom_pull.return_value = None
                    authorize()
                    self.assertEqual(child_ti.xcom_push.call_args.kwargs['value'], scope)
                child_ti.xcom_pull.return_value = scope
                context['dag_run'].run_type = DagRunType.SCHEDULED
                context['dag_run'].conf = {'auto_publish': not expected}
                authorize()  # A retained authorized scope also wins over re-evaluation.
                self.assertEqual(child_ti.xcom_push.call_args.kwargs['value'], scope)
                publish('original')
                publish('original')
                self.assertEqual(publish_sql.call_count, 2)
                publish_sql.assert_called_with(conn, 'original', auto_publish=expected)
            # Every following wave carries the authorized boolean again.
            child_ti.xcom_pull.side_effect = lambda task_ids, key=None: scope if key == 'scope' else 'original'
            following = copy.deepcopy(dag.get_task('continue_batch'))
            following.render_template_fields(context)
            self.assertIs(following.conf['effective_auto_publish'], expected)

    def test_initial_authorization_retries_reproduce_same_decision_after_xcom_clear(self):
        import pendulum
        from airflow.utils.types import DagRunType
        from unittest.mock import MagicMock
        dag = self.bag.dags['taldau_statistics_incremental']
        authorize = dag.get_task('authorize_batch').python_callable
        module = sys.modules[authorize.__module__]
        for kind, conf, expected in ((DagRunType.SCHEDULED, {}, True), (DagRunType.MANUAL, {}, False),
                                     (DagRunType.MANUAL, {'auto_publish': True}, True),
                                     (DagRunType.MANUAL, {'auto_publish': False}, False)):
            ti = Mock()
            ti.xcom_pull.return_value = None
            context = {'dag_run': Mock(run_type=kind, conf=conf), 'params': {**dict(dag.params), **conf},
                       'logical_date': pendulum.datetime(2027, 1, 20, tz='Asia/Almaty'),
                       'run_id': 'initial', 'ti': ti}
            with patch.object(module, 'connection', return_value=MagicMock()), \
                 patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'original'}), \
                 patch('airflow.operators.python.get_current_context', return_value=context):
                for _ in range(2):
                    authorize()
                    self.assertIs(ti.xcom_push.call_args.kwargs['value']['effective_auto_publish'], expected)

    def test_invalid_decision_or_missing_scope_blocks_publication_before_connection(self):
        from airflow.utils.types import DagRunType
        from taldau_elt.orchestration import effective_auto_publish
        for conf in ({'auto_publish': 'false'}, {'auto_publish': 1},
                     {'effective_auto_publish': True},
                     {'batch_id': 'original', 'effective_auto_publish': True, 'auto_publish': False}):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                effective_auto_publish({'ti': Mock(), 'dag_run': Mock(conf=conf, run_type=DagRunType.MANUAL)})
        publish = self.bag.dags['taldau_statistics_incremental'].get_task('publish_batch').python_callable
        module = sys.modules[publish.__module__]
        for scope in (None, {}, {'effective_auto_publish': 'true'}):
            ti = Mock()
            ti.xcom_pull.return_value = scope
            with patch.object(module, 'connection') as connect, \
                 patch('airflow.operators.python.get_current_context', return_value={'ti': ti}), \
                 self.assertRaisesRegex(ValueError, 'Missing authorized'):
                publish('original')
            connect.assert_not_called()

    def test_historical_never_resolves_scheduled_publication(self):
        from airflow.utils.types import DagRunType
        dag = self.bag.dags['taldau_statistics_2023_2026']
        authorize = dag.get_task('authorize_batch').python_callable
        module = sys.modules[authorize.__module__]
        for kind in (DagRunType.SCHEDULED, DagRunType.MANUAL):
            ti = Mock()
            context = {'params': {'batch_id': 'history', 'allow_extraction': True, 'auto_publish': True},
                       'dag_run': Mock(run_type=kind, conf={'auto_publish': True}), 'ti': ti}
            with patch.object(module, 'connection', return_value=Mock()), \
                 patch.object(module, 'effective_auto_publish', side_effect=AssertionError('Historical publication forbidden')), \
                 patch('taldau_elt.statistics.create_batch', return_value={'batch_id': 'history'}), \
                 patch('airflow.operators.python.get_current_context', return_value=context):
                authorize()
            ti.xcom_push.assert_not_called()
            self.assertNotIn('publish_batch', dag.task_ids)
            self.assertNotIn('effective_auto_publish', dag.get_task('continue_batch').conf)

    def test_january_timetable_logical_date_is_january(self):
        import pendulum
        from airflow.timetables.base import TimeRestriction
        dag = self.bag.dags['taldau_statistics_incremental']
        result = dag.timetable.next_dagrun_info(last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=pendulum.datetime(2027, 1, 1, tz='Asia/Almaty'),
                                        latest=None, catchup=True))
        local = result.logical_date.in_timezone('Asia/Almaty')
        self.assertEqual((local.year, local.month, local.day, local.hour), (2027, 1, 20, 7))

    def test_generic_dag_mapping_pool_stop_and_concurrency(self):
        from airflow.models.mappedoperator import MappedOperator
        from taldau_elt.airflow_compat import SkipExistingDagRunOperator
        dag = self.bag.dags['taldau_statistics_2023_2026']
        self.assertEqual(set(dag.task_ids), {'authorize_batch','list_snapshots','discover','plan_wave',
            'load_chunk','route_wave','continue_batch','validate_batch','diagnostics','final_batch_summary'})
        self.assertEqual(dag.max_active_runs,1)
        self.assertFalse(dag.params['allow_extraction'])
        for task_id in ('discover','load_chunk'):
            mapped=dag.get_task(task_id)
            self.assertIsInstance(mapped,MappedOperator)
            self.assertEqual((mapped.pool,mapped.pool_slots,mapped.max_active_tis_per_dag),('taldau_api',1,3))
        continuation=dag.get_task('continue_batch')
        self.assertIsInstance(continuation,SkipExistingDagRunOperator)
        self.assertFalse(continuation.reset_dag_run)
        self.assertEqual({task.task_id for task in dag.leaves},{'continue_batch','final_batch_summary'})

    def test_branch_keeps_deterministic_continuation_id(self):
        dag = self.bag.dags['taldau_statistics_incremental']
        route = dag.get_task('route_wave')
        module = sys.modules[route.python_callable.__module__]
        context = {'run_id': 'fixture-parent-run', 'ti': Mock()}
        with patch.object(module, 'connection', return_value=Mock()), \
             patch('taldau_elt.statistics.batch_wave_outcome', return_value='continue') as outcome, \
             patch('airflow.operators.python.get_current_context', return_value=context):
            self.assertEqual(route.python_callable('fixture', [1]), 'continue_batch')
            self.assertEqual(route.python_callable('fixture', [1]), 'continue_batch')
            calls = context['ti'].xcom_push.call_args_list
            self.assertEqual(calls[0], calls[1])
            outcome.return_value = 'validate'
            self.assertEqual(route.python_callable('fixture', [1]), 'validate_batch')


if __name__ == '__main__':
    unittest.main()
