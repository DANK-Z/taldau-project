"""CLI contract checks with no database or network access."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('snapshot_cli',ROOT/'tools/manage_investments_snapshot.py')
cli=importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


class PublishCliTests(unittest.TestCase):
    def test_publish_keeps_arguments_and_json_result(self):
        for args in (['--snapshot-id','fixture','publish'],['publish','--snapshot-id','fixture']):
            with self.subTest(args=args):
                conn=MagicMock()
                output=io.StringIO()
                with patch.object(sys,'argv',['manage_investments_snapshot.py']+args), \
                     patch.object(cli,'db_connection',return_value=conn), \
                     patch.object(cli,'publish_snapshot',return_value=23) as publish, \
                     patch.object(cli,'launch') as launch, contextlib.redirect_stdout(output):
                    cli.main()
                publish.assert_called_once_with(conn,'fixture')
                launch.assert_not_called()
                conn.close.assert_called_once()
                self.assertEqual(json.loads(output.getvalue()),{'published_rows':23})

    def test_existing_migration_command_includes_gold_function(self):
        conn=MagicMock()
        output=io.StringIO()
        with patch.object(sys,'argv',['manage_investments_snapshot.py','migrate']), \
             patch.object(cli,'db_connection',return_value=conn), contextlib.redirect_stdout(output):
            cli.main()
        result=json.loads(output.getvalue())
        self.assertEqual(result['migrations'][0],'009_single_taldau_schema.sql')
        self.assertEqual(result['migrations'][-1],'008_multi_year_snapshot.sql')
        self.assertEqual(result['http_requests'],0)
        executed=conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        self.assertEqual(len(executed),len(result['migrations']))
        self.assertIn('CREATE OR REPLACE FUNCTION taldau.gold_publish_inv_snapshot',executed[-2].args[0])
        self.assertIn('year_start',executed[-1].args[0])


if __name__=='__main__': unittest.main()
