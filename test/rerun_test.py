# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pickle
import shlex

from helpers import LuigiTestCase, with_config

import luigi
from luigi.cmdline_parser import CmdlineParser
from luigi.scheduler import Scheduler
from luigi.worker import Worker


class RerunTest(LuigiTestCase):
    def assert_round_trip(self, task):
        command = task._get_cmdline_params()
        self.assertIsNotNone(command)
        with CmdlineParser.global_instance([task.task_family, *shlex.split(command)]) as parser:
            restored = parser.get_task_obj()
        self.assertEqual(restored.param_kwargs, task.param_kwargs)
        self.assertEqual(restored.task_id, task.task_id)
        return command

    def test_implicit_bool(self):
        class T(luigi.Task):
            rerun = luigi.BoolParameter()

        self.assertEqual(self.assert_round_trip(T(rerun=False)), "")
        self.assertEqual(self.assert_round_trip(T(rerun=True)), "--rerun")
        self.assertEqual(T(rerun=False).to_str_params(), {"rerun": "False"})

    def test_explicit_bool(self):
        class T(luigi.Task):
            rerun = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

        for value in (True, False):
            with self.subTest(value=value):
                self.assertEqual(self.assert_round_trip(T(rerun=value)), "--rerun=" + str(value).lower())

    def test_optional_and_empty_string(self):
        class T(luigi.Task):
            absent = luigi.OptionalParameter()
            count = luigi.OptionalIntParameter()
            empty = luigi.Parameter(default="")
            text = luigi.Parameter()

        for text in ("False", "null", "two words", 'quoted "value"', "it's $HOME `literal`", "-foo", "--other"):
            with self.subTest(text=text):
                command = self.assert_round_trip(T(text=text))
                self.assertNotIn("--absent", command)
                self.assertNotIn("--count", command)
                self.assertIn("--empty=", command)
        self.assert_round_trip(T(text="value", absent="present", count=0))

    def test_optional_explicit_bool(self):
        class T(luigi.Task):
            enabled = luigi.OptionalBoolParameter(parsing=luigi.BoolParameter.EXPLICIT_PARSING)

        for value in (None, False, True):
            with self.subTest(value=value):
                self.assert_round_trip(T(enabled=value))

    def test_private_parameter_not_in_command(self):
        class T(luigi.Task):
            secret = luigi.Parameter(default="do-not-show", visibility=luigi.parameter.ParameterVisibility.PRIVATE)
            enabled = luigi.BoolParameter()

        self.assertEqual(T(enabled=True)._get_cmdline_params(), "--enabled")

    def test_unrepresentable_implicit_false(self):
        class T(luigi.Task):
            enabled = luigi.BoolParameter(default=True)

        self.assertIsNone(T(enabled=False)._get_cmdline_params())
        self.assert_round_trip(T(enabled=True))

    @with_config({"T": {"enabled": "true"}})
    def test_unrepresentable_configured_implicit_false(self):
        class T(luigi.Task):
            enabled = luigi.BoolParameter()

        self.assertIsNone(T(enabled=False)._get_cmdline_params())

    def test_unrepresentable_optional_none(self):
        class T(luigi.Task):
            value = luigi.OptionalParameter(default="configured")

        self.assertIsNone(T(value=None)._get_cmdline_params())

    @with_config({"T": {"enabled": "not-a-bool"}})
    def test_explicit_value_overrides_invalid_config(self):
        class T(luigi.Task):
            enabled = luigi.BoolParameter()

            def complete(self):
                return False

        task = T(enabled=False)
        scheduler = Scheduler()
        with Worker(scheduler=scheduler, no_install_shutdown_handler=True) as worker:
            self.assertTrue(worker.add(task))
        self.assertIsNone(scheduler.fetch_error(task.task_id)["taskCmdline"])

    @with_config({"T": {"count": "not-an-int"}})
    def test_optional_none_overrides_invalid_config(self):
        class T(luigi.Task):
            count = luigi.OptionalIntParameter()

        self.assertIsNone(T(count=None)._get_cmdline_params())

    def test_worker_scheduler_command(self):
        class T(luigi.Task):
            enabled = luigi.BoolParameter()
            absent = luigi.OptionalParameter()

            def complete(self):
                return False

        scheduler = Scheduler(prune_on_get_work=False)
        task = T(enabled=True)
        with Worker(scheduler=scheduler, no_install_shutdown_handler=True) as worker:
            worker.add(task)
        # JSON serialization and persisted scheduler state both preserve the command.
        scheduler._state._tasks = pickle.loads(pickle.dumps(scheduler._state._tasks))
        response = json.loads(json.dumps(scheduler.fetch_error(task.task_id)))
        self.assertEqual(response["taskCmdline"], "--enabled")
        self.assertEqual(response["taskParams"], {"enabled": "True", "absent": ""})
        scheduler.add_task(worker="older-worker", task_id=task.task_id, status="FAILED")
        self.assertEqual(scheduler.fetch_error(task.task_id)["taskCmdline"], "--enabled")

    def test_scheduler_legacy_and_unavailable(self):
        scheduler = Scheduler()
        scheduler.add_task(worker="worker", task_id="legacy", params={"flag": "False"})
        self.assertNotIn("taskCmdline", scheduler.fetch_error("legacy"))
        scheduler.add_task(worker="worker", task_id="unavailable", params={"flag": "False"}, cmdline_params=None)
        self.assertIsNone(scheduler.fetch_error("unavailable")["taskCmdline"])
        scheduler.add_task(worker="worker", task_id="empty", params={"flag": "False"}, cmdline_params="")
        self.assertEqual(scheduler.fetch_error("empty")["taskCmdline"], "")
