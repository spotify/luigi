# Copyright (c) 2015
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import luigi
from luigi.file import LocalTarget
from luigi.scheduler import CentralPlannerScheduler
import luigi.server
import luigi.worker
from mock import patch
from helpers import with_config, unittest
import os
import tempfile


class TestExternalFileTask(luigi.ExternalTask):
    """ Mocking tasks is a pain, so touch a file instead """
    path = luigi.Parameter()
    times_to_call = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(TestExternalFileTask, self).__init__(*args, **kwargs)
        self.times_called = 0

    def complete(self):
        """
        Create the file we need after a number of preconfigured attempts
        """
        self.times_called += 1

        if self.times_called >= self.times_to_call:
            open(self.path, 'a').close()

        return os.path.exists(self.path)

    def output(self):
        return LocalTarget(path=self.path)


class TestTask(luigi.Task):
    """
    Requires a single file dependency
    """
    tempdir = luigi.Parameter()
    complete_after = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(TestTask, self).__init__(*args, **kwargs)
        self.output_path = os.path.join(self.tempdir, "test.output")
        self.dep_path = os.path.join(self.tempdir, "test.dep")
        self.dependency = TestExternalFileTask(path=self.dep_path,
                                               times_to_call=self.complete_after)

    def requires(self):
        yield self.dependency

    def output(self):
        return LocalTarget(
            path=self.output_path)

    def run(self):
        open(self.output_path, 'a').close()


class WorkerExternalTaskTest(unittest.TestCase):

    def setUp(self):
        self.scheduler = CentralPlannerScheduler(retry_delay=0.01,
                                                 remove_delay=3,
                                                 worker_disconnect_delay=3,
                                                 disable_persist=3,
                                                 disable_window=5,
                                                 disable_failures=2)

    def _assert_complete(self, tasks):
        for t in tasks:
            self.assert_(t.complete())

    def _build(self, tasks):
        w = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=1)
        for t in tasks:
            w.add(t)
        w.run()
        w.stop()

    def test_external_dependency_already_complete(self):
        """
        Test that the test task completes when its dependency exists at the
        start of the execution.
        """
        tempdir = tempfile.mkdtemp(prefix='luigi-test-')
        test_task = TestTask(tempdir=tempdir, complete_after=1)
        luigi.build([test_task], local_scheduler=True)

        assert os.path.exists(test_task.dep_path)
        assert os.path.exists(test_task.output_path)

        os.unlink(test_task.dep_path)
        os.unlink(test_task.output_path)
        os.rmdir(tempdir)

        # complete() is called once per failure, twice per success
        assert test_task.dependency.times_called == 2

    @with_config({'core': {'retry-external-tasks': 'true',
                           'disable-num-failures': '4',
                           'max-reschedules': '4',
                           'worker-keep-alive': 'true',
                           'retry-delay': '0.01'}})
    def test_external_dependency_completes_later(self):
        """
        Test that an external dependency that is not `complete` when luigi is invoked, but \
        becomes `complete` while the workflow is executing is re-evaluated and
        allows dependencies to run.
        """
        assert luigi.configuration.get_config().getboolean('core',
                                                           'retry-external-tasks',
                                                           False) is True

        original_get_work = self.scheduler.get_work

        def decorated_get_work(*args, **kwargs):
            # need to call `prune()` to make the scheduler run the retry logic
            self.scheduler.prune()
            return original_get_work(*args, **kwargs)

        self.scheduler.get_work = decorated_get_work

        tempdir = tempfile.mkdtemp(prefix='luigi-test-')

        with patch('random.randint', return_value=0.1):
            test_task = TestTask(tempdir=tempdir, complete_after=3)
            self._build([test_task])

        assert os.path.exists(test_task.dep_path)
        assert os.path.exists(test_task.output_path)

        os.unlink(test_task.dep_path)
        os.unlink(test_task.output_path)
        os.rmdir(tempdir)

        # complete() is called once per failure, twice per success
        assert test_task.dependency.times_called == 4


if __name__ == '__main__':
    unittest.main()
