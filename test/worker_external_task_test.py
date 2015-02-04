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
import unittest
from mock import Mock, patch
from helpers import with_config


mock_external_task = Mock(spec=luigi.ExternalTask)
mock_external_task.complete.side_effect = [False, False, True]

class TestTask(luigi.Task):
    """
    Requires a single file dependency
    """
    def __init__(self):
        super(TestTask, self).__init__()
        self.has_run = False
    
    def requires(self):
        return mock_external_task

    def output(self):
        mock_target = Mock(spec=luigi.Target)
        # the return is False so that this task will be scheduled
        mock_target.exists.return_value = False

    def run(self):
        self.has_run = True


class WorkerExternalTaskTest(unittest.TestCase):

    @with_config({'core': {'retry-external-tasks': 'true'}})
    def test_external_dependency_satisified_later(self):
        """
        Test that an external dependency that is not `complete` when luigi is invoked, but \
        becomes `complete` while the workflow is executing is re-evaluated.
        """
        assert luigi.configuration.get_config().getboolean('core',
                                                           'retry-external-tasks',
                                                           False) == True

        test_task = TestTask()
        luigi.build([test_task], local_scheduler=True)

        assert test_task.has_run == True
        assert mock_external_task.complete.call_count == 3


if __name__ == '__main__':
    unittest.main()
