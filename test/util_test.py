# -*- coding: utf-8 -*-
#
# Copyright 2016 VNG Corporation
#
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
#
from helpers import LuigiTestCase

import luigi
import luigi.task
from luigi.util import requires


class BasicsTest(LuigiTestCase):

    def test_requries(self):
        class BaseTask(luigi.Task):
            my_param = luigi.Parameter()
        luigi.namespace('blah')

        @requires(BaseTask)
        class ChildTask(luigi.Task):
            pass
        luigi.namespace('')
        child_task = ChildTask(my_param='hello')
        self.assertEqual(str(child_task), 'blah.ChildTask(my_param=hello)')
        self.assertIn(BaseTask(my_param='hello'), luigi.task.flatten(child_task.requires()))

    def test_requries_weird_way(self):
        # Here we use this decorator in a unnormal way.
        # But it should still work.
        class BaseTask(luigi.Task):
            my_param = luigi.Parameter()
        decorator = requires(BaseTask)
        luigi.namespace('blah')

        class ChildTask(luigi.Task):
            pass
        luigi.namespace('')
        ChildTask = decorator(ChildTask)
        child_task = ChildTask(my_param='hello')
        self.assertEqual(str(child_task), 'blah.ChildTask(my_param=hello)')
        self.assertIn(BaseTask(my_param='hello'), luigi.task.flatten(child_task.requires()))
