# -*- coding: utf-8 -*-
#
# Copyright 2017 VNG Corporation
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
from luigi.task_register import (Register,
                                 TaskClassNotFoundException,
                                 TaskClassAmbigiousException,
                                 )


class TaskRegisterTest(LuigiTestCase):

    def test_externalize_taskclass(self):
        with self.assertRaises(TaskClassNotFoundException):
            Register.get_task_cls('scooby.Doo')

        class Task1(luigi.Task):
            @classmethod
            def get_task_family(cls):
                return "scooby.Doo"

        self.assertEqual(Task1, Register.get_task_cls('scooby.Doo'))

        class Task2(luigi.Task):
            @classmethod
            def get_task_family(cls):
                return "scooby.Doo"

        with self.assertRaises(TaskClassAmbigiousException):
            Register.get_task_cls('scooby.Doo')

        class Task3(luigi.Task):
            @classmethod
            def get_task_family(cls):
                return "scooby.Doo"

        # There previously was a rare bug where the third installed class could
        # "undo" class ambiguity.
        with self.assertRaises(TaskClassAmbigiousException):
            Register.get_task_cls('scooby.Doo')
