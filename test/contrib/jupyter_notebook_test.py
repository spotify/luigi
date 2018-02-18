# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
"""
Tests for `JupyterNotebookTask`.

Requires the following modules: `nbformat`, `nbconvert`, `jupyter`.
If these modules are not available, some of the tests are skipped.
"""
import json
import logging
import os
import tempfile
import unittest

from luigi import Parameter
from luigi.local_target import LocalTarget

from luigi.contrib.jupyter_notebook import JupyterNotebookTask

logger = logging.getLogger('luigi-interface')

try:
    import jupyter
    from jupyter_client import KernelManager
    import nbformat as nbf
    from nbformat.v4 import (
        new_notebook,
        new_code_cell
    )
    execute_run_tests = True
except ImportError:
    execute_run_tests = False

# set tempdir
# tempfile.tempdir = '/tmp'
tmp = tempfile.gettempdir()

# if the nbformat and nbconvert modules are available, create an empty
# notebook that simply
# 1. reads the parameters from the task
# 2. writes the parameters to file.
if execute_run_tests:
    notebook = new_notebook()
    notebook['cells'] = [
        new_code_cell(
            'import json'
        ),
        new_code_cell(
            'import os'
        ),
        new_code_cell(
            'from luigi.contrib.jupyter_notebook import load_parameters'
        ),
        new_code_cell(
            "pars = load_parameters('LUIGI_JUPYTER_TASK_ENV')"
        ),
        new_code_cell(
            "file = open(pars.get('output'), 'w')"
        ),
        new_code_cell(
            'json.dump(pars, file)'
        ),
        new_code_cell(
            'file.close()'
        )
    ]

    notebook_file = tempfile.NamedTemporaryFile()
    with open(notebook_file.name, 'w') as file:
        nbf.write(notebook, file)

    # get kernel on host
    host_kernel = KernelManager().kernel_name

# test parameters
test_parameters = {
    'notebook_path': './my_notebook.ipynb',
    'kernel_name': 'python3',
    'timeout': 60,
    'json_action': 'keep',
    'environment_variable': 'LUIGI_JUPYTER_TASK_ENV'
}


class BaseTestTask(JupyterNotebookTask):
    test_parameter = Parameter(
        default='foo'
    )

    def run(self):
        pass


class OutputNoPath(BaseTestTask):
    def output(self):
        return 'foo'


class OutputSingle(BaseTestTask):
    def output(self):
        return LocalTarget(os.path.join(tmp, 'single_local_target'))


class OutputSingleListNoPath(BaseTestTask):
    def output(self):
        return ['foo']


class OutputSingleList(BaseTestTask):
    def output(self):
        return [LocalTarget(os.path.join(tmp, 'single_local_target'))]


class OutputSingleDictNoPath(BaseTestTask):
    def output(self):
        return {'out': 'foo'}


class OutputSingleDict(BaseTestTask):
    def output(self):
        return {
            'out': LocalTarget(os.path.join(tmp, 'single_local_target'))
        }


class OutputList(BaseTestTask):
    def output(self):
        return [
            LocalTarget(os.path.join(tmp, 'single_local_target_one')),
            LocalTarget(os.path.join(tmp, 'single_local_target_two'))
        ]


class OutputDict(BaseTestTask):
    def output(self):
        return {
            'out_one': LocalTarget(
                os.path.join(tmp, 'single_local_target_one')
            ),
            'out_two': LocalTarget(
                os.path.join(tmp, 'single_local_target_two')
            )
        }


class TestNoPathRequire(BaseTestTask):
    def requires(self):
        return OutputNoPath(**test_parameters)


class TestSingleRequire(BaseTestTask):
    def requires(self):
        return OutputSingle(**test_parameters)


class TestSingleListNoPathRequire(BaseTestTask):
    def requires(self):
        return OutputSingleListNoPath(**test_parameters)


class TestSingleListRequire(BaseTestTask):
    def requires(self):
        return OutputSingleList(**test_parameters)


class TestSingleDictNoPathRequire(BaseTestTask):
    def requires(self):
        return OutputSingleDictNoPath(**test_parameters)


class TestSingleDictRequire(BaseTestTask):
    def requires(self):
        return OutputSingleDict(**test_parameters)


class TestListRequire(BaseTestTask):
    def requires(self):
        return [
            OutputNoPath(**test_parameters),
            OutputSingle(**test_parameters),
            OutputList(**test_parameters),
            OutputDict(**test_parameters)
        ]


class TestDictRequire(BaseTestTask):
    def requires(self):
        return {
            'input_one': OutputNoPath(**test_parameters),
            'input_two': OutputSingle(**test_parameters),
            'input_three': OutputList(**test_parameters),
            'input_four': OutputDict(**test_parameters)
        }


class TestRun(JupyterNotebookTask):
    def output(self):
        notebook_output_file = tempfile.NamedTemporaryFile()
        return LocalTarget(notebook_output_file.name)


#
# Testing
#
class TestJupyterNotebookTask(unittest.TestCase):
    # test ability to detect invalid keys
    def test_invalid_key(self):
        pass

    # test ability to catch Luigi parameters along with the base parameters of
    # `JupyterNotebookTask`
    def test_form_luigi_pars(self):
        test_task = BaseTestTask(test_parameter='bar', **test_parameters)
        expected = test_parameters.copy()
        expected['test_parameter'] = 'bar'
        expected['input'] = []
        expected['output'] = []
        self.assertEqual(
            test_task.parameters,
            expected
        )

    # test ability to form `self.parameters['input']`
    def test_form_input_no_path(self):
        test_task = TestNoPathRequire(**test_parameters)
        expected = None
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_single(self):
        test_task = TestSingleRequire(**test_parameters)
        expected = OutputSingle(**test_parameters).output().path
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_single_list_no_path(self):
        test_task = TestSingleListNoPathRequire(**test_parameters)
        expected = None
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_single_list(self):
        test_task = TestSingleListRequire(**test_parameters)
        expected = OutputSingleList(**test_parameters).output()[0].path
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_single_dict_no_path(self):
        test_task = TestSingleDictNoPathRequire(**test_parameters)
        expected = {
            k: None for k in
            OutputSingleDict(**test_parameters).output().keys()
        }
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_single_dict(self):
        test_task = TestSingleDictRequire(**test_parameters)
        expected = {
            k: v.path for k, v in
            OutputSingleDict(**test_parameters).output().items()
        }
        self.assertEqual(
            test_task.parameters.get('input'),
            expected
        )

    def test_form_input_list(self):
        test_task = TestListRequire(**test_parameters)
        expected = [
            None,
            OutputSingle(**test_parameters).output().path,
            [item.path for item in OutputList(**test_parameters).output()],
            {
                k: v.path for k, v in
                OutputDict(**test_parameters).output().items()
            }
        ]
        self.assertEqual(test_task.parameters.get('input'), expected)

    def test_form_input_dict(self):
        test_task = TestDictRequire(**test_parameters)
        expected = {
            'input_one': None,
            'input_two': OutputSingle(**test_parameters).output().path,
            'input_three': [
                item.path for item in OutputList(**test_parameters).output()
            ],
            'input_four': {
                k: v.path for k, v in
                OutputDict(**test_parameters).output().items()
            }
        }
        self.assertEqual(test_task.parameters.get('input'), expected)

    # test ability to form `self.parameters('output')`
    def test_form_output_no_path(self):
        test_task = OutputNoPath(**test_parameters)
        expected = None
        self.assertEqual(test_task.parameters.get('output'), expected)

    def test_form_output_single(self):
        test_task = OutputSingle(**test_parameters)
        expected = OutputSingle(**test_parameters).output().path
        self.assertEqual(
            test_task.parameters.get('output'),
            expected
        )

    def test_form_output_single_list(self):
        test_task = OutputSingleList(**test_parameters)
        expected = OutputSingleList(**test_parameters).output()[0].path
        self.assertEqual(
            test_task.parameters.get('output'),
            expected
        )

    def test_form_output_single_dict(self):
        test_task = OutputSingleDict(**test_parameters)
        expected = {
            k: v.path for k, v in
            OutputSingleDict(**test_parameters).output().items()
        }
        self.assertEqual(
            test_task.parameters.get('output'),
            expected
        )

    def test_form_output_list(self):
        test_task = OutputList(**test_parameters)
        expected = [
            item.path for item in OutputList(**test_parameters).output()
        ]
        self.assertEqual(
            test_task.parameters.get('output'),
            expected
        )

    def test_form_output_dict(self):
        test_task = OutputDict(**test_parameters)
        expected = {
            k: v.path for k, v in
            OutputDict(**test_parameters).output().items()
        }
        self.assertEqual(
            test_task.parameters.get('output'),
            expected
        )

    # test ability to run a notebook
    @unittest.skipIf(not execute_run_tests, 'missing requirements')
    def test_run(self):
        test_task = TestRun(
            notebook_path=notebook_file.name,
            kernel_name=host_kernel,
            environment_variable='LUIGI_JUPYTER_TASK_ENV'
        )
        test_task.run()
        with open(test_task.parameters.get('output')) as pars:
            nb_pars = json.load(pars)
        expected = {}
        expected['notebook_path'] = notebook_file.name
        expected['environment_variable'] = 'LUIGI_JUPYTER_TASK_ENV'
        expected['kernel_name'] = host_kernel
        expected['json_action'] = 'delete'
        expected['timeout'] = -1
        expected['input'] = []
        expected['output'] = test_task.parameters.get('output')
        self.assertEqual(nb_pars, expected)

    # def test_environment_variable(self):
    #     pass

    # def test_json_action_keep(self):
    #     pass

    # def test_json_action_delete(self):
    #     pass


if __name__ == '__main__':
    unittest.main()
