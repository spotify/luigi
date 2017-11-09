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
This module is intended for when you need to execute a Jupyter notebook as a
task within a Luigi pipeline. This can be accomplished via the
:class:`JupyterNotebookTask` class.

:class:`JupyterNotebookTask` allows you to pass parameters to the Jupyter
notebook through the ``parameters`` dictionary.

When the task is executed, the ``parameters`` dictionary is written to a
temporary JSON file in the same directory that contains the notebook.

Inside the notebook, you can retrieve the values of the parameters in
``parameters`` by reading the temporary JSON file. The path to the temporary
file is accessible via the environment variable ``PARS``.
For example, in a Python notebook, you can read the ``parameters`` dictionary
as follows:

.. code-block:: python

    import os
    import json

    # read the temporary JSON file
    with open(os.environ['PARS']) as parameters:
        pars = json.load(parameters)

    # extract the task's self.input() paths (included by default)
    requires_paths = pars.get('input')

    # extract the task's self.output() paths (included by default)
    output_paths = pars.get('output')

    # extract a *user-defined* parameter named `my_par`
    my_par = pars.get('my_par')

The paths of the task's ``self.input()`` and ``self.output()`` are automatically
added to ``parameters`` with keys *input* and *output* respectively.
These paths are meaningful when ``self.input()`` and ``self.output()`` return
single objects with the ``path`` attribute, or iterables or dictionaries whose
values are themselves objects or collections of objects from which the ``path``
attribute can be extracted (e.g. :class:`luigi.local_target.LocalTarget`).
Whenever a ``path`` attribute can't be extracted, the corresponding entry
inside of ``parameters`` is set to ``None``.

:class:`JupyterNotebookTask` inherits from the standard
:class:`luigi.Task` class. As usual, you should override the
:class:`luigi.Task` default :meth:`requires` and :meth:`output` methods.

The :meth:`run` method of :class:`JupyterNotebookTask` wraps the
:mod:`nbformat`/:mod:`nbconvert` approach to executing Jupyter notebooks
as scripts. See the
`Executing notebooks using the Python API interface
<http://nbconvert.readthedocs.io/en/latest/execute_api.html#executing-notebooks-using-the-python-api-interface>`_
section of the :mod:`nbconvert` module documentation for more information.

**The jupyter_notebook module depends on both the nbconvert (>=5.3.1)
and the nbformat (>=4.4.0) modules. Please make sure they are installed.**

Written by `@mattiaciollaro <https://github.com/mattiaciollaro>`_.
"""

import json
import logging
import os
from datetime import datetime

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import nbformat
    from nbconvert.preprocessors import (
        CellExecutionError,
        ExecutePreprocessor
    )

except ImportError:
    logger.warning('Loading jupyter_notebook module without nbconvert '
                   'and/or nbformat installed. The nbconvert and nbformat '
                   'modules are required to use the jupyter_notebook module. '
                   'Please install nbconvert and nbformat.')


def _get_file_name_from_path(input_path):
    """
    A simple utility to extract the name of a file without the file extension
    from a given path.

    This function extracts the file name without the file extension from a
    given path.
    For example, if `path=~/some_dir/some_file.ext`, then
    `get_filename_from_path(path)` will return `some_file`.
    """
    base_name = os.path.basename(os.path.normpath(input_path))
    file_name = base_name.split('.')[0]
    return file_name


def _flatten(obj):
    """
    A modified version of `luigi.task.flatten` that preserves dictionaries.
    """
    if obj is None:
        return []
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, luigi.six.string_types):
        return [obj]
    flat = []
    try:
        # if iterable
        iterator = iter(obj)
    except TypeError:
        return [obj]
    for result in iterator:
        flat.append(result)
    return flat


def _get_path(obj):
    """
    Extracts the `path` attribute from `obj` if available; otherwise, simply
    returns None.
    """
    obj = _flatten(obj)
    try:
        out = obj[0].path
    except AttributeError:
        out = None
    return out


def _get_path_from_collection(coll):
    """
    Extracts the `path` attribute from atomic objects or objects organized in a
    collection (dict or iterable) wherever the `path` attribute is available
    (else extracts `None`).
    """
    if isinstance(coll, dict):
        out = {k: _get_path(v) for k, v in coll.items()}
    else:
        coll = _flatten(coll)
        if len(coll) == 1:
            out = _get_path(coll)
        else:
            out = [_get_path(v) for v in _flatten(coll)]
    return out


class JupyterNotebookTask(luigi.Task):
    """
    This is a template task to execute Jupyter notebooks as Luigi tasks.
    This task has the following key parameters:

    :param notebook_path: the full path to the Jupyter notebook (**required**).

    :param kernel_name: the name of the Jupyter kernel to be used in the
        notebook execution (**required**).

    :param timeout: maximum time (in seconds) allocated to run each cell.
        If -1 (the default), no timeout limit is imposed.

    :param parameters: a dictionary of user-defined parameters to be passed to
        the Jupyter notebook.

    :param json_action: if `delete` (default), the temporary JSON file with
        **parameters** is deleted at the end of the task execution; if `keep`,
        the temporary JSON file is kept (useful for debugging purposes).
    """
    notebook_path = luigi.Parameter(
        description='The full path to the Jupyter notebook'
    )

    kernel_name = luigi.Parameter(
        description='The Jupyter kernel to be used in the notebook execution'
    )

    timeout = luigi.IntParameter(
        description='Max time (in seconds) allocated to run each notebook cell',
        default=-1
    )

    json_action = luigi.ChoiceParameter(
        description="'delete' or 'keep' the temporary JSON file",
        choices=['delete', 'keep'],
        var_type=str,
        default='delete'
    )

    parameters = {}

    def _form_input(self):
        """
        This method is used to loop through the return value of the task's
        `requires` method and extract paths (where available).
        """
        task_input = _flatten(self.input())
        # case 1 - `requires` returns a dictionary
        if isinstance(task_input, dict):
            out = {
                k: _get_path_from_collection(v)
                for k, v in task_input.items()
            }
        # case 2 - `requires` returns a list, iterable, or single object
        else:
            if len(task_input) == 1:
                out = _get_path_from_collection(task_input)
            else:
                out = [_get_path_from_collection(v) for v in task_input]

        return out

    def _form_output(self):
        """
        This method is used to loop through the return value of the task's
        `output` method and extract paths (where available).
        """
        task_output = _flatten(self.output())
        out = _get_path_from_collection(task_output)

        return out

    def run(self):

        # get current date and time
        time = datetime.strftime(datetime.now(), '%Y-%m-%d_%H-%M-%S')

        # get notebook name
        notebook_name = _get_file_name_from_path(self.notebook_path)

        # get task id
        task_id = self.task_id

        # set requires parameters
        self.parameters['input'] = self._form_input()

        # set output parameters
        self.parameters['output'] = self._form_output()

        # write parameters to temporary file
        tmp_file_path = os.path.join(
            os.path.dirname(self.notebook_path), '%s_%s_%s.ipynbpars'
            % (notebook_name, task_id, time)
        )

        with open(tmp_file_path, 'w') as params:
            json.dump(self.parameters, params)

        # set environment variable with tmp_file_path
        os.environ['PARS'] = tmp_file_path

        # run notebook
        logger.info('===== Running notebook: %s =====' % notebook_name)

        notebook_version = int(nbformat.__version__.split('.')[0])

        try:
            with open(self.notebook_path, 'r') as nb:
                nb = nbformat.read(nb, as_version=notebook_version)

            ep = ExecutePreprocessor(
                timeout=self.timeout,
                kernel_name=self.kernel_name
            )

            ep.preprocess(nb, {
                'metadata': {
                    'path': os.path.dirname(self.notebook_path)
                }
            })

            logger.info('===== Done! =====')

        except CellExecutionError:
            logger.info('===== Done with errors! =====')
            raise

        finally:
            # clean up (remove temporary JSON file)
            if self.json_action == 'delete':
                os.remove(tmp_file_path)
