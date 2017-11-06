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
notebook through the ``pars`` dictionary.

When the task is executed, the ``pars`` dictionary is written to a
temporary JSON file in the same directory that contains the notebook.

Inside the notebook, you can retrieve the values of the parameters in
``pars`` by reading the temporary JSON file. The path to the temporary file is
accessible via the environment variable ``PARS``.
For example, in a Python notebook, you can read the ``pars`` dictionary as
follows:

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

Similarly, in a R notebook, you can read the ``pars`` dictionary with

.. code-block:: r

    library(jsonlite)

    # read the temporary JSON file
    pars <- fromJSON(Sys.getenv('PARS'))

    # extract the task's self.input() paths (included by default)
    requires_paths <- pars$input

    # extract the task's self.output() paths (included by default)
    output_paths <- pars$output

    # extract a *user-defined* parameter named `my_par`
    my_par <- pars$my_par

The paths of the task's ``self.input()`` and
``self.output()`` are automatically added to ``pars`` with keys
*input* and *output* respectively. These paths are meaningful when
``self.input()`` and ``self.output()`` return iterables or dictionaries whose
values have a ``path`` attribute
(e.g. as in :class:`luigi.local_target.LocalTarget`).
Whenever this is not the case, the corresponding entries of
``pars.get('input')`` and ``pars.get('output')`` are ``None``.

Note that in the above Python code block, ``requires_paths`` is a dictionary of
lists if the task's :meth:`requires` method returns a dictionary; otherwise,
``requires_paths`` is a list of lists.
Similarly, ``output_paths`` is a dictionary if the :meth:`output` method returns
a dictionary or a list otherwise.

In R notebooks or notebooks written in other languages, the specific details of
the structure of the ``input`` and ``output`` components of ``pars`` depends on
how JSON files are read.

:class:`JupyterNotebookTask` inherits from the standard
:class:`luigi.Task` class. As usual, you should override the 
:class:`luigi.Task` default :meth:`requires` and :meth:`output` methods.
**Please make sure that your requires and output methods return
dictionaries or iterables.**

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

    This function extracts the file name without the file extension from a given
    path.
    For example, if `path=~/some_dir/some_file.ext`, then
    `get_filename_from_path(path)` will return `some_file`.

    :param input_path: a path to a file

    :returns: a string containing the name of the file without the file
        extension
    """
    base_name = os.path.basename(os.path.normpath(input_path))
    file_name = base_name.split('.')[0]
    return file_name


def _get_values(obj):
    """
    A simple utility to extract the values of a dictionary, list, or other
    iterable and recombine them into a list.

    :param obj: a dictionary, list, or other iterable

    :returns: a list with the values of obj
    """
    if isinstance(obj, dict):
        out = obj.values()
    else:
        out = [val for val in obj]
    return out


class JupyterNotebookTask(luigi.Task):
    """
    This is a template task to execute Jupyter notebooks as Luigi tasks.
    This task has the following key parameters:

    :param nb_path: the full path to the Jupyter notebook (**required**).

    :param kernel_name: the name of the kernel to be used in the notebook
        execution (**required**).

    :param timeout: maximum time (in seconds) allocated to run each cell.
        If -1 (the default), no timeout limit is imposed.

    :param pars: a dictionary of user-defined parameters to be passed to the
        Jupyter notebook.

    :param json_action: if `delete` (default), the temporary JSON file with
        **pars** is deleted at the end of the task execution; if `keep`, the
        temporary JSON file is kept (useful for debugging purposes).
    """
    nb_path = luigi.Parameter(
        default=None
    )

    kernel_name = luigi.Parameter(
        default=None
    )

    timeout = luigi.IntParameter(
        default=-1
    )

    json_action = luigi.Parameter(
        default = 'delete'
    )

    pars = {}

    def _form_input(self):

        if isinstance(self.input(), dict):
            out = {
                tag: list(map(
                    lambda x: x.path, _get_values(self.input().get(tag)))
                ) for tag in self.input().keys()
            }
        else:
            out = [
                list(map(lambda x: x.path, _get_values(req)))
                for req in self.input()
            ]

        return out

    def _form_output(self):

        if isinstance(self.output(), dict):
            out = {
                tag: req.path for tag, req in self.output().items()
            }
        else:
            out = [req.path for req in self.output()]

        return out

    def run(self):

        # check arguments
        if not self.nb_path:
            raise TypeError(
                'nb_path cannot be None; '
                'nb_path must be a valid path to a Jupyter notebook'
            )

        if not os.path.exists(self.nb_path):
            raise IOError(
                "I can't find the Jupyter notebook %s" % self.nb_path
            )

        if not self.kernel_name:
            raise TypeError(
                'kernel_name cannot be None; '
                'kernel_name must be the name of a valid Jupyter kernel'
            )

        self.timeout = int(self.timeout)

        if not self.json_action in ['keep', 'delete']:
            raise ValueError(
                "json_action must be one of 'keep' or 'delete'"
            )

        # get notebook name
        notebook_name = _get_file_name_from_path(self.nb_path)

        # get task id
        task_id = self.task_id

        # set requires pars
        self.pars['input'] = self._form_input()

        # set output pars
        self.pars['output'] = self._form_output()

        # write pars to temporary file
        tmp_file_path = os.path.join(
            os.path.dirname(self.nb_path), '%s_%s.ipynbpars'
            % (notebook_name, task_id)
        )

        with open(tmp_file_path, 'w') as parameters:
            json.dump(self.pars, parameters)

        # set environment variable with tmp_file_path
        os.environ['PARS'] = tmp_file_path

        # run notebook
        logger.info('=== Running notebook: %s ===' % notebook_name)

        nb_version = int(nbformat.__version__.split('.')[0])

        try:
            with open(self.nb_path, 'r') as nb:
                nb = nbformat.read(nb, as_version=nb_version)

            ep = ExecutePreprocessor(
                timeout=self.timeout,
                kernel_name=self.kernel_name
            )

            ep.preprocess(nb, {
                'metadata': {
                    'path': os.path.dirname(self.nb_path)
                }
            })

            logger.info('=== Done! ===')

        except CellExecutionError:
            raise

        finally:
            # clean up (remove temporary JSON file)
            if self.json_action == 'delete':
                os.remove(tmp_file_path)