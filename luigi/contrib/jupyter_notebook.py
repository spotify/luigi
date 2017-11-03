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
Template tasks for executing Jupyter notebooks as luigi tasks.

This module is intended for when you need to execute a Jupyter notebook as a 
task within a luigi pipeline.

`JupyterNobebookTask` allows you to pass parameters to the Jupyter notebook 
through a dictionary (`pars`). When the `run` method is executed, the `pars`
dictionary is written to a `notebook_title.ipynbpars` temporary JSON file
in the same directory where the notebook that you want to execute is saved
(here `notebook_title` is the title of the notebook).
Inside the notebook, you can then retrieve the values of the parameters in 
`pars` by reading the temporary `notebook_title.ipynbpars` JSON file.
The temporary `notebook_title.ipynbpars` is deleted once the run method is
exited (regardless of whether or not the notebook was executed successfully).

By default, the `path` attributes of `self.input()` and `self.output()` 
are added to the temporary JSON file via the `pars` attribute with keys
`input` and `output` respectively.

`JupyterNotebookTask` inherits from the standard `luigi.Task` class.
As usual, you should override the default `requires` and `output` methods.

The template task `JupyterNotebookTask` implements a run method that wraps the 
nbformat/nbconvert approach to executing Jupyter notebooks as scripts.
See the `Executing notebooks from the command line` section of the `nbconvert`
module documentation for more information.

Requires the following modules:
- nbconvert
- nbformat

Written by Mattia Ciollaro (@mattiaciollaro).
"""

import json
import logging
import os

import luigi

try:
    import nbconvert
    import nbformat

except ImportError:
    logger.warning('Loading jupyter_notebook module without nbconvert '
        'and/or nbformat installed. The nbconvert and nbformat modules are '
        'required to use the jupyter_notebook module. '
        'Please install nbconvert and nbformat.')


logger = logging.getLogger('luigi-interface')


def get_file_name_from_path(input_path):
    """
    Utility to extract the name of a file without the file extension from a 
    given path.

    This function extracts the file name without the file extension from a given
    path.
    For example, if path='~/some_dir/some_file.ext', then
    get_filename_from_path(path) will return 'some_file'.

    Args:
        input_path: a path to a file

    Returns:
        a string containing the name of the file without file extension
    """

    base_name = os.path.basename(os.path.normpath(input_path))
    file_name = base_name.split('.')[0]
    return(file_name)


class JupyterNotebookTask(luigi.Task):
    """
    This is a template task to execute a Jupyter notebook as a luigi task.

    This class has the following key attributes:

    - nb_path: the full path to the Jupyter notebook (required).

    - kernel_name: the name of the kernel to be used in the notebook execution
                   (required).

    - timeout: maximum time (in seconds) allocated to run each cell. If -1 (the
               default), no timeout limit is imposed.

    - pars: a dictionary of parameters to be passed to the Jupyter notebook.
            These parameters can be retrieved inside the notebook by reading
            the `notebook_title.ipynbpars` temporary JSON file created during the
            execution of the task (where `notebook_title` is the title
            of the Jupyter notebook you want to execute).
            By default, paths associated with `self.input()` and `self.output()` 
            are included with keys `input` and `output` in `pars`, and therefore
            they can be read from inside the notebook as well.
    

    Example: accessing `pars` inside the Jupyter notebook 
    #####################################################
    You would add something like the following block inside a notebook titled 
    `my_notebook`:
    
    ```
    # reading the temporary JSON file
    with open('./my_notebook.ipynbpars') as pars:
        parameters = json.load(pars)

    # extracting the task's self.input() paths 
    requires_paths = parameters.get('input')

    # extracting the task's self.output() paths
    output_paths = parameters.get('output')

    # extracting a custom parameter named `my_par`
    my_own_par = parameters.get('my_par')

    ```

    `requires_paths` (`output_paths`) is a dictionary if the task's
    `requires` (`output`) method returns a dictionary;
    otherwise, `requires_paths` (`output_paths`) is a list.
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

    pars = {}

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

        if not self.kernel:
            raise TypeError(
                'kernel cannot be None; '
                'kernel must be the name of a valid Jupyter kernel'
            )

        # get notebook name
        notebook_name = get_file_name_from_path(self.nb_path)

        # set requires pars
        if type(self.requires()) is dict:
            self.pars['input'] = {
                tag: req.path for tag, req in self.input().items()
            }
        else:
            self.pars['input'] = [req.path for req in self.input()]

        # set output pars
        if type(self.output()) is dict:
            self.pars['output'] = {tag: req.path for tag, req in
                                   self.output().items()}
        else:
            self.pars['output'] = [req.path for req in self.output()]

        # write pars to temporary file
        tmp_file_path = os.path.join(
            os.path.dirname(self.nb_path), '%s.ipynbpars' % notebook_name
        )

        with open(tmp_file_path, 'w') as parameters:
            json.dump(self.pars, parameters)

        # run notebook
        logger.info('=== Running notebook: %s ===' % notebook_name)

        nb_version = int(nbformat.__version__.split('.')[0])

        try:
            with open(self.nb_path, 'r') as nb:
                nb = nbformat.read(nb, as_version=nb_version)

            ep = nbconvert.preprocessors.ExecutePreprocessor(
                timeout=self.timeout,
                kernel_name=self.kernel_name
            )

            ep.preprocess(nb, {
                'metadata': {
                    'path': os.path.dirname(self.nb_path)
                }
            })

            logger.info('=== Done! ===')

        except:
            raise

        finally:
            # clean up (remove temporary JSON file)
            os.remove(tmp_file_path)
