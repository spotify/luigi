# -*- coding: utf-8 -*-
#
# Copyright 2012-2016 Spotify AB
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
Template tasks for running external programs as luigi tasks.

This module is primarily intended for when you need to call a single external
program or shell script, and it's enough to specify program arguments and
environment variables.

If you need to run multiple commands, chain them together or pipe output
from one command to the next, you're probably better off using something like
`plumbum`_, and wrapping plumbum commands in normal luigi
:py:class:`~luigi.task.Task` s.

.. _plumbum: https://plumbum.readthedocs.io/
"""

import logging
import os
import signal
import subprocess
import sys
import tempfile

import luigi
from ..target import FileSystemTarget

logger = logging.getLogger('luigi-interface')


class ExternalProgramTask(luigi.Task):
    """
    Template task for running an external program in a subprocess

    The program is run using :py:class:`subprocess.Popen`, with ``args`` passed
    as a list, generated by :py:meth:`program_args` (where the first element should
    be the executable). See :py:class:`subprocess.Popen` for details.

    Your must override :py:meth:`program_args` to specify the arguments you want,
    and you can optionally override :py:meth:`program_environment` if you want to
    control the environment variables (see :py:class:`ExternalPythonProgramTask`
    for an example).
    """

    def program_args(self):
        """
        Override this method to map your task parameters to the program arguments

        :return: list to pass as ``args`` to :py:class:`subprocess.Popen`
        """
        raise NotImplementedError

    def program_environment(self):
        """
        Override this method to control environment variables for the program

        :return: dict mapping environment variable names to values
        """
        env = os.environ.copy()
        return env

    @property
    def always_log_stderr(self):
        """
        When True, stderr will be logged even if program execution succeeded

        Override to False to log stderr only when program execution fails.
        """
        return True

    def _clean_output_file(self, file_object):
        file_object.seek(0)
        return ''.join(map(lambda s: s.decode('utf-8'), file_object.readlines()))

    def run(self):
        args = list(map(str, self.program_args()))

        logger.info('Running command: %s', ' '.join(args))
        tmp_stdout, tmp_stderr = tempfile.TemporaryFile(), tempfile.TemporaryFile()
        env = self.program_environment()
        proc = subprocess.Popen(
            args,
            env=env,
            stdout=tmp_stdout,
            stderr=tmp_stderr
        )

        try:
            with ExternalProgramRunContext(proc):
                proc.wait()
            success = proc.returncode == 0

            stdout = self._clean_output_file(tmp_stdout)
            stderr = self._clean_output_file(tmp_stderr)

            if stdout:
                logger.info('Program stdout:\n{}'.format(stdout))
            if stderr:
                if self.always_log_stderr or not success:
                    logger.info('Program stderr:\n{}'.format(stderr))

            if not success:
                raise ExternalProgramRunError(
                    'Program failed with return code={}:'.format(proc.returncode),
                    args, env=env, stdout=stdout, stderr=stderr)
        finally:
            tmp_stderr.close()
            tmp_stdout.close()


class ExternalProgramRunContext(object):
    def __init__(self, proc):
        self.proc = proc

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)

    def kill_job(self, captured_signal=None, stack_frame=None):
        self.proc.kill()
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)


class ExternalProgramRunError(RuntimeError):
    def __init__(self, message, args, env=None, stdout=None, stderr=None):
        super(ExternalProgramRunError, self).__init__(message, args, env, stdout, stderr)
        self.message = message
        self.args = args
        self.env = env
        self.out = stdout
        self.err = stderr

    def __str__(self):
        info = self.message
        info += '\nCOMMAND: {}'.format(' '.join(self.args))
        info += '\nSTDOUT: {}'.format(self.out or '[empty]')
        info += '\nSTDERR: {}'.format(self.err or '[empty]')
        env_string = None
        if self.env:
            env_string = ' '.join(['='.join([k, '\'{}\''.format(v)]) for k, v in self.env.items()])
        info += '\nENVIRONMENT: {}'.format(env_string or '[empty]')
        # reset terminal color in case the ENVIRONMENT changes colors
        info += '\033[m'
        return info


class ExternalPythonProgramTask(ExternalProgramTask):
    """
    Template task for running an external Python program in a subprocess

    Simple extension of :py:class:`ExternalProgramTask`, adding two
    :py:class:`luigi.parameter.Parameter` s for setting a virtualenv and for
    extending the ``PYTHONPATH``.
    """
    virtualenv = luigi.Parameter(
        default=None,
        positional=False,
        description='path to the virtualenv directory to use. It should point to '
                    'the directory containing the ``bin/activate`` file used for '
                    'enabling the virtualenv.')
    extra_pythonpath = luigi.Parameter(
        default=None,
        positional=False,
        description='extend the search path for modules by prepending this '
                    'value to the ``PYTHONPATH`` environment variable.')

    def program_environment(self):
        env = super(ExternalPythonProgramTask, self).program_environment()

        if self.extra_pythonpath:
            pythonpath = ':'.join([self.extra_pythonpath, env.get('PYTHONPATH', '')])
            env.update({'PYTHONPATH': pythonpath})

        if self.virtualenv:
            # Make the same changes to the env that a normal venv/bin/activate script would
            path = ':'.join(['{}/bin'.format(self.virtualenv), env.get('PATH', '')])
            env.update({
                'PATH': path,
                'VIRTUAL_ENV': self.virtualenv
            })
            # remove PYTHONHOME env variable, if it exists
            env.pop('PYTHONHOME', None)

        return env


class FileSystemOutputExternalProgramTask(ExternalProgramTask):
    '''

    Alternative to :py:class:`ExternalProgramTask` but captures stdout
    into the output of the task. Works with targets supporting
    FileSystemOutput

    '''

    def getstdout(self):
        outputTarget = self.output()
        if outputTarget is None or not issubclass(outputTarget.__class__, FileSystemTarget):
            raise ValueError('task output is not a FileSystemTarget')

        return self.output().open('w')

    def run(self):
        args = list(map(str, self.program_args()))
        stdout = self.getstdout()

        logger.info('Running command: %s', ' '.join(args))
        tmp_stdout, tmp_stderr = self.getstdout(), tempfile.TemporaryFile()
        env = self.program_environment()
        proc = subprocess.Popen(
            args,
            env=env,
            stdout=tmp_stdout,
            stderr=tmp_stderr
        )

        try:
            with ExternalProgramRunContext(proc):
                proc.wait()
            success = proc.returncode == 0

            stderr = self._clean_output_file(tmp_stderr)

            if stderr:
                if self.always_log_stderr or not success:
                    logger.info('Program stderr:\n{}'.format(stderr))

            if not success:
                raise ExternalProgramRunError(
                    'Program failed with return code={}:'.format(proc.returncode),
                    args, env=env, stdout=stdout, stderr=stderr)

        finally:
            tmp_stderr.close()
            tmp_stdout.close()