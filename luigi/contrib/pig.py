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
Apache Pig support.
Example configuration section in luigi.cfg::

    [pig]
    # pig home directory
    home: /usr/share/pig
"""
from contextlib import contextmanager
import logging
import os
import select
import signal
import subprocess
import sys
import tempfile

import luigi
from luigi import configuration

logger = logging.getLogger('luigi-interface')


class PigJobTask(luigi.Task):

    def pig_home(self):
        return configuration.get_config().get('pig', 'home', '/usr/share/pig')

    def pig_command_path(self):
        return os.path.join(self.pig_home(), "bin/pig")

    def pig_env_vars(self):
        """
        Dictionary of environment variables that should be set when running Pig.

        Ex::
            return { 'PIG_CLASSPATH': '/your/path' }
        """
        return {}

    def pig_properties(self):
        """
        Dictionary of properties that should be set when running Pig.

        Example::

            return { 'pig.additional.jars':'/path/to/your/jar' }
        """
        return {}

    def pig_parameters(self):
        """
        Dictionary of parameters that should be set for the Pig job.

        Example::

            return { 'YOUR_PARAM_NAME':'Your param value' }
        """
        return {}

    def pig_options(self):
        """
        List of options that will be appended to the Pig command.

        Example::

            return ['-x', 'local']
        """
        return []

    def output(self):
        raise NotImplementedError("subclass should define output path")

    def pig_script_path(self):
        """
        Return the path to the Pig script to be run.
        """
        raise NotImplementedError("subclass should define pig_script_path")

    @contextmanager
    def _build_pig_cmd(self):
        opts = self.pig_options()

        def line(k, v):
            return ('%s=%s%s' % (k, v, os.linesep)).encode('utf-8')

        with tempfile.NamedTemporaryFile() as param_file, tempfile.NamedTemporaryFile() as prop_file:
            if self.pig_parameters():
                items = self.pig_parameters().items()
                param_file.writelines(line(k, v) for (k, v) in items)
                param_file.flush()
                opts.append('-param_file')
                opts.append(param_file.name)

            if self.pig_properties():
                items = self.pig_properties().items()
                prop_file.writelines(line(k, v) for k, v in items)
                prop_file.flush()
                opts.append('-propertyFile')
                opts.append(prop_file.name)

            cmd = [self.pig_command_path()] + opts + ["-f", self.pig_script_path()]

            logger.info(subprocess.list2cmdline(cmd))
            yield cmd

    def run(self):
        with self._build_pig_cmd() as cmd:
            self.track_and_progress(cmd)

    def track_and_progress(self, cmd):
        temp_stdout = tempfile.TemporaryFile('wb')
        env = os.environ.copy()
        env['PIG_HOME'] = self.pig_home()
        for k, v in self.pig_env_vars().items():
            env[k] = v

        proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        reads = [proc.stderr.fileno(), proc.stdout.fileno()]
        # tracking the possible problems with this job
        err_lines = []
        with PigRunContext():
            while proc.poll() is None:
                ret = select.select(reads, [], [])
                for fd in ret[0]:
                    if fd == proc.stderr.fileno():
                        line = proc.stderr.readline().decode('utf8')
                        err_lines.append(line)
                    if fd == proc.stdout.fileno():
                        line_bytes = proc.stdout.readline()
                        temp_stdout.write(line_bytes)
                        line = line_bytes.decode('utf8')

                err_line = line.lower()
                if err_line.find('More information at:') != -1:
                    logger.info(err_line.split('more information at: ')[-1].strip())
                if err_line.find(' - '):
                    t = err_line.split(' - ')[-1].strip()
                    if t != "":
                        logger.info(t)

        # Read the rest + stdout
        err = ''.join(err_lines + [an_err_line.decode('utf8') for an_err_line in proc.stderr])
        if proc.returncode == 0:
            logger.info("Job completed successfully!")
        else:
            logger.error("Error when running script:\n%s", self.pig_script_path())
            logger.error(err)
            raise PigJobError("Pig script failed with return value: %s" % (proc.returncode,), err=err)


class PigRunContext:
    def __init__(self):
        self.job_id = None

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def kill_job(self, captured_signal=None, stack_frame=None):
        if self.job_id:
            logger.info('Job interrupted, killing job %s', self.job_id)
            subprocess.call(['pig', '-e', '"kill %s"' % self.job_id])
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)


class PigJobError(RuntimeError):
    def __init__(self, message, out=None, err=None):
        super(PigJobError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err

    def __str__(self):
        info = self.message
        if self.out:
            info += "\nSTDOUT: " + str(self.out)
        if self.err:
            info += "\nSTDERR: " + str(self.err)
        return info
