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
Run Hadoop Mapreduce jobs using Hadoop Streaming. To run a job, you need
to subclass :py:class:`luigi.contrib.hadoop.JobTask` and implement a
``mapper`` and ``reducer`` methods. See :doc:`/example_top_artists` for
an example of how to run a Hadoop job.
"""

from __future__ import print_function

import abc
import datetime
import glob
import logging
import os
import pickle
import random
import re
import shutil
import signal
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import subprocess
import sys
import tempfile
import warnings
from hashlib import md5
from itertools import groupby
from luigi import six

from luigi import configuration
import luigi
import luigi.task
import luigi.contrib.gcs
import luigi.contrib.hdfs
import luigi.contrib.s3
from luigi.contrib import mrrunner

if six.PY2:
    from itertools import imap as map

try:
    # See benchmark at https://gist.github.com/mvj3/02dca2bcc8b0ef1bbfb5
    import ujson as json
except ImportError:
    import json

logger = logging.getLogger('luigi-interface')

_attached_packages = []


TRACKING_RE = re.compile(r'(tracking url|the url to track the job):\s+(?P<url>.+)$')


class hadoop(luigi.task.Config):
    pool = luigi.Parameter(default=None,
                           description='Hadoop pool so use for Hadoop tasks. '
                           'To specify pools per tasks, see '
                           'BaseHadoopJobTask.pool')


def attach(*packages):
    """
    Attach a python package to hadoop map reduce tarballs to make those packages available
    on the hadoop cluster.
    """
    _attached_packages.extend(packages)


def dereference(f):
    if os.path.islink(f):
        # by joining with the dirname we are certain to get the absolute path
        return dereference(os.path.join(os.path.dirname(f), os.readlink(f)))
    else:
        return f


def get_extra_files(extra_files):
    result = []
    for f in extra_files:
        if isinstance(f, str):
            src, dst = f, os.path.basename(f)
        elif isinstance(f, tuple):
            src, dst = f
        else:
            raise Exception()

        if os.path.isdir(src):
            src_prefix = os.path.join(src, '')
            for base, dirs, files in os.walk(src):
                for f in files:
                    f_src = os.path.join(base, f)
                    f_src_stripped = f_src[len(src_prefix):]
                    f_dst = os.path.join(dst, f_src_stripped)
                    result.append((f_src, f_dst))
        else:
            result.append((src, dst))

    return result


def create_packages_archive(packages, filename):
    """
    Create a tar archive which will contain the files for the packages listed in packages.
    """
    import tarfile
    tar = tarfile.open(filename, "w")

    def add(src, dst):
        logger.debug('adding to tar: %s -> %s', src, dst)
        tar.add(src, dst)

    def add_files_for_package(sub_package_path, root_package_path, root_package_name):
        for root, dirs, files in os.walk(sub_package_path):
            if '.svn' in dirs:
                dirs.remove('.svn')
            for f in files:
                if not f.endswith(".pyc") and not f.startswith("."):
                    add(dereference(root + "/" + f), root.replace(root_package_path, root_package_name) + "/" + f)

    for package in packages:
        # Put a submodule's entire package in the archive. This is the
        # magic that usually packages everything you need without
        # having to attach packages/modules explicitly
        if not getattr(package, "__path__", None) and '.' in package.__name__:
            package = __import__(package.__name__.rpartition('.')[0], None, None, 'non_empty')

        n = package.__name__.replace(".", "/")

        if getattr(package, "__path__", None):
            # TODO: (BUG) picking only the first path does not
            # properly deal with namespaced packages in different
            # directories
            p = package.__path__[0]

            if p.endswith('.egg') and os.path.isfile(p):
                raise 'egg files not supported!!!'
                # Add the entire egg file
                # p = p[:p.find('.egg') + 4]
                # add(dereference(p), os.path.basename(p))

            else:
                # include __init__ files from parent projects
                root = []
                for parent in package.__name__.split('.')[0:-1]:
                    root.append(parent)
                    module_name = '.'.join(root)
                    directory = '/'.join(root)

                    add(dereference(__import__(module_name, None, None, 'non_empty').__path__[0] + "/__init__.py"),
                        directory + "/__init__.py")

                add_files_for_package(p, p, n)

                # include egg-info directories that are parallel:
                for egg_info_path in glob.glob(p + '*.egg-info'):
                    logger.debug(
                        'Adding package metadata to archive for "%s" found at "%s"',
                        package.__name__,
                        egg_info_path
                    )
                    add_files_for_package(egg_info_path, p, n)

        else:
            f = package.__file__
            if f.endswith("pyc"):
                f = f[:-3] + "py"
            if n.find(".") == -1:
                add(dereference(f), os.path.basename(f))
            else:
                add(dereference(f), n + ".py")
    tar.close()


def flatten(sequence):
    """
    A simple generator which flattens a sequence.

    Only one level is flattened.

    .. code-block:: python

        (1, (2, 3), 4) -> (1, 2, 3, 4)

    """
    for item in sequence:
        if hasattr(item, "__iter__") and not isinstance(item, str) and not isinstance(item, bytes):
            for i in item:
                yield i
        else:
            yield item


class HadoopRunContext(object):

    def __init__(self):
        self.job_id = None
        self.application_id = None

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def kill_job(self, captured_signal=None, stack_frame=None):
        if self.application_id:
            logger.info('Job interrupted, killing application %s' % self.application_id)
            subprocess.call(['yarn', 'application', '-kill', self.application_id])
        elif self.job_id:
            logger.info('Job interrupted, killing job %s', self.job_id)
            subprocess.call(['mapred', 'job', '-kill', self.job_id])
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)


class HadoopJobError(RuntimeError):

    def __init__(self, message, out=None, err=None):
        super(HadoopJobError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err

    def __str__(self):
        return self.message


def run_and_track_hadoop_job(arglist, tracking_url_callback=None, env=None):
    """
    Runs the job by invoking the command from the given arglist.
    Finds tracking urls from the output and attempts to fetch errors using those urls if the job fails.
    Throws HadoopJobError with information about the error
    (including stdout and stderr from the process)
    on failure and returns normally otherwise.

    :param arglist:
    :param tracking_url_callback:
    :param env:
    :return:
    """
    logger.info('%s', subprocess.list2cmdline(arglist))

    def write_luigi_history(arglist, history):
        """
        Writes history to a file in the job's output directory in JSON format.
        Currently just for tracking the job ID in a configuration where
        no history is stored in the output directory by Hadoop.
        """
        history_filename = configuration.get_config().get('core', 'history-filename', '')
        if history_filename and '-output' in arglist:
            output_dir = arglist[arglist.index('-output') + 1]
            f = luigi.contrib.hdfs.HdfsTarget(os.path.join(output_dir, history_filename)).open('w')
            f.write(json.dumps(history))
            f.close()

    def track_process(arglist, tracking_url_callback, env=None):
        # Dump stdout to a temp file, poll stderr and log it
        temp_stdout = tempfile.TemporaryFile('w+t')
        proc = subprocess.Popen(arglist, stdout=temp_stdout, stderr=subprocess.PIPE, env=env, close_fds=True, universal_newlines=True)

        # We parse the output to try to find the tracking URL.
        # This URL is useful for fetching the logs of the job.
        tracking_url = None
        job_id = None
        application_id = None
        err_lines = []

        with HadoopRunContext() as hadoop_context:
            while proc.poll() is None:
                err_line = proc.stderr.readline()
                err_lines.append(err_line)
                err_line = err_line.strip()
                if err_line:
                    logger.info('%s', err_line)
                err_line = err_line.lower()
                tracking_url_match = TRACKING_RE.search(err_line)
                if tracking_url_match:
                    tracking_url = tracking_url_match.group('url')
                    try:
                        tracking_url_callback(tracking_url)
                    except Exception as e:
                        logger.error("Error in tracking_url_callback, disabling! %s", e)

                        def tracking_url_callback(x):
                            return None
                if err_line.find('running job') != -1:
                    # hadoop jar output
                    job_id = err_line.split('running job: ')[-1]
                if err_line.find('submitted hadoop job:') != -1:
                    # scalding output
                    job_id = err_line.split('submitted hadoop job: ')[-1]
                if err_line.find('submitted application ') != -1:
                    application_id = err_line.split('submitted application ')[-1]
                hadoop_context.job_id = job_id
                hadoop_context.application_id = application_id

        # Read the rest + stdout
        err = ''.join(err_lines + [an_err_line for an_err_line in proc.stderr])
        temp_stdout.seek(0)
        out = ''.join(temp_stdout.readlines())

        if proc.returncode == 0:
            write_luigi_history(arglist, {'job_id': job_id})
            return (out, err)

        # Try to fetch error logs if possible
        message = 'Streaming job failed with exit code %d. ' % proc.returncode
        if not tracking_url:
            raise HadoopJobError(message + 'Also, no tracking url found.', out, err)

        try:
            task_failures = fetch_task_failures(tracking_url)
        except Exception as e:
            raise HadoopJobError(message + 'Additionally, an error occurred when fetching data from %s: %s' %
                                 (tracking_url, e), out, err)

        if not task_failures:
            raise HadoopJobError(message + 'Also, could not fetch output from tasks.', out, err)
        else:
            raise HadoopJobError(message + 'Output from tasks below:\n%s' % task_failures, out, err)

    if tracking_url_callback is None:
        def tracking_url_callback(x): return None

    return track_process(arglist, tracking_url_callback, env)


def fetch_task_failures(tracking_url):
    """
    Uses mechanize to fetch the actual task logs from the task tracker.

    This is highly opportunistic, and we might not succeed.
    So we set a low timeout and hope it works.
    If it does not, it's not the end of the world.

    TODO: Yarn has a REST API that we should probably use instead:
    http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html
    """
    import mechanize
    timeout = 3.0
    failures_url = tracking_url.replace('jobdetails.jsp', 'jobfailures.jsp') + '&cause=failed'
    logger.debug('Fetching data from %s', failures_url)
    b = mechanize.Browser()
    b.open(failures_url, timeout=timeout)
    links = list(b.links(text_regex='Last 4KB'))  # For some reason text_regex='All' doesn't work... no idea why
    links = random.sample(links, min(10, len(links)))  # Fetch a random subset of all failed tasks, so not to be biased towards the early fails
    error_text = []
    for link in links:
        task_url = link.url.replace('&start=-4097', '&start=-100000')  # Increase the offset
        logger.debug('Fetching data from %s', task_url)
        b2 = mechanize.Browser()
        try:
            r = b2.open(task_url, timeout=timeout)
            data = r.read()
        except Exception as e:
            logger.debug('Error fetching data from %s: %s', task_url, e)
            continue
        # Try to get the hex-encoded traceback back from the output
        for exc in re.findall(r'luigi-exc-hex=[0-9a-f]+', data):
            error_text.append('---------- %s:' % task_url)
            error_text.append(exc.split('=')[-1].decode('hex'))

    return '\n'.join(error_text)


class JobRunner(object):
    run_job = NotImplemented


class HadoopJobRunner(JobRunner):
    """
    Takes care of uploading & executing a Hadoop job using Hadoop streaming.

    TODO: add code to support Elastic Mapreduce (using boto) and local execution.
    """

    def __init__(self, streaming_jar, modules=None, streaming_args=None,
                 libjars=None, libjars_in_hdfs=None, jobconfs=None,
                 input_format=None, output_format=None,
                 end_job_with_atomic_move_dir=True, archives=None):
        def get(x, default):
            return x is not None and x or default
        self.streaming_jar = streaming_jar
        self.modules = get(modules, [])
        self.streaming_args = get(streaming_args, [])
        self.libjars = get(libjars, [])
        self.libjars_in_hdfs = get(libjars_in_hdfs, [])
        self.archives = get(archives, [])
        self.jobconfs = get(jobconfs, {})
        self.input_format = input_format
        self.output_format = output_format
        self.end_job_with_atomic_move_dir = end_job_with_atomic_move_dir
        self.tmp_dir = False

    def run_job(self, job, tracking_url_callback=None):
        if tracking_url_callback is not None:
            warnings.warn("tracking_url_callback argument is deprecated, task.set_tracking_url is "
                          "used instead.", DeprecationWarning)

        packages = [luigi] + self.modules + job.extra_modules() + list(_attached_packages)

        # find the module containing the job
        packages.append(__import__(job.__module__, None, None, 'dummy'))

        # find the path to out runner.py
        runner_path = mrrunner.__file__
        # assume source is next to compiled
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"

        base_tmp_dir = configuration.get_config().get('core', 'tmp-dir', None)
        if base_tmp_dir:
            warnings.warn("The core.tmp-dir configuration item is"
                          " deprecated, please use the TMPDIR"
                          " environment variable if you wish"
                          " to control where luigi.contrib.hadoop may"
                          " create temporary files and directories.")
            self.tmp_dir = os.path.join(base_tmp_dir, 'hadoop_job_%016x' % random.getrandbits(64))
            os.makedirs(self.tmp_dir)
        else:
            self.tmp_dir = tempfile.mkdtemp()

        logger.debug("Tmp dir: %s", self.tmp_dir)

        # build arguments
        config = configuration.get_config()
        python_executable = config.get('hadoop', 'python-executable', 'python')
        runner_arg = 'mrrunner.pex' if job.package_binary is not None else 'mrrunner.py'
        command = '{0} {1} {{step}}'.format(python_executable, runner_arg)
        map_cmd = command.format(step='map')
        cmb_cmd = command.format(step='combiner')
        red_cmd = command.format(step='reduce')

        output_final = job.output().path
        # atomic output: replace output with a temporary work directory
        if self.end_job_with_atomic_move_dir:
            illegal_targets = (
                luigi.contrib.s3.S3FlagTarget, luigi.contrib.gcs.GCSFlagTarget)
            if isinstance(job.output(), illegal_targets):
                raise TypeError("end_job_with_atomic_move_dir is not supported"
                                " for {}".format(illegal_targets))
            output_hadoop = '{output}-temp-{time}'.format(
                output=output_final,
                time=datetime.datetime.now().isoformat().replace(':', '-'))
        else:
            output_hadoop = output_final

        arglist = luigi.contrib.hdfs.load_hadoop_cmd() + ['jar', self.streaming_jar]

        # 'libjars' is a generic option, so place it first
        libjars = [libjar for libjar in self.libjars]

        for libjar in self.libjars_in_hdfs:
            run_cmd = luigi.contrib.hdfs.load_hadoop_cmd() + ['fs', '-get', libjar, self.tmp_dir]
            logger.debug(subprocess.list2cmdline(run_cmd))
            subprocess.call(run_cmd)
            libjars.append(os.path.join(self.tmp_dir, os.path.basename(libjar)))

        if libjars:
            arglist += ['-libjars', ','.join(libjars)]

        # 'archives' is also a generic option
        archives = []
        extra_archives = job.extra_archives()

        if self.archives:
            archives = self.archives

        if extra_archives:
            archives += extra_archives

        if archives:
            arglist += ['-archives', ','.join(archives)]

        # Add static files and directories
        extra_files = get_extra_files(job.extra_files())

        files = []
        for src, dst in extra_files:
            dst_tmp = '%s_%09d' % (dst.replace('/', '_'), random.randint(0, 999999999))
            files += ['%s#%s' % (src, dst_tmp)]
            # -files doesn't support subdirectories, so we need to create the dst_tmp -> dst manually
            job.add_link(dst_tmp, dst)

        if files:
            arglist += ['-files', ','.join(files)]

        jobconfs = job.jobconfs()

        for k, v in six.iteritems(self.jobconfs):
            jobconfs.append('%s=%s' % (k, v))

        for conf in jobconfs:
            arglist += ['-D', conf]

        arglist += self.streaming_args

        # Add additonal non-generic  per-job streaming args
        extra_streaming_args = job.extra_streaming_arguments()
        for (arg, value) in extra_streaming_args:
            if not arg.startswith('-'):  # safety first
                arg = '-' + arg
            arglist += [arg, value]

        arglist += ['-mapper', map_cmd]

        if job.combiner != NotImplemented:
            arglist += ['-combiner', cmb_cmd]
        if job.reducer != NotImplemented:
            arglist += ['-reducer', red_cmd]
        packages_fn = 'mrrunner.pex' if job.package_binary is not None else 'packages.tar'
        files = [
            runner_path if job.package_binary is None else None,
            os.path.join(self.tmp_dir, packages_fn),
            os.path.join(self.tmp_dir, 'job-instance.pickle'),
        ]

        for f in filter(None, files):
            arglist += ['-file', f]

        if self.output_format:
            arglist += ['-outputformat', self.output_format]
        if self.input_format:
            arglist += ['-inputformat', self.input_format]

        allowed_input_targets = (
            luigi.contrib.hdfs.HdfsTarget,
            luigi.contrib.s3.S3Target,
            luigi.contrib.gcs.GCSTarget)
        for target in luigi.task.flatten(job.input_hadoop()):
            if not isinstance(target, allowed_input_targets):
                raise TypeError('target must one of: {}'.format(
                    allowed_input_targets))
            arglist += ['-input', target.path]

        allowed_output_targets = (
            luigi.contrib.hdfs.HdfsTarget,
            luigi.contrib.s3.S3FlagTarget,
            luigi.contrib.gcs.GCSFlagTarget)
        if not isinstance(job.output(), allowed_output_targets):
            raise TypeError('output must be one of: {}'.format(
                allowed_output_targets))
        arglist += ['-output', output_hadoop]

        # submit job
        if job.package_binary is not None:
            shutil.copy(job.package_binary, os.path.join(self.tmp_dir, 'mrrunner.pex'))
        else:
            create_packages_archive(packages, os.path.join(self.tmp_dir, 'packages.tar'))

        job.dump(self.tmp_dir)

        run_and_track_hadoop_job(arglist, tracking_url_callback=job.set_tracking_url)

        if self.end_job_with_atomic_move_dir:
            luigi.contrib.hdfs.HdfsTarget(output_hadoop).move_dir(output_final)
        self.finish()

    def finish(self):
        # FIXME: check for isdir?
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.debug('Removing directory %s', self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self.finish()


class DefaultHadoopJobRunner(HadoopJobRunner):
    """
    The default job runner just reads from config and sets stuff.
    """

    def __init__(self):
        config = configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar')
        super(DefaultHadoopJobRunner, self).__init__(streaming_jar=streaming_jar)
        # TODO: add more configurable options


class LocalJobRunner(JobRunner):
    """
    Will run the job locally.

    This is useful for debugging and also unit testing. Tries to mimic Hadoop Streaming.

    TODO: integrate with JobTask
    """

    def __init__(self, samplelines=None):
        self.samplelines = samplelines

    def sample(self, input_stream, n, output):
        for i, line in enumerate(input_stream):
            if n is not None and i >= n:
                break
            output.write(line)

    def group(self, input_stream):
        output = StringIO()
        lines = []
        for i, line in enumerate(input_stream):
            parts = line.rstrip('\n').split('\t')
            blob = md5(str(i).encode('ascii')).hexdigest()  # pseudo-random blob to make sure the input isn't sorted
            lines.append((parts[:-1], blob, line))
        for _, _, line in sorted(lines):
            output.write(line)
        output.seek(0)
        return output

    def run_job(self, job):
        map_input = StringIO()

        for i in luigi.task.flatten(job.input_hadoop()):
            self.sample(i.open('r'), self.samplelines, map_input)

        map_input.seek(0)

        if job.reducer == NotImplemented:
            # Map only job; no combiner, no reducer
            map_output = job.output().open('w')
            job.run_mapper(map_input, map_output)
            map_output.close()
            return

        # run job now...
        map_output = StringIO()
        job.run_mapper(map_input, map_output)
        map_output.seek(0)

        if job.combiner == NotImplemented:
            reduce_input = self.group(map_output)
        else:
            combine_input = self.group(map_output)
            combine_output = StringIO()
            job.run_combiner(combine_input, combine_output)
            combine_output.seek(0)
            reduce_input = self.group(combine_output)

        reduce_output = job.output().open('w')
        job.run_reducer(reduce_input, reduce_output)
        reduce_output.close()


class BaseHadoopJobTask(luigi.Task):
    pool = luigi.Parameter(default=None, significant=False, positional=False)
    # This value can be set to change the default batching increment. Default is 1 for backwards compatibility.
    batch_counter_default = 1

    final_mapper = NotImplemented
    final_combiner = NotImplemented
    final_reducer = NotImplemented

    mr_priority = NotImplemented
    package_binary = None

    _counter_dict = {}
    task_id = None

    def _get_pool(self):
        """ Protected method """
        if self.pool:
            return self.pool
        if hadoop().pool:
            return hadoop().pool

    @abc.abstractmethod
    def job_runner(self):
        pass

    def jobconfs(self):
        jcs = []
        jcs.append('mapred.job.name=%s' % self)
        if self.mr_priority != NotImplemented:
            jcs.append('mapred.job.priority=%s' % self.mr_priority())
        pool = self._get_pool()
        if pool is not None:
            # Supporting two schedulers: fair (default) and capacity using the same option
            scheduler_type = configuration.get_config().get('hadoop', 'scheduler', 'fair')
            if scheduler_type == 'fair':
                jcs.append('mapred.fairscheduler.pool=%s' % pool)
            elif scheduler_type == 'capacity':
                jcs.append('mapred.job.queue.name=%s' % pool)
        return jcs

    def init_local(self):
        """
        Implement any work to setup any internal datastructure etc here.

        You can add extra input using the requires_local/input_local methods.

        Anything you set on the object will be pickled and available on the Hadoop nodes.
        """
        pass

    def init_hadoop(self):
        pass

    # available formats are "python" and "json".
    data_interchange_format = "python"

    def run(self):
        # The best solution is to store them as lazy `cached_property`, but it
        # has extraneous dependency. And `property` is slow (need to be
        # calculated every time when called), so we save them as attributes
        # directly.
        self.serialize = DataInterchange[self.data_interchange_format]['serialize']
        self.internal_serialize = DataInterchange[self.data_interchange_format]['internal_serialize']
        self.deserialize = DataInterchange[self.data_interchange_format]['deserialize']

        self.init_local()
        self.job_runner().run_job(self)

    def requires_local(self):
        """
        Default impl - override this method if you need any local input to be accessible in init().
        """
        return []

    def requires_hadoop(self):
        return self.requires()  # default impl

    def input_local(self):
        return luigi.task.getpaths(self.requires_local())

    def input_hadoop(self):
        return luigi.task.getpaths(self.requires_hadoop())

    def deps(self):
        # Overrides the default implementation
        return luigi.task.flatten(self.requires_hadoop()) + luigi.task.flatten(self.requires_local())

    def on_failure(self, exception):
        if isinstance(exception, HadoopJobError):
            return """Hadoop job failed with message: {message}

    stdout:
    {stdout}


    stderr:
    {stderr}
      """.format(message=exception.message, stdout=exception.out, stderr=exception.err)
        else:
            return super(BaseHadoopJobTask, self).on_failure(exception)


DataInterchange = {
    "python": {"serialize": str,
               "internal_serialize": repr,
               "deserialize": eval},
    "json": {"serialize": json.dumps,
             "internal_serialize": json.dumps,
             "deserialize": json.loads}
}


class JobTask(BaseHadoopJobTask):
    jobconf_truncate = 20000
    n_reduce_tasks = 25
    reducer = NotImplemented

    def jobconfs(self):
        jcs = super(JobTask, self).jobconfs()
        if self.reducer == NotImplemented:
            jcs.append('mapred.reduce.tasks=0')
        else:
            jcs.append('mapred.reduce.tasks=%s' % self.n_reduce_tasks)
        if self.jobconf_truncate >= 0:
            jcs.append('stream.jobconf.truncate.limit=%i' % self.jobconf_truncate)
        return jcs

    def init_mapper(self):
        pass

    def init_combiner(self):
        pass

    def init_reducer(self):
        pass

    def _setup_remote(self):
        self._setup_links()

    def job_runner(self):
        # We recommend that you define a subclass, override this method and set up your own config
        """
        Get the MapReduce runner for this job.

        If all outputs are HdfsTargets, the DefaultHadoopJobRunner will be used.
        Otherwise, the LocalJobRunner which streams all data through the local machine
        will be used (great for testing).
        """
        outputs = luigi.task.flatten(self.output())
        for output in outputs:
            if not isinstance(output, luigi.contrib.hdfs.HdfsTarget):
                warnings.warn("Job is using one or more non-HdfsTarget outputs" +
                              " so it will be run in local mode")
                return LocalJobRunner()
        else:
            return DefaultHadoopJobRunner()

    def reader(self, input_stream):
        """
        Reader is a method which iterates over input lines and outputs records.

        The default implementation yields one argument containing the line for each line in the input."""
        for line in input_stream:
            yield line,

    def writer(self, outputs, stdout, stderr=sys.stderr):
        """
        Writer format is a method which iterates over the output records
        from the reducer and formats them for output.

        The default implementation outputs tab separated items.
        """
        for output in outputs:
            try:
                output = flatten(output)
                if self.data_interchange_format == "json":
                    # Only dump one json string, and skip another one, maybe key or value.
                    output = filter(lambda x: x, output)
                else:
                    # JSON is already serialized, so we put `self.serialize` in a else statement.
                    output = map(self.serialize, output)
                print("\t".join(output), file=stdout)
            except:
                print(output, file=stderr)
                raise

    def mapper(self, item):
        """
        Re-define to process an input item (usually a line of input data).

        Defaults to identity mapper that sends all lines to the same reducer.
        """
        yield None, item

    combiner = NotImplemented

    def incr_counter(self, *args, **kwargs):
        """
        Increments a Hadoop counter.

        Since counters can be a bit slow to update, this batches the updates.
        """
        threshold = kwargs.get("threshold", self.batch_counter_default)
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            key = (group_name,)
        else:
            group, name, count = args
            key = (group, name)

        ct = self._counter_dict.get(key, 0)
        ct += count
        if ct >= threshold:
            new_arg = list(key) + [ct]
            self._incr_counter(*new_arg)
            ct = 0
        self._counter_dict[key] = ct

    def _flush_batch_incr_counter(self):
        """
        Increments any unflushed counter values.
        """
        for key, count in six.iteritems(self._counter_dict):
            if count == 0:
                continue
            args = list(key) + [count]
            self._incr_counter(*args)
            self._counter_dict[key] = 0

    def _incr_counter(self, *args):
        """
        Increments a Hadoop counter.

        Note that this seems to be a bit slow, ~1 ms

        Don't overuse this function by updating very frequently.
        """
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            print('reporter:counter:%s,%s' % (group_name, count), file=sys.stderr)
        else:
            group, name, count = args
            print('reporter:counter:%s,%s,%s' % (group, name, count), file=sys.stderr)

    def extra_modules(self):
        return []  # can be overridden in subclass

    def extra_files(self):
        """
        Can be overriden in subclass.

        Each element is either a string, or a pair of two strings (src, dst).

        * `src` can be a directory (in which case everything will be copied recursively).
        * `dst` can include subdirectories (foo/bar/baz.txt etc)

        Uses Hadoop's -files option so that the same file is reused across tasks.
        """
        return []

    def extra_streaming_arguments(self):
        """
        Extra arguments to Hadoop command line.
        Return here a list of (parameter, value) tuples.
        """
        return []

    def extra_archives(self):
        """List of paths to archives """
        return []

    def add_link(self, src, dst):
        if not hasattr(self, '_links'):
            self._links = []
        self._links.append((src, dst))

    def _setup_links(self):
        if hasattr(self, '_links'):
            missing = []
            for src, dst in self._links:
                d = os.path.dirname(dst)
                if d:
                    try:
                        os.makedirs(d)
                    except OSError:
                        pass
                if not os.path.exists(src):
                    missing.append(src)
                    continue
                if not os.path.exists(dst):
                    # If the combiner runs, the file might already exist,
                    # so no reason to create the link again
                    os.link(src, dst)
            if missing:
                raise HadoopJobError(
                    'Missing files for distributed cache: ' +
                    ', '.join(missing))

    def dump(self, directory=''):
        """
        Dump instance to file.
        """
        with self.no_unpicklable_properties():
            file_name = os.path.join(directory, 'job-instance.pickle')
            if self.__module__ == '__main__':
                d = pickle.dumps(self)
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                d = d.replace(b'(c__main__', "(c" + module_name)
                open(file_name, "wb").write(d)

            else:
                pickle.dump(self, open(file_name, "wb"))

    def _map_input(self, input_stream):
        """
        Iterate over input and call the mapper for each item.
        If the job has a parser defined, the return values from the parser will
        be passed as arguments to the mapper.

        If the input is coded output from a previous run,
        the arguments will be splitted in key and value.
        """
        for record in self.reader(input_stream):
            for output in self.mapper(*record):
                yield output
        if self.final_mapper != NotImplemented:
            for output in self.final_mapper():
                yield output
        self._flush_batch_incr_counter()

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        """
        Iterate over input, collect values with the same key, and call the reducer for each unique key.
        """
        for key, values in groupby(inputs, key=lambda x: self.internal_serialize(x[0])):
            for output in reducer(self.deserialize(key), (v[1] for v in values)):
                yield output
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def run_mapper(self, stdin=sys.stdin, stdout=sys.stdout):
        """
        Run the mapper on the hadoop node.
        """
        self.init_hadoop()
        self.init_mapper()
        outputs = self._map_input((line[:-1] for line in stdin))
        if self.reducer == NotImplemented:
            self.writer(outputs, stdout)
        else:
            self.internal_writer(outputs, stdout)

    def run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        """
        Run the reducer on the hadoop node.
        """
        self.init_hadoop()
        self.init_reducer()
        outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.reducer, self.final_reducer)
        self.writer(outputs, stdout)

    def run_combiner(self, stdin=sys.stdin, stdout=sys.stdout):
        self.init_hadoop()
        self.init_combiner()
        outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.combiner, self.final_combiner)
        self.internal_writer(outputs, stdout)

    def internal_reader(self, input_stream):
        """
        Reader which uses python eval on each part of a tab separated string.
        Yields a tuple of python objects.
        """
        for input_line in input_stream:
            yield list(map(self.deserialize, input_line.split("\t")))

    def internal_writer(self, outputs, stdout):
        """
        Writer which outputs the python repr for each item.
        """
        for output in outputs:
            print("\t".join(map(self.internal_serialize, output)), file=stdout)
