# Copyright (c) 2012 Spotify AB
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

import random
import sys
import os
import datetime
import subprocess
import tempfile
from itertools import groupby
from operator import itemgetter
import pickle
import binascii
import logging
import StringIO
import re
import shutil
from hashlib import md5
import luigi
import luigi.hdfs
import configuration
import warnings
import mrrunner

logger = logging.getLogger('luigi-interface')

_attached_packages = []


def attach(*packages):
    """ Attach a python package to hadoop map reduce tarballs to make those packages available on the hadoop cluster"""
    _attached_packages.extend(packages)


def dereference(file):
    if os.path.islink(file):
        #by joining with the dirname we are certain to get the absolute path
        return dereference(os.path.join(os.path.dirname(file), os.readlink(file)))
    else:
        return file


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
                for file in files:
                    f_src = os.path.join(base, file)
                    f_src_stripped = f_src[len(src_prefix):]
                    f_dst = os.path.join(dst, f_src_stripped)
                    result.append((f_src, f_dst))
        else:
            result.append((src, dst))

    return result


def create_packages_archive(packages, filename):
    """Create a tar archive which will contain the files for the packages listed in packages. """
    import tarfile
    tar = tarfile.open(filename, "w")

    def add(src, dst):
        logger.debug('adding to tar: %s -> %s', src, dst)
        tar.add(src, dst)
    for package in packages:
        if package.__package__ and package.__package__ != package.__name__:
            # Replace with package instead of submodule
            package = __import__(package.__package__, None, None, 'non_empty')

        n = package.__name__.replace(".", "/")

        if hasattr(package, "__path__"):
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

                for root, dirs, files in os.walk(p):
                    if '.svn' in dirs:
                        dirs.remove('.svn')
                    for f in files:
                        if not f.endswith(".pyc") and not f.startswith("."):
                            add(dereference(root + "/" + f), root.replace(p, n) + "/" + f)
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
    """A simple generator which flattens a sequence.

    Only one level is flattned.

    (1, (2, 3), 4) -> (1, 2, 3, 4)
    """
    for item in sequence:
        if hasattr(item, "__iter__"):
            for i in item:
                yield i
        else:
            yield item


class HadoopJobError(RuntimeError):
    def __init__(self, message, out=None, err=None):
        super(HadoopJobError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err


def run_and_track_hadoop_job(arglist, tracking_url_callback=None, env=None):
    ''' Runs the job by invoking the command from the given arglist. Finds tracking urls from the output and attempts to fetch
    errors using those urls if the job fails. Throws HadoopJobError with information about the error (including stdout and stderr
    from the process) on failure and returns normally otherwise.
    '''
    logger.info('%s', ' '.join(arglist))

    def track_process(arglist, tracking_url_callback, env=None):
        # Dump stdout to a temp file, poll stderr and log it
        temp_stdout = tempfile.TemporaryFile()
        proc = subprocess.Popen(arglist, stdout=temp_stdout, stderr=subprocess.PIPE, env=env)

        # We parse the output to try to find the tracking URL.
        # This URL is useful for fetching the logs of the job.
        tracking_url = None
        job_id = None
        err_lines = []
        while True:
            try:
                if proc.poll() is not None:
                    break
                err_line = proc.stderr.readline()
            except KeyboardInterrupt:
                if job_id:
                    logger.info('Job interrupted by user, killing job %s', job_id)
                    kill_job(job_id)
                raise
            err_lines.append(err_line)
            err_line = err_line.strip()
            if err_line:
                logger.info('%s', err_line)
            if err_line.find('Tracking URL') != -1:
                tracking_url = err_line.split('Tracking URL: ')[-1]
                try:
                    tracking_url_callback(tracking_url)
                except Exception as e:
                    logger.error("Error in tracking_url_callback, disabling! %s", e)
                    tracking_url_callback = lambda x: None
            if err_line.find('Running job') != -1:
                # hadoop jar output
                job_id = err_line.split('Running job: ')[-1]
            if err_line.find('submitted hadoop job:') != -1:
                # scalding output
                job_id = err_line.split('submitted hadoop job: ')[-1]

        # Read the rest + stdout
        err = ''.join(err_lines + [err_line for err_line in proc.stderr])
        temp_stdout.seek(0)
        out = ''.join(temp_stdout.readlines())

        if proc.returncode == 0:
            return

        # Try to fetch error logs if possible
        message = 'Streaming job failed with exit code %d. ' % proc.returncode
        if not tracking_url:
            raise HadoopJobError(message + 'Also, no tracking url found.', out, err)

        try:
            task_failures = fetch_task_failures(tracking_url)
        except Exception, e:
            raise HadoopJobError(message + 'Additionally, an error occurred when fetching data from %s: %s' %
                                 (tracking_url, e), out, err)

        if not task_failures:
            raise HadoopJobError(message + 'Also, could not fetch output from tasks.', out, err)
        else:
            raise HadoopJobError(message + 'Output from tasks below:\n%s' % task_failures, out, err)

    if tracking_url_callback is None:
        tracking_url_callback = lambda x: None

    track_process(arglist, tracking_url_callback, env)


def kill_job(job_id):
    subprocess.call(['mapred', 'job', '-kill', job_id])


def fetch_task_failures(tracking_url):
    ''' Uses mechanize to fetch the actual task logs from the task tracker.

    This is highly opportunistic, and we might not succeed. So we set a low timeout and hope it works.
    If it does not, it's not the end of the world.

    TODO: Yarn has a REST API that we should probably use instead:
    http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/MapredAppMasterRest.html
    '''
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
        except Exception, e:
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
    ''' Takes care of uploading & executing a Hadoop job using Hadoop streaming

    TODO: add code to support Elastic Mapreduce (using boto) and local execution.
    '''
    def __init__(self, streaming_jar, modules=[], streaming_args=[], libjars=[], libjars_in_hdfs=[], jobconfs={}, input_format=None, output_format=None):
        self.streaming_jar = streaming_jar
        self.modules = modules
        self.streaming_args = streaming_args
        self.libjars = libjars
        self.libjars_in_hdfs = libjars_in_hdfs
        self.jobconfs = jobconfs
        self.input_format = input_format
        self.output_format = output_format
        self.tmp_dir = False

    def run_job(self, job):
        packages = [luigi] + self.modules + job.extra_modules() + list(_attached_packages)

        # find the module containing the job
        packages.append(__import__(job.__module__, None, None, 'dummy'))

        # find the path to out runner.py
        runner_path = mrrunner.__file__
        # assume source is next to compiled
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"

        base_tmp_dir = configuration.get_config().get('core', 'tmp-dir', '/tmp/luigi')
        self.tmp_dir = os.path.join(base_tmp_dir, 'hadoop_job_%016x' % random.getrandbits(64))
        logger.debug("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # build arguments
        map_cmd = 'python mrrunner.py map'
        cmb_cmd = 'python mrrunner.py combiner'
        red_cmd = 'python mrrunner.py reduce'

        # replace output with a temporary work directory
        output_final = job.output().path
        output_tmp_fn = output_final + '-temp-' + datetime.datetime.now().isoformat().replace(':', '-')
        tmp_target = luigi.hdfs.HdfsTarget(output_tmp_fn, is_tmp=True)

        arglist = [luigi.hdfs.load_hadoop_cmd(), 'jar', self.streaming_jar]

        # 'libjars' is a generic option, so place it first
        libjars = [libjar for libjar in self.libjars]

        for libjar in self.libjars_in_hdfs:
            subprocess.call([luigi.hdfs.load_hadoop_cmd(), 'fs', '-get', libjar, self.tmp_dir])
            libjars.append(os.path.join(self.tmp_dir, os.path.basename(libjar)))

        if libjars:
            arglist += ['-libjars', ','.join(libjars)]

        # Add static files and directories
        extra_files = get_extra_files(job.extra_files())

        files = []
        for src, dst in extra_files:
            dst_tmp = '%s_%09d' % (dst.replace('/', '_'), random.randint(0, 999999999))
            files += ['%s#%s' % (src, dst_tmp)]
            # -files doesn't support subdirectories, so we need to create the dst_tmp -> dst manually
            job._add_link(dst_tmp, dst)

        if files:
            arglist += ['-files', ','.join(files)]

        jobconfs = job.jobconfs()

        for k, v in self.jobconfs.iteritems():
            jobconfs.append('%s=%s' % (k, v))

        for conf in jobconfs:
            arglist += ['-D', conf]

        arglist += self.streaming_args

        arglist += ['-mapper', map_cmd]
        if job.combiner != NotImplemented:
            arglist += ['-combiner', cmb_cmd]
        if job.reducer != NotImplemented:
            arglist += ['-reducer', red_cmd]
        files = [runner_path, self.tmp_dir + '/packages.tar', self.tmp_dir + '/job-instance.pickle']

        for f in files:
            arglist += ['-file', f]

        if self.output_format:
            arglist += ['-outputformat', self.output_format]
        if self.input_format:
            arglist += ['-inputformat', self.input_format]

        for target in luigi.task.flatten(job.input_hadoop()):
            assert isinstance(target, luigi.hdfs.HdfsTarget)
            arglist += ['-input', target.path]

        assert isinstance(job.output(), luigi.hdfs.HdfsTarget)
        arglist += ['-output', output_tmp_fn]

        # submit job
        create_packages_archive(packages, self.tmp_dir + '/packages.tar')

        job._dump(self.tmp_dir)

        run_and_track_hadoop_job(arglist)

        # rename temporary work directory to given output
        tmp_target.move(output_final, fail_if_exists=True)
        self.finish()

    def finish(self):
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.debug('Removing directory %s', self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self.finish()


class DefaultHadoopJobRunner(HadoopJobRunner):
    ''' The default job runner just reads from config and sets stuff '''
    def __init__(self):
        config = configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar')
        super(DefaultHadoopJobRunner, self).__init__(streaming_jar=streaming_jar)
        # TODO: add more configurable options


class LocalJobRunner(JobRunner):
    ''' Will run the job locally

    This is useful for debugging and also unit testing. Tries to mimic Hadoop Streaming.

    TODO: integrate with JobTask
    '''
    def __init__(self, samplelines=None):
        self.samplelines = samplelines

    def sample(self, input, n, output):
        for i, line in enumerate(input):
            if n is not None and i >= n:
                break
            output.write(line)

    def group(self, input):
        output = StringIO.StringIO()
        lines = []
        for i, line in enumerate(input):
            parts = line.rstrip('\n').split('\t')
            blob = md5(str(i)).hexdigest()  # pseudo-random blob to make sure the input isn't sorted
            lines.append((parts[:-1], blob, line))
        for k, _, line in sorted(lines):
            output.write(line)
        output.seek(0)
        return output

    def run_job(self, job):
        map_input = StringIO.StringIO()

        for i in luigi.task.flatten(job.input_hadoop()):
            self.sample(i.open('r'), self.samplelines, map_input)

        map_input.seek(0)

        if job.reducer == NotImplemented:
            # Map only job; no combiner, no reducer
            map_output = job.output().open('w')
            job._run_mapper(map_input, map_output)
            map_output.close()
            return

        # run job now...
        map_output = StringIO.StringIO()
        job._run_mapper(map_input, map_output)
        map_output.seek(0)

        if job.combiner == NotImplemented:
            reduce_input = self.group(map_output)
        else:
            combine_input = self.group(map_output)
            combine_output = StringIO.StringIO()
            job._run_combiner(combine_input, combine_output)
            combine_output.seek(0)
            reduce_input = self.group(combine_output)

        reduce_output = job.output().open('w')
        job._run_reducer(reduce_input, reduce_output)
        reduce_output.close()


class BaseHadoopJobTask(luigi.Task):
    n_reduce_tasks = 25
    pool = luigi.Parameter(is_global=True, default=None, significant=False)
    # This value can be set to change the default batching increment. Default is 1 for backwards compatibility.
    batch_counter_default = 1

    final_mapper = NotImplemented
    final_combiner = NotImplemented
    final_reducer = NotImplemented

    reducer = NotImplemented

    _counter_dict = {}
    task_id = None

    def jobconfs(self):
        jcs = []
        jcs.append('mapred.job.name=%s' % self.task_id)
        if self.reducer == NotImplemented:
            jcs.append('mapred.reduce.tasks=0')
        else:
            jcs.append('mapred.reduce.tasks=%s' % self.n_reduce_tasks)
        pool = self.pool
        if pool is not None:
            # Supporting two schedulers: fair (default) and capacity using the same option
            scheduler_type = configuration.get_config().get('hadoop', 'scheduler', 'fair')
            if scheduler_type == 'fair':
                jcs.append('mapred.fairscheduler.pool=%s' % pool)
            elif scheduler_type == 'capacity':
                jcs.append('mapred.job.queue.name=%s' % pool)
        return jcs


    def init_local(self):
        ''' Implement any work to setup any internal datastructure etc here.

        You can add extra input using the requires_local/input_local methods.

        Anything you set on the object will be pickled and available on the Hadoop nodes.
        '''
        pass

    def init_hadoop(self):
        pass

    def run(self):
        self.init_local()
        self.job_runner().run_job(self)

    def requires_local(self):
        ''' Default impl - override this method if you need any local input to be accessible in init() '''
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



class JobTask(BaseHadoopJobTask):

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
        """ Get the MapReduce runner for this job

        If all outputs are HdfsTargets, the DefaultHadoopJobRunner will be used. Otherwise, the LocalJobRunner which streams all data through the local machine will be used (great for testing).
        """
        outputs = luigi.task.flatten(self.output())
        for output in outputs:
            if not isinstance(output, luigi.hdfs.HdfsTarget):
                warnings.warn("Job is using one or more non-HdfsTarget outputs" +
                              " so it will be run in local mode")
                return LocalJobRunner()
        else:
            return DefaultHadoopJobRunner()

    def reader(self, input_stream):
        """Reader is a method which iterates over input lines and outputs records.
           The default implementation yields one argument containing the line for each line in the input."""
        for line in input_stream:
            yield line,

    def writer(self, outputs, stdout, stderr=sys.stderr):
        """Writer format is a method which iterates over the output records from the reducer and formats
           them for output.
           The default implementation outputs tab separated items"""
        for output in outputs:
            try:
                print >> stdout, "\t".join(map(str, flatten(output)))
            except:
                print >> stderr, output
                raise

    def mapper(self, item):
        """Re-define to process an input item (usually a line of input data)

        Defaults to identity mapper that sends all lines to the same reducer"""
        yield None, item

    combiner = NotImplemented

    def incr_counter(self, *args, **kwargs):
        """ Increments a Hadoop counter

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
            new_arg = list(key)+[ct]
            self._incr_counter(*new_arg)
            ct = 0
        self._counter_dict[key] = ct

    def _flush_batch_incr_counter(self):
        """ Increments any unflushed counter values
        """
        for key, count in self._counter_dict.iteritems():
            if count == 0:
                continue
            args = list(key) + [count]
            self._incr_counter(*args)

    def _incr_counter(self, *args):
        """ Increments a Hadoop counter

        Note that this seems to be a bit slow, ~1 ms. Don't overuse this function by updating very frequently.
        """
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            print >> sys.stderr, 'reporter:counter:%s,%s' % (group_name, count)
        else:
            group, name, count = args
            print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, name, count)

    def extra_modules(self):
        return []  # can be overridden in subclass

    def extra_files(self):
        '''
        Can be overriden in subclass. Each element is either a string, or a pair of two strings (src, dst).
        src can be a directory (in which case everything will be copied recursively).
        dst can include subdirectories (foo/bar/baz.txt etc)
        Uses Hadoop's -files option so that the same file is reused across tasks.
        '''
        return []

    def _add_link(self, src, dst):
        if not hasattr(self, '_links'):
            self._links = []
        self._links.append((src, dst))

    def _setup_links(self):
        if hasattr(self, '_links'):
            for src, dst in self._links:
                d = os.path.dirname(dst)
                if d and not os.path.exists(d):
                    os.makedirs(d)
                if not os.path.exists(dst):
                    # If the combiner runs, the file might already exist,
                    # so no reason to create the link again
                    os.link(src, dst)

    def _dump(self, dir=''):
        """Dump instance to file."""
        file_name = os.path.join(dir, 'job-instance.pickle')
        if self.__module__ == '__main__':
            d = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            d = d.replace('(c__main__', "(c" + module_name)
            open(file_name, "w").write(d)

        else:
            pickle.dump(self, open(file_name, "w"))

    def _map_input(self, input_stream):
        """Iterate over input and call the mapper for each item.
           If the job has a parser defined, the return values from the parser will
           be passed as arguments to the mapper.

           If the input is coded output from a previous run, the arguments will be splitted in key and value."""
        for record in self.reader(input_stream):
            for output in self.mapper(*record):
                yield output
        if self.final_mapper != NotImplemented:
            for output in self.final_mapper():
                yield output
        self._flush_batch_incr_counter()

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        """Iterate over input, collect values with the same key, and call the reducer for each uniqe key."""
        for key, values in groupby(inputs, itemgetter(0)):
            for output in reducer(key, (v[1] for v in values)):
                yield output
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def _run_mapper(self, stdin=sys.stdin, stdout=sys.stdout):
        """Run the mapper on the hadoop node."""
        self.init_hadoop()
        self.init_mapper()
        outputs = self._map_input((line[:-1] for line in stdin))
        if self.reducer == NotImplemented:
            self.writer(outputs, stdout)
        else:
            self.internal_writer(outputs, stdout)

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        """Run the reducer on the hadoop node."""
        self.init_hadoop()
        self.init_reducer()
        outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.reducer, self.final_reducer)
        self.writer(outputs, stdout)

    def _run_combiner(self, stdin=sys.stdin, stdout=sys.stdout):
        self.init_hadoop()
        self.init_combiner()
        outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.combiner, self.final_combiner)
        self.internal_writer(outputs, stdout)

    def internal_reader(self, input_stream):
        """Reader which uses python eval on each part of a tab separated string.
        Yields a tuple of python objects."""
        for input in input_stream:
            yield map(eval, input.split("\t"))

    def internal_writer(self, outputs, stdout):
        """Writer which outputs the python repr for each item"""
        for output in outputs:
            print >> stdout, "\t".join(map(repr, output))


def pickle_reader(job, input_stream):
    def decode(item):
        return pickle.loads(binascii.a2b_base64(item))
    for line in input_stream:
        items = line.split('\t')
        yield map(decode, items)


def pickle_writer(job, outputs, stdout):
    def encode(item):
        return binascii.b2a_base64(pickle.dumps(item))[:-1]  # remove trailing newline
    for keyval in outputs:
        print >> stdout, "\t".join(map(encode, keyval))
