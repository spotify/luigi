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

import subprocess
import os
import random
import luigi.format


def exists(path):
    cmd = ['hadoop', 'fs', '-test', '-e', path]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, _ = p.communicate()

    if stdout or p.returncode not in (0, 1):
        raise RuntimeError("Command %s failed with return code %s.\n---Output---\n%s\n------------" % (repr(cmd), p.returncode, stdout))

    if p.returncode == 0:
        return True
    elif p.returncode == 1:
        return False
    assert False


def rename(path, dest):
    parent_dir = os.path.dirname(dest)
    if parent_dir != '' and not exists(parent_dir):
        mkdir(parent_dir)
    cmd = ['hadoop', 'fs', '-mv', path, dest]
    if subprocess.call(cmd):
        raise RuntimeError('Command %s failed' % repr(cmd))


def remove(path, recursive=True):
    if recursive:
        cmd = ['hadoop', 'fs', '-rm', '-r', path]
    else:
        cmd = ['hadoop', 'fs', '-rm', path]
    if subprocess.call(cmd):
        raise RuntimeError('Command %s failed' % repr(cmd))


def mkdir(path):
    cmd = ['hadoop', 'fs', '-mkdir', path]
    if subprocess.call(cmd):
        raise RuntimeError('Command %s failed' % repr(cmd))


def listdir(path, ignore_directories=False, ignore_files=False, include_size=False, include_type=False):
    if not path:
        path = "."  # default to current/home catalog

    cmd = ['hadoop', 'fs', '-ls', path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    lines = proc.stdout

    for line in lines:
        if line.startswith('Found'):
            continue  # "hadoop fs -ls" outputs "Found %d items" as its first line
        line = line.rstrip("\n")
        if ignore_directories and line[0] == 'd':
            continue
        if ignore_files and line[0] == '-':
            continue
        data = line.split(' ')

        file = data[-1]
        size = int(data[-4])
        line_type = line[0]
        extra_data = ()

        if include_size:
            extra_data += (size,)
        if include_type:
            extra_data += (line_type,)

        if len(extra_data) > 0:
            yield (file,) + extra_data
        else:
            yield file


class HdfsReadPipe(luigi.format.InputPipeProcessWrapper):
    def __init__(self, path):
        super(HdfsReadPipe, self).__init__(['hadoop', 'fs', '-cat', path])


class HdfsAtomicWritePipe(luigi.format.OutputPipeProcessWrapper):
    """ File like object for writing to HDFS

    The referenced file is first written to a temporary location and then
    renamed to final location on close(). If close() isn't called
    the temporary file will be cleaned up when this object is
    garbage collected

    TODO: if this is buggy, change it so it first writes to a
    local temporary file and then uploads it on completion
    """

    def __init__(self, path):
        self.path = path
        self.tmppath = '/tmp/' + self.path + "-luigitemp-%08d" % random.randrange(1e9)
        tmpdir = os.path.dirname(self.tmppath)
        if subprocess.Popen(['hadoop', 'fs', '-mkdir', '-p', tmpdir]).wait():
            raise RuntimeError("Could not create directory: %s" % tmpdir)
        super(HdfsAtomicWritePipe, self).__init__(['hadoop', 'fs', '-put', '-', self.tmppath])

    def abort(self):
        print "Aborting %s('%s'). Removing temporary file '%s'" % (self.__class__.__name__, self.path, self.tmppath)
        super(HdfsAtomicWritePipe, self).abort()
        remove(self.tmppath)

    def close(self):
        super(HdfsAtomicWritePipe, self).close()
        rename(self.tmppath, self.path)

class HdfsAtomicWriteDirPipe(luigi.format.OutputPipeProcessWrapper):
    """ Writes a data<data_extension> file to a directory at <path> """
    def __init__(self, path, data_extension):
        self.path = path
        self.tmppath = '/tmp/' + self.path + "-luigitemp-%08d" % random.randrange(1e9)
        self.datapath = self.tmppath + ("/data%s" % data_extension)
        super(HdfsAtomicWriteDirPipe, self).__init__(['hadoop', 'fs', '-put', '-', self.datapath])

    def abort(self):
        print "Aborting %s('%s'). Removing temporary dir '%s'" % (self.__class__.__name__, self.path, self.tmppath)
        super(HdfsAtomicWriteDirPipe, self).abort()
        remove(self.tmppath)

    def close(self):
        super(HdfsAtomicWriteDirPipe, self).close()
        rename(self.tmppath, self.path)


class Plain(luigi.format.Format):
    @classmethod
    def hdfs_reader(cls, path):
        return HdfsReadPipe(path)

    @classmethod
    def pipe_writer(cls, output_pipe):
        return output_pipe


class HdfsTarget(luigi.Target):
    def __init__(self, path=None, format=Plain, is_tmp=False):
        if path is None:
            assert is_tmp
            path = "/tmp/luigi-temp-%08d" % random.randrange(1e9)
        self.path = path
        self.format = format
        self.is_tmp = is_tmp
        assert ":" not in self.path  # colon is not allowed in hdfs filenames

    def __del__(self):
        #TODO: not sure is_tmp belongs in Targets construction arguments
        if self.is_tmp and self.exists():
            self.remove()

    @property
    def fn(self):
        """ Deprecated. Use path property instead """
        import warnings
        warnings.warn("target.fn is deprecated and will be removed soon\
in luigi. Use target.path instead", stacklevel=2)
        return self.path

    def get_fn(self):
        """ Deprecated. Use path property instead """
        import warnings
        warnings.warn("target.get_fn() is deprecated and will be removed soon\
in luigi. Use target.path instead", stacklevel=2)
        return self.path

    def exists(self):
        return exists(self.path)

    def glob_exists(self, expected_files):
        ls = list(listdir(self.path))
        if len(ls) == expected_files:
            return True
        return False

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            try:
                return self.format.hdfs_reader(self.path)
            except NotImplementedError:
                return self.format.pipe_reader(HdfsReadPipe(self.path))
        else:
            try:
                return self.format.hdfs_writer(self.path)
            except NotImplementedError:
                return self.format.pipe_writer(HdfsAtomicWritePipe(self.path))

    def remove(self):
        remove(self.path)

    def rename(self, path, fail_if_exists=False):
        # rename does not change self.path, so be careful with assumptions
        if isinstance(path, HdfsTarget):
            path = path.path
        if fail_if_exists and exists(path):
            raise RuntimeError('Destination exists: %s' % path)
        rename(self.path, path)

    def move(self, path, fail_if_exists=False):
        self.rename(path, fail_if_exists=fail_if_exists)

    def move_dir(self, path):
        # mkdir will fail if directory already exists, thereby ensuring atomicity
        if isinstance(path, HdfsTarget):
            path = path.path
        mkdir(path)
        rename(self.path + '/*', path)
        self.remove()

    def has_write_access(self):
        test_path = self.path + '.test_write_access-%09d' % random.randrange(1e10)
        return_value = subprocess.call(['hadoop', 'fs', '-touchz', test_path])
        if return_value != 0:
            return False
        else:
            remove(test_path, recursive=False)
            return True
