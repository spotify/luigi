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
import tempfile
import urlparse
import luigi.format
import datetime

def use_cdh4_syntax():
    """
    CDH4 (hadoop 2+) has a slightly different syntax for interacting with
    hdfs via the command line. The default version is CDH4, but one can
    override this setting with "cdh3" in the hadoop section of the config in
    order to use the old syntax
    """
    import interface
    return interface.get_config().get("hadoop", "version", "cdh4").lower() == "cdh4"


def tmppath(path=None):
    return tempfile.gettempdir() + '/' + (path + "-" if path else "") + "luigitemp-%08d" % random.randrange(1e9)


class HdfsClient(object):
    def exists(self, path):
        cmd = ['hadoop', 'fs', '-test', '-e', path]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, _ = p.communicate()
        if stdout and 'No such file or directory' not in stdout:
            # TODO2: Using globs with -test -e will print 'No such file or directory'
            # when no files exists
            # TODO: Having certain stuff on the classpath might trigger this case.
            # Ideally we should be able to ignore it, because apparently there is
            # also some case where data on stdout signals an error.
            # From @freider:
            # If I remember correctly there are some cases where
            # `hadoop fs -test` exits with exit status 0 (which would mean
            # the file exists) although the file doesn't exist, if there is some
            # special error. We ran into this, that's why we added detection of
            # output of the command.
            # Afaicr this is a known bug but it hadn't been fixed in our version
            # of hadoop.
            raise RuntimeError("Command %r failed [exit code %d] because it wrote output.\n---Output---\n%s\n------------" % (cmd, p.returncode, stdout))
        elif p.returncode not in (0, 1):
            raise RuntimeError("Command %r failed with return code %s" % (cmd, p.returncode))

        if p.returncode == 0:
            return True
        elif p.returncode == 1:
            return False
        assert False


    def rename(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not exists(parent_dir):
            mkdir(parent_dir)
        cmd = ['hadoop', 'fs', '-mv', path, dest]
        if subprocess.call(cmd):
            raise RuntimeError('Command %s failed' % repr(cmd))


    def remove(self, path, recursive=True):
        if recursive:
            if use_cdh4_syntax():
                cmd = ['hadoop', 'fs', '-rm', '-r', path]
            else:
                cmd = ['hadoop', 'fs', '-rmr', path]
        else:
            cmd = ['hadoop', 'fs', '-rm', path]
        if subprocess.call(cmd):
            raise RuntimeError('Command %s failed' % repr(cmd))


    def mkdir(self, path):
        cmd = ['hadoop', 'fs', '-mkdir', path]
        if subprocess.call(cmd):
            raise RuntimeError('Command %s failed' % repr(cmd))


    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False):
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
            if include_time:
                time_str = '%sT%s' % (data[-3], data[-2])
                modification_time = datetime.datetime.strptime(time_str,
                                                               '%Y-%m-%dT%H:%M')
                extra_data += (modification_time,)

            if len(extra_data) > 0:
                yield (file,) + extra_data
            else:
                yield file

client = HdfsClient()

exists = client.exists
rename = client.rename
remove = client.remove
mkdir = client.mkdir
listdir = client.listdir

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
        self.tmppath = tmppath(self.path)
        tmpdir = os.path.dirname(self.tmppath)
        if use_cdh4_syntax():
            if subprocess.Popen(['hadoop', 'fs', '-mkdir', '-p', tmpdir]).wait():
                raise RuntimeError("Could not create directory: %s" % tmpdir)
        else:
            if not exists(tmpdir) and subprocess.Popen(['hadoop', 'fs', '-mkdir', tmpdir]).wait():
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
    def __init__(self, path, data_extension=""):
        self.path = path
        self.tmppath = tmppath(self.path)
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


class PlainDir(luigi.format.Format):
    @classmethod
    def hdfs_reader(cls, path):
        # exclude underscore-prefixedfiles/folders (created by MapReduce)
        return HdfsReadPipe("%s/[^_]*" % path)

    @classmethod
    def hdfs_writer(cls, path):
        return HdfsAtomicWriteDirPipe(path)


class HdfsTarget(luigi.Target):
    def __init__(self, path=None, format=Plain, is_tmp=False):
        if path is None:
            assert is_tmp
            path = tmppath()
        self.path = path
        self.format = format
        self.is_tmp = is_tmp
        (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)
        assert ":" not in path  # colon is not allowed in hdfs filenames

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

    def is_writable(self):
        if "/" in self.path:
            # example path: /log/ap/2013-01-17/00
            parts = self.path.split("/")
            # start with the full path and then up the tree until we can check
            length = len(parts)
            for part in xrange(length):
                path = "/".join(parts[0:length - part])+"/"
                if exists(path):
                    # if the path exists and we can write there, great!
                    if self._is_writable(path):
                        return True
                    # if it exists and we can't =( sad panda
                    else:
                        return False
            # We went through all parts of the path and we still couldn't find
            # one that exists.
            return False

    def _is_writable(self, path):
        test_path = path + '.test_write_access-%09d' % random.randrange(1e10)
        return_value = subprocess.call(['hadoop', 'fs', '-touchz', test_path])
        if return_value != 0:
            return False
        else:
            remove(test_path, recursive=False)
            return True
