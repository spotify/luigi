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
import re
from luigi.target import FileSystem, FileSystemTarget, FileAlreadyExists
import configuration


class HDFSCliError(Exception):
    def __init__(self, command, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        msg = ("Command %r failed [exit code %d]\n" +
               "---stdout---\n" +
               "%s\n" +
               "---stderr---\n" +
               "%s" +
               "------------") % (command, returncode, stdout, stderr)
        super(HDFSCliError, self).__init__(msg)


def call_check(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise HDFSCliError(command, p.returncode, stdout, stderr)
    return stdout


def get_hdfs_syntax():
    """
    CDH4 (hadoop 2+) has a slightly different syntax for interacting with
    hdfs via the command line. The default version is CDH4, but one can
    override this setting with "cdh3" or "apache1" in the hadoop section of the config in
    order to use the old syntax
    """
    return configuration.get_config().get("hadoop", "version", "cdh4").lower()


def load_hadoop_cmd():
    return luigi.configuration.get_config().get('hadoop', 'command', 'hadoop')


def tmppath(path=None):
    return tempfile.gettempdir() + '/' + (path + "-" if path else "") + "luigitemp-%08d" % random.randrange(1e9)


class HdfsClient(FileSystem):
    """This client uses Apache 2.x syntax for file system commands, which also matched CDH4"""
    def exists(self, path):
        """ Use `hadoop fs -stat to check file existance
        """

        cmd = [load_hadoop_cmd(), 'fs', '-stat', path]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        else:
            not_found_pattern = "^stat: `.*': No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split('\n'):
                if not_found_re.match(line):
                    return False
            raise HDFSCliError(cmd, p.returncode, stdout, stderr)

    def rename(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not self.exists(parent_dir):
            self.mkdir(parent_dir)
        call_check([load_hadoop_cmd(), 'fs', '-mv', path, dest])

    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            cmd = [load_hadoop_cmd(), 'fs', '-rm', '-r']
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-rm']

        if skip_trash:
            cmd = cmd + ['-skipTrash']

        cmd = cmd + [path]
        call_check(cmd)

    def chmod(self, path, permissions, recursive=False):
        if recursive:
            cmd = [load_hadoop_cmd(), 'fs', '-chmod', '-R', permissions, path]
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-chmod', permissions, path]
        call_check(cmd)

    def chown(self, path, owner, group, recursive=False):
        if owner is None:
            owner = ''
        if group is None:
            group = ''
        ownership = "%s:%s" % (owner, group)
        if recursive:
            cmd = [load_hadoop_cmd(), 'fs', '-chown', '-R', ownership, path]
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-chown', ownership, path]
        call_check(cmd)

    def count(self, path):
        cmd = [load_hadoop_cmd(), 'fs', '-count', path]
        stdout = call_check(cmd)
        (dir_count, file_count, content_size, ppath) = stdout.split()
        results = {'content_size': content_size, 'dir_count': dir_count, 'file_count': file_count}
        return results

    def copy(self, path, destination):
        call_check([load_hadoop_cmd(), 'fs', '-cp', path, destination])

    def put(self, local_path, destination):
        call_check([load_hadoop_cmd(), 'fs', '-put', local_path, destination])

    def get(self, path, local_destination):
        call_check([load_hadoop_cmd(), 'fs', '-get', path, local_destination])

    def getmerge(self, path, local_destination, new_line=False):
        if new_line:
            cmd = [load_hadoop_cmd(), 'fs', '-getmerge', '-nl', path, local_destination]
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-getmerge', path, local_destination]
        call_check(cmd)

    def mkdir(self, path):
        try:
            call_check([load_hadoop_cmd(), 'fs', '-mkdir', path])
        except HDFSCliError, ex:
            if "File exists" in ex.stderr:
                raise FileAlreadyExists(ex.stderr)
            else:
                raise

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        if not path:
            path = "."  # default to current/home catalog

        if recursive:
            cmd = [load_hadoop_cmd(), 'fs', '-ls', '-R', path]
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-ls', path]
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

class HdfsClientCdh3(HdfsClient):
    """This client uses CDH3 syntax for file system commands"""
    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            cmd = [load_hadoop_cmd(), 'fs', '-rmr']
        else:
            cmd = [load_hadoop_cmd(), 'fs', '-rm']

        if skip_trash:
            cmd = cmd + ['-skipTrash']

        cmd = cmd + [path]
        call_check(cmd)

class HdfsClientApache1(HdfsClientCdh3):
    """This client uses Apache 1.x syntax for file system commands,
    which are similar to CDH3 except for the file existence check"""
    def exists(self, path):
        cmd = [load_hadoop_cmd(), 'fs', '-test', '-e', path]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        elif p.returncode == 1:
            return False
        else:
            raise HDFSCliError(cmd, p.returncode, stdout, stderr)

if get_hdfs_syntax() == "cdh4":
    client = HdfsClient()
elif get_hdfs_syntax() == "cdh3":
    client = HdfsClientCdh3()
elif get_hdfs_syntax() == "apache1":
    client = HdfsClientApache1()
else:
    raise Exception("Error: Unknown version specified in Hadoop version configuration parameter")

exists = client.exists
rename = client.rename
remove = client.remove
mkdir = client.mkdir
listdir = client.listdir


class HdfsReadPipe(luigi.format.InputPipeProcessWrapper):
    def __init__(self, path):
        super(HdfsReadPipe, self).__init__([load_hadoop_cmd(), 'fs', '-cat', path])


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
        if get_hdfs_syntax() == "cdh4":
            if subprocess.Popen([load_hadoop_cmd(), 'fs', '-mkdir', '-p', tmpdir]).wait():
                raise RuntimeError("Could not create directory: %s" % tmpdir)
        else:
            if not exists(tmpdir) and subprocess.Popen([load_hadoop_cmd(), 'fs', '-mkdir', tmpdir]).wait():
                raise RuntimeError("Could not create directory: %s" % tmpdir)
        super(HdfsAtomicWritePipe, self).__init__([load_hadoop_cmd(), 'fs', '-put', '-', self.tmppath])

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
        super(HdfsAtomicWriteDirPipe, self).__init__([load_hadoop_cmd(), 'fs', '-put', '-', self.datapath])

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


class HdfsTarget(FileSystemTarget):
    fs = client  # underlying file system

    def __init__(self, path=None, format=Plain, is_tmp=False):
        if path is None:
            assert is_tmp
            path = tmppath()
        super(HdfsTarget, self).__init__(path)
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

    def remove(self, skip_trash=False):
        remove(self.path, skip_trash=skip_trash)

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
                path = "/".join(parts[0:length - part]) + "/"
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
        return_value = subprocess.call([load_hadoop_cmd(), 'fs', '-touchz', test_path])
        if return_value != 0:
            return False
        else:
            remove(test_path, recursive=False)
            return True
