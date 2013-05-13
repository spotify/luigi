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
import re
import os
import random
import tempfile
import urlparse
import luigi.format
import datetime
import snakebite.client
import luigi.interface


class HDFSCliError(Exception):
    def __init__(self, command, returncode, stdout, stderr):
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


luigi_config = luigi.interface.LuigiConfigParser.instance()


def get_config_property(config_filename, property):
    """Get a hadoop config property value from xml config file

    Args:
        config_name (str): Filename of the config to read, e.g 'hdfs-site.xml'
        property (str): The name of the property to get
    """
    hadoop_conf_dir = luigi_config.get('hadoop', 'confdir', None)
    if hadoop_conf_dir is None:
        hadoop_conf_dir = os.path.join(os.environ.get('HADOOP_HOME'), 'conf')
    if hadoop_conf_dir is None:
        hadoop_conf_dir = '/etc/hadoop/conf'
    hdfs_conf = os.path.join(hadoop_conf_dir, config_filename)
    if not os.path.exists(hdfs_conf):
        return None

    from xml.etree import ElementTree
    tree = ElementTree.parse(hdfs_conf)
    root = tree.getroot()
    for prop in root.findall('property'):
        name_element = prop.find('name')
        if name_element is None:
            return None
        if name_element.text == property:
            value_element = prop.find('value')
            if value_element is not None:
                return value_element.text
    return None


hdfs_re = re.compile('hdfs://([^:]+):(\d+)')


def get_namenode_details():
    namenode = luigi_config.get('hdfs', 'namenode_host', None)
    port = luigi_config.get('hdfs', 'namenode_port', None)
    if namenode is None or port is None:
        path = get_config_property('core-site.xml', 'fs.defaultFS')
        match = hdfs_re.match(path)
        return match.group(1), match.group(2)
    else:
        return (namenode, port)


class HdfsClient(object):
    def exists(self, path):
        """ Use `hadoop fs -ls -d` to check file existence

        `hadoop fs -test -e` can't be (reliably) used at this time since there
        is no good way of distinguishing file non-existence of files
        from errors when running the comman (same return code)
        """

        cmd = ['hadoop', 'fs', '-ls', '-d', path]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        else:
            not_found_pattern = "^ls: `.*': No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split('\n'):
                if not_found_re.match(line):
                    return False
            raise HDFSCliError(cmd, p.returncode, stdout, stderr)

    def rename(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not self.exists(parent_dir):
            self.mkdir(parent_dir)
        call_check(['hadoop', 'fs', '-mv', path, dest])

    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            if use_cdh4_syntax():
                cmd = ['hadoop', 'fs', '-rm', '-r']
            else:
                cmd = ['hadoop', 'fs', '-rmr']
        else:
            cmd =  ['hadoop', 'fs', '-rm']
        if skip_trash:
             cmd = cmd + ['-skipTrash']
        cmd = cmd + [path]
        call_check(cmd)

    def chmod(self, path, permissions, recursive=False):
        if recursive:
            cmd = ['hadoop', 'fs', '-chmod', '-R', permissions, path]
        else:
            cmd = ['hadoop', 'fs', '-chmod', permissions, path]
        call_check(cmd)

    def chown(self, path, owner, group, recursive=False):
        if owner is None:
            owner = ''
        if group is None:
            group = ''
        ownership = "%s:%s" % (owner, group)
        if recursive:
            cmd = ['hadoop', 'fs', '-chown', '-R', ownership, path]
        else:
            cmd = ['hadoop', 'fs', '-chown', ownership, path]
        call_check(cmd)

    def count(self, path):
        cmd = ['hadoop', 'fs', '-count', path]
        stdout = call_check(cmd)
        (dir_count, file_count, content_size, ppath) = stdout.split()
        results = {'content_size': content_size, 'dir_count': dir_count, 'file_count': file_count}
        return results

    def copy(self, path, destination):
        call_check(['hadoop', 'fs', '-cp', path, destination])

    def put(self, local_path, destination):
        call_check(['hadoop', 'fs', '-put', local_path, destination])

    def get(self, path, local_destination):
        call_check(['hadoop', 'fs', '-get', path, local_destination])

    def getmerge(self, path, local_destination, new_line=False):
        if new_line:
            cmd = ['hadoop', 'fs', '-getmerge', '-nl', path, local_destination]
        else:
            cmd = ['hadoop', 'fs', '-getmerge', path, local_destination]
        call_check(cmd)

    def mkdir(self, path):
        call_check(['hadoop', 'fs', '-mkdir', path])

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        if not path:
            path = "."  # default to current/home catalog

        if recursive:
            cmd = ['hadoop', 'fs', '-ls', '-R', path]
        else:
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


class SnakebiteHdfsClient(object):
    @staticmethod
    def convert_permission_to_string(file_type, permission):
        def number_to_permission_string(permission_number):
            x = 'x' if permission_number % 2 else '-'
            permission_number /= 2
            w = 'w' if permission_number % 2 else '-'
            permission_number /= 2
            r = 'r' if permission_number % 2 else '-'
            return r + w + x
        permission = int(oct(permission))
        file_flag = file_type if file_type == 'd' else '-'
        everyone = number_to_permission_string(permission % 10)
        permission /= 10
        group = number_to_permission_string(permission % 10)
        permission /= 10
        owner = number_to_permission_string(permission % 10)
        return file_flag + owner + group + everyone

    def __init__(self):
        namenode, port = get_namenode_details()
        self.client = snakebite.client.Client(namenode, port)

    def exists(self, path):
        """ Use `hadoop fs -ls -d` to check file existence

        `hadoop fs -test -e` can't be (reliably) used at this time since there
        is no good way of distinguishing file non-existence of files
        from errors when running the comman (same return code)
        """
        return all(self.client.test(path, exists=True))

    def rename(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not self.exists(parent_dir):
            self.mkdir(parent_dir)
        self.client.rename(path, dest)

    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            if use_cdh4_syntax():
                cmd = ['hadoop', 'fs', '-rm', '-r']
            else:
                cmd = ['hadoop', 'fs', '-rmr']
        else:
            cmd = ['hadoop', 'fs', '-rm']
        if skip_trash:
            cmd = cmd + ['-skipTrash']
        cmd = cmd + [path]
        call_check(cmd)

    def chmod(self, path, permissions, recursive=False):
        list(self.client.chmod([path], permissions, recurse=recursive))

    def chown(self, path, owner, group, recursive=False):
        if owner is None:
            owner = ''
        if group is None:
            group = ''
        ownership = "%s:%s" % (owner, group)
        self.client.chown([path], ownership, recurse=recursive)

    def count(self, path):
        client_results = self.client.count([path])
        return {
            'content_size': client_results['spaceConsumed'],
            'dir_count': client_results['directoryCount'],
            'file_count': client_results['fileCount']
        }

    def copy(self, path, destination):
        call_check(['hadoop', 'fs', '-cp', path, destination])

    def put(self, local_path, destination):
        call_check(['hadoop', 'fs', '-put', local_path, destination])

    def get(self, path, local_destination):
        call_check(['hadoop', 'fs', '-get', path, local_destination])

    def getmerge(self, path, local_destination, new_line=False):
        if new_line:
            cmd = ['hadoop', 'fs', '-getmerge', '-nl', path, local_destination]
        else:
            cmd = ['hadoop', 'fs', '-getmerge', path, local_destination]
        call_check(cmd)

    def mkdir(self, path):
        list(self.client.mkdir([path], create_parent=True))

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        if not path:
            path = "."  # default to current/home catalog

        for result in self.client.ls([path], recurse=recursive):
            file_type = result['file_type']
            if ignore_directories and file_type == 'd':
                continue
            if ignore_files and file_type == 'f':
                continue
            file = result['path']
            extra_data = ()
            if include_size:
                extra_data += (result['length'],)
            if include_type:
                extra_data += (SnakebiteHdfsClient.convert_permission_to_string(file_type, result['permission']),)
            if include_time:
                extra_data += (result['modification_time'],)
            if len(extra_data) > 0:
                yield (file,) + extra_data
            else:
                yield file

HDFS_CLIENT_MAP = {
    'default': HdfsClient,
    'snakebite': SnakebiteHdfsClient,
    'hdfsclient': HdfsClient,
}

hdfs_client_value = luigi_config.get('hdfs', 'client', default='default')
client = HDFS_CLIENT_MAP[hdfs_client_value]()

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
        return_value = subprocess.call(['hadoop', 'fs', '-touchz', test_path])
        if return_value != 0:
            return False
        else:
            remove(test_path, recursive=False)
            return True
