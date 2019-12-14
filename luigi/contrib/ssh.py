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
Light-weight remote execution library and utilities.

There are some examples in the unittest but I added another that is more
luigi-specific in the examples directory (examples/ssh_remote_execution.py)

:class:`RemoteContext` is meant to provide functionality similar to that of the
standard library subprocess module, but where the commands executed are run on
a remote machine instead, without the user having to think about prefixing
everything with "ssh" and credentials etc.

Using this mini library (which is just a convenience wrapper for subprocess),
:class:`RemoteTarget` is created to let you stream data from a remotely stored file using
the luigi :class:`~luigi.target.FileSystemTarget` semantics.

As a bonus, :class:`RemoteContext` also provides a really cool feature that let's you
set up ssh tunnels super easily using a python context manager (there is an example
in the integration part of unittests).

This can be super convenient when you want secure communication using a non-secure
protocol or circumvent firewalls (as long as they are open for ssh traffic).
"""

import contextlib
import logging
import os
import random
import subprocess
import posixpath

import luigi
import luigi.format
import luigi.target


logger = logging.getLogger('luigi-interface')


class RemoteCalledProcessError(subprocess.CalledProcessError):
    def __init__(self, returncode, command, host, output=None):
        super(RemoteCalledProcessError, self).__init__(returncode, command, output)
        self.host = host

    def __str__(self):
        return "Command '%s' on host %s returned non-zero exit status %d" % (
            self.cmd, self.host, self.returncode)


class RemoteContext:

    def __init__(self, host, **kwargs):
        self.host = host
        self.username = kwargs.get('username', None)
        self.key_file = kwargs.get('key_file', None)
        self.connect_timeout = kwargs.get('connect_timeout', None)
        self.port = kwargs.get('port', None)
        self.no_host_key_check = kwargs.get('no_host_key_check', False)
        self.sshpass = kwargs.get('sshpass', False)
        self.tty = kwargs.get('tty', False)

    def __repr__(self):
        return '%s(%r, %r, %r, %r, %r)' % (
            type(self).__name__, self.host, self.username, self.key_file, self.connect_timeout, self.port)

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __hash__(self):
        return hash(repr(self))

    def _host_ref(self):
        if self.username:
            return "{0}@{1}".format(self.username, self.host)
        else:
            return self.host

    def _prepare_cmd(self, cmd):
        connection_cmd = ["ssh", self._host_ref(), "-o", "ControlMaster=no"]
        if self.sshpass:
            connection_cmd = ["sshpass", "-e"] + connection_cmd
        else:
            connection_cmd += ["-o", "BatchMode=yes"]  # no password prompts etc
        if self.port:
            connection_cmd.extend(["-p", self.port])

        if self.connect_timeout is not None:
            connection_cmd += ['-o', 'ConnectTimeout=%d' % self.connect_timeout]

        if self.no_host_key_check:
            connection_cmd += ['-o', 'UserKnownHostsFile=/dev/null',
                               '-o', 'StrictHostKeyChecking=no']

        if self.key_file:
            connection_cmd.extend(["-i", self.key_file])

        if self.tty:
            connection_cmd.append('-t')
        return connection_cmd + cmd

    def Popen(self, cmd, **kwargs):
        """
        Remote Popen.
        """
        prefixed_cmd = self._prepare_cmd(cmd)
        return subprocess.Popen(prefixed_cmd, **kwargs)

    def check_output(self, cmd):
        """
        Execute a shell command remotely and return the output.

        Simplified version of Popen when you only want the output as a string and detect any errors.
        """
        p = self.Popen(cmd, stdout=subprocess.PIPE)
        output, _ = p.communicate()
        if p.returncode != 0:
            raise RemoteCalledProcessError(p.returncode, cmd, self.host, output=output)
        return output

    @contextlib.contextmanager
    def tunnel(self, local_port, remote_port=None, remote_host="localhost"):
        """
        Open a tunnel between localhost:local_port and remote_host:remote_port via the host specified by this context.

        Remember to close() the returned "tunnel" object in order to clean up
        after yourself when you are done with the tunnel.
        """
        tunnel_host = "{0}:{1}:{2}".format(local_port, remote_host, remote_port)
        proc = self.Popen(
            # cat so we can shut down gracefully by closing stdin
            ["-L", tunnel_host, "echo -n ready && cat"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        # make sure to get the data so we know the connection is established
        ready = proc.stdout.read(5)
        assert ready == b"ready", "Didn't get ready from remote echo"
        yield  # user code executed here
        proc.communicate()
        assert proc.returncode == 0, "Tunnel process did an unclean exit (returncode %s)" % (proc.returncode,)


class RemoteFileSystem(luigi.target.FileSystem):

    def __init__(self, host, **kwargs):
        self.remote_context = RemoteContext(host, **kwargs)

    def exists(self, path):
        """
        Return `True` if file or directory at `path` exist, False otherwise.
        """
        try:
            self.remote_context.check_output(["test", "-e", path])
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return False
            else:
                raise
        return True

    def listdir(self, path):
        while path.endswith('/'):
            path = path[:-1]

        path = path or '.'
        listing = self.remote_context.check_output(["find", "-L", path, "-type", "f"]).splitlines()
        return [v.decode('utf-8') for v in listing]

    def isdir(self, path):
        """
        Return `True` if directory at `path` exist, False otherwise.
        """
        try:
            self.remote_context.check_output(["test", "-d", path])
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return False
            else:
                raise
        return True

    def remove(self, path, recursive=True):
        """
        Remove file or directory at location `path`.
        """
        if recursive:
            cmd = ["rm", "-r", path]
        else:
            cmd = ["rm", path]

        self.remote_context.check_output(cmd)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            elif not self.isdir(path):
                raise luigi.target.NotADirectory()
            else:
                return

        if parents:
            cmd = ['mkdir', '-p', path]
        else:
            cmd = ['mkdir', path, '2>&1']

        try:
            self.remote_context.check_output(cmd)
        except subprocess.CalledProcessError as e:
            if b'no such file' in e.output.lower():
                raise luigi.target.MissingParentDirectory()
            raise

    def _scp(self, src, dest):
        cmd = ["scp", "-q", "-C", "-o", "ControlMaster=no"]
        if self.remote_context.sshpass:
            cmd = ["sshpass", "-e"] + cmd
        else:
            cmd.append("-B")
        if self.remote_context.no_host_key_check:
            cmd.extend(['-o', 'UserKnownHostsFile=/dev/null',
                        '-o', 'StrictHostKeyChecking=no'])
        if self.remote_context.key_file:
            cmd.extend(["-i", self.remote_context.key_file])
        if self.remote_context.port:
            cmd.extend(["-P", self.remote_context.port])
        if os.path.isdir(src):
            cmd.extend(["-r"])
        cmd.extend([src, dest])
        p = subprocess.Popen(cmd)
        output, _ = p.communicate()
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd, output=output)

    def put(self, local_path, path):
        # create parent folder if not exists
        normpath = posixpath.normpath(path)
        folder = os.path.dirname(normpath)
        if folder and not self.exists(folder):
            self.remote_context.check_output(['mkdir', '-p', folder])

        tmp_path = path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        self._scp(local_path, "%s:%s" % (self.remote_context._host_ref(), tmp_path))
        self.remote_context.check_output(['mv', tmp_path, path])

    def get(self, path, local_path):
        # Create folder if it does not exist
        normpath = os.path.normpath(local_path)
        folder = os.path.dirname(normpath)
        if folder:
            try:
                os.makedirs(folder)
            except OSError:
                pass

        tmp_local_path = local_path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        self._scp("%s:%s" % (self.remote_context._host_ref(), path), tmp_local_path)
        os.rename(tmp_local_path, local_path)


class AtomicRemoteFileWriter(luigi.format.OutputPipeProcessWrapper):

    def __init__(self, fs, path):
        self._fs = fs
        self.path = path

        # create parent folder if not exists
        normpath = os.path.normpath(self.path)
        folder = os.path.dirname(normpath)
        if folder:
            self.fs.mkdir(folder)

        self.__tmp_path = self.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        super(AtomicRemoteFileWriter, self).__init__(
            self.fs.remote_context._prepare_cmd(['cat', '>', self.__tmp_path]))

    def __del__(self):
        super(AtomicRemoteFileWriter, self).__del__()

        try:
            if self.fs.exists(self.__tmp_path):
                self.fs.remote_context.check_output(['rm', self.__tmp_path])
        except Exception:
            # Don't propagate the exception; bad things can happen.
            logger.exception('Failed to delete in-flight file')

    def close(self):
        super(AtomicRemoteFileWriter, self).close()
        self.fs.remote_context.check_output(['mv', self.__tmp_path, self.path])

    @property
    def tmp_path(self):
        return self.__tmp_path

    @property
    def fs(self):
        return self._fs


class RemoteTarget(luigi.target.FileSystemTarget):
    """
    Target used for reading from remote files.

    The target is implemented using ssh commands streaming data over the network.
    """

    def __init__(self, path, host, format=None, **kwargs):
        super(RemoteTarget, self).__init__(path)
        if format is None:
            format = luigi.format.get_default_format()
        self.format = format
        self._fs = RemoteFileSystem(host, **kwargs)

    @property
    def fs(self):
        return self._fs

    def open(self, mode='r'):
        if mode == 'w':
            file_writer = AtomicRemoteFileWriter(self.fs, self.path)
            if self.format:
                return self.format.pipe_writer(file_writer)
            else:
                return file_writer
        elif mode == 'r':
            file_reader = luigi.format.InputPipeProcessWrapper(
                self.fs.remote_context._prepare_cmd(["cat", self.path]))
            if self.format:
                return self.format.pipe_reader(file_reader)
            else:
                return file_reader
        else:
            raise Exception("mode must be 'r' or 'w' (got: %s)" % mode)

    def put(self, local_path):
        self.fs.put(local_path, self.path)

    def get(self, local_path):
        self.fs.get(self.path, local_path)
