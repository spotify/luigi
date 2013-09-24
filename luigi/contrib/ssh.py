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

"""
Light-weight remote execution library and utilities

There are some examples in the unittest, but I added another more luigi-specific in the examples directory (examples/ssh_remote_execution.py

contrib.ssh.RemoteContext is meant to provide functionality similar to that of the standard library subprocess module, but where the commands executed are run on a remote machine instead, without the user having to think about prefixing everything with "ssh" and credentials etc.

Using this mini library (which is just a convenience wrapper for subprocess),
RemoteTarget is created to let you stream data from a remotely stored file using
the luigi FileSystemTarget semantics.

As a bonus, RemoteContext also provides a really cool feature that let's you
set up ssh tunnels super easily using a python context manager (there is an example
in the integration part of unittests).

This can be super convenient when you want secure communication using a non-secure
protocol or circumvent firewalls (as long as they are open for ssh traffic).
"""

import luigi
import luigi.target
import luigi.format
import subprocess
import contextlib


class RemoteContext(object):
    def __init__(self, host, username=None, key_file=None):
        self.host = host
        self.username = username
        self.key_file = key_file

    def _prepare_cmd(self, cmd):
        if self.username:
            host = "{0}@{1}".format(self.username, self.host)
        else:
            host = self.host

        connection_cmd = ["ssh", host,
                          "-S", "none",  # disable ControlMaster since it causes all sorts of weird behaviour with subprocesses...
                          "-o", "BatchMode=yes",  # no password prompts etc
                          ]

        if self.key_file:
            connection_cmd.extend(["-i", self.key_file])
        return connection_cmd + cmd

    def Popen(self, cmd, **kwargs):
        """ Remote Popen """
        prefixed_cmd = self._prepare_cmd(cmd)
        return subprocess.Popen(prefixed_cmd, **kwargs)

    def check_output(self, cmd):
        """ Execute a shell command remotely and return the output

        Simplified version of Popen when you only want the output as a string and detect any errors
        """
        p = self.Popen(cmd, stdout=subprocess.PIPE)
        output, _ = p.communicate()
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd)
        return output

    @contextlib.contextmanager
    def tunnel(self, local_port, remote_port=None, remote_host="localhost"):
        """ Open a tunnel between localhost:local_port and remote_host:remote_port via the host specified by this context

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
        assert ready == "ready", "Didn't get ready from remote echo"
        yield  # user code executed here
        proc.communicate()
        assert proc.returncode == 0, "Tunnel process did an unclean exit (returncode %s)" % (proc.returncode,)


class RemoteFileSystem(luigi.target.FileSystem):
    def __init__(self, host, username=None, key_file=None):
        self.remote_context = RemoteContext(host, username, key_file)

    def exists(self, path):
        """ Return `True` if file or directory at `path` exist, False otherwise """
        try:
            self.remote_context.check_output(["test", "-e", path])
        except subprocess.CalledProcessError, e:
            if e.returncode == 1:
                return False
            else:
                raise
        return True

    def remove(self, path, recursive=True):
        """ Remove file or directory at location `path` """
        if recursive:
            cmd = ["rm", "-r", path]
        else:
            cmd = ["rm", path]

        self.remote_context.check_output(cmd)


class RemoteTarget(luigi.target.FileSystemTarget):
    """
    Target used for reading from remote files. The target is implemented using
    ssh commands streaming data over the network.
    """
    def __init__(self, path, host, format=None, username=None, key_file=None):
        self.path = path
        self.format = format
        self._fs = RemoteFileSystem(host, username, key_file)

    @property
    def fs(self):
        return self._fs

    def open(self, mode='r'):
        if mode == 'w':
            raise NotImplementedError("Writing to remote files it not yet implemented")
        elif mode == 'r':
            file_reader = luigi.format.InputPipeProcessWrapper(
                self.fs.remote_context._prepare_cmd(["cat", self.path]))
            if self.format:
                return self.format.pipe_reader(file_reader)
            else:
                return file_reader
        else:
            raise Exception("mode must be r/w")
