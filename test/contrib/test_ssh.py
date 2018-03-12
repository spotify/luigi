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
Integration tests for ssh module.
"""

from __future__ import print_function

import os
import random
import socket
import subprocess
from helpers import unittest
import target_test

from luigi.contrib.ssh import RemoteContext, RemoteFileSystem, RemoteTarget, RemoteCalledProcessError
from luigi.target import MissingParentDirectory, FileAlreadyExists

working_ssh_host = os.environ.get('SSH_TEST_HOST', 'localhost')
# set this to a working ssh host string (e.g. "localhost") to activate integration tests
# The following tests require a working ssh server at `working_ssh_host`
# the test runner can ssh into using password-less authentication

# since `nc` has different syntax on different platforms
# we use a short python command to start
# a 'hello'-server on the remote machine
HELLO_SERVER_CMD = """
import socket, sys
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('localhost', 2134))
listener.listen(1)
sys.stdout.write('ready')
sys.stdout.flush()
conn = listener.accept()[0]
conn.sendall(b'hello')
"""

try:
    x = subprocess.check_output(
        "ssh %s -S none -o BatchMode=yes 'echo 1'" % working_ssh_host,
        shell=True
    )
    if x != b'1\n':
        raise unittest.SkipTest('Not able to connect to ssh server')
except Exception:
    raise unittest.SkipTest('Not able to connect to ssh server')


class TestRemoteContext(unittest.TestCase):

    def setUp(self):
        self.context = RemoteContext(working_ssh_host)

    def tearDown(self):
        try:
            self.remote_server_handle.terminate()
        except Exception:
            pass

    def test_check_output(self):
        """ Test check_output ssh

        Assumes the running user can ssh to working_ssh_host
        """
        output = self.context.check_output(["echo", "-n", "luigi"])
        self.assertEqual(output, b"luigi")

    def test_tunnel(self):
        print("Setting up remote listener...")

        self.remote_server_handle = self.context.Popen([
            "python", "-c", '"{0}"'.format(HELLO_SERVER_CMD)
        ], stdout=subprocess.PIPE)

        print("Setting up tunnel")
        with self.context.tunnel(2135, 2134):
            print("Tunnel up!")
            # hack to make sure the listener process is up
            # and running before we write to it
            server_output = self.remote_server_handle.stdout.read(5)
            self.assertEqual(server_output, b"ready")
            print("Connecting to server via tunnel")
            s = socket.socket()
            s.connect(("localhost", 2135))
            print("Receiving...",)
            response = s.recv(5)
            self.assertEqual(response, b"hello")
            print("Closing connection")
            s.close()
            print("Waiting for listener...")
            output, _ = self.remote_server_handle.communicate()
            self.assertEqual(self.remote_server_handle.returncode, 0)
            print("Closing tunnel")


class TestRemoteTarget(unittest.TestCase):

    """ These tests assume RemoteContext working
    in order for setUp and tearDown to work
    """

    def setUp(self):
        self.ctx = RemoteContext(working_ssh_host)
        self.filepath = "/tmp/luigi_remote_test.dat"
        self.target = RemoteTarget(
            self.filepath,
            working_ssh_host,
        )
        self.ctx.check_output(["rm", "-rf", self.filepath])
        self.ctx.check_output(["echo -n 'hello' >", self.filepath])

    def tearDown(self):
        self.ctx.check_output(["rm", "-rf", self.filepath])

    def test_exists(self):
        self.assertTrue(self.target.exists())
        no_file = RemoteTarget(
            "/tmp/_file_that_doesnt_exist_",
            working_ssh_host,
        )
        self.assertFalse(no_file.exists())

    def test_remove(self):
        self.target.remove()
        self.assertRaises(
            subprocess.CalledProcessError,
            self.ctx.check_output,
            ["cat", self.filepath]
        )

    def test_open(self):
        f = self.target.open('r')
        file_content = f.read()
        f.close()
        self.assertEqual(file_content, "hello")

        self.assertTrue(self.target.fs.exists(self.filepath))
        self.assertFalse(self.target.fs.isdir(self.filepath))

    def test_context_manager(self):
        with self.target.open('r') as f:
            file_content = f.read()

        self.assertEqual(file_content, "hello")


class TestRemoteFilesystem(unittest.TestCase):
    def setUp(self):
        self.fs = RemoteFileSystem(working_ssh_host)
        self.root = '/tmp/luigi-remote-test'
        self.directory = self.root + '/dir'
        self.filepath = self.directory + '/file'
        self.target = RemoteTarget(
            self.filepath,
            working_ssh_host,
        )

        self.fs.remote_context.check_output(['rm', '-rf', self.root])
        self.addCleanup(self.fs.remote_context.check_output, ['rm', '-rf', self.root])

    def test_mkdir(self):
        self.assertFalse(self.fs.isdir(self.directory))

        self.assertRaises(MissingParentDirectory, self.fs.mkdir, self.directory, parents=False)
        self.fs.mkdir(self.directory)
        self.assertTrue(self.fs.isdir(self.directory))

        # Shouldn't throw
        self.fs.mkdir(self.directory)

        self.assertRaises(FileAlreadyExists, self.fs.mkdir, self.directory, raise_if_exists=True)

    def test_list(self):
        with self.target.open('w'):
            pass

        self.assertEqual([self.target.path], list(self.fs.listdir(self.directory)))


class TestGetAttrRecursion(unittest.TestCase):
    def test_recursion_on_delete(self):
        target = RemoteTarget("/etc/this/does/not/exist", working_ssh_host)
        with self.assertRaises(RemoteCalledProcessError):
            with target.open('w') as fh:
                fh.write("test")


class TestRemoteTargetAtomicity(unittest.TestCase, target_test.FileSystemTargetTestMixin):
    path = '/tmp/luigi_remote_atomic_test.txt'
    ctx = RemoteContext(working_ssh_host)

    def create_target(self, format=None):
        return RemoteTarget(self.path, working_ssh_host, format=format)

    def _exists(self, path):
        try:
            self.ctx.check_output(["test", "-e", path])
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return False
            else:
                raise
        return True

    def assertCleanUp(self, tp):
        self.assertFalse(self._exists(tp))

    def setUp(self):
        self.ctx.check_output(["rm", "-rf", self.path])
        self.local_file = '/tmp/local_luigi_remote_atomic_test.txt'
        if os.path.exists(self.local_file):
            os.remove(self.local_file)

    def tearDown(self):
        self.ctx.check_output(["rm", "-rf", self.path])
        if os.path.exists(self.local_file):
            os.remove(self.local_file)

    def test_put(self):
        f = open(self.local_file, 'w')
        f.write('hello')
        f.close()
        t = RemoteTarget(self.path, working_ssh_host)
        t.put(self.local_file)
        self.assertTrue(self._exists(self.path))

    def test_get(self):
        self.ctx.check_output(["echo -n 'hello' >", self.path])
        t = RemoteTarget(self.path, working_ssh_host)
        t.get(self.local_file)
        f = open(self.local_file, 'r')
        file_content = f.read()
        self.assertEqual(file_content, 'hello')

    test_move_on_fs = None  # ssh don't have move (yet?)
    test_rename_dont_move_on_fs = None  # ssh don't have move (yet?)


class TestRemoteTargetCreateDirectories(TestRemoteTargetAtomicity):
    path = '/tmp/%s/xyz/luigi_remote_atomic_test.txt' % random.randint(0, 999999999)


class TestRemoteTargetRelative(TestRemoteTargetAtomicity):
    path = 'luigi_remote_atomic_test.txt'
