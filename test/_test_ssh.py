"""Integration tests for ssh module"""
import gc
import gzip
import os
import random
import luigi.format

from luigi.contrib.ssh import RemoteContext, RemoteTarget
import unittest
import subprocess
import socket

working_ssh_host = None  # set this to a working ssh host string (e.g. "localhost") to activate integration tests
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
conn.sendall('hello')
"""


class TestRemoteContext(unittest.TestCase):
    def setUp(self):
        self.context = RemoteContext(working_ssh_host)

    def test_check_output(self):
        """ Test check_output ssh

        Assumes the running user can ssh to working_ssh_host
        """
        output = self.context.check_output(["echo", "-n", "luigi"])
        self.assertEquals(output, "luigi")

    def test_tunnel(self):
        print "Setting up remote listener..."

        remote_server_handle = self.context.Popen([
            "python", "-c", '"{0}"'.format(HELLO_SERVER_CMD)
        ], stdout=subprocess.PIPE)

        print "Setting up tunnel"
        with self.context.tunnel(2135, 2134):
            print "Tunnel up!"
            # hack to make sure the listener process is up
            # and running before we write to it
            server_output = remote_server_handle.stdout.read(5)
            self.assertEquals(server_output, "ready")
            print "Connecting to server via tunnel"
            s = socket.socket()
            s.connect(("localhost", 2135))
            print "Receiving...",
            response = s.recv(5)
            self.assertEquals(response, "hello")
            print "Closing connection"
            s.close()
            print "Waiting for listener..."
            output, _ = remote_server_handle.communicate()
            self.assertEquals(remote_server_handle.returncode, 0)
            print "Closing tunnel"


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
        self.assertEquals(file_content, "hello")

    def test_context_manager(self):
        with self.target.open('r') as f:
            file_content = f.read()

        self.assertEquals(file_content, "hello")


class TestRemoteTargetAtomicity(unittest.TestCase):
    path = '/tmp/luigi_remote_atomic_test.txt'
    ctx = RemoteContext(working_ssh_host)

    def _exists(self, path):
        try:
            self.ctx.check_output(["test", "-e", path])
        except subprocess.CalledProcessError, e:
            if e.returncode == 1:
                return False
            else:
                raise
        return True

    def setUp(self):
        self.ctx.check_output(["rm", "-rf", self.path])
        self.local_file = '/tmp/local_luigi_remote_atomic_test.txt'
        if os.path.exists(self.local_file):
            os.remove(self.local_file)

    def tearDown(self):
        self.ctx.check_output(["rm", "-rf", self.path])
        if os.path.exists(self.local_file):
            os.remove(self.local_file)

    def test_close(self):
        t = RemoteTarget(self.path, working_ssh_host)
        p = t.open('w')
        print >> p, 'test'
        self.assertFalse(self._exists(self.path))
        p.close()
        self.assertTrue(self._exists(self.path))

    def test_del(self):
        t = RemoteTarget(self.path, working_ssh_host)
        p = t.open('w')
        print >> p, 'test'
        tp = p.tmp_path
        del p

        self.assertFalse(self._exists(tp))
        self.assertFalse(self._exists(self.path))

    def test_write_cleanup_no_close(self):
        t = RemoteTarget(self.path, working_ssh_host)

        def context():
            f = t.open('w')
            f.write('stuff')
        context()
        gc.collect()  # force garbage collection of f variable
        self.assertFalse(t.exists())

    def test_write_cleanup_with_error(self):
        t = RemoteTarget(self.path, working_ssh_host)
        try:
            with t.open('w'):
                raise Exception('something broke')
        except:
            pass
        self.assertFalse(t.exists())

    def test_write_with_success(self):
        t = RemoteTarget(self.path, working_ssh_host)
        with t.open('w') as p:
            p.write("hello")
        self.assertTrue(t.exists())

    def test_gzip(self):
        t = RemoteTarget(self.path, working_ssh_host, luigi.format.Gzip)
        p = t.open('w')
        test_data = 'test'
        p.write(test_data)
        self.assertFalse(self._exists(self.path))
        p.close()
        self.assertTrue(self._exists(self.path))

        # Using gzip module as validation
        cmd = 'scp %s:%s %s > /dev/null 2>&1' % (working_ssh_host, self.path, self.local_file)
        assert os.system(cmd) == 0
        f = gzip.open(self.local_file, 'rb')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying our own gzip remote reader
        f = RemoteTarget(self.path, working_ssh_host, luigi.format.Gzip).open('r')
        self.assertTrue(test_data == f.read())
        f.close()


class TestRemoteTargetCreateDirectories(TestRemoteTargetAtomicity):
    path = '/tmp/%s/xyz/luigi_remote_atomic_test.txt' % random.randint(0, 999999999)


class TestRemoteTargetRelative(TestRemoteTargetAtomicity):
    path = 'luigi_remote_atomic_test.txt'
