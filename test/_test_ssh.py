"""Integration tests for ssh module"""

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
