from collections import defaultdict
import luigi
from luigi.contrib.ssh import RemoteContext, RemoteTarget
from luigi.mock import MockFile

SSH_HOST = "some.accessible.host"


class CreateRemoteData(luigi.Task):
    """ Dump info on running processes on remote host.
    Data is still stored on the remote host
    """
    def output(self):
        return RemoteTarget(
            "/tmp/stuff",
            SSH_HOST
        )

    def run(self):
        remote = RemoteContext(SSH_HOST)
        print remote.check_output([
            "ps aux > {0}".format(self.output().path)
        ])


class ProcessRemoteData(luigi.Task):
    """ Create a toplist of users based on how many running processed they have
        on a remote machine

    In this example the processed data is stored in a MockFile
    """
    def requires(self):
        return CreateRemoteData()

    def run(self):
        processes_per_user = defaultdict(int)
        with self.input().open('r') as infile:
            for line in infile:
                username = line.split()[0]
                processes_per_user[username] += 1

        toplist = sorted(
            processes_per_user.iteritems(),
            key=lambda x: x[1],
            reverse=True
        )

        with self.output().open('w') as outfile:
            for user, n_processes in toplist:
                print >> outfile, n_processes, user

    def output(self):
        return MockFile("output", mirror_on_stderr=True)


if __name__ == "__main__":
    luigi.run()