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

from __future__ import with_statement
import os
import signal
import random


def check_pid(pidfile):
    if pidfile and os.path.exists(pidfile):
        try:
            pid = int(open(pidfile).read().strip())
            os.kill(pid, 0)
            return pid
        except:
            return 0
    return 0


def write_pid(pidfile):
    print "Writing pid file"
    with open(pidfile, 'w') as fobj:
        fobj.write(str(os.getpid()))


def daemonize(cmd, pidfile=None):
    import daemon
    existing_pid = check_pid(pidfile)
    if pidfile and existing_pid:
        print "Server already running (pid=%s)" % (existing_pid,)
        return
    log = open('/var/log/luigi/luigi-server.log', 'a+')  # TODO: better log location...
    ctx = daemon.DaemonContext(stdout=log, stderr=log, working_directory='.')
    with ctx:
        if pidfile:
            print "Checking pid file"
            existing_pid = check_pid(pidfile)
            if not existing_pid:
                write_pid(pidfile)
                cmd()
            else:
                print "Server already running (pid=%s)" % (existing_pid,)
                return
        else:
            cmd()


def fork_linked_workers(num_processes):
    """ Forks num_processes child processes.

    Returns an id between 0 and num_processes - 1 for each child process.
    Will consume the parent process and kill it and all child processes as soon as one child exits with status 0

    If a child dies with exist status != 0 it will be restarted.
    TODO: If the parent is force-terminated (kill -9) the child processes will terminate after a while when they notice it.
    """

    children = {}  # keep child indices

    def shutdown_handler(signum=None, frame=None):
        print "Parent shutting down. Killing ALL THE children"
        if not signum:
            signum = signal.SIGTERM
        for c in children:
            print "Killing child %d" % c
            try:
                os.kill(c, signum)
                os.waitpid(c, 0)
            except OSError:
                print "Child %d is already dead" % c
                pass
        os._exit(0)  # exit without calling exit handler again...

    sigs = [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]
    for s in sigs:
        signal.signal(s, shutdown_handler)
        signal.signal(s, shutdown_handler)
        signal.signal(s, shutdown_handler)
    #haven't found a way to unregister: atexit.register(shutdown_handler) #

    def fork_child(child_id, attempt):
        child_pid = os.fork()

        if not child_pid:
            random.seed(os.getpid())
            for s in sigs:
                signal.signal(s, signal.SIG_DFL)  # only want these signal handlers in the parent process
            return True  # in child

        children[child_pid] = (child_id, attempt)
        return False  # in parent

    for i in xrange(num_processes):
        child_id = len(children)
        if fork_child(child_id, 0):
            return child_id, 0

    assert len(children) == num_processes

    while 1:
        pid, status = os.wait()
        if status != 0:
            # unclean exit, restart process
            child_id, last_attempt = children.pop(pid)
            attempt = last_attempt + 1
            if fork_child(child_id, attempt):
                return child_id, attempt
        else:
            shutdown_handler()
            exit(0)
