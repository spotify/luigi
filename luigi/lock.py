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
Locking functionality when launching things from the command line.
Uses a pidfile.
This prevents multiple identical workflows to be launched simultaneously.
"""
from __future__ import print_function

import hashlib
import os

from luigi import six


def getpcmd(pid):
    """
    Returns command of process.

    :param pid:
    """
    cmd = 'ps -o pid,args'
    with os.popen(cmd, 'r') as p:
        # Skip the column titles
        p.readline()
        for line in p:
            spid, scmd = line.strip().split(' ', 1)
            if int(spid) == int(pid):
                return scmd


def get_info(pid_dir, my_pid=None):
    # Check the name and pid of this process
    if my_pid is None:
        my_pid = os.getpid()

    my_cmd = getpcmd(my_pid)

    if six.PY3:
        cmd_hash = my_cmd.encode('utf8')
    else:
        cmd_hash = my_cmd

    pid_file = os.path.join(pid_dir, hashlib.md5(cmd_hash).hexdigest()) + '.pid'

    return my_pid, my_cmd, pid_file


def acquire_for(pid_dir, num_available=1, kill_signal=None):
    """
    Makes sure the process is only run once at the same time with the same name.

    Notice that we since we check the process name, different parameters to the same
    command can spawn multiple processes at the same time, i.e. running
    "/usr/bin/my_process" does not prevent anyone from launching
    "/usr/bin/my_process --foo bar".
    """

    my_pid, my_cmd, pid_file = get_info(pid_dir)

    # Check if there is a pid file corresponding to this name
    if not os.path.exists(pid_dir):
        os.mkdir(pid_dir)
        os.chmod(pid_dir, 0o777)

    pids = set()
    pid_cmds = {}
    if os.path.exists(pid_file):
        # There is such a file - read the pid and look up its process name
        pids.update(filter(None, map(str.strip, open(pid_file))))
        pid_cmds = dict((pid, getpcmd(pid)) for pid in pids)
        matching_pids = list(filter(lambda pid: pid_cmds[pid] == my_cmd, pids))

        if kill_signal is not None:
            for pid in map(int, matching_pids):
                os.kill(pid, kill_signal)
        elif len(matching_pids) >= num_available:
            # We are already running under a different pid
            print('Pid(s)', ', '.join(matching_pids), 'already running')
            return False
        else:
            # The pid belongs to something else, we could
            pass
    pid_cmds[str(my_pid)] = my_cmd

    # Write pids
    pids.add(str(my_pid))
    with open(pid_file, 'w') as f:
        f.writelines('%s\n' % (pid, ) for pid in filter(pid_cmds.__getitem__, pids))

    # Make the file writable by all
    if os.name == 'nt':
        pass
    else:
        s = os.stat(pid_file)
        if os.getuid() == s.st_uid:
            os.chmod(pid_file, s.st_mode | 0o777)

    return True
