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

import errno
import hashlib
import os
import sys
from subprocess import Popen, PIPE


def getpcmd(pid):
    """
    Returns command of process.

    :param pid:
    """
    if os.name == "nt":
        # Use wmic command instead of ps on Windows.
        cmd = 'wmic path win32_process where ProcessID=%s get Commandline 2> nul' % (pid, )
        with os.popen(cmd, 'r') as p:
            lines = [line for line in p.readlines() if line.strip("\r\n ") != ""]
            if lines:
                _, val = lines
                return val
    elif sys.platform == "darwin":
        # Use pgrep instead of /proc on macOS.
        pidfile = ".%d.pid" % (pid, )
        with open(pidfile, 'w') as f:
            f.write(str(pid))
        try:
            p = Popen(['pgrep', '-lf', '-F', pidfile], stdout=PIPE)
            stdout, _ = p.communicate()
            line = stdout.decode('utf8').strip()
            if line:
                _, scmd = line.split(' ', 1)
                return scmd
        finally:
            os.unlink(pidfile)
    else:
        # Use the /proc filesystem
        # At least on android there have been some issues with not all
        # process infos being readable. In these cases using the `ps` command
        # worked. See the pull request at
        # https://github.com/spotify/luigi/pull/1876
        try:
            with open('/proc/{0}/cmdline'.format(pid), 'r') as fh:
                return fh.read().replace('\0', ' ').rstrip()
        except IOError:
            # the system may not allow reading the command line
            # of a process owned by another user
            pass

    # Fallback instead of None, for e.g. Cygwin where -o is an "unknown option" for the ps command:
    return '[PROCESS_WITH_PID={}]'.format(pid)


def get_info(pid_dir, my_pid=None):
    # Check the name and pid of this process
    if my_pid is None:
        my_pid = os.getpid()

    my_cmd = getpcmd(my_pid)
    cmd_hash = my_cmd.encode('utf8')
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

    # Create a pid file if it does not exist
    try:
        os.mkdir(pid_dir)
        os.chmod(pid_dir, 0o777)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        pass

    # Let variable "pids" be all pids who exist in the .pid-file who are still
    # about running the same command.
    pids = {pid for pid in _read_pids_file(pid_file) if getpcmd(pid) == my_cmd}

    if kill_signal is not None:
        for pid in pids:
            os.kill(pid, kill_signal)
        print('Sent kill signal to Pids: {}'.format(pids))
        # We allow for the killer to progress, yet we don't want these to stack
        # up! So we only allow it once.
        num_available += 1

    if len(pids) >= num_available:
        # We are already running under a different pid
        print('Pid(s) {} already running'.format(pids))
        if kill_signal is not None:
            print('Note: There have (probably) been 1 other "--take-lock"'
                  ' process which continued to run! Probably no need to run'
                  ' this one as well.')
        return False

    _write_pids_file(pid_file, pids | {my_pid})

    return True


def _read_pids_file(pid_file):
    # First setup a python 2 vs 3 compatibility
    # http://stackoverflow.com/a/21368622/621449
    try:
        FileNotFoundError  # noqa: F823
    except NameError:
        # Should only happen on python 2
        FileNotFoundError = IOError
    # If the file happen to not exist, simply return
    # an empty set()
    try:
        with open(pid_file, 'r') as f:
            return {int(pid_str.strip()) for pid_str in f if pid_str.strip()}
    except FileNotFoundError:
        return set()


def _write_pids_file(pid_file, pids_set):
    with open(pid_file, 'w') as f:
        f.writelines('{}\n'.format(pid) for pid in pids_set)

    # Make the .pid-file writable by all (when the os allows for it)
    if os.name != 'nt':
        s = os.stat(pid_file)
        if os.getuid() == s.st_uid:
            os.chmod(pid_file, s.st_mode | 0o777)
