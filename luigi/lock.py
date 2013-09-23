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

import os, sys, hashlib

def getpcmd(pid):
    ''' Returns command of process
    '''
    cmd = 'ps -p %s -o cmd=' % (pid,)
    p = os.popen(cmd, 'r')
    return p.readline().strip()

def acquire_for(pid_dir):
    ''' Makes sure the process is only run once at the same time with the same name.

    Notice that we since we check the process name, different parameters to the same
    command can spawn multiple processes at the same time, i.e. running
    "/usr/bin/my_process" does not prevent anyone from launching
    "/usr/bin/my_process --foo bar".
    '''

    # Check the name and pid of this process
    my_pid = os.getpid()
    my_cmd = getpcmd(my_pid)

    # Check if there is a pid file corresponding to this name
    if not os.path.exists(pid_dir):
        os.mkdir(pid_dir)

    pidfile = os.path.join(pid_dir, hashlib.md5(my_cmd).hexdigest()) + '.pid'

    if os.path.exists(pidfile):
        # There is such a file - read the pid and look up its process name
        try:
            pid = int(open(pidfile).readline().strip())
        except ValueError:
            pid = -1

        cmd = getpcmd(pid)

        if cmd == my_cmd:
            # We are already running under a different pid
            print 'Pid', pid, 'running'
            return False
        else:
            # The pid belongs to something else, we could 
            pass

    # Write pid
    pid = os.getpid()
    f = open(pidfile, 'w')
    f.write('%d\n' % (pid, ))
    f.close()

    return True

