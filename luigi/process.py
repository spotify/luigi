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
Contains some helper functions to run luigid in daemon mode
"""
from __future__ import print_function

import datetime
import logging
import logging.handlers
import os

rootlogger = logging.getLogger()
server_logger = logging.getLogger("luigi.server")


def check_pid(pidfile):
    if pidfile and os.path.exists(pidfile):
        try:
            pid = int(open(pidfile).read().strip())
            os.kill(pid, 0)
            return pid
        except BaseException:
            return 0
    return 0


def write_pid(pidfile):
    server_logger.info("Writing pid file")
    piddir = os.path.dirname(pidfile)
    if piddir != '':
        try:
            os.makedirs(piddir)
        except OSError:
            pass

    with open(pidfile, 'w') as fobj:
        fobj.write(str(os.getpid()))


def get_log_format():
    return "%(asctime)s %(name)s[%(process)s] %(levelname)s: %(message)s"


def get_spool_handler(filename):
    handler = logging.handlers.TimedRotatingFileHandler(
        filename=filename,
        when='d',
        encoding='utf8',
        backupCount=7  # keep one week of historical logs
    )
    formatter = logging.Formatter(get_log_format())
    handler.setFormatter(formatter)
    return handler


def _server_already_running(pidfile):
    existing_pid = check_pid(pidfile)
    if pidfile and existing_pid:
        return True
    return False


def daemonize(cmd, pidfile=None, logdir=None, api_port=8082, address=None, unix_socket=None):
    import daemon

    logdir = logdir or "/var/log/luigi"
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    log_path = os.path.join(logdir, "luigi-server.log")

    # redirect stdout/stderr
    today = datetime.date.today()
    stdout_path = os.path.join(
        logdir,
        "luigi-server-{0:%Y-%m-%d}.out".format(today)
    )
    stderr_path = os.path.join(
        logdir,
        "luigi-server-{0:%Y-%m-%d}.err".format(today)
    )
    stdout_proxy = open(stdout_path, 'a+')
    stderr_proxy = open(stderr_path, 'a+')

    try:
        ctx = daemon.DaemonContext(
            stdout=stdout_proxy,
            stderr=stderr_proxy,
            working_directory='.',
            initgroups=False,
        )
    except TypeError:
        # Older versions of python-daemon cannot deal with initgroups arg.
        ctx = daemon.DaemonContext(
            stdout=stdout_proxy,
            stderr=stderr_proxy,
            working_directory='.',
        )

    with ctx:
        loghandler = get_spool_handler(log_path)
        rootlogger.addHandler(loghandler)

        if pidfile:
            server_logger.info("Checking pid file")
            existing_pid = check_pid(pidfile)
            if pidfile and existing_pid:
                server_logger.info("Server already running (pid=%s)", existing_pid)
                return
            write_pid(pidfile)

        cmd(api_port=api_port, address=address, unix_socket=unix_socket)
