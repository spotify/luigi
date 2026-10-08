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

import logging
import os
import os.path
import shlex
import stat
import subprocess
import tempfile
import unittest
from glob import glob

import pytest
from mock import patch

import luigi
from luigi.contrib.sge import SGEJobTask, _build_job_str, _build_qsub_command, _parse_qstat_state

DEFAULT_HOME = "/home"

logger = logging.getLogger("luigi-interface")


QSTAT_OUTPUT = """job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
-----------------------------------------------------------------------------------------------------------------
     1 0.55500 job1 root         r     07/09/2015 16:56:45 all.q@node001                      1
     2 0.55500 job2 root         qw    07/09/2015 16:56:42                                    1
     3 0.00000 job3 root         t    07/09/2015 16:56:45                                    1
"""


def on_sge_master():
    try:
        subprocess.check_output("qstat", shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


@pytest.mark.contrib
class TestSGEWrappers(unittest.TestCase):
    def test_track_job(self):
        """`track_job` returns the state using qstat"""
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 1), "r")
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 2), "qw")
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 3), "t")
        self.assertEqual(_parse_qstat_state("", 1), "u")
        self.assertEqual(_parse_qstat_state("", 4), "u")


def _make_stub_qsub(bin_dir):
    """Write a fake `qsub` into `bin_dir` that records its argv and the piped-in
    job script, then *runs* that script -- mirroring what a real SGE `qsub`
    does with the job it's handed on stdin. Used to prove end-to-end that a
    malicious Parameter value reaches qsub as a literal argument/script
    instead of being executed as a separate shell command.
    """
    argv_file = os.path.join(bin_dir, "qsub_argv.txt")
    stdin_file = os.path.join(bin_dir, "qsub_stdin.txt")
    qsub_path = os.path.join(bin_dir, "qsub")
    with open(qsub_path, "w") as f:
        f.write(
            "#!/bin/sh\n"
            'printf "%s\\n" "$@" > {argv_file}\n'
            "cat > {stdin_file}\n"
            'sh -c "$(cat {stdin_file})"\n'
            "echo 'Your job 1 (\"job\") has been submitted'\n".format(argv_file=shlex.quote(argv_file), stdin_file=shlex.quote(stdin_file))
        )
    os.chmod(qsub_path, os.stat(qsub_path).st_mode | stat.S_IEXEC)
    return argv_file, stdin_file


@pytest.mark.contrib
class TestQsubCommandInjection(unittest.TestCase):
    """`_build_job_str()` and `_build_qsub_command()` interpolate Parameter
    values (`shared_tmp_dir`, `parallel_env`, `job_name`/`job_name_format`)
    into a string executed with `subprocess.check_output(cmd, shell=True)`.
    Every value must be shell-quoted as a single token; these tests both
    check the generated strings directly and prove -- by actually running
    them through a shell -- that a malicious value can't break out.
    """

    def test_build_job_str_round_trips_benign_values(self):
        job_str = _build_job_str("/opt/luigi/sge_runner.py", "/home/user/tmp123", "/home/user", False)
        self.assertEqual(
            shlex.split(job_str),
            ["python", "/opt/luigi/sge_runner.py", "/home/user/tmp123", "/home/user"],
        )

    def test_build_job_str_appends_no_tarball_flag(self):
        job_str = _build_job_str("/opt/luigi/sge_runner.py", "/home/user/tmp123", "/home/user", True)
        self.assertEqual(
            shlex.split(job_str),
            ["python", "/opt/luigi/sge_runner.py", "/home/user/tmp123", "/home/user", "--no-tarball"],
        )

    def test_build_qsub_command_round_trips_benign_values(self):
        cmd = _build_qsub_command("python runner.py", "MyTask", "/tmp/job.out", "/tmp/job.err", "orte", 4)
        self.assertEqual(
            shlex.split(cmd.split(" | ", 1)[1]),
            ["qsub", "-o", ":/tmp/job.out", "-e", ":/tmp/job.err", "-V", "-r", "y", "-pe", "orte", "4", "-N", "MyTask"],
        )

    def test_build_qsub_command_blocks_shell_injection(self):
        with tempfile.TemporaryDirectory() as bin_dir:
            argv_file, stdin_file = _make_stub_qsub(bin_dir)
            marker = os.path.join(bin_dir, "PWNED")
            payload = "; touch {}".format(marker)

            # Each Parameter-derived value gets the same malicious payload in turn
            # (job_name, parallel_env, and -- via outfile/errfile -- shared_tmp_dir).
            submit_cmd = _build_qsub_command("real-job-command", payload, payload, payload, payload, 1)

            env = dict(os.environ)
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            result = subprocess.run(submit_cmd, shell=True, env=env, capture_output=True, text=True)

            self.assertFalse(os.path.exists(marker), "injected `; touch` ran as a separate shell command")
            self.assertEqual(result.returncode, 0)
            with open(argv_file) as f:
                received_args = f.read().splitlines()
            # qsub must receive the payload as one literal argument value per flag,
            # not have it interpreted -- e.g. "-N" followed by the payload string.
            self.assertIn(payload, received_args)
            with open(stdin_file) as f:
                self.assertEqual(f.read().strip(), "real-job-command")

    def test_build_qsub_command_isolates_injection_per_argument(self):
        """Same proof as test_build_qsub_command_blocks_shell_injection, but with
        the payload in exactly one argument at a time, so a regression in any
        single argument's escaping is diagnosed on its own rather than being
        masked by the others still being escaped correctly.
        """
        for field in ("cmd", "job_name", "outfile", "errfile", "pe"):
            with tempfile.TemporaryDirectory() as bin_dir:
                _make_stub_qsub(bin_dir)
                marker = os.path.join(bin_dir, "PWNED")
                payload = "; touch {}".format(marker)

                args = {"cmd": "real-job-command", "job_name": "benign-job", "outfile": "/tmp/o", "errfile": "/tmp/e", "pe": "orte"}
                args[field] = payload
                submit_cmd = _build_qsub_command(args["cmd"], args["job_name"], args["outfile"], args["errfile"], args["pe"], 1)

                env = dict(os.environ)
                env["PATH"] = bin_dir + os.pathsep + env["PATH"]
                subprocess.run(submit_cmd, shell=True, env=env, capture_output=True, text=True)

                self.assertFalse(os.path.exists(marker), "payload in {!r} ran as a separate shell command".format(field))

    def test_build_job_str_blocks_shell_injection_via_tmp_dir(self):
        with tempfile.TemporaryDirectory() as bin_dir:
            argv_file, stdin_file = _make_stub_qsub(bin_dir)
            marker = os.path.join(bin_dir, "PWNED")
            malicious_tmp_dir = "$(touch {})".format(marker)

            job_str = _build_job_str("/opt/luigi/sge_runner.py", malicious_tmp_dir, "/home/user", False)
            submit_cmd = _build_qsub_command(job_str, "job", "/tmp/o", "/tmp/e", "orte", 1)

            env = dict(os.environ)
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            result = subprocess.run(submit_cmd, shell=True, env=env, capture_output=True, text=True)

            self.assertFalse(os.path.exists(marker), "command substitution in tmp_dir executed")
            self.assertEqual(result.returncode, 0)
            # The stub's "sh -c" step is what would run the job on a real
            # cluster; it must see tmp_dir as one literal argument, not have
            # `$(...)` expanded again.
            with open(stdin_file) as f:
                self.assertEqual(shlex.split(f.read()), ["python", "/opt/luigi/sge_runner.py", malicious_tmp_dir, "/home/user"])


class TestJobTask(SGEJobTask):
    """Simple SGE job: write a test file to NSF shared drive and waits a minute"""

    i = luigi.Parameter()

    def work(self):
        logger.info("Running test job...")
        with open(self.output().path, "w") as f:
            f.write("this is a test\n")

    def output(self):
        return luigi.LocalTarget(os.path.join(DEFAULT_HOME, "testfile_" + str(self.i)))


@pytest.mark.contrib
class TestSGEJob(unittest.TestCase):
    """Test from SGE master node"""

    def test_run_job(self):
        if on_sge_master():
            outfile = os.path.join(DEFAULT_HOME, "testfile_1")
            tasks = [TestJobTask(i=str(i), n_cpu=1) for i in range(3)]
            luigi.build(tasks, local_scheduler=True, workers=3)
            self.assertTrue(os.path.exists(outfile))

    @patch("subprocess.check_output")
    def test_run_job_with_dump(self, mock_check_output):
        mock_check_output.side_effect = ['Your job 12345 ("test_job") has been submitted', ""]
        task = TestJobTask(i="1", n_cpu=1, shared_tmp_dir="/tmp")
        luigi.build([task], local_scheduler=True)
        self.assertEqual(mock_check_output.call_count, 2)

    def tearDown(self):
        for fpath in glob(os.path.join(DEFAULT_HOME, "test_file_*")):
            try:
                os.remove(fpath)
            except OSError:
                pass
