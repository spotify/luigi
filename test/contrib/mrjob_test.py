# -*- coding: utf-8 -*-
#
# Copyright 2016 Enreach Oy
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
from __future__ import print_function
import luigi
from luigi.contrib import mrjob as luigi_mrjob

from .mrjob_test_job import MRFreqWithCounters
import os
import random
import shutil
import tempfile
import unittest
import json
import re


WORD_RE = re.compile(r"[\w']+")


class ExternalInputTask(luigi.ExternalTask):
    destination = luigi.Parameter()

    def output(self):
        return [luigi.LocalTarget(os.path.join(self.destination, "input"))]


class CustomFileParameterTask(luigi.ExternalTask):
    destination = luigi.Parameter()

    def output(self):
        return [luigi.LocalTarget(os.path.join(self.destination,
                "extra_input_lookup"))]


class MRJobWordCountTask(luigi_mrjob.MRJobTask):
    destination = luigi.Parameter()
    job_cls = MRFreqWithCounters

    def mrjob_opts(self):
        return {}

    def requires(self):
        return ExternalInputTask(self.destination)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination, "output"),
                                 format=luigi.format.Nop)


class MrJobWordCountTaskWithInputDict(MRJobWordCountTask):
    def requires(self):
        return {"input":
                ExternalInputTask(self.destination)}


class MrJobWordCountTaskWithInputDictAndParameter(MRJobWordCountTask):
    def requires(self):
        return {"input":
                ExternalInputTask(self.destination),
                "--lookup-file": CustomFileParameterTask(self.destination)}


class MrJobWordCountWithCounters(MRJobWordCountTask):
    def handle_counters(self, counters):
        with open(os.path.join(self.destination, "counters"), "w") as file_:
            file_.write(json.dumps(counters))


class MrJobWordCountWithCustomParameter(MRJobWordCountTask):
    def mrjob_opts(self):
        """ Pass a custom option. Option parsing is much more difficult to
        break than the local runner test that also uses this feature.
        """
        return {
                "--invert-words": True
               }


class MrJobWordCountLocalRunner(MRJobWordCountTask):
    def mrjob_opts(self):
        """ The other tests are run against the inline runner for simplicity
        and speed. The local runner is run in a subprocess and is a much
        better tool for testing that environment setup steps etc. actually
        work as intended.
        """
        return {
                "-r": "local"
               }


class MrJobWordCountOutputToDir(MRJobWordCountTask):
    def output(self):
        return luigi_mrjob.LocalMRJobTarget(os.path.join(self.destination, "output/"))


class MRJobIntegrationTest(unittest.TestCase):
    """ Test against a real MRJob class with real input data dir etc.
        Run locally, so not in HDFS (could be tested separately) or EMR
        (not so simple to automate test against a paid resource).
    """

    N_WORDS = {
                "luigi": random.randint(1, 20),
                "waluigi": random.randint(1, 20),
                "daisy": random.randint(1, 20)
              }

    def setUp(self):
        self.mrjob_tmpdir = tempfile.mkdtemp()
        with open(os.path.join(self.mrjob_tmpdir, "input"), "w") as file_:
            for key, val in self.N_WORDS.items():
                file_.write(" ".join([key]*val) + " ")
                print("wrote %s %d" % (key, val))
        with open(os.path.join(self.mrjob_tmpdir, "extra_input_lookup"), "w") as file_:
            lookup = {}
            for key, val in self.N_WORDS.items():
                lookup[key] = key.replace("i", "1")
            json.dump(lookup, file_)

    def tearDown(self):
        shutil.rmtree(self.mrjob_tmpdir)

    def test_integration_mrjob(self):
        """ most basic test with no frills
        """
        self._run_task("MRJobWordCountTask")

    def test_dict_inputs_mrjob(self):
        """ test passing a dict as inputs and only usin "input"

        ToDo: pass another file and assert it is not touched if it's not under
        "input"
        """
        self._run_task("MrJobWordCountTaskWithInputDict")

    def test_local_runner_mrjob(self):
        """ Test using --runner local
        """
        self._run_task("MrJobWordCountLocalRunner")

    def test_word_counter_mrjob(self):
        self._run_task("MrJobWordCountWithCounters")
        with open(os.path.join(self.mrjob_tmpdir, "counters"), "r") as file_:
            counters = json.loads(file_.readline())
            assert counters[0]["test_counters"]["words_seen_total"] == \
                sum(self.N_WORDS.values())

    def test_custom_command_line_param_mrjob(self):
        """ Test command line parameters
        """
        self._run_task("MrJobWordCountWithCustomParameter",
                       processor=lambda x: x[::-1])

    def test_custom_file_parameter(self):
        """ Test requirements that are custom file parameters
        """
        self._run_task("MrJobWordCountTaskWithInputDictAndParameter",
                       processor=lambda x: x.replace("1", "i"))

    def test_output_dir_mrjob(self):
        """ Test outputting to a local dir and reading output from (probably)
        multiple files.
        """
        success = luigi.run([
                            "MrJobWordCountOutputToDir",
                            '--destination',
                            self.mrjob_tmpdir,
                            '--local-scheduler',
                            '--no-lock'
                            ])
        self.assertTrue(success)
        task = luigi_mrjob.LocalMRJobTask(path=os.path.join(
                                            self.mrjob_tmpdir,
                                            "output" + os.sep))

        output = task.output()
        assert output.exists()
        for line in output.open("r"):
            key, val = self.read_default_output_format(line)
            print("read %s %d" % (key, val))
            assert self.N_WORDS[key] == val

    def _run_task(self, taskname, processor=lambda x: x):
        success = luigi.run([
                            taskname,
                            '--destination',
                            self.mrjob_tmpdir,
                            '--local-scheduler',
                            '--no-lock'
                            ])
        self.assertTrue(success)
        with open(os.path.join(self.mrjob_tmpdir, "output")) as file_:
            for line in file_:
                key, val = self.read_default_output_format(line)
                print("read %s %d" % (key, val))
                key = processor(key)
                assert self.N_WORDS[key] == val

    def read_default_output_format(self, line):
        parts = line.split("\t")
        return json.loads(parts[0]), json.loads(parts[1])

    # ToDo: test with Mock as job_cls
    # ToDo: smoketest the rather cryptic functions that create the args list
    # and ensure presence of s3 and hdfs in url
