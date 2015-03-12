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

import os
import sys
import unittest

import luigi
import luigi.format
import luigi.hadoop
import luigi.hdfs
import luigi.mrrunner
import luigi.notifications
import minicluster
import mock
from luigi.mock import MockTarget
from nose.plugins.attrib import attr

luigi.notifications.DEBUG = True

luigi.hadoop.attach(minicluster)


class OutputMixin(luigi.Task):
    use_hdfs = luigi.BoolParameter(default=False)

    def get_output(self, fn):
        if self.use_hdfs:
            return luigi.hdfs.HdfsTarget('/tmp/' + fn, format=luigi.format.get_default_format() >> luigi.hdfs.PlainDir)
        else:
            return MockTarget(fn)


class HadoopJobTask(luigi.hadoop.JobTask, OutputMixin):

    def job_runner(self):
        if self.use_hdfs:
            return minicluster.MiniClusterHadoopJobRunner()
        else:
            return luigi.hadoop.LocalJobRunner()


class Words(OutputMixin):

    def output(self):
        return self.get_output('words')

    def run(self):
        f = self.output().open('w')
        f.write('kj kj lkj lkj ljoi j iljlk jlk jlk jk jkl jlk jlkj j ioj ioj kuh kjh\n')
        f.write('kjsfsdfkj sdjkf kljslkj flskjdfj jkkd jjfk jk jk jk jk jk jklkjf kj lkj lkj\n')
        f.close()


class WordCountJob(HadoopJobTask):

    def mapper(self, line):
        for word in line.strip().split():
            self.incr_counter('word', word, 1)
            yield word, 1

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires(self):
        return Words(self.use_hdfs)

    def output(self):
        return self.get_output('wordcount')


class WordFreqJob(HadoopJobTask):

    def init_local(self):
        self.n = 0
        for line in self.input_local().open('r'):
            word, count = line.strip().split()
            self.n += int(count)

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1.0 / self.n

    def combiner(self, word, occurrences):
        yield word, sum(occurrences)

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires_local(self):
        return WordCountJob(self.use_hdfs)

    def requires_hadoop(self):
        return Words(self.use_hdfs)

    def output(self):
        return self.get_output('luigitest-2')

    def extra_files(self):
        fn = os.listdir('.')[0]  # Just return some file, doesn't matter which
        return [(fn, 'my_dir/my_file')]

    def init_remote(self):
        f = open('my_dir/my_file')  # make sure it exists


class MapOnlyJob(HadoopJobTask):

    def mapper(self, line):
        for word in line.strip().split():
            yield (word,)

    def requires_hadoop(self):
        return Words(self.use_hdfs)

    def output(self):
        return self.get_output('luigitest-3')


class UnicodeJob(HadoopJobTask):

    def mapper(self, line):
        yield u'test', 1
        yield b'test', 1

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires(self):
        return Words(self.use_hdfs)

    def output(self):
        return self.get_output('luigitest-4')


class FailingJobException(Exception):
    pass


class FailingJob(HadoopJobTask):

    def init_hadoop(self):
        raise FailingJobException('failure')

    def output(self):
        return self.get_output('failing')


class MyStreamingJob(luigi.hadoop.JobTask):
    param = luigi.Parameter()


def read_wordcount_output(p):
    count = {}
    for line in p.open('r'):
        k, v = line.strip().split()
        count[k] = v
    return count


class CommonTests(object):

    @staticmethod
    def test_run(test_case):
        job = WordCountJob(use_hdfs=test_case.use_hdfs)
        luigi.build([job], local_scheduler=True)
        c = read_wordcount_output(job.output())
        test_case.assertEqual(int(c['jk']), 6)

    @staticmethod
    def test_run_2(test_case):
        job = WordFreqJob(use_hdfs=test_case.use_hdfs)
        luigi.build([job], local_scheduler=True)
        c = read_wordcount_output(job.output())
        test_case.assertAlmostEquals(float(c['jk']), 6.0 / 33.0)

    @staticmethod
    def test_map_only(test_case):
        job = MapOnlyJob(use_hdfs=test_case.use_hdfs)
        luigi.build([job], local_scheduler=True)
        c = []
        for line in job.output().open('r'):
            c.append(line.strip())
        test_case.assertEqual(c[0], 'kj')
        test_case.assertEqual(c[4], 'ljoi')

    @staticmethod
    def test_unicode_job(test_case):
        job = UnicodeJob(use_hdfs=test_case.use_hdfs)
        luigi.build([job], local_scheduler=True)
        c = []
        for line in job.output().open('r'):
            c.append(line)
        # Make sure unicode('test') isnt grouped with str('test')
        # Since this is what happens when running on cluster
        test_case.assertEqual(len(c), 2)
        test_case.assertEqual(c[0], "test\t2\n")
        test_case.assertEqual(c[0], "test\t2\n")

    @staticmethod
    def test_failing_job(test_case):
        job = FailingJob(use_hdfs=test_case.use_hdfs)

        success = luigi.build([job], local_scheduler=True)
        test_case.assertFalse(success)


class MapreduceLocalTest(unittest.TestCase):
    use_hdfs = False

    def test_run(self):
        CommonTests.test_run(self)

    def test_run_2(self):
        CommonTests.test_run_2(self)

    def test_map_only(self):
        CommonTests.test_map_only(self)

    def test_unicode_job(self):
        CommonTests.test_unicode_job(self)

    def test_failing_job(self):
        CommonTests.test_failing_job(self)

    def test_instantiate_job(self):
        # See https://github.com/spotify/luigi/issues/738
        MyStreamingJob('param_value')

    def setUp(self):
        MockTarget.fs.clear()


@attr('minicluster')
class MapreduceIntegrationTest(minicluster.MiniClusterTestCase):

    """ Uses the Minicluster functionality to test this against Hadoop """
    use_hdfs = True

    def test_run(self):
        CommonTests.test_run(self)

    def test_run_2(self):
        CommonTests.test_run_2(self)

    def test_map_only(self):
        CommonTests.test_map_only(self)

    # TODO(erikbern): some really annoying issue with minicluster causes
    # test_unicode_job to hang

    def test_failing_job(self):
        CommonTests.test_failing_job(self)


class CreatePackagesArchive(unittest.TestCase):

    def setUp(self):
        sys.path.append(os.path.join('test', 'create_packages_archive_root'))

    def tearDown(self):
        sys.path.remove(os.path.join('test', 'create_packages_archive_root'))

    def _assert_module(self, add):
        add.assert_called_once_with('test/create_packages_archive_root/module.py',
                                    'module.py')

    def _assert_package(self, add):
        add.assert_any_call('test/create_packages_archive_root/package/__init__.py', 'package/__init__.py')
        add.assert_any_call('test/create_packages_archive_root/package/submodule.py', 'package/submodule.py')
        add.assert_any_call('test/create_packages_archive_root/package/submodule_with_absolute_import.py', 'package/submodule_with_absolute_import.py')
        add.assert_any_call('test/create_packages_archive_root/package/submodule_without_imports.py', 'package/submodule_without_imports.py')
        add.assert_any_call('test/create_packages_archive_root/package/subpackage/__init__.py', 'package/subpackage/__init__.py')
        add.assert_any_call('test/create_packages_archive_root/package/subpackage/submodule.py', 'package/subpackage/submodule.py')
        add.assert_any_call('test/create_packages_archive_root/package.egg-info/top_level.txt', 'package.egg-info/top_level.txt')
        assert add.call_count == 7

    def _assert_package_subpackage(self, add):
        add.assert_any_call('test/create_packages_archive_root/package/__init__.py', 'package/__init__.py')
        add.assert_any_call('test/create_packages_archive_root/package/subpackage/__init__.py', 'package/subpackage/__init__.py')
        add.assert_any_call('test/create_packages_archive_root/package/subpackage/submodule.py', 'package/subpackage/submodule.py')
        assert add.call_count == 3

    @mock.patch('tarfile.open')
    def test_create_packages_archive_module(self, tar):
        module = __import__("module", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([module], '/dev/null')
        self._assert_module(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package(self, tar):
        package = __import__("package", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package], '/dev/null')
        self._assert_package(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package_submodule(self, tar):
        package_submodule = __import__("package.submodule", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package_submodule], '/dev/null')
        self._assert_package(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package_submodule_with_absolute_import(self, tar):
        package_submodule_with_absolute_import = __import__("package.submodule_with_absolute_import", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package_submodule_with_absolute_import], '/dev/null')
        self._assert_package(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package_submodule_without_imports(self, tar):
        package_submodule_without_imports = __import__("package.submodule_without_imports", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package_submodule_without_imports], '/dev/null')
        self._assert_package(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package_subpackage(self, tar):
        package_subpackage = __import__("package.subpackage", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package_subpackage], '/dev/null')
        self._assert_package_subpackage(tar.return_value.add)

    @mock.patch('tarfile.open')
    def test_create_packages_archive_package_subpackage_submodule(self, tar):
        package_subpackage_submodule = __import__("package.subpackage.submodule", None, None, 'dummy')
        luigi.hadoop.create_packages_archive([package_subpackage_submodule], '/dev/null')
        self._assert_package_subpackage(tar.return_value.add)


if __name__ == '__main__':
    HadoopJobTest.test_run_real()
