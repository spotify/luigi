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

import mock
import os
import sys
import unittest
import subprocess
import luigi
import luigi.hadoop
import luigi.hdfs
import luigi.mrrunner
from luigi.mock import MockFile
import StringIO
import luigi.notifications
luigi.notifications.DEBUG = True
File = MockFile


class Words(luigi.Task):
    def output(self):
        return File('words')

    def run(self):
        f = self.output().open('w')
        f.write('kj kj lkj lkj ljoi j iljlk jlk jlk jk jkl jlk jlkj j ioj ioj kuh kjh\n')
        f.write('kjsfsdfkj sdjkf kljslkj flskjdfj jkkd jjfk jk jk jk jk jk jklkjf kj lkj lkj\n')
        f.close()


class TestJobTask(luigi.hadoop.JobTask):
    def job_runner(self):
        return luigi.hadoop.LocalJobRunner()


class WordCountJob(TestJobTask):
    def mapper(self, line):
        for word in line.strip().split():
            self.incr_counter('word', word, 1)
            yield word, 1

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires(self):
        return Words()

    def output(self):
        return File("luigitest")


class WordCountJobReal(WordCountJob):
    def job_runner(self):
        return luigi.hadoop.HadoopJobRunner(streaming_jar='test.jar')


class WordFreqJob(TestJobTask):
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
        return WordCountJob()

    def requires_hadoop(self):
        return Words()

    def output(self):
        return File("luigitest-2")


class MapOnlyJob(TestJobTask):
    def mapper(self, line):
        for word in line.strip().split():
            yield (word,)

    def requires_hadoop(self):
        return Words()

    def output(self):
        return File("luigitest-3")


class HadoopJobTest(unittest.TestCase):
    def setUp(self):
        MockFile._file_contents = {}

    def read_output(self, p):
        count = {}
        for line in p.open('r'):
            k, v = line.strip().split()
            count[k] = v
        return count

    def test_run(self):
        luigi.build([WordCountJob()], local_scheduler=True)
        c = self.read_output(File('luigitest'))
        self.assertEquals(int(c['jk']), 6)

    def test_run_2(self):
        luigi.build([WordFreqJob()], local_scheduler=True)
        c = self.read_output(File('luigitest-2'))
        self.assertAlmostEquals(float(c['jk']), 6.0 / 33.0)

    def test_map_only(self):
        luigi.build([MapOnlyJob()], local_scheduler=True)
        c = []
        for line in File('luigitest-3').open('r'):
            c.append(line.strip())
        self.assertEquals(c[0], 'kj')
        self.assertEquals(c[4], 'ljoi')

    def test_run_hadoop_job_failure(self):
        def Popen_fake(arglist, stdout=None, stderr=None, env=None, close_fds=True):
            class P(object):
                def wait(self):
                    pass

                def poll(self):
                    return 1


            p = P()
            p.returncode = 1
            if stdout == subprocess.PIPE:
                p.stdout = StringIO.StringIO('stdout')
            else:
                stdout.write('stdout')
            if stderr == subprocess.PIPE:
                p.stderr = StringIO.StringIO('stderr')
            else:
                stderr.write('stderr')
            return p

        p = subprocess.Popen
        subprocess.Popen = Popen_fake
        try:
            luigi.hadoop.run_and_track_hadoop_job([])
        except luigi.hadoop.HadoopJobError as e:
            self.assertEquals(e.out, 'stdout')
            self.assertEquals(e.err, 'stderr')
        else:
            self.fail("Should have thrown HadoopJobError")
        finally:
            subprocess.Popen = p


    def test_run_real(self):
        # Will attempt to run a real hadoop job, but we will secretly mock subprocess.Popen
        arglist_result = []

        def Popen_fake(arglist, stdout=None, stderr=None, env=None, close_fds=True):
            arglist_result.append(arglist)

            class P(object):
                def wait(self):
                    pass
                def poll(self):
                    return 0
            p = P()
            p.returncode = 0
            p.stderr = StringIO.StringIO()
            p.stdout = StringIO.StringIO()
            return p

        h, p = luigi.hdfs.HdfsTarget, subprocess.Popen
        luigi.hdfs.HdfsTarget, subprocess.Popen = MockFile, Popen_fake
        try:
            MockFile.move = lambda *args, **kwargs: None
            WordCountJobReal().run()
            self.assertEquals(len(arglist_result), 1)
            self.assertEquals(arglist_result[0][0:3], ['hadoop', 'jar', 'test.jar'])
        finally:
            luigi.hdfs.HdfsTarget, subprocess.Popen = h, p  # restore


class FailingJobException(Exception):
    pass


class FailingJob(TestJobTask):
    def init_hadoop(self):
        raise FailingJobException('failure')


class MrrunnerTest(unittest.TestCase):
    def test_mrrunner(self):
        # TODO: we're doing a lot of stuff here that depends on the internals of how
        # we run Hadoop streaming job (in particular the create_packages_archive).
        # We should abstract these things out into helper methods in luigi.hadoop so
        # that we don't have to recreate all steps
        job = WordCountJob()
        packages = [__import__(job.__module__, None, None, 'dummy')]
        luigi.hadoop.create_packages_archive(packages, 'packages.tar')
        job._dump()
        input = StringIO.StringIO('xyz fdklslkjsdf kjfdk jfdkj kdjf kjdkfj dkjf fdj j j k k l l')
        output = StringIO.StringIO()
        luigi.mrrunner.main(args=['mrrunner.py', 'map'], stdin=input, stdout=output)

    def test_mrrunner_failure(self):
        job = FailingJob()
        packages = [__import__(job.__module__, None, None, 'dummy')]
        luigi.hadoop.create_packages_archive(packages, 'packages.tar')
        job._dump()
        excs = []
        def print_exception(traceback):
            excs.append(traceback)

        def run():
            input = StringIO.StringIO()
            output = StringIO.StringIO()
            luigi.mrrunner.main(args=['mrrunner.py', 'map'], stdin=input, stdout=output, print_exception=print_exception)
        self.assertRaises(FailingJobException, run)        
        self.assertEquals(len(excs), 1) # should have been set
        self.assertTrue(type(excs[0]), FailingJobException)

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
        assert add.call_count == 6

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
