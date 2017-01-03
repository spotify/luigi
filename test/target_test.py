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
from __future__ import print_function

from helpers import unittest, skipOnTravis
from mock import Mock
import re
import random

import luigi.target
import luigi.format


class TestException(Exception):
    pass


class TargetTest(unittest.TestCase):

    def test_cannot_instantiate(self):
        def instantiate_target():
            luigi.target.Target()

        self.assertRaises(TypeError, instantiate_target)

    def test_abstract_subclass(self):
        class ExistsLessTarget(luigi.target.Target):
            pass

        def instantiate_target():
            ExistsLessTarget()

        self.assertRaises(TypeError, instantiate_target)

    def test_instantiate_subclass(self):
        class GoodTarget(luigi.target.Target):

            def exists(self):
                return True

            def open(self, mode):
                return None

        GoodTarget()


class FileSystemTargetTestMixin(object):
    """All Target that take bytes (python2: str) should pass those
    tests. In addition, a test to verify the method `exists`should be added
    """

    def create_target(self, format=None):
        raise NotImplementedError()

    def assertCleanUp(self, tmp_path=''):
        pass

    def test_atomicity(self):
        target = self.create_target()

        fobj = target.open("w")
        self.assertFalse(target.exists())
        fobj.close()
        self.assertTrue(target.exists())

    def test_readback(self):
        target = self.create_target()

        origdata = 'lol\n'
        fobj = target.open("w")
        fobj.write(origdata)
        fobj.close()

        fobj = target.open('r')
        data = fobj.read()
        self.assertEqual(origdata, data)

    def test_unicode_obj(self):
        target = self.create_target()

        origdata = u'lol\n'
        fobj = target.open("w")
        fobj.write(origdata)
        fobj.close()

        fobj = target.open('r')
        data = fobj.read()
        self.assertEqual(origdata, data)

    def test_with_close(self):
        target = self.create_target()

        with target.open('w') as fobj:
            tp = getattr(fobj, 'tmp_path', '')
            fobj.write('hej\n')

        self.assertCleanUp(tp)
        self.assertTrue(target.exists())

    def test_with_exception(self):
        target = self.create_target()

        a = {}

        def foo():
            with target.open('w') as fobj:
                fobj.write('hej\n')
                a['tp'] = getattr(fobj, 'tmp_path', '')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertCleanUp(a['tp'])
        self.assertFalse(target.exists())

    def test_del(self):
        t = self.create_target()
        p = t.open('w')
        print('test', file=p)
        tp = getattr(p, 'tmp_path', '')
        del p

        self.assertCleanUp(tp)
        self.assertFalse(t.exists())

    def test_write_cleanup_no_close(self):
        t = self.create_target()

        def context():
            f = t.open('w')
            f.write('stuff')
            return getattr(f, 'tmp_path', '')

        tp = context()
        import gc
        gc.collect()  # force garbage collection of f variable
        self.assertCleanUp(tp)
        self.assertFalse(t.exists())

    def test_text(self):
        t = self.create_target(luigi.format.UTF8)
        a = u'我éçф'
        with t.open('w') as f:
            f.write(a)
        with t.open('r') as f:
            b = f.read()
        self.assertEqual(a, b)

    def test_del_with_Text(self):
        t = self.create_target(luigi.format.UTF8)
        p = t.open('w')
        print(u'test', file=p)
        tp = getattr(p, 'tmp_path', '')
        del p

        self.assertCleanUp(tp)
        self.assertFalse(t.exists())

    def test_format_injection(self):
        class CustomFormat(luigi.format.Format):

            def pipe_reader(self, input_pipe):
                input_pipe.foo = "custom read property"
                return input_pipe

            def pipe_writer(self, output_pipe):
                output_pipe.foo = "custom write property"
                return output_pipe

        t = self.create_target(CustomFormat())
        with t.open("w") as f:
            self.assertEqual(f.foo, "custom write property")

        with t.open("r") as f:
            self.assertEqual(f.foo, "custom read property")

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/73693470')
    def test_binary_write(self):
        t = self.create_target(luigi.format.Nop)
        with t.open('w') as f:
            f.write(b'a\xf2\xf3\r\nfd')

        with t.open('r') as f:
            c = f.read()

        self.assertEqual(c, b'a\xf2\xf3\r\nfd')

    def test_writelines(self):
        t = self.create_target()
        with t.open('w') as f:
            f.writelines([
                'a\n',
                'b\n',
                'c\n',
            ])

        with t.open('r') as f:
            c = f.read()

        self.assertEqual(c, 'a\nb\nc\n')

    def test_read_iterator(self):
        t = self.create_target()
        with t.open('w') as f:
            f.write('a\nb\nc\n')

        c = []
        with t.open('r') as f:
            for x in f:
                c.append(x)

        self.assertEqual(c, ['a\n', 'b\n', 'c\n'])

    def test_gzip(self):
        t = self.create_target(luigi.format.Gzip)
        p = t.open('w')
        test_data = b'test'
        p.write(test_data)
        tp = getattr(p, 'tmp_path', '')
        self.assertFalse(t.exists())
        p.close()
        self.assertCleanUp(tp)
        self.assertTrue(t.exists())

    def test_gzip_works_and_cleans_up(self):
        t = self.create_target(luigi.format.Gzip)

        test_data = b'123testing'
        with t.open('w') as f:
            tp = getattr(f, 'tmp_path', '')
            f.write(test_data)

        self.assertCleanUp(tp)
        with t.open() as f:
            result = f.read()

        self.assertEqual(test_data, result)

    def test_move_on_fs(self):
        # We're cheating and retrieving the fs from target.
        # TODO: maybe move to "filesystem_test.py" or something
        t = self.create_target()
        other_path = t.path + '-' + str(random.randint(0, 999999999))
        t._touchz()
        fs = t.fs
        self.assertTrue(t.exists())
        fs.move(t.path, other_path)
        self.assertFalse(t.exists())

    def test_rename_dont_move_on_fs(self):
        # We're cheating and retrieving the fs from target.
        # TODO: maybe move to "filesystem_test.py" or something
        t = self.create_target()
        other_path = t.path + '-' + str(random.randint(0, 999999999))
        t._touchz()
        fs = t.fs
        self.assertTrue(t.exists())
        fs.rename_dont_move(t.path, other_path)
        self.assertFalse(t.exists())
        self.assertRaises(luigi.target.FileAlreadyExists,
                          lambda: fs.rename_dont_move(t.path, other_path))


class TemporaryPathTest(unittest.TestCase):
    def setUp(self):
        super(TemporaryPathTest, self).setUp()
        self.fs = Mock()

        class MyFileSystemTarget(luigi.target.FileSystemTarget):
            open = None  # Must be implemented due to abc stuff
            fs = self.fs

        self.target_cls = MyFileSystemTarget

    def test_temporary_path_files(self):
        target_outer = self.target_cls('/tmp/notreal.xls')
        target_inner = self.target_cls('/tmp/blah.txt')

        class MyException(Exception):
            pass

        orig_ex = MyException()
        try:
            with target_outer.temporary_path() as tmp_path_outer:
                assert 'notreal' in tmp_path_outer
                with target_inner.temporary_path() as tmp_path_inner:
                    assert 'blah' in tmp_path_inner
                    with target_inner.temporary_path() as tmp_path_inner_2:
                        assert tmp_path_inner != tmp_path_inner_2
                    self.fs.rename_dont_move.assert_called_once_with(tmp_path_inner_2, target_inner.path)
                self.fs.rename_dont_move.assert_called_with(tmp_path_inner, target_inner.path)
                self.fs.rename_dont_move.call_count == 2
                raise orig_ex
        except MyException as ex:
            self.fs.rename_dont_move.call_count == 2
            assert ex is orig_ex
        else:
            assert False

    def test_temporary_path_directory(self):
        target_slash = self.target_cls('/tmp/dir/')
        target_noslash = self.target_cls('/tmp/dir')

        with target_slash.temporary_path() as tmp_path:
            assert re.match(r'/tmp/dir-luigi-tmp-\d{10}/', tmp_path)
        self.fs.rename_dont_move.assert_called_once_with(tmp_path, target_slash.path)

        with target_noslash.temporary_path() as tmp_path:
            assert re.match(r'/tmp/dir-luigi-tmp-\d{10}', tmp_path)
        self.fs.rename_dont_move.assert_called_with(tmp_path, target_noslash.path)

    def test_windowsish_dir(self):
        target = self.target_cls(r'''C:\my\folder''' + "\\")
        pattern = r'''C:\\my\\folder-luigi-tmp-\d{10}''' + r"\\"

        with target.temporary_path() as tmp_path:
            assert re.match(pattern, tmp_path)
        self.fs.rename_dont_move.assert_called_once_with(tmp_path, target.path)

    def test_hadoopish_dir(self):
        target = self.target_cls(r'''hdfs:///user/arash/myfile.uids''')

        with target.temporary_path() as tmp_path:
            assert re.match(r'''hdfs:///user/arash/myfile.uids-luigi-tmp-\d{10}''', tmp_path)
        self.fs.rename_dont_move.assert_called_once_with(tmp_path, target.path)

    def test_creates_dir_for_file(self):
        target = self.target_cls('/my/file/is/awesome.txt')

        with target.temporary_path():
            self.fs.mkdir.assert_called_once_with('/my/file/is', parents=True, raise_if_exists=False)

    def test_creates_dir_for_dir(self):
        target = self.target_cls('/my/dir/is/awesome/')

        with target.temporary_path():
            self.fs.mkdir.assert_called_once_with('/my/dir/is', parents=True, raise_if_exists=False)
