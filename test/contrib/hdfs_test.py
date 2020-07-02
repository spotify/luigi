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

import re
import random
import pickle

import luigi
import luigi.format
from luigi.contrib import hdfs
import luigi.contrib.hdfs.clients

from target_test import FileSystemTargetTestMixin


class ComplexOldFormat(luigi.format.Format):
    """Should take unicode but output bytes
    """

    def hdfs_writer(self, output_pipe):
        return self.pipe_writer(luigi.contrib.hdfs.Plain.hdfs_writer(output_pipe))

    def pipe_writer(self, output_pipe):
        return luigi.format.UTF8.pipe_writer(output_pipe)

    def pipe_reader(self, output_pipe):
        return output_pipe


class TestException(Exception):
    pass


class HdfsTargetTestMixin(FileSystemTargetTestMixin):

    def create_target(self, format=None):
        target = hdfs.HdfsTarget(self._test_file(), format=format)
        if target.exists():
            target.remove(skip_trash=True)
        return target

    def test_slow_exists(self):
        target = hdfs.HdfsTarget(self._test_file())
        try:
            target.remove(skip_trash=True)
        except BaseException:
            pass

        self.assertFalse(self.fs.exists(target.path))
        target.open("w").close()
        self.assertTrue(self.fs.exists(target.path))

        def should_raise():
            self.fs.exists("hdfs://doesnotexist/foo")

        self.assertRaises(hdfs.HDFSCliError, should_raise)

        def should_raise_2():
            self.fs.exists("hdfs://_doesnotexist_/foo")

        self.assertRaises(hdfs.HDFSCliError, should_raise_2)

    def test_create_ancestors(self):
        parent = self._test_dir()
        target = hdfs.HdfsTarget("%s/foo/bar/baz" % parent)
        if self.fs.exists(parent):
            self.fs.remove(parent, skip_trash=True)
        self.assertFalse(self.fs.exists(parent))
        fobj = target.open('w')
        fobj.write('lol\n')
        fobj.close()
        self.assertTrue(self.fs.exists(parent))
        self.assertTrue(target.exists())

    def test_tmp_cleanup(self):
        path = self._test_file()
        target = hdfs.HdfsTarget(path, is_tmp=True)
        if target.exists():
            target.remove(skip_trash=True)
        with target.open('w') as fobj:
            fobj.write('lol\n')
        self.assertTrue(target.exists())
        del target
        import gc
        gc.collect()
        self.assertFalse(self.fs.exists(path))

    def test_luigi_tmp(self):
        target = hdfs.HdfsTarget(is_tmp=True)
        self.assertFalse(target.exists())
        with target.open('w'):
            pass
        self.assertTrue(target.exists())

    def test_tmp_move(self):
        target = hdfs.HdfsTarget(is_tmp=True)
        target2 = hdfs.HdfsTarget(self._test_file())
        if target2.exists():
            target2.remove(skip_trash=True)
        with target.open('w'):
            pass
        self.assertTrue(target.exists())
        target.move(target2.path)
        self.assertFalse(target.exists())
        self.assertTrue(target2.exists())

    def test_rename_no_parent(self):
        parent = self._test_dir() + '/foo'
        if self.fs.exists(parent):
            self.fs.remove(parent, skip_trash=True)

        target1 = hdfs.HdfsTarget(is_tmp=True)
        target2 = hdfs.HdfsTarget(parent + '/bar')
        with target1.open('w'):
            pass
        self.assertTrue(target1.exists())
        target1.move(target2.path)
        self.assertFalse(target1.exists())
        self.assertTrue(target2.exists())

    def test_rename_no_grandparent(self):
        grandparent = self._test_dir() + '/foo'
        if self.fs.exists(grandparent):
            self.fs.remove(grandparent, skip_trash=True)

        target1 = hdfs.HdfsTarget(is_tmp=True)
        target2 = hdfs.HdfsTarget(grandparent + '/bar/baz')
        with target1.open('w'):
            pass
        self.assertTrue(target1.exists())
        target1.move(target2.path)
        self.assertFalse(target1.exists())
        self.assertTrue(target2.exists())

    def test_glob_exists(self):
        target_dir = hdfs.HdfsTarget(self._test_dir())
        if target_dir.exists():
            target_dir.remove(skip_trash=True)
        self.fs.mkdir(target_dir.path)
        t1 = hdfs.HdfsTarget(target_dir.path + "/part-00001")
        t2 = hdfs.HdfsTarget(target_dir.path + "/part-00002")
        t3 = hdfs.HdfsTarget(target_dir.path + "/another")

        with t1.open('w') as f:
            f.write('foo\n')
        with t2.open('w') as f:
            f.write('bar\n')
        with t3.open('w') as f:
            f.write('biz\n')

        files = hdfs.HdfsTarget("%s/part-0000*" % target_dir.path)

        self.assertTrue(files.glob_exists(2))
        self.assertFalse(files.glob_exists(3))
        self.assertFalse(files.glob_exists(1))

    def assertRegexpMatches(self, text, expected_regexp, msg=None):
        """Python 2.7 backport."""
        if isinstance(expected_regexp, str):
            expected_regexp = re.compile(expected_regexp)
        if not expected_regexp.search(text):
            msg = msg or "Regexp didn't match"
            msg = '%s: %r not found in %r' % (msg, expected_regexp.pattern, text)
            raise self.failureException(msg)

    def test_tmppath_not_configured(self):
        # Given: several target paths to test
        path1 = "/dir1/dir2/file"
        path2 = "hdfs:///dir1/dir2/file"
        path3 = "hdfs://somehost/dir1/dir2/file"
        path4 = "file:///dir1/dir2/file"
        path5 = "/tmp/dir/file"
        path6 = "file:///tmp/dir/file"
        path7 = "hdfs://somehost/tmp/dir/file"
        path8 = None
        path9 = "/tmpdir/file"

        # When: I create a temporary path for targets
        res1 = hdfs.tmppath(path1, include_unix_username=False)
        res2 = hdfs.tmppath(path2, include_unix_username=False)
        res3 = hdfs.tmppath(path3, include_unix_username=False)
        res4 = hdfs.tmppath(path4, include_unix_username=False)
        res5 = hdfs.tmppath(path5, include_unix_username=False)
        res6 = hdfs.tmppath(path6, include_unix_username=False)
        res7 = hdfs.tmppath(path7, include_unix_username=False)
        res8 = hdfs.tmppath(path8, include_unix_username=False)
        res9 = hdfs.tmppath(path9, include_unix_username=False)

        # Then: I should get correct results relative to Luigi temporary directory
        self.assertRegexpMatches(res1, "^/tmp/dir1/dir2/file-luigitemp-\\d+")
        # it would be better to see hdfs:///path instead of hdfs:/path, but single slash also works well
        self.assertRegexpMatches(res2, "^hdfs:/tmp/dir1/dir2/file-luigitemp-\\d+")
        self.assertRegexpMatches(res3, "^hdfs://somehost/tmp/dir1/dir2/file-luigitemp-\\d+")
        self.assertRegexpMatches(res4, "^file:///tmp/dir1/dir2/file-luigitemp-\\d+")
        self.assertRegexpMatches(res5, "^/tmp/dir/file-luigitemp-\\d+")
        # known issue with duplicated "tmp" if schema is present
        self.assertRegexpMatches(res6, "^file:///tmp/tmp/dir/file-luigitemp-\\d+")
        # known issue with duplicated "tmp" if schema is present
        self.assertRegexpMatches(res7, "^hdfs://somehost/tmp/tmp/dir/file-luigitemp-\\d+")
        self.assertRegexpMatches(res8, "^/tmp/luigitemp-\\d+")
        self.assertRegexpMatches(res9, "/tmp/tmpdir/file")

    def test_tmppath_username(self):
        self.assertRegexpMatches(hdfs.tmppath('/path/to/stuff', include_unix_username=True),
                                 "^/tmp/[a-z0-9_]+/path/to/stuff-luigitemp-\\d+")

    def test_pickle(self):
        t = hdfs.HdfsTarget("/tmp/dir")
        pickle.dumps(t)

    def test_flag_target(self):
        target = hdfs.HdfsFlagTarget("/some/dir/", format=format)
        if target.exists():
            target.remove(skip_trash=True)
        self.assertFalse(target.exists())

        t1 = hdfs.HdfsTarget(target.path + "part-00000", format=format)
        with t1.open('w'):
            pass
        t2 = hdfs.HdfsTarget(target.path + "_SUCCESS", format=format)
        with t2.open('w'):
            pass
        self.assertTrue(target.exists())

    def test_flag_target_fails_if_not_directory(self):
        with self.assertRaises(ValueError):
            hdfs.HdfsFlagTarget("/home/file.txt")


class _MiscOperationsMixin:

    # TODO: chown/chmod/count should really be methods on HdfsTarget rather than the client!

    def get_target(self):
        fn = '/tmp/foo-%09d' % random.randint(0, 999999999)
        t = luigi.contrib.hdfs.HdfsTarget(fn)
        with t.open('w') as f:
            f.write('test')
        return t

    def test_count(self):
        t = self.get_target()
        res = self.get_client().count(t.path)
        for key in ['content_size', 'dir_count', 'file_count']:
            self.assertTrue(key in res)

    def test_chmod(self):
        t = self.get_target()
        self.get_client().chmod(t.path, '777')

    def test_chown(self):
        t = self.get_target()
        self.get_client().chown(t.path, 'root', 'root')
