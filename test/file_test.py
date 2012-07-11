# -*- coding: utf-8 -*-
# Copyright (c) 2011 Spotify Ltd

from luigi import File
import unittest
import os
import gzip
import luigi.format

class FileTest(unittest.TestCase):
    path = '/tmp/test.txt'
    def setUp(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_close(self):
        t = File(self.path)
        p = t.open('w')
        print >> p, 'test'
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

    def test_del(self):
        t = File(self.path)
        p = t.open('w')
        print >> p, 'test'
        tp = p.tmp_path
        del p

        self.assertFalse(os.path.exists(tp))
        self.assertFalse(os.path.exists(self.path))

    def test_gzip(self):
        t = File(self.path, luigi.format.Gzip)
        p = t.open('w')
        test_data = 'test'
        p.write(test_data)
        print self.path
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Using gzip module as validation
        f = gzip.open(self.path, 'rb')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying our own gzip reader
        f = File(self.path, luigi.format.Gzip).open('r')
        self.assertTrue(test_data == f.read())
        f.close()
