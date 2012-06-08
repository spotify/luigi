# -*- coding: utf-8 -*-
# Copyright (c) 2011 Spotify Ltd

from luigi import File
import unittest
import os

class FileTest(unittest.TestCase):
    path = '/tmp/test.txt'
    def setUp(self):
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
