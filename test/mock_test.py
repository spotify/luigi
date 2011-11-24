# -*- coding: utf-8 -*-
# Copyright (c) 2011 Spotify Ltd

from spotify.luigi.mock import MockFile
from spotify.util.test import *

class MockFileTest(TestCase):
    def test_1(self):
        t = MockFile('test')
        p = t.open('w')
        print >> p, 'test'
        p.close()

        q = t.open('r')
        self.assertEqual(list(q), ['test\n'])
        q.close()
