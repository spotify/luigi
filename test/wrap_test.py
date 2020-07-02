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

import datetime
from helpers import unittest

import luigi
import luigi.notifications
from luigi.mock import MockTarget
from luigi.util import inherits

luigi.notifications.DEBUG = True


class A(luigi.Task):

    def output(self):
        return MockTarget('/tmp/a.txt')

    def run(self):
        f = self.output().open('w')
        print('hello, world', file=f)
        f.close()


class B(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return MockTarget(self.date.strftime('/tmp/b-%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')
        print('goodbye, space', file=f)
        f.close()


def XMLWrapper(cls):
    @inherits(cls)
    class XMLWrapperCls(luigi.Task):

        def requires(self):
            return self.clone_parent()

        def run(self):
            f = self.input().open('r')
            g = self.output().open('w')
            print('<?xml version="1.0" ?>', file=g)
            for line in f:
                print('<dummy-xml>' + line.strip() + '</dummy-xml>', file=g)
            g.close()

    return XMLWrapperCls


class AXML(XMLWrapper(A)):

    def output(self):
        return MockTarget('/tmp/a.xml')


class BXML(XMLWrapper(B)):

    def output(self):
        return MockTarget(self.date.strftime('/tmp/b-%Y-%m-%d.xml'))


class WrapperTest(unittest.TestCase):

    ''' This test illustrates how a task class can wrap another task class by modifying its behavior.

    See instance_wrap_test.py for an example of how instances can wrap each other. '''
    workers = 1

    def setUp(self):
        MockTarget.fs.clear()

    def test_a(self):
        luigi.build([AXML()], local_scheduler=True, no_lock=True, workers=self.workers)
        self.assertEqual(MockTarget.fs.get_data('/tmp/a.xml'), b'<?xml version="1.0" ?>\n<dummy-xml>hello, world</dummy-xml>\n')

    def test_b(self):
        luigi.build([BXML(datetime.date(2012, 1, 1))], local_scheduler=True, no_lock=True, workers=self.workers)
        self.assertEqual(MockTarget.fs.get_data('/tmp/b-2012-01-01.xml'), b'<?xml version="1.0" ?>\n<dummy-xml>goodbye, space</dummy-xml>\n')


class WrapperWithMultipleWorkersTest(WrapperTest):
    workers = 7
