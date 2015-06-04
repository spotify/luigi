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

from helpers import unittest

import luigi.target
from luigi.contrib.target import CascadingClient


class CascadingClientTest(unittest.TestCase):

    def setUp(self):
        class FirstClient:

            def exists(self, pos_arg, kw_arg='first'):
                if pos_arg < 10:
                    return pos_arg
                elif pos_arg < 20:
                    return kw_arg
                elif kw_arg == 'raise_fae':
                    raise luigi.target.FileAlreadyExists('oh noes!')
                else:
                    raise Exception()

        class SecondClient:

            def exists(self, pos_arg, other_kw_arg='second',
                       kw_arg='for-backwards-compatibility'):
                if pos_arg < 30:
                    return -pos_arg
                elif pos_arg < 40:
                    return other_kw_arg
                else:
                    raise Exception()

        self.clients = [FirstClient(), SecondClient()]
        self.client = CascadingClient(self.clients)

    def test_successes(self):
        self.assertEqual(5, self.client.exists(5))
        self.assertEqual('yay', self.client.exists(15, kw_arg='yay'))

    def test_fallbacking(self):
        self.assertEqual(-25, self.client.exists(25))
        self.assertEqual('lol', self.client.exists(35, kw_arg='yay',
                                                   other_kw_arg='lol'))
        # Note: the first method don't accept the other keyword argument
        self.assertEqual(-15, self.client.exists(15, kw_arg='yay',
                                                 other_kw_arg='lol'))

    def test_failings(self):
        self.assertRaises(Exception, lambda: self.client.exists(45))
        self.assertRaises(AttributeError, lambda: self.client.mkdir())

    def test_FileAlreadyExists_propagation(self):
        self.assertRaises(luigi.target.FileAlreadyExists,
                          lambda: self.client.exists(25, kw_arg='raise_fae'))

    def test_method_names_kwarg(self):
        self.client = CascadingClient(self.clients, method_names=[])
        self.assertRaises(AttributeError, lambda: self.client.exists())
        self.client = CascadingClient(self.clients, method_names=['exists'])
        self.assertEqual(5, self.client.exists(5))
