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

import luigi
from fib_test import FibTestBase
from luigi.mock import MockTarget


class OptParseTest(FibTestBase):

    def test_cmdline_optparse(self):
        luigi.run(['--local-scheduler', '--no-lock', '--task', 'Fib', '--n', '100'], use_optparse=True)

        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_10'), b'55\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_100'), b'354224848179261915075\n')

    def test_cmdline_optparse_existing(self):
        import optparse
        parser = optparse.OptionParser()
        parser.add_option('--blaha')

        luigi.run(['--local-scheduler', '--no-lock', '--task', 'Fib', '--n', '100'], use_optparse=True, existing_optparse=parser)

        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_10'), b'55\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_100'), b'354224848179261915075\n')
