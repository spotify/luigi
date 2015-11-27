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
import decimal
from helpers import unittest

import luigi
import luigi.notifications
from luigi.mock import MockTarget

luigi.notifications.DEBUG = True


class Report(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        f = self.output().open('w')
        f.write('10.0 USD\n')
        f.write('4.0 EUR\n')
        f.write('3.0 USD\n')
        f.close()

    def output(self):
        return MockTarget(self.date.strftime('/tmp/report-%Y-%m-%d'))


class ReportReader(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return Report(self.date)

    def run(self):
        self.lines = list(self.input().open('r').readlines())

    def get_line(self, line):
        amount, currency = self.lines[line].strip().split()
        return decimal.Decimal(amount), currency

    def complete(self):
        return False


class CurrencyExchanger(luigi.Task):
    task = luigi.Parameter()
    currency_to = luigi.Parameter()

    exchange_rates = {('USD', 'USD'): decimal.Decimal(1),
                      ('EUR', 'USD'): decimal.Decimal('1.25')}

    def requires(self):
        return self.task  # Note that you still need to state this explicitly

    def get_line(self, line):
        amount, currency_from = self.task.get_line(line)
        return amount * self.exchange_rates[(currency_from, self.currency_to)], self.currency_to

    def complete(self):
        return False


class InstanceWrapperTest(unittest.TestCase):

    ''' This test illustrates that tasks can have tasks as parameters

    This is a more complicated variant of factorial_test.py which is an example of
    tasks communicating directly with other tasks. In this case, a task takes another
    task as a parameter and wraps it.

    Also see wrap_test.py for an example of a task class wrapping another task class.

    Not the most useful pattern, but there's actually been a few cases where it was
    pretty handy to be able to do that. I'm adding it as a unit test to make sure that
    new code doesn't break the expected behavior.
    '''

    def test(self):
        d = datetime.date(2012, 1, 1)
        r = ReportReader(d)
        ex = CurrencyExchanger(r, 'USD')

        luigi.build([ex], local_scheduler=True)
        self.assertEqual(ex.get_line(0), (decimal.Decimal('10.0'), 'USD'))
        self.assertEqual(ex.get_line(1), (decimal.Decimal('5.0'), 'USD'))
