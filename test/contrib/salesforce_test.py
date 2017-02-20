# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Simply Measured
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
#
# This method will be used by the mock to replace requests.get

"""
Unit test for the Salesforce contrib package
"""

from luigi.contrib.salesforce import SalesforceAPI, QuerySalesforce

from helpers import unittest
import mock
from luigi.mock import MockTarget
from luigi.six import PY3
import re


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, body, status_code):
            self.body = body
            self.status_code = status_code

        @property
        def text(self):
            return self.body

        def raise_for_status(self):
            return None

    result_list = (
        '<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
        '<result>1234</result><result>1235</result><result>1236</result>'
        '</result-list>'
    )
    return MockResponse(result_list, 200)


# Keep open around so we can use it in the mock responses
old__open = open


def mocked_open(*args, **kwargs):
    if re.match("job_data", str(args[0])):
        return MockTarget(args[0]).open(args[1])
    else:
        return old__open(*args)


class TestSalesforceAPI(unittest.TestCase):
    # We patch 'requests.get' with our own method. The mock object is passed in to our test case method.
    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_deprecated_results(self, mock_get):
        sf = SalesforceAPI('xx', 'xx', 'xx')
        result_id = sf.get_batch_results('job_id', 'batch_id')
        self.assertEqual('1234', result_id)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_result_ids(self, mock_get):
        sf = SalesforceAPI('xx', 'xx', 'xx')
        result_ids = sf.get_batch_result_ids('job_id', 'batch_id')
        self.assertEqual(['1234', '1235', '1236'], result_ids)


class TestQuerySalesforce(QuerySalesforce):
    def output(self):
        return MockTarget('job_data.csv')

    @property
    def object_name(self):
        return 'dual'

    @property
    def soql(self):
        return "SELECT * FROM %s" % self.object_name


class TestSalesforceQuery(unittest.TestCase):
    patch_name = '__builtin__.open'
    if PY3:
        patch_name = 'builtins.open'

    @mock.patch(patch_name, side_effect=mocked_open)
    def setUp(self, mock_open):
        MockTarget.fs.clear()
        self.result_ids = ['a', 'b', 'c']

        counter = 1
        self.all_lines = "Lines\n"
        self.header = "Lines"
        for i, id in enumerate(self.result_ids):
            filename = "%s.%d" % ('job_data.csv', i)
            with MockTarget(filename).open('w') as f:
                line = "%d line\n%d line" % ((counter), (counter+1))
                f.write(self.header + "\n" + line + "\n")
                self.all_lines += line+"\n"
                counter += 2

    @mock.patch(patch_name, side_effect=mocked_open)
    def test_multi_csv_download(self, mock_open):
        qsf = TestQuerySalesforce()

        qsf.merge_batch_results(self.result_ids)
        self.assertEqual(MockTarget(qsf.output().path).open('r').read(), self.all_lines)
