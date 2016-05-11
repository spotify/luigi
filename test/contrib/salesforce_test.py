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

import luigi
from luigi.contrib.salesforce import SalesforceAPI

from helpers import unittest
import mock

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

    return MockResponse('<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload"><result>1234</result><result>1235</result><result>1236</result></result-list>', 200)

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
