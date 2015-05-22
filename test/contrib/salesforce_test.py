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

from helpers import with_config, unittest
import luigi
from luigi.contrib.salesforce import UploadToSalesforceTask, SalesforceAPI
from mock import patch
import os
import requests
import tempfile


class UploadToSalesforceTaskTest(unittest.TestCase):
    def setUp(self):
        self.data = 'somedata'
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.write(self.data)
        self.tmp.close()

        self.unicode_data = u"aàçñ"
        self.unicode_tmp = tempfile.NamedTemporaryFile(delete=False)
        self.unicode_tmp.write(self.unicode_data.encode('utf8'))
        self.unicode_tmp.close()

    def tearDown(self):
        if self.tmp:
            os.remove(self.tmp.name)
            self.tmp = None

    @with_config({'salesforce': {'username': 'user',
                                 'password': "pw",
                                 'security_token': 'st'}})
    @patch('luigi.contrib.salesforce.SalesforceAPI')
    def test_run__success(self, mock):
        job_id = "123"
        batch_id = "456"

        sf_object_name = "object_name"
        sf_external_id_field_name = "external_id_field_name"
        token_path = tempfile.gettempdir()

        mock.return_value.create_upsert_job = self._get_mock_create_job(sf_object_name, sf_external_id_field_name, job_id)
        mock.return_value.create_batch = self._get_mock_create_batch(job_id, self.tmp.name, batch_id)
        mock.return_value.block_on_batch = self._get_mock_block_on_batch(job_id, batch_id, True)
        mock.return_value.close_job = self._get_mock_close_job(job_id)

        job = UploadToSalesforceTask(token_path=token_path,
                                     sf_object_name=sf_object_name,
                                     sf_external_id_field_name=sf_external_id_field_name,
                                     sf_use_sandbox=False,
                                     sf_upload_file_path=self.tmp.name)

        success_token = job.success_token()

        if success_token.exists():
            success_token.remove()

        self.assertFalse(success_token.exists())
        job.run()
        self.assertTrue(success_token.exists())

    @with_config({'salesforce': {'username': 'user-unicode',
                                 'password': "pw-unicode",
                                 'security_token': 'st-unicode'}})
    @patch('luigi.contrib.salesforce.SalesforceAPI')
    def test_run__unicode_success(self, mock):
        job_id = "123"
        batch_id = "456"

        sf_object_name = "object_name"
        sf_external_id_field_name = "external_id_field_name"
        token_path = tempfile.gettempdir()

        mock.return_value.create_upsert_job = self._get_mock_create_job(sf_object_name, sf_external_id_field_name, job_id)
        mock.return_value.create_batch = self._get_mock_create_batch(job_id, self.unicode_tmp.name, batch_id)
        mock.return_value.block_on_batch = self._get_mock_block_on_batch(job_id, batch_id, True)
        mock.return_value.close_job = self._get_mock_close_job(job_id)

        job = UploadToSalesforceTask(token_path=token_path,
                                     sf_object_name=sf_object_name,
                                     sf_external_id_field_name=sf_external_id_field_name,
                                     sf_use_sandbox=False,
                                     sf_upload_file_path=self.unicode_tmp.name)

        success_token = job.success_token()

        if success_token.exists():
            success_token.remove()

        self.assertFalse(success_token.exists())
        job.run()
        self.assertTrue(success_token.exists())

    @with_config({'salesforce': {'username': 'user',
                                 'password': "pw",
                                 'security_token': 'st'}})
    @patch('luigi.contrib.salesforce.SalesforceAPI')
    def test_run_batch_fails(self, mock):
        job_id = "23"
        batch_id = "56"

        sf_object_name = "object_name"
        sf_external_id_field_name = "external_id_field_name"
        token_path = tempfile.gettempdir()

        mock.return_value.create_upsert_job = self._get_mock_create_job(sf_object_name, sf_external_id_field_name, job_id)
        mock.return_value.create_batch = self._get_mock_create_batch(job_id, self.tmp.name, batch_id)
        mock.return_value.block_on_batch = self._get_mock_block_on_batch(job_id, batch_id, False)
        mock.return_value.close_job = self._get_mock_close_job(job_id)

        job = UploadToSalesforceTask(token_path=token_path,
                                     sf_object_name=sf_object_name,
                                     sf_external_id_field_name=sf_external_id_field_name,
                                     sf_use_sandbox=False,
                                     sf_upload_file_path=self.tmp.name)

        success_token = job.success_token()

        if success_token.exists():
            success_token.remove()

        self.assertFalse(success_token.exists())
        self.assertRaises(Exception, job.run)
        self.assertFalse(success_token.exists())

    def _get_mock_create_job(self, expected_object, expected_field_name, returned_job_id):
        def _create_job(obj, field_name):
            self.assertEqual(expected_object, obj)
            self.assertEqual(expected_field_name, field_name)
            return returned_job_id
        return _create_job

    def _get_mock_create_batch(self, expected_job_id, expected_path, returned_batch_id):
        def _create_batch(job_id, upload_path_target):
            self.assertEqual(expected_job_id, job_id)
            self.assertEqual(expected_path, upload_path_target.path)
            return returned_batch_id
        return _create_batch

    def _get_mock_block_on_batch(self, expected_job_id, expected_batch_id, should_return):
        def _create_block_on_batch(job_id, batch_id):
            self.assertEqual(expected_job_id, job_id)
            self.assertEqual(expected_batch_id, batch_id)
            if not should_return:
                raise Exception("Timed out")
            return {}
        return _create_block_on_batch

    def _get_mock_close_job(self, expected_job_id):
        def _create_close_job(job_id):
            self.assertEqual(expected_job_id, job_id)
        return _create_close_job


class SalesforceAPITest(unittest.TestCase):

    def setUp(self):
        self.sfa = SalesforceAPI('user', 'pw', 'eid')

    @patch('requests.post')
    def test_start_session(self, mock):
        instance = "some-server-instance.salesforce.com"
        server_url = "https://%s/services/Soap/m/33.0/" % instance
        session_id = "some-session-id"

        mock.return_value = MockResponse(self._get_login_xml(session_id, server_url))
        self.sfa.start_session()
        self.assertEquals(instance, self.sfa.hostname)
        self.assertEquals(server_url, self.sfa.server_url)
        self.assertEquals(session_id, self.sfa.session_id)

    @patch('requests.post')
    def test_create_upsert_job(self, mock):
        self.sfa.session_id = "some-session-id"
        self.sfa.server_url = "some-server-url"
        job_id = "12345"

        mock.return_value = MockResponse(self._get_upsert_xml(job_id))

        result_job_id = self.sfa.create_upsert_job('obj', 'eid')
        self.assertEquals(job_id, result_job_id)

    @patch('requests.post')
    def test_create_batch(self, mock):
        self.sfa.session_id = "some-session-id"
        self.sfa.server_url = "some-server-url"
        batch_id = "abcdef"

        mock.return_value = MockResponse(self._get_create_batch_xml(batch_id))

        data = 'somedata'
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_file.write(data)
        tmp_file.close()

        result_batch_id = self.sfa.create_batch('job-id', luigi.target_factory.get_target(tmp_file.name))
        self.assertEquals(batch_id, result_batch_id)

        os.remove(tmp_file.name)

    @patch('requests.get')
    def test_block_on_batch(self, mock):
        self.sfa.session_id = "some-session-id"
        self.sfa.server_url = "some-server-url"

        # Test Multiple Calls - success
        mock.side_effect = [
            MockResponse(self._get_batch_status_xml("Queued")),
            MockResponse(self._get_batch_status_xml("Completed")),
        ]
        status = self.sfa.block_on_batch('job-id', 'batch-id', sleep_time_seconds=0, max_wait_time_seconds=2)
        self.assertEquals('5', status['num_processed'])
        self.assertEquals('Completed', status['state'])

        # Test Multiple Calls - timeout
        mock.return_value = MockResponse(self._get_batch_status_xml("Queued"))
        self.assertRaises(Exception, self.sfa.block_on_batch, 'job-id', 'batch-id', sleep_time_seconds=0, max_wait_time_seconds=1)

    def _get_login_xml(self, session_id="a-session-id", server_url="a-server-url"):
        return """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:partner.soap.sforce.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <soapenv:Body>
      <loginResponse>
         <result>
            <metadataServerUrl>%smeta</metadataServerUrl>
            <passwordExpired>false</passwordExpired>
            <sandbox>true</sandbox>
            <serverUrl>%s</serverUrl>
            <sessionId>%s</sessionId>
            <userId>some-user-id</userId>
         </result>
      </loginResponse>
   </soapenv:Body>
</soapenv:Envelope>
        """ % (server_url, server_url, session_id)

    def _get_upsert_xml(self, job_id):
        return """<?xml version="1.0" encoding="UTF-8"?>
<jobInfo
   xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <id>%s</id>
  <operation>insert</operation>
  <object>Contact</object>
  <createdById>005x0000000wPWdAAM</createdById>
  <createdDate>2009-09-01T16:42:46.000Z</createdDate>
  <systemModstamp>2009-09-01T16:42:46.000Z</systemModstamp>
  <state>Open</state>
  <concurrencyMode>Parallel</concurrencyMode>
  <contentType>CSV</contentType>
  <numberBatchesQueued>0</numberBatchesQueued>
  <numberBatchesInProgress>0</numberBatchesInProgress>
  <numberBatchesCompleted>5</numberBatchesCompleted>
  <numberBatchesFailed>0</numberBatchesFailed>
  <numberBatchesTotal>0</numberBatchesTotal>
  <numberRecordsProcessed>0</numberRecordsProcessed>
  <numberRetries>0</numberRetries>
  <apiVersion>33.0</apiVersion>
</jobInfo>
        """ % job_id

    def _get_create_batch_xml(self, batch_id):
        return """<?xml version="1.0" encoding="UTF-8"?>
<batchInfo
   xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <id>%s</id>
  <jobId>750x0000000005LAAQ</jobId>
  <state>Queued</state>
  <createdDate>2009-09-01T17:44:45.000Z</createdDate>
  <systemModstamp>2009-09-01T17:44:45.000Z</systemModstamp>
  <numberRecordsProcessed>0</numberRecordsProcessed>
</batchInfo>
        """ % batch_id

    def _get_batch_status_xml(self, state):
        return """<?xml version="1.0" encoding="UTF-8"?><batchInfo
   xmlns="http://www.force.com/2009/06/asyncapi/dataload">
 <id>some-id</id>
 <jobId>some-job-id</jobId>
 <state>%s</state>
 <createdDate>2015-05-21T15:09:35.000Z</createdDate>
 <systemModstamp>2015-05-21T15:09:35.000Z</systemModstamp>
 <numberRecordsProcessed>5</numberRecordsProcessed>
 <numberRecordsFailed>0</numberRecordsFailed>
 <totalProcessingTime>0</totalProcessingTime>
 <apiActiveProcessingTime>0</apiActiveProcessingTime>
 <apexProcessingTime>0</apexProcessingTime>
</batchInfo>
        """ % state


class MockResponse(object):
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass
