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

import requests
import time
try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

import luigi
from luigi import BoolParameter, Parameter
from luigi import target_factory
from luigi import Task
from luigi.s3 import S3Target


import logging
logger = logging.getLogger('luigi-interface')


class SalesforceConfig(luigi.Config):
    sf_username = luigi.Parameter(config_path=dict(section="salesforce", name="username"))
    sf_password = luigi.Parameter(config_path=dict(section="salesforce", name="password"))
    sf_security_token = luigi.Parameter(config_path=dict(section="salesforce", name="security_token"))


class UploadToSalesforceTask(Task):
    """
    This task will upload a file to Salesforce.

    The data will be uploaded as an upsert job in order to ensure idempotency.  A token
    file is written on job success to track whether the task has completed or not.
    """

    # The file path where token files will be written to track the start/finish
    # start of this task by Luigi.
    #
    # This path should contain components for any custom task parameters that influence
    # the output.  Ex. dates, environments, versions, etc.
    token_path = Parameter()

    # The SF name of the field that ties to a unique id in the original source of the data.
    # This field is required by SF when doing an upsert operation.
    #
    # Must have the SF "__c" suffix if this is for a custom object.
    sf_external_id_field_name = Parameter()

    # The name of the object we're uploading.  Must have the SF "__c" suffix if it
    # is a custom object.
    sf_object_name = Parameter()

    # True iff we should be uploading to a sandbox environment instead of the
    # Production organization.
    sf_use_sandbox = BoolParameter()

    # Name of the sandbox environment being used.  None if using Production.
    sf_sandbox_name = Parameter(default=None)

    # This is the path to the file to be uploaded.  May be a local file or an S3 file.
    sf_upload_file_path = Parameter()

    def output(self):
        """
        Uses the success token as its output to avoid having to query Salesforce to
        determine if the job has completed.
        """
        return [self.success_token()]

    def run(self):
        if self.sf_use_sandbox and not self.sf_sandbox_name:
            raise Exception("Parameter sf_sandbox_name must be provided when uploading to a Salesforce Sandbox")

        sfa = SalesforceAPI(SalesforceConfig().sf_username,
                            SalesforceConfig().sf_password,
                            SalesforceConfig().sf_security_token,
                            self.sf_sandbox_name)

        # Checks to make sure we have a valid target before creating the job in SF
        upload_target = target_factory.get_target(self.sf_upload_file_path)

        job_id = sfa.create_upsert_job(self.sf_object_name, self.sf_external_id_field_name)
        logger.info("Started upsert job %s in salesforce for object %s" % (job_id, self.sf_object_name))

        try:
            batch_id = sfa.create_batch(job_id, upload_target)
            logger.info("Creating new batch %s to upload file: %s for job: %s." % (batch_id, self.sf_upload_file_path, job_id))
            sfa.block_on_batch(job_id, batch_id)
        finally:
            logger.info("Closing job %s" % job_id)
            sfa.close_job(job_id)

        target_factory.write_file(self.success_token(), job_id)

    def success_token(self):
        """
        Writes a token file that indicates this task has finished successfully.
        """
        root_token_path = self.token_path[:-1] if self.token_path[-1] == "/" else self.token_path
        sf_org_name = self.sf_sandbox_name if self.sf_use_sandbox else "production"

        token_path = "%s/%s-%s-Success" % (root_token_path, sf_org_name, self.__class__.__name__)
        logger.info("UploadToSalesforceTask token path: %s" % token_path)
        return target_factory.get_target(token_path)


class SalesforceAPI(object):
    """
    Class used to interact with the SalesforceAPI.  Currently provides only the
    methods necessary for performing a bulk upload operation.
    """
    API_VERSION = 33.0
    SOAP_NS = "{urn:partner.soap.sforce.com}"
    API_NS = "{http://www.force.com/2009/06/asyncapi/dataload}"

    def __init__(self, username, password, security_token, sandbox_name=None):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.sandbox_name = sandbox_name

        if self.sandbox_name:
            self.username += ".%s" % self.sandbox_name

        self.session_id = None
        self.server_url = None
        self.hostname = None

    def start_session(self):
        """
        Starts a Salesforce session and determines which SF instance to use for future requests.
        """
        if self.has_active_session():
            raise Exception("Session already in progress.")

        response = requests.post(self._get_login_url(),
                                 headers=self._get_login_headers(),
                                 data=self._get_login_xml())
        response.raise_for_status()

        root = ET.fromstring(response.text)
        for e in root.iter("%ssessionId" % self.SOAP_NS):
            if self.session_id:
                raise Exception("Invalid login attempt.  Multiple session ids found.")
            self.session_id = e.text

        for e in root.iter("%sserverUrl" % self.SOAP_NS):
            if self.server_url:
                raise Exception("Invalid login attempt.  Multiple server urls found.")
            self.server_url = e.text

        if not self.has_active_session():
            raise Exception("Invalid login attempt resulted in null sessionId [%s] and/or serverUrl [%s]." %
                            (self.session_id, self.server_url))
        self.hostname = urlsplit(self.server_url).hostname

    def create_upsert_job(self, obj, external_id_field_name, content_type='CSV'):
        """
        Creates a new SF job that for doing an upsert upload
        """
        if not self.has_active_session():
            self.start_session()

        response = requests.post(self._get_create_job_url(),
                                 headers=self._get_create_job_headers(),
                                 data=self._get_create_upsert_xml(obj, external_id_field_name, content_type))
        response.raise_for_status()

        root = ET.fromstring(response.text)
        job_id = root.find('%sid' % self.API_NS).text
        return job_id

    def close_job(self, job_id):
        """
        Closes the SF job.
        """
        if not job_id or not self.has_active_session():
            raise Exception("Can not close job without valid job_id and an active session.")

        response = requests.post(self._get_close_job_url(job_id),
                                 headers=self._get_close_job_headers(),
                                 data=self._get_close_job_xml())
        response.raise_for_status()

    def create_batch(self, job_id, file_target):
        """
        Creates a batch for uploading.

        This will pull the contents of the file_target into memory when running.
        That shouldn't be a problem for any files that meet the Salesforce single batch upload
        size limit (10MB) and is done to ensure compressed files can be uploaded properly.
        """
        if not job_id or not self.has_active_session():
            raise Exception("Can not create a batch without a valid job_id and an active session.")

        data = "".join([l for l in file_target.open('r')])
        headers = self._get_create_batch_headers()
        headers['Content-Length'] = len(data)

        response = requests.post(self._get_create_batch_url(job_id),
                                 headers=self._get_create_batch_headers(),
                                 data=data)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        batch_id = root.find('%sid' % self.API_NS).text
        return batch_id

    def block_on_batch(self, job_id, batch_id, sleep_time_seconds=5, max_wait_time_seconds=-1):
        """
        Blocks until @batch_id is completed or failed.
        """
        if not job_id or not batch_id or not self.has_active_session():
            raise Exception("Can not block on a batch without a valid batch_id, job_id and an active session.")

        start_time = time.time()
        status = {}
        while max_wait_time_seconds < 0 or time.time() - start_time < max_wait_time_seconds:
            status = self._get_batch_status(job_id, batch_id)
            logger.info("Batch %s Job %s in state %s.  %s records processed.  %s records failed." %
                        (batch_id, job_id, status['state'], status['num_processed'], status['num_failed']))
            if status['state'].lower() in ["completed", "failed"]:
                return status
            time.sleep(sleep_time_seconds)

        raise Exception("Batch did not complete in %s seconds.  Final status was: %s" % (sleep_time_seconds, status))

    def _get_batch_status(self, job_id, batch_id):
        response = requests.get(self._get_check_batch_status_url(job_id, batch_id),
                                headers=self._get_check_batch_status_headers())
        response.raise_for_status()

        root = ET.fromstring(response.text)

        result = {
            "state": root.find('%sstate' % self.API_NS).text,
            "num_processed": root.find('%snumberRecordsProcessed' % self.API_NS).text,
            "num_failed": root.find('%snumberRecordsFailed' % self.API_NS).text,
        }
        return result

    def has_active_session(self):
        return self.session_id and self.server_url

    def _get_login_url(self):
        server = "login" if not self.sandbox_name else "test"
        return "https://%s.salesforce.com/services/Soap/u/%s" % (server, self.API_VERSION)

    def _get_create_job_url(self):
        return "https://%s/services/async/%s/job" % (self.hostname, self.API_VERSION)

    def _get_close_job_url(self, job_id):
        return "https://%s/services/async/%s/job/%s" % (self.hostname, self.API_VERSION, job_id)

    def _get_create_batch_url(self, job_id):
        return "https://%s/services/async/%s/job/%s/batch" % (self.hostname, self.API_VERSION, job_id)

    def _get_check_batch_status_url(self, job_id, batch_id):
        return "https://%s/services/async/%s/job/%s/batch/%s" % (self.hostname, self.API_VERSION, job_id, batch_id)

    def _get_login_headers(self):
        headers = {
            'Content-Type': "text/xml; charset=UTF-8",
            'SOAPAction': 'login'
        }
        return headers

    def _get_create_job_headers(self):
        headers = {
            'X-SFDC-Session': self.session_id,
            'Content-Type': "application/xml; charset=UTF-8"
        }
        return headers

    def _get_close_job_headers(self):
        headers = {
            'X-SFDC-Session': self.session_id,
            'Content-Type': "application/xml; charset=UTF-8"
        }
        return headers

    def _get_create_batch_headers(self):
        headers = {
            'X-SFDC-Session': self.session_id,
            'Content-Type': "text/csv; charset=UTF-8"
        }
        return headers

    def _get_check_batch_status_headers(self):
        headers = {
            'X-SFDC-Session': self.session_id,
        }
        return headers

    def _get_login_xml(self):
        xml = """<?xml version="1.0" encoding="utf-8" ?>
            <env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
              <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                  <n1:username>%s</n1:username>
                  <n1:password>%s%s</n1:password>
                </n1:login>
              </env:Body>
            </env:Envelope>
        """ % (self.username, self.password, self.security_token)
        return xml

    def _get_create_upsert_xml(self, obj, external_id_field_name, content_type):
        return self._get_create_job_xml('upsert', obj, content_type,
                                        external_id_field_name=external_id_field_name)

    def _get_create_job_xml(self, operation, obj, content_type, external_id_field_name=None):
        external_id_field_name_element = "" if not external_id_field_name else \
            "\n<externalIdFieldName>%s</externalIdFieldName>" % external_id_field_name

        # Note: "Unable to parse job" error may be caused by reordering fields.
        #       ExternalIdFieldName element must be before contentType element.
        xml = """<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                <operation>%s</operation>
                <object>%s</object>
                %s
                <contentType>%s</contentType>
            </jobInfo>
        """ % (operation, obj, external_id_field_name_element, content_type)
        return xml

    def _get_close_job_xml(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
              <state>Closed</state>
            </jobInfo>
        """
        return xml
