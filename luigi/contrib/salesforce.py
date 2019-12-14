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
import time
import abc
import logging
import warnings
import xml.etree.ElementTree as ET
from collections import OrderedDict
import re
import csv
import tempfile
from urllib.parse import urlsplit

import luigi
from luigi import Task

logger = logging.getLogger('luigi-interface')

try:
    import requests
except ImportError:
    logger.warning("This module requires the python package 'requests'.")


def get_soql_fields(soql):
    """
    Gets queried columns names.
    """
    soql_fields = re.search('(?<=select)(?s)(.*)(?=from)', soql, re.IGNORECASE)     # get fields
    soql_fields = re.sub(' ', '', soql_fields.group())                              # remove extra spaces
    soql_fields = re.sub('\t', '', soql_fields)                                     # remove tabs
    fields = re.split(',|\n|\r|', soql_fields)                                      # split on commas and newlines
    fields = [field for field in fields if field != '']                             # remove empty strings
    return fields


def ensure_utf(value):
    return value.encode("utf-8") if isinstance(value, unicode) else value


def parse_results(fields, data):
    """
    Traverses ordered dictionary, calls _traverse_results() to recursively read into the dictionary depth of data
    """
    master = []

    for record in data['records']:  # for each 'record' in response
        row = [None] * len(fields)  # create null list the length of number of columns
        for obj, value in record.items():  # for each obj in record
            if not isinstance(value, (dict, list, tuple)):  # if not data structure
                if obj in fields:
                    row[fields.index(obj)] = ensure_utf(value)

            elif isinstance(value, dict) and obj != 'attributes':  # traverse down into object
                path = obj
                _traverse_results(value, fields, row, path)

        master.append(row)
    return master


def _traverse_results(value, fields, row, path):
    """
    Helper method for parse_results().

    Traverses through ordered dict and recursively calls itself when encountering a dictionary
    """
    for f, v in value.items():  # for each item in obj
        field_name = '{path}.{name}'.format(path=path, name=f) if path else f

        if not isinstance(v, (dict, list, tuple)):  # if not data structure
            if field_name in fields:
                row[fields.index(field_name)] = ensure_utf(v)

        elif isinstance(v, dict) and f != 'attributes':  # it is a dict
            _traverse_results(v, fields, row, field_name)


class salesforce(luigi.Config):
    """
    Config system to get config vars from 'salesforce' section in configuration file.

    Did not include sandbox_name here, as the user may have multiple sandboxes.
    """
    username = luigi.Parameter(default='')
    password = luigi.Parameter(default='')
    security_token = luigi.Parameter(default='')

    # sandbox token
    sb_security_token = luigi.Parameter(default='')


class QuerySalesforce(Task):
    @property
    @abc.abstractmethod
    def object_name(self):
        """
        Override to return the SF object we are querying.
        Must have the SF "__c" suffix if it is a customer object.
        """
        return None

    @property
    def use_sandbox(self):
        """
        Override to specify use of SF sandbox.
        True iff we should be uploading to a sandbox environment instead of the production organization.
        """
        return False

    @property
    def sandbox_name(self):
        """Override to specify the sandbox name if it is intended to be used."""
        return None

    @property
    @abc.abstractmethod
    def soql(self):
        """Override to return the raw string SOQL or the path to it."""
        return None

    @property
    def is_soql_file(self):
        """Override to True if soql property is a file path."""
        return False

    @property
    def content_type(self):
        """
        Override to use a different content type. Salesforce allows XML, CSV, ZIP_CSV, or ZIP_XML. Defaults to CSV.
        """
        return "CSV"

    def run(self):
        if self.use_sandbox and not self.sandbox_name:
            raise Exception("Parameter sf_sandbox_name must be provided when uploading to a Salesforce Sandbox")

        sf = SalesforceAPI(salesforce().username,
                           salesforce().password,
                           salesforce().security_token,
                           salesforce().sb_security_token,
                           self.sandbox_name)

        job_id = sf.create_operation_job('query', self.object_name, content_type=self.content_type)
        logger.info("Started query job %s in salesforce for object %s" % (job_id, self.object_name))

        batch_id = ''
        msg = ''
        try:
            if self.is_soql_file:
                with open(self.soql, 'r') as infile:
                    self.soql = infile.read()

            batch_id = sf.create_batch(job_id, self.soql, self.content_type)
            logger.info("Creating new batch %s to query: %s for job: %s." % (batch_id, self.object_name, job_id))
            status = sf.block_on_batch(job_id, batch_id)
            if status['state'].lower() == 'failed':
                msg = "Batch failed with message: %s" % status['state_message']
                logger.error(msg)
                # don't raise exception if it's b/c of an included relationship
                # normal query will execute (with relationship) after bulk job is closed
                if 'foreign key relationships not supported' not in status['state_message'].lower():
                    raise Exception(msg)
            else:
                result_ids = sf.get_batch_result_ids(job_id, batch_id)

                # If there's only one result, just download it, otherwise we need to merge the resulting downloads
                if len(result_ids) == 1:
                    data = sf.get_batch_result(job_id, batch_id, result_ids[0])
                    with open(self.output().path, 'wb') as outfile:
                        outfile.write(data)
                else:
                    # Download each file to disk, and then merge into one.
                    # Preferring to do it this way so as to minimize memory consumption.
                    for i, result_id in enumerate(result_ids):
                        logger.info("Downloading batch result %s for batch: %s and job: %s" % (result_id, batch_id, job_id))
                        with open("%s.%d" % (self.output().path, i), 'wb') as outfile:
                            outfile.write(sf.get_batch_result(job_id, batch_id, result_id))

                    logger.info("Merging results of batch %s" % batch_id)
                    self.merge_batch_results(result_ids)
        finally:
            logger.info("Closing job %s" % job_id)
            sf.close_job(job_id)

        if 'state_message' in status and 'foreign key relationships not supported' in status['state_message'].lower():
            logger.info("Retrying with REST API query")
            data_file = sf.query_all(self.soql)

            reader = csv.reader(data_file)
            with open(self.output().path, 'wb') as outfile:
                writer = csv.writer(outfile, dialect='excel')
                for row in reader:
                    writer.writerow(row)

    def merge_batch_results(self, result_ids):
        """
        Merges the resulting files of a multi-result batch bulk query.
        """
        outfile = open(self.output().path, 'w')

        if self.content_type.lower() == 'csv':
            for i, result_id in enumerate(result_ids):
                with open("%s.%d" % (self.output().path, i), 'r') as f:
                    header = f.readline()
                    if i == 0:
                        outfile.write(header)
                    for line in f:
                        outfile.write(line)
        else:
            raise Exception("Batch result merging not implemented for %s" % self.content_type)

        outfile.close()


class SalesforceAPI:
    """
    Class used to interact with the SalesforceAPI.  Currently provides only the
    methods necessary for performing a bulk upload operation.
    """
    API_VERSION = 34.0
    SOAP_NS = "{urn:partner.soap.sforce.com}"
    API_NS = "{http://www.force.com/2009/06/asyncapi/dataload}"

    def __init__(self, username, password, security_token, sb_token=None, sandbox_name=None):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.sb_security_token = sb_token
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

    def has_active_session(self):
        return self.session_id and self.server_url

    def query(self, query, **kwargs):
        """
        Return the result of a Salesforce SOQL query as a dict decoded from the Salesforce response JSON payload.

        :param query: the SOQL query to send to Salesforce, e.g. "SELECT id from Lead WHERE email = 'a@b.com'"
        """
        params = {'q': query}
        response = requests.get(self._get_norm_query_url(),
                                headers=self._get_rest_headers(),
                                params=params,
                                **kwargs)
        if response.status_code != requests.codes.ok:
            raise Exception(response.content)

        return response.json()

    def query_more(self, next_records_identifier, identifier_is_url=False, **kwargs):
        """
        Retrieves more results from a query that returned more results
        than the batch maximum. Returns a dict decoded from the Salesforce
        response JSON payload.

        :param next_records_identifier: either the Id of the next Salesforce
                                     object in the result, or a URL to the
                                     next record in the result.
        :param identifier_is_url: True if `next_records_identifier` should be
                               treated as a URL, False if
                               `next_records_identifer` should be treated as
                               an Id.
        """
        if identifier_is_url:
            # Don't use `self.base_url` here because the full URI is provided
            url = (u'https://{instance}{next_record_url}'
                   .format(instance=self.hostname,
                           next_record_url=next_records_identifier))
        else:
            url = self._get_norm_query_url() + '{next_record_id}'
            url = url.format(next_record_id=next_records_identifier)
        response = requests.get(url, headers=self._get_rest_headers(), **kwargs)

        response.raise_for_status()

        return response.json()

    def query_all(self, query, **kwargs):
        """
        Returns the full set of results for the `query`. This is a
        convenience wrapper around `query(...)` and `query_more(...)`.
        The returned dict is the decoded JSON payload from the final call to
        Salesforce, but with the `totalSize` field representing the full
        number of results retrieved and the `records` list representing the
        full list of records retrieved.

        :param query: the SOQL query to send to Salesforce, e.g.
                   `SELECT Id FROM Lead WHERE Email = "waldo@somewhere.com"`
        """
        # Make the initial query to Salesforce
        response = self.query(query, **kwargs)

        # get fields
        fields = get_soql_fields(query)

        # put fields and first page of results into a temp list to be written to TempFile
        tmp_list = [fields]
        tmp_list.extend(parse_results(fields, response))

        tmp_dir = luigi.configuration.get_config().get('salesforce', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(mode='a+b', dir=tmp_dir)

        writer = csv.writer(tmp_file)
        writer.writerows(tmp_list)

        # The number of results might have exceeded the Salesforce batch limit
        # so check whether there are more results and retrieve them if so.

        length = len(response['records'])
        while not response['done']:
            response = self.query_more(response['nextRecordsUrl'], identifier_is_url=True, **kwargs)

            writer.writerows(parse_results(fields, response))
            length += len(response['records'])
            if not length % 10000:
                logger.info('Requested {0} lines...'.format(length))

        logger.info('Requested a total of {0} lines.'.format(length))

        tmp_file.seek(0)
        return tmp_file

    # Generic Rest Function
    def restful(self, path, params):
        """
        Allows you to make a direct REST call if you know the path
        Arguments:
        :param path: The path of the request. Example: sobjects/User/ABC123/password'
        :param params: dict of parameters to pass to the path
        """

        url = self._get_norm_base_url() + path
        response = requests.get(url, headers=self._get_rest_headers(), params=params)

        if response.status_code != 200:
            raise Exception(response)
        json_result = response.json(object_pairs_hook=OrderedDict)
        if len(json_result) == 0:
            return None
        else:
            return json_result

    def create_operation_job(self, operation, obj, external_id_field_name=None, content_type=None):
        """
        Creates a new SF job that for doing any operation (insert, upsert, update, delete, query)

        :param operation: delete, insert, query, upsert, update, hardDelete. Must be lowercase.
        :param obj: Parent SF object
        :param external_id_field_name: Optional.
        """
        if not self.has_active_session():
            self.start_session()

        response = requests.post(self._get_create_job_url(),
                                 headers=self._get_create_job_headers(),
                                 data=self._get_create_job_xml(operation, obj, external_id_field_name, content_type))
        response.raise_for_status()

        root = ET.fromstring(response.text)
        job_id = root.find('%sid' % self.API_NS).text
        return job_id

    def get_job_details(self, job_id):
        """
        Gets all details for existing job

        :param job_id: job_id as returned by 'create_operation_job(...)'
        :return: job info as xml
        """
        response = requests.get(self._get_job_details_url(job_id))

        response.raise_for_status()

        return response

    def abort_job(self, job_id):
        """
        Abort an existing job. When a job is aborted, no more records are processed.
        Changes to data may already have been committed and aren't rolled back.

        :param job_id: job_id as returned by 'create_operation_job(...)'
        :return: abort response as xml
        """
        response = requests.post(self._get_abort_job_url(job_id),
                                 headers=self._get_abort_job_headers(),
                                 data=self._get_abort_job_xml())
        response.raise_for_status()

        return response

    def close_job(self, job_id):
        """
        Closes job

        :param job_id: job_id as returned by 'create_operation_job(...)'
        :return: close response as xml
        """
        if not job_id or not self.has_active_session():
            raise Exception("Can not close job without valid job_id and an active session.")

        response = requests.post(self._get_close_job_url(job_id),
                                 headers=self._get_close_job_headers(),
                                 data=self._get_close_job_xml())
        response.raise_for_status()

        return response

    def create_batch(self, job_id, data, file_type):
        """
        Creates a batch with either a string of data or a file containing data.

        If a file is provided, this will pull the contents of the file_target into memory when running.
        That shouldn't be a problem for any files that meet the Salesforce single batch upload
        size limit (10MB) and is done to ensure compressed files can be uploaded properly.

        :param job_id: job_id as returned by 'create_operation_job(...)'
        :param data:

        :return: Returns batch_id
        """
        if not job_id or not self.has_active_session():
            raise Exception("Can not create a batch without a valid job_id and an active session.")

        headers = self._get_create_batch_content_headers(file_type)
        headers['Content-Length'] = str(len(data))

        response = requests.post(self._get_create_batch_url(job_id),
                                 headers=headers,
                                 data=data)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        batch_id = root.find('%sid' % self.API_NS).text
        return batch_id

    def block_on_batch(self, job_id, batch_id, sleep_time_seconds=5, max_wait_time_seconds=-1):
        """
        Blocks until @batch_id is completed or failed.
        :param job_id:
        :param batch_id:
        :param sleep_time_seconds:
        :param max_wait_time_seconds:
        """
        if not job_id or not batch_id or not self.has_active_session():
            raise Exception("Can not block on a batch without a valid batch_id, job_id and an active session.")

        start_time = time.time()
        status = {}
        while max_wait_time_seconds < 0 or time.time() - start_time < max_wait_time_seconds:
            status = self._get_batch_info(job_id, batch_id)
            logger.info("Batch %s Job %s in state %s.  %s records processed.  %s records failed." %
                        (batch_id, job_id, status['state'], status['num_processed'], status['num_failed']))
            if status['state'].lower() in ["completed", "failed"]:
                return status
            time.sleep(sleep_time_seconds)

        raise Exception("Batch did not complete in %s seconds.  Final status was: %s" % (sleep_time_seconds, status))

    def get_batch_results(self, job_id, batch_id):
        """
        DEPRECATED: Use `get_batch_result_ids`
        """
        warnings.warn("get_batch_results is deprecated and only returns one batch result. Please use get_batch_result_ids")
        return self.get_batch_result_ids(job_id, batch_id)[0]

    def get_batch_result_ids(self, job_id, batch_id):
        """
        Get result IDs of a batch that has completed processing.

        :param job_id: job_id as returned by 'create_operation_job(...)'
        :param batch_id: batch_id as returned by 'create_batch(...)'
        :return: list of batch result IDs to be used in 'get_batch_result(...)'
        """
        response = requests.get(self._get_batch_results_url(job_id, batch_id),
                                headers=self._get_batch_info_headers())
        response.raise_for_status()

        root = ET.fromstring(response.text)
        result_ids = [r.text for r in root.findall('%sresult' % self.API_NS)]

        return result_ids

    def get_batch_result(self, job_id, batch_id, result_id):
        """
        Gets result back from Salesforce as whatever type was originally sent in create_batch (xml, or csv).
        :param job_id:
        :param batch_id:
        :param result_id:

        """
        response = requests.get(self._get_batch_result_url(job_id, batch_id, result_id),
                                headers=self._get_session_headers())
        response.raise_for_status()

        return response.content

    def _get_batch_info(self, job_id, batch_id):
        response = requests.get(self._get_batch_info_url(job_id, batch_id),
                                headers=self._get_batch_info_headers())
        response.raise_for_status()

        root = ET.fromstring(response.text)

        result = {
            "state": root.find('%sstate' % self.API_NS).text,
            "num_processed": root.find('%snumberRecordsProcessed' % self.API_NS).text,
            "num_failed": root.find('%snumberRecordsFailed' % self.API_NS).text,
        }
        if root.find('%sstateMessage' % self.API_NS) is not None:
            result['state_message'] = root.find('%sstateMessage' % self.API_NS).text
        return result

    def _get_login_url(self):
        server = "login" if not self.sandbox_name else "test"
        return "https://%s.salesforce.com/services/Soap/u/%s" % (server, self.API_VERSION)

    def _get_base_url(self):
        return "https://%s/services" % self.hostname

    def _get_bulk_base_url(self):
        # Expands on Base Url for Bulk
        return "%s/async/%s" % (self._get_base_url(), self.API_VERSION)

    def _get_norm_base_url(self):
        # Expands on Base Url for Norm
        return "%s/data/v%s" % (self._get_base_url(), self.API_VERSION)

    def _get_norm_query_url(self):
        # Expands on Norm Base Url
        return "%s/query" % self._get_norm_base_url()

    def _get_create_job_url(self):
        # Expands on Bulk url
        return "%s/job" % (self._get_bulk_base_url())

    def _get_job_id_url(self, job_id):
        # Expands on Job Creation url
        return "%s/%s" % (self._get_create_job_url(), job_id)

    def _get_job_details_url(self, job_id):
        # Expands on basic Job Id url
        return self._get_job_id_url(job_id)

    def _get_abort_job_url(self, job_id):
        # Expands on basic Job Id url
        return self._get_job_id_url(job_id)

    def _get_close_job_url(self, job_id):
        # Expands on basic Job Id url
        return self._get_job_id_url(job_id)

    def _get_create_batch_url(self, job_id):
        # Expands on basic Job Id url
        return "%s/batch" % (self._get_job_id_url(job_id))

    def _get_batch_info_url(self, job_id, batch_id):
        # Expands on Batch Creation url
        return "%s/%s" % (self._get_create_batch_url(job_id), batch_id)

    def _get_batch_results_url(self, job_id, batch_id):
        # Expands on Batch Info url
        return "%s/result" % (self._get_batch_info_url(job_id, batch_id))

    def _get_batch_result_url(self, job_id, batch_id, result_id):
        # Expands on Batch Results url
        return "%s/%s" % (self._get_batch_results_url(job_id, batch_id), result_id)

    def _get_login_headers(self):
        headers = {
            'Content-Type': "text/xml; charset=UTF-8",
            'SOAPAction': 'login'
        }
        return headers

    def _get_session_headers(self):
        headers = {
            'X-SFDC-Session': self.session_id
        }
        return headers

    def _get_norm_session_headers(self):
        headers = {
            'Authorization': 'Bearer %s' % self.session_id
        }
        return headers

    def _get_rest_headers(self):
        headers = self._get_norm_session_headers()
        headers['Content-Type'] = 'application/json'
        return headers

    def _get_job_headers(self):
        headers = self._get_session_headers()
        headers['Content-Type'] = "application/xml; charset=UTF-8"
        return headers

    def _get_create_job_headers(self):
        return self._get_job_headers()

    def _get_abort_job_headers(self):
        return self._get_job_headers()

    def _get_close_job_headers(self):
        return self._get_job_headers()

    def _get_create_batch_content_headers(self, content_type):
        headers = self._get_session_headers()
        content_type = 'text/csv' if content_type.lower() == 'csv' else 'application/xml'
        headers['Content-Type'] = "%s; charset=UTF-8" % content_type
        return headers

    def _get_batch_info_headers(self):
        return self._get_session_headers()

    def _get_login_xml(self):
        return """<?xml version="1.0" encoding="utf-8" ?>
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
        """ % (self.username, self.password, self.security_token if self.sandbox_name is None else self.sb_security_token)

    def _get_create_job_xml(self, operation, obj, external_id_field_name, content_type):
        external_id_field_name_element = "" if not external_id_field_name else \
            "\n<externalIdFieldName>%s</externalIdFieldName>" % external_id_field_name

        # Note: "Unable to parse job" error may be caused by reordering fields.
        #       ExternalIdFieldName element must be before contentType element.
        return """<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                <operation>%s</operation>
                <object>%s</object>
                %s
                <contentType>%s</contentType>
            </jobInfo>
        """ % (operation, obj, external_id_field_name_element, content_type)

    def _get_abort_job_xml(self):
        return """<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
              <state>Aborted</state>
            </jobInfo>
        """

    def _get_close_job_xml(self):
        return """<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
              <state>Closed</state>
            </jobInfo>
        """
