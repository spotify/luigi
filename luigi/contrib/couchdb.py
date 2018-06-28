# -*- coding: utf-8 -*-
#
# Copyright 2017 Leipzig University Library <info@ub.uni-leipzig.de>
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
# Author: F. Rämisch <raemisch@ub.uni-leipzig.de>

from __future__ import absolute_import

import luigi
import logging

logger = logging.getLogger('luigi-interface')

try:
    import couchdb
except ImportError as e:
    logger.warning("Loading CouchDB module without the python package CouchDB. \
        This will crash at runtime if CouchDB functionality is used.")


class CouchTarget(luigi.Target):
    """
    Target for resources in CouchDB
    """

    def __init__(self, couch_client, database):
        """
        :param couch_client: CouchClient instance
        :type couch_client: CouchClient
        :param database: database name
        :type database: str
        """
        self._couch_client = couch_client
        self._database = database

        try:
            self._db = self._couch_client[self._database]
        except couchdb.http.ResourceNotFound:
            self._db = self._couch_client.create(self._database)


class CouchExistTarget(CouchTarget):
    """
    Target for a resources which simply requires document existense
    """

    def __init__(self, couch_client, database, doc_id):
        """
        :param couch_client: CouchClient instance
        :type couch_client: CouchClient
        :param database: database name
        :type database: str
        :param document_id: identifier of the document
        :type document: str
        """
        super(CouchExistTarget, self).__init__(couch_client, database)
        self._doc_id = doc_id

    def exists(self):
        try:
            return self._doc_id in self._couch_client[self._database]
        except couchdb.http.ResourceNotFound:
            return False


class CouchRevTarget(CouchTarget):
    """
    Target for a resources which requires updated document revision
    """

    def __init__(self, couch_client, database, doc_id):
        """
        :param couch_client: CouchClient instance
        :type couch_client: CouchClient
        :param database: database name
        :type database: str
        :param document_id: identifier of the document
        :type document: str
        """
        super(CouchRevTarget, self).__init__(couch_client, database)
        self._doc_id = doc_id

        try:
            self._rev_orig = self._db[self._doc_id]['_rev']
        except couchdb.http.ResourceNotFound:
            self._rev_orig = None

    def exists(self):
        try:
            return self._db[self._doc_id]['_rev'] != self._rev_orig
        except couchdb.http.ResourceNotFound:
            return False


class CouchCountTarget(CouchTarget):
    """
    Target for a resources which requires a specific number of documents in a database or view.
    """

    def __init__(self, couch_client, database, count, view_id='_all_docs'):
        """
        :param couch_client: CouchClient instance
        :type couch_client: CouchClient
        :param database: database name
        :type database: str
        :param count: No. of documents needed to be finished
        :type count: int
        :param count: Identifier of a custom view, default: _all_docs
        :type count: str
        """
        super(CouchCountTarget, self).__init__(couch_client, database)
        self._count = count
        self._view_id = view_id

    def exists(self):
        return len(self._db.view(self._view_id)) == self._count
