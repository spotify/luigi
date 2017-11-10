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

import unittest

from luigi.contrib.couchdb import CouchExistTarget, CouchRevTarget, CouchCountTarget

HOST = 'localhost'
PORT = 5984
DATABASE = 'luigi_test'

try:
    import couchdb
    from couchdb.design import ViewDefinition
    couch = couchdb.Server('http://{0}:{1}/'.format(HOST, PORT))
except ImportError:
    raise unittest.SkipTest('Unable to load CouchDB module')
except Exception:
    raise unittest.SkipTest('Unable to connect to local CouchDB instance')

TEST_DOCS = [
    {'_id': 'person_1', 'name': 'Martin'},
    {'_id': 'person_2', 'name': 'Peter', 'surname': 'Klaussen'},
    {'_id': 'person_3', 'surname': 'Oledottir'},
    {'_id': 'person_4', 'surname': ''}
]


class CouchExistTargetTest(unittest.TestCase):

    """ CouchExistTarget unittest on local test database """

    def setUp(self):
        """
        Fill test database with fake data
        """
        self.couch_client = couchdb.Server('http://{0}:{1}/'.format(HOST, PORT))
        try:
            self.couch_client.delete(DATABASE)
        except couchdb.http.ResourceNotFound:
            pass
        self.db = self.couch_client.create(DATABASE)

        for doc in TEST_DOCS:
            self.db.save(doc)

    def tearDown(self):
        """
        Make sure the test database is in clean state
        """
        self.couch_client.delete(DATABASE)

    def test_exists(self):
        test_values = [
            ('person_1', True),
            ('person_2', True),
            ('person_3', True),
            ('unknow_person', False),
        ]

        for id_, result in test_values:
            target = CouchExistTarget(self.couch_client, DATABASE, id_)
            self.assertEqual(result, target.exists())


class CouchRevTargetTest(unittest.TestCase):

    """ CouchRevTarget unittest on local test database """

    def setUp(self):
        """
        Fill test database with fake data
        """
        self.couch_client = couchdb.Server('http://{0}:{1}/'.format(HOST, PORT))
        try:
            self.couch_client.delete(DATABASE)
        except couchdb.http.ResourceNotFound:
            pass
        self.db = self.couch_client.create(DATABASE)

        for doc in TEST_DOCS:
            self.db.save(doc)

    def tearDown(self):
        """
        Make sure the test database is in clean state
        """
        self.couch_client.delete(DATABASE)

    def test_exists(self):
        test_values_before_update = [
            ('person_1', False),
            ('person_2', False),
            ('person_3', False),
            ('unknow_person', False),
        ]
        test_values_after_update = [
            ('person_1', True),
            ('person_2', True),
            ('person_3', False),
            ('unknow_person', False),
        ]
        targets = list()

        # First all targets are instanciated, and _rev_orig is set
        for i, (id_, result) in enumerate(test_values_before_update):
            targets.append(CouchRevTarget(self.couch_client, DATABASE, id_))
            self.assertEqual(result, targets[i].exists())

        # Update all documents, except one
        for doc in self.db:
            if doc == "person_3":
                continue
            self.db.save(self.db[doc])

        # Check again for target existence
        for i, (id_, result) in enumerate(test_values_after_update):
            self.assertEqual(result, targets[i].exists())


class CouchCountTargetTest(unittest.TestCase):

    """ CouchCountTarget unittest on local test database """

    def setUp(self):
        self.couch_client = couchdb.Server('http://{0}:{1}/'.format(HOST, PORT))
        try:
            self.couch_client.delete(DATABASE)
        except couchdb.http.ResourceNotFound:
            pass
        self.db = self.couch_client.create(DATABASE)

    def tearDown(self):
        """
        Make sure the test database is in clean state
        """
        self.couch_client.delete(DATABASE)

    def test_exists(self):
        test_values = [
            (1, False),
            (2, False),
            (3, True),
            (4, False),
        ]

        target = CouchCountTarget(self.couch_client, DATABASE, 3)

        for doc, (d, result) in zip(TEST_DOCS, test_values):
            self.db.save(doc)
            self.assertEqual(result, target.exists())

    def test_custom_view(self):
        cview = ViewDefinition('cview',
                               'cview',
                               """function(doc) { emit(doc.name, 1); }""")
        cview.sync(self.db)

        init_length = len(self.db.view('_design/cview/_view/cview'))

        test_values = [
            (init_length, False),
            (init_length+1, True),
            (init_length+2, False),
        ]

        target = CouchCountTarget(self.couch_client,
                                  DATABASE,
                                  init_length+1,
                                  '_design/cview/_view/cview')

        for (d, result), doc in zip(test_values, TEST_DOCS):
            self.assertEqual(result, target.exists())
            self.db.save(doc)


if __name__ == '__main__':
    unittest.main()
