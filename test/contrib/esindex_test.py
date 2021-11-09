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
"""
Tests for Elasticsearch index (esindex) target and indexing.

An Elasticsearch server must be running for these tests.

To use a non-standard host and port, use `ESINDEX_TEST_HOST`,
`ESINDEX_TEST_PORT` environment variables to override defaults.

To test HTTP basic authentication `ESINDEX_TEST_HTTP_AUTH`.

Example running tests against port 9201 with basic auth:

    $ ESINDEX_TEST_PORT=9201 ESINDEX_TEST_HTTP_AUTH='admin:admin' nosetests test/_esindex_test.py

"""

# pylint: disable=C0103,E1101,F0401
import collections
import datetime
import os
from helpers import unittest

import elasticsearch
import luigi
from elasticsearch.connection import Urllib3HttpConnection
from luigi.contrib.esindex import CopyToIndex, ElasticsearchTarget

import pytest

HOST = os.getenv('ESINDEX_TEST_HOST', 'localhost')
PORT = os.getenv('ESINDEX_TEST_PORT', 9200)
HTTP_AUTH = os.getenv('ESINDEX_TEST_HTTP_AUTH', None)
INDEX = 'esindex_luigi_test'
DOC_TYPE = 'esindex_test_type'
MARKER_INDEX = 'esindex_luigi_test_index_updates'
MARKER_DOC_TYPE = 'esindex_test_entry'


def _create_test_index():
    """ Create content index, if if does not exists. """
    es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)
    if not es.indices.exists(INDEX):
        es.indices.create(INDEX)


try:
    _create_test_index()
except Exception:
    raise unittest.SkipTest('Unable to connect to ElasticSearch')


@pytest.mark.aws
class ElasticsearchTargetTest(unittest.TestCase):

    """ Test touch and exists. """

    def test_touch_and_exists(self):
        """ Basic test. """
        target = ElasticsearchTarget(HOST, PORT, INDEX, DOC_TYPE, 'update_id', http_auth=HTTP_AUTH)
        target.marker_index = MARKER_INDEX
        target.marker_doc_type = MARKER_DOC_TYPE

        delete()
        self.assertFalse(target.exists(),
                         'Target should not exist before touching it')
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it')
        delete()


def delete():
    """ Delete marker_index, if it exists. """
    es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)
    if es.indices.exists(MARKER_INDEX):
        es.indices.delete(MARKER_INDEX)
    es.indices.refresh()


class CopyToTestIndex(CopyToIndex):

    """ Override the default `marker_index` table with a test name. """
    host = HOST
    port = PORT
    http_auth = HTTP_AUTH
    index = INDEX
    doc_type = DOC_TYPE
    marker_index_hist_size = 0

    def output(self):
        """ Use a test target with an own marker_index. """
        target = ElasticsearchTarget(
            host=self.host,
            port=self.port,
            http_auth=self.http_auth,
            index=self.index,
            doc_type=self.doc_type,
            update_id=self.update_id(),
            marker_index_hist_size=self.marker_index_hist_size
        )
        target.marker_index = MARKER_INDEX
        target.marker_doc_type = MARKER_DOC_TYPE
        return target


class IndexingTask1(CopyToTestIndex):

    """ Test the redundant version, where `_index` and `_type` are
    given in the `docs` as well. A more DRY example is `IndexingTask2`. """

    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 123, '_index': self.index, '_type': self.doc_type,
                 'name': 'sample', 'date': 'today'}]


class IndexingTask2(CopyToTestIndex):

    """ Just another task. """

    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 234, '_index': self.index, '_type': self.doc_type,
                 'name': 'another', 'date': 'today'}]


class IndexingTask3(CopyToTestIndex):

    """ This task will request an empty index to start with. """
    purge_existing_index = True

    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 234, '_index': self.index, '_type': self.doc_type,
                 'name': 'yet another', 'date': 'today'}]


def _cleanup():
    """ Delete both the test marker index and the content index. """
    es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)
    if es.indices.exists(MARKER_INDEX):
        es.indices.delete(MARKER_INDEX)
    if es.indices.exists(INDEX):
        es.indices.delete(INDEX)


@pytest.mark.aws
class CopyToIndexTest(unittest.TestCase):

    """ Test indexing tasks. """

    @classmethod
    def setUpClass(cls):
        cls.es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)

    def setUp(self):
        """ Cleanup before each test. """
        _cleanup()

    def tearDown(self):
        """ Remove residues after each test. """
        _cleanup()

    def test_copy_to_index(self):
        """ Test a single document upload. """
        task = IndexingTask1()
        self.assertFalse(self.es.indices.exists(task.index))
        self.assertFalse(task.complete())
        luigi.build([task], local_scheduler=True)
        self.assertTrue(self.es.indices.exists(task.index))
        self.assertTrue(task.complete())
        self.assertEqual(1, self.es.count(index=task.index).get('count'))
        self.assertEqual({u'date': u'today', u'name': u'sample'},
                         self.es.get_source(index=task.index,
                                            doc_type=task.doc_type, id=123))

    def test_copy_to_index_incrementally(self):
        """ Test two tasks that upload docs into the same index. """
        task1 = IndexingTask1()
        task2 = IndexingTask2()
        self.assertFalse(self.es.indices.exists(task1.index))
        self.assertFalse(self.es.indices.exists(task2.index))
        self.assertFalse(task1.complete())
        self.assertFalse(task2.complete())
        luigi.build([task1, task2], local_scheduler=True)
        self.assertTrue(self.es.indices.exists(task1.index))
        self.assertTrue(self.es.indices.exists(task2.index))
        self.assertTrue(task1.complete())
        self.assertTrue(task2.complete())
        self.assertEqual(2, self.es.count(index=task1.index).get('count'))
        self.assertEqual(2, self.es.count(index=task2.index).get('count'))

        self.assertEqual({u'date': u'today', u'name': u'sample'},
                         self.es.get_source(index=task1.index,
                                            doc_type=task1.doc_type, id=123))

        self.assertEqual({u'date': u'today', u'name': u'another'},
                         self.es.get_source(index=task2.index,
                                            doc_type=task2.doc_type, id=234))

    def test_copy_to_index_purge_existing(self):
        """ Test purge_existing_index purges index. """
        task1 = IndexingTask1()
        task2 = IndexingTask2()
        task3 = IndexingTask3()
        luigi.build([task1, task2], local_scheduler=True)
        luigi.build([task3], local_scheduler=True)
        self.assertTrue(self.es.indices.exists(task3.index))
        self.assertTrue(task3.complete())
        self.assertEqual(1, self.es.count(index=task3.index).get('count'))

        self.assertEqual({u'date': u'today', u'name': u'yet another'},
                         self.es.get_source(index=task3.index,
                                            doc_type=task3.doc_type, id=234))


@pytest.mark.aws
class MarkerIndexTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)

    def setUp(self):
        """ Cleanup before each test. """
        _cleanup()

    def tearDown(self):
        """ Remove residues after each test. """
        _cleanup()

    def test_update_marker(self):
        def will_raise():
            self.es.count(
                index=MARKER_INDEX,
                doc_type=MARKER_DOC_TYPE,
                body={'query': {'match_all': {}}}
            )

        self.assertRaises(elasticsearch.NotFoundError, will_raise)

        task1 = IndexingTask1()
        luigi.build([task1], local_scheduler=True)

        result = self.es.count(index=MARKER_INDEX, doc_type=MARKER_DOC_TYPE,
                               body={'query': {'match_all': {}}})
        self.assertEqual(1, result.get('count'))

        result = self.es.search(index=MARKER_INDEX, doc_type=MARKER_DOC_TYPE,
                                body={'query': {'match_all': {}}})
        marker_doc = result.get('hits').get('hits')[0].get('_source')
        self.assertEqual(task1.task_id, marker_doc.get('update_id'))
        self.assertEqual(INDEX, marker_doc.get('target_index'))
        self.assertEqual(DOC_TYPE, marker_doc.get('target_doc_type'))
        self.assertTrue('date' in marker_doc)

        task2 = IndexingTask2()
        luigi.build([task2], local_scheduler=True)

        result = self.es.count(index=MARKER_INDEX, doc_type=MARKER_DOC_TYPE,
                               body={'query': {'match_all': {}}})
        self.assertEqual(2, result.get('count'))

        result = self.es.search(index=MARKER_INDEX, doc_type=MARKER_DOC_TYPE,
                                body={'query': {'match_all': {}}})
        hits = result.get('hits').get('hits')
        Entry = collections.namedtuple('Entry', ['date', 'update_id'])
        dates_update_id = []
        for hit in hits:
            source = hit.get('_source')
            update_id = source.get('update_id')
            date = source.get('date')
            dates_update_id.append(Entry(date, update_id))

        it = iter(sorted(dates_update_id))
        first = next(it)
        second = next(it)
        self.assertTrue(first.date < second.date)
        self.assertEqual(first.update_id, task1.task_id)
        self.assertEqual(second.update_id, task2.task_id)


class IndexingTask4(CopyToTestIndex):

    """ Just another task. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    marker_index_hist_size = 1

    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 234, '_index': self.index, '_type': self.doc_type,
                 'name': 'another', 'date': 'today'}]


@pytest.mark.aws
class IndexHistSizeTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.es = elasticsearch.Elasticsearch(connection_class=Urllib3HttpConnection, host=HOST, port=PORT, http_auth=HTTP_AUTH)

    def setUp(self):
        """ Cleanup before each test. """
        _cleanup()

    def tearDown(self):
        """ Remove residues after each test. """
        _cleanup()

    def test_limited_history(self):

        task4_1 = IndexingTask4(date=datetime.date(2000, 1, 1))
        luigi.build([task4_1], local_scheduler=True)

        task4_2 = IndexingTask4(date=datetime.date(2001, 1, 1))
        luigi.build([task4_2], local_scheduler=True)

        task4_3 = IndexingTask4(date=datetime.date(2002, 1, 1))
        luigi.build([task4_3], local_scheduler=True)

        result = self.es.count(index=MARKER_INDEX, doc_type=MARKER_DOC_TYPE,
                               body={'query': {'match_all': {}}})
        self.assertEqual(1, result.get('count'))
        marker_index_document_id = task4_3.output().marker_index_document_id()
        result = self.es.get(id=marker_index_document_id, index=MARKER_INDEX,
                             doc_type=MARKER_DOC_TYPE)
        self.assertEqual(task4_3.task_id,
                         result.get('_source').get('update_id'))
