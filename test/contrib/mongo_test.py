# -*- coding: utf-8 -*-
#
# Copyright 2017 Big Datext Inc
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


from helpers import unittest

from luigi.contrib.mongodb import MongoCellTarget, MongoRangeTarget

import pytest

HOST = 'localhost'
PORT = 27017
INDEX = 'luigi_test'
COLLECTION = 'luigi_collection'

try:
    import pymongo
    mongo_client = pymongo.MongoClient(HOST, PORT)
    mongo_client.server_info()
except ImportError:
    raise unittest.SkipTest('Unable to load pymongo module')
except Exception:
    raise unittest.SkipTest('Unable to connect to local mongoDB instance')


@pytest.mark.contrib
class MongoCellTargetTest(unittest.TestCase):

    """ MongoCellTarget unittest on local test database """

    def setUp(self):
        """
        Fill test database with fake data
        """
        self.mongo_client = pymongo.MongoClient(HOST, PORT)
        self.collection = self.mongo_client[INDEX][COLLECTION]

        self.collection.delete_many({})

        test_docs = [
            {'_id': 'person_1', 'name': 'Mike', 'infos': {'family': 'single'}},
            {'_id': 'person_2', 'name': 'Laura', 'surname': 'Gilmore'},
            {'_id': 'person_3', 'surname': 'Specter'},
            {'_id': 'person_4', 'surname': '', 'infos': {'family': {'children': ['jack', 'rose']}}}
        ]

        self.collection.insert_many(test_docs)

    def tearDown(self):
        """
        Make sure the test database is in clean state
        """
        self.collection.drop()
        self.mongo_client.drop_database(INDEX)

    def test_exists(self):
        test_values = [
            ('person_1', 'surname', False),
            ('person_2', 'surname', True),
            ('person_3', 'surname', True),
            ('unknow_person', 'surname', False),
        ]

        for id_, field, result in test_values:
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, field)
            self.assertEqual(result, target.exists())

    def test_exists_nested(self):
        test_values = [
            ('person_1', 'infos', True),
            ('person_1', 'infos.family', True),
            ('person_2', 'family', False),
            ('person_4', 'infos', True),
            ('person_4', 'infos.family', True),
            ('person_4', 'infos.sexe', False),
            ('person_4', 'infos.family.children', True),
            ('person_4', 'infos.family.aunt', False),
        ]

        for id_, path, result in test_values:
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, path)
            self.assertEqual(result, target.exists())

    def test_read(self):
        test_values = [
            ('person_1', 'surname', None),
            ('person_2', 'surname', 'Gilmore'),
            ('person_3', 'surname', 'Specter'),
            ('person_4', 'surname', ''),
            ('unknown_person', 'surname', None),
        ]

        for id_, field, result in test_values:
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, field)
            self.assertEqual(result, target.read())

    def test_read_nested(self):
        test_values = [
            ('person_1', 'infos', {'family': 'single'}),
            ('person_1', 'infos.family', 'single'),
            ('person_2', 'family', None),
            ('person_4', 'infos', {'family': {'children': ['jack', 'rose']}}),
            ('person_4', 'infos.family', {'children': ['jack', 'rose']}),
            ('person_4', 'infos.sexe', None),
            ('person_4', 'infos.family.children', ['jack', 'rose']),
        ]

        for id_, path, result in test_values:
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, path)
            self.assertEqual(result, target.read())

    def test_write(self):
        ids = ['person_1', 'person_2', 'person_3', 'person_4', 'unknow_person']

        for id_ in ids:
            self.setUp()
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, 'age')
            target.write('100')
            self.assertEqual(target.read(), '100')

    def test_write_nested(self):
        test_values = [
            ('person_1', 'infos', 12),
            ('person_1', 'infos.family', ['ambre', 'justin', 'sophia']),
            ('person_2', 'hobbies', {'soccer': True}),
            ('person_3', 'infos', {'age': '100'}),
            ('person_3', 'infos.hobbies', {'soccer': True}),
            ('person_3', 'infos.hobbies.soccer', [{'status': 'young'}, 'strong', 'fast']),
        ]

        for id_, path, new_value in test_values:
            self.setUp()
            target = MongoCellTarget(self.mongo_client, INDEX, COLLECTION, id_, path)
            target.write(new_value)
            self.assertEqual(target.read(), new_value)
            self.tearDown()


@pytest.mark.contrib
class MongoRangerTargetTest(unittest.TestCase):

    """ MongoRangelTarget unittest on local test database """

    def setUp(self):
        """
        Fill test database with fake data
        """
        self.mongo_client = pymongo.MongoClient(HOST, PORT)
        self.collection = self.mongo_client[INDEX][COLLECTION]

        self.collection.delete_many({})

        test_docs = [
            {'_id': 'person_1', 'age': 11, 'experience': 10, 'content': "Lorem ipsum, dolor sit amet. Consectetur adipiscing elit."},
            {'_id': 'person_2', 'age': 12, 'experience': 22, 'content': "Sed purus nisl. Faucibus in, erat eu. Rhoncus mattis velit."},
            {'_id': 'person_3', 'age': 13, 'content': "Nulla malesuada, fringilla lorem at pellentesque."},
            {'_id': 'person_4', 'age': 14, 'content': "Curabitur condimentum. Venenatis fringilla."}
        ]

        self.collection.insert_many(test_docs)

    def tearDown(self):
        """
        Make sure the test database is in clean state
        """
        self.collection.drop()
        self.mongo_client.drop_database(INDEX)

    def test_exists(self):
        test_values = [
            ('age', [], True),
            ('age', ['person_1', 'person_2', 'person_3'], True),
            ('experience', ['person_1', 'person_2', 'person_3', 'person_4'], False),
            ('experience', ['person_1', 'person_2'], True),
            ('unknow_field', ['person_1', 'person_2'], False),
            ('experience', ['unknow_person'], False),
            ('experience', ['person_1', 'unknown_person'], False),
            ('experience', ['person_3', 'unknown_person'], False),
        ]

        for field, ids, result in test_values:
            target = MongoRangeTarget(self.mongo_client, INDEX, COLLECTION, ids, field)
            self.assertEqual(result, target.exists())

    def test_read(self):
        test_values = [
            ('age', [], {}),
            ('age', ['unknown_person'], {}),
            ('age', ['person_1', 'person_3'], {'person_1': 11, 'person_3': 13}),
            ('age', ['person_1', 'person_3', 'person_5'], {'person_1': 11, 'person_3': 13}),
            ('experience', ['person_1', 'person_3'], {'person_1': 10}),
            ('experience', ['person_1', 'person_3', 'person_5'], {'person_1': 10}),
        ]

        for field, ids, result in test_values:
            target = MongoRangeTarget(self.mongo_client, INDEX, COLLECTION, ids, field)
            self.assertEqual(result, target.read())

    def test_write(self):
        test_values = [
            (
                'age',                                              # feature
                ['person_1'],                                       # ids
                {'person_1': 31},                                   # arg of write()
                ({'_id': {'$in': ['person_1']}}, {'age': True}),    # mongo request to fetch result
                [{'_id': 'person_1', 'age': 31}],                   # result
            ),
            (
                'experience',
                ['person_1', 'person_3'],
                {'person_1': 31, 'person_3': 32},
                ({'_id': {'$in': ['person_1', 'person_3']}}, {'experience': True}),
                [{'_id': 'person_1', 'experience': 31}, {'_id': 'person_3', 'experience': 32}],
            ),
            (
                'experience',
                [],
                {'person_3': 18},
                ({'_id': {'$in': ['person_1', 'person_3']}}, {'experience': True}),
                [{'_id': 'person_1', 'experience': 10}, {'_id': 'person_3'}],
            ),
            (
                'age',
                ['person_1'],
                {'person_1': ['young', 'old']},
                ({'_id': 'person_1'}, {'age': True}),
                [{'_id': 'person_1', 'age': ['young', 'old']}],
            ),
            (
                'age',
                ['person_1'],
                {'person_1': {'feeling_like': 60}},
                ({'_id': 'person_1'}, {'age': True}),
                [{'_id': 'person_1', 'age': {'feeling_like': 60}}],
            ),
            (
                'age',
                ['person_1'],
                {'person_1': [{'feeling_like': 60}, 24]},
                ({'_id': 'person_1'}, {'age': True}),
                [{'_id': 'person_1', 'age': [{'feeling_like': 60}, 24]}],
            ),
        ]

        for field, ids, docs, req, result in test_values:
            self.setUp()
            target = MongoRangeTarget(self.mongo_client, INDEX, COLLECTION, ids, field)
            target.write(docs)
            self.assertEqual(result, list(self.collection.find(*req)))
            self.tearDown()
