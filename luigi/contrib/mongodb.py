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

from luigi.target import Target


class MongoTarget(Target):

    """ Target for a resource in MongoDB """

    def __init__(self, mongo_client, index, collection):
        """
        :param mongo_client: MongoClient instance
        :type mongo_client: MongoClient
        :param index: database index
        :type index: str
        :param collection: index collection
        :type collection: str
        """
        self._mongo_client = mongo_client
        self._index = index
        self._collection = collection

    def get_collection(self):
        """
        Return targeted mongo collection to query on
        """
        db_mongo = self._mongo_client[self._index]
        return db_mongo[self._collection]

    def get_index(self):
        """
        Return targeted mongo index to query on
        """
        return self._mongo_client[self._index]


class MongoCellTarget(MongoTarget):

    """ Target for a ressource in a specific field from a MongoDB document """

    def __init__(self, mongo_client, index, collection, document_id, path):
        """
        :param document_id: targeted mongo document
        :type document_id: str
        :param path: full path to the targeted field in the mongo document
        :type path: str
        """
        super(MongoCellTarget, self).__init__(mongo_client, index, collection)

        self._document_id = document_id
        self._path = path

    def exists(self):
        """
        Test if target has been run
        Target is considered run if the targeted field exists
        """
        return self.read() is not None

    def read(self):
        """
        Read the target value
        Use $project aggregate operator in order to support nested objects
        """
        result = self.get_collection().aggregate([
            {'$match': {'_id': self._document_id}},
            {'$project': {'_value': '$' + self._path, '_id': False}}
        ])

        for doc in result:
            if '_value' not in doc:
                break

            return doc['_value']

    def write(self, value):
        """
        Write value to the target
        """
        self.get_collection().update_one(
            {'_id': self._document_id},
            {'$set': {self._path: value}},
            upsert=True
        )


class MongoRangeTarget(MongoTarget):

    """ Target for a level 0 field in a range of documents """

    def __init__(self, mongo_client, index, collection, document_ids, field):
        """
        :param document_ids: targeted mongo documents
        :type documents_ids: list of str
        :param field: targeted field in documents
        :type field: str
        """
        super(MongoRangeTarget, self).__init__(mongo_client, index, collection)

        self._document_ids = document_ids
        self._field = field

    def exists(self):
        """
        Test if target has been run
        Target is considered run if the targeted field exists in ALL documents
        """
        return not self.get_empty_ids()

    def read(self):
        """
        Read the targets value
        """
        cursor = self.get_collection().find(
            {
                '_id': {'$in': self._document_ids},
                self._field: {'$exists': True}
            },
            {self._field: True}
        )

        return {doc['_id']: doc[self._field] for doc in cursor}

    def write(self, values):
        """
        Write values to the targeted documents
        Values need to be a dict as : {document_id: value}
        """
        # Insert only for docs targeted by the target
        filtered = {_id: value for _id, value in values.items() if _id in self._document_ids}

        if not filtered:
            return

        bulk = self.get_collection().initialize_ordered_bulk_op()
        for _id, value in filtered.items():
            bulk.find({'_id': _id}).upsert() \
                    .update_one({'$set': {self._field: value}})

        bulk.execute()

    def get_empty_ids(self):
        """
        Get documents id with missing targeted field
        """
        cursor = self.get_collection().find(
            {
                '_id': {'$in': self._document_ids},
                self._field: {'$exists': True}
            },
            {'_id': True}
        )

        return set(self._document_ids) - {doc['_id'] for doc in cursor}


class MongoCollectionTarget(MongoTarget):

    """ Target for existing collection """

    def __init__(self, mongo_client, index, collection):
        super(MongoCollectionTarget, self).__init__(mongo_client, index, collection)

    def exists(self):
        """
        Test if target has been run
        Target is considered run if the targeted collection exists in the database
        """
        return self.read()

    def read(self):
        """
        Return if the target collection exists in the database
        """
        return self._collection in self.get_index().collection_names()


class MongoCountTarget(MongoTarget):

    """ Target for documents count """

    def __init__(self, mongo_client, index, collection, target_count):
        """
        :param target_count: Value of the desired item count in the target
        :type field: int
        """
        super(MongoCountTarget, self).__init__(mongo_client, index, collection)

        self._target_count = target_count

    def exists(self):
        """
        Test if the target has been run
        Target is considered run if the number of items in the target matches value of self._target_count
        """
        return self.read() == self._target_count

    def read(self):
        """
        Using the aggregate method to avoid inaccurate count if using a sharded cluster
        https://docs.mongodb.com/manual/reference/method/db.collection.count/#behavior
        """
        for res in self.get_collection().aggregate([{'$group': {'_id': None, 'count': {'$sum': 1}}}]):
            return res.get('count', None)
        return None
