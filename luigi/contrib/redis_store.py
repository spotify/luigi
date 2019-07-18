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

import datetime
import logging

from luigi.target import Target
from luigi.parameter import Parameter

logger = logging.getLogger('luigi-interface')

try:
    import redis

except ImportError:
    logger.warning("Loading redis_store module without redis installed. "
                   "Will crash at runtime if redis_store functionality is used.")


class RedisTarget(Target):

    """ Target for a resource in Redis."""

    marker_prefix = Parameter(default='luigi',
                              config_path=dict(section='redis', name='marker-prefix'))

    def __init__(self, host, port, db, update_id, password=None,
                 socket_timeout=None, expire=None):
        """
        :param host: Redis server host
        :type host: str
        :param port: Redis server port
        :type port: int
        :param db: database index
        :type db: int
        :param update_id: an identifier for this data hash
        :type update_id: str
        :param password: a password to connect to the redis server
        :type password: str
        :param socket_timeout: client socket timeout
        :type socket_timeout: int
        :param expire: timeout before the target is deleted
        :type expire: int

        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.update_id = update_id
        self.expire = expire

        self.redis_client = redis.StrictRedis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db,
            socket_timeout=self.socket_timeout,
        )

    def marker_key(self):
        """
        Generate a key for the indicator hash.
        """
        return '%s:%s' % (self.marker_prefix, self.update_id)

    def touch(self):
        """
        Mark this update as complete.

        We index the parameters `update_id` and `date`.
        """
        marker_key = self.marker_key()
        self.redis_client.hset(marker_key, 'update_id', self.update_id)
        self.redis_client.hset(marker_key, 'date', datetime.datetime.now().isoformat())

        if self.expire is not None:
            self.redis_client.expire(marker_key, self.expire)

    def exists(self):
        """
        Test, if this task has been run.
        """
        return self.redis_client.exists(self.marker_key()) == 1
