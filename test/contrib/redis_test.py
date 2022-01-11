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

# pylint: disable=F0401
from time import sleep
from helpers import unittest

import pytest

try:
    import redis
except ImportError:
    raise unittest.SkipTest('Unable to load redis module')

from luigi.contrib.redis_store import RedisTarget

HOST = 'localhost'
PORT = 6379
DB = 15
PASSWORD = None
SOCKET_TIMEOUT = None
MARKER_PREFIX = 'luigi_test'
EXPIRE = 5


@pytest.mark.contrib
class RedisTargetTest(unittest.TestCase):

    """ Test touch, exists and target expiration"""

    def test_touch_and_exists(self):
        target = RedisTarget(HOST, PORT, DB, 'update_id', PASSWORD)
        target.marker_prefix = MARKER_PREFIX
        flush()
        self.assertFalse(target.exists(),
                         'Target should not exist before touching it')
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it')
        flush()

    def test_expiration(self):
        target = RedisTarget(
            HOST, PORT, DB, 'update_id', PASSWORD, None, EXPIRE)
        target.marker_prefix = MARKER_PREFIX
        flush()
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it and before expiring')
        sleep(EXPIRE)
        self.assertFalse(target.exists(),
                         'Target should not exist after expiring')
        flush()


def flush():
    """ Flush test DB"""
    redis_client = redis.StrictRedis(
        host=HOST, port=PORT, db=DB, socket_timeout=SOCKET_TIMEOUT)
    redis_client.flushdb()
