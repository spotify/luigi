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

from helpers import unittest

from helpers import with_config
from mock import patch
from luigi import notifications
from boto.sns import SNSConnection

class TestEmail(unittest.TestCase):

    def testEmailNoPrefix(self):
        self.assertEqual("subject", notifications._prefix('subject'))

    @with_config({"core": {"email-prefix": "[prefix]"}})
    def testEmailPrefix(self):
        self.assertEqual("[prefix] subject", notifications._prefix('subject'))

class TestSNSNotifications(unittest.TestCase):

    def testNotificationsNoSNS(self):
        with patch.object(SNSConnection, 'publish') as mock_publish:
            notifications.send_error_email("error", "something bad happened!")
        
        assert not mock_publish.called

    @with_config({"core": {"error-sns-topic": "test-topic"}})
    def testNotificationsSNS(self):
        with patch.object(SNSConnection, 'publish') as mock_publish:
            notifications.send_error_email("error", "something bad happened!")

        mock_publish.assert_called_once_with("test-topic", "something bad happened!", "error")
    
