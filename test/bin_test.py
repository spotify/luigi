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

import imp
import mock
from server_test import ServerTest


class LuigidTest(ServerTest):

    def run_server(self):
        luigid = imp.load_source('luigid', 'bin/luigid')
        luigid.main(['--port', str(self._api_port)])


class LuigidDaemonTest(ServerTest):

    @mock.patch('daemon.DaemonContext')
    def run_server(self, daemon_context):
        luigid = imp.load_source('luigid', 'bin/luigid')
        luigid.main(['--port', str(self._api_port), '--background', '--logdir', '.'])
