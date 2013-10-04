# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import unittest

import luigi
from luigi import interface
from helpers import with_config



class NoopTask(luigi.ExternalTask):
    def complete(self):
        return True

class LoggingTest(unittest.TestCase):
    
    @with_config({'loggers': {'keys': 'root,luigi-interface'}, 
                  'handlers': {'keys': 'console'}, 
                  'formatters': {'keys': 'generic'}, 
                  'logger_root': {'level': 'DEBUG', 
                                  'handlers': 'console'}, 
                  'logger_luigi-interface': {'level': 'DEBUG', 
                                             'handlers': 'console',
                                             'qualname': 'luigi-interface',
                                             'propagate': '0'}, 
                  'handler_console': {'class':'StreamHandler', 
                                      'args': '(sys.stdout,)',
                                      'level': 'DEBUG',
                                      'formatter': 'generic'},
                  'formatter_generic': {'format':'%%(message)s'}})
    def test_configured_logging(self):
        # make interface logging reset
        interface.setup_interface_logging.has_run = False
        luigi.run(['--local-scheduler'], main_task_cls=NoopTask)

if __name__ == '__main__':
    unittest.main()
