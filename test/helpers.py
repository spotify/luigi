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

import functools
import sys

from luigi import six

# import unittest on python 2.6 for support of test skip
if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
else:
    import unittest


class with_config(object):

    """Decorator to override config settings for the length of a function. Example:

      >>> @with_config({'foo': {'bar': 'baz'}})
      >>> def test():
      >>>  print luigi.configuration.get_config.get("foo", "bar")
      >>> test()
      baz
    """

    def __init__(self, config, replace_sections=False):
        self.config = config
        self.replace_sections = replace_sections

    def __call__(self, fun):
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            import luigi.configuration
            orig_conf = luigi.configuration.get_config()
            luigi.configuration.LuigiConfigParser._instance = None
            conf = luigi.configuration.get_config()
            for (section, settings) in six.iteritems(self.config):
                if not conf.has_section(section):
                    conf.add_section(section)
                elif self.replace_sections:
                    conf.remove_section(section)
                    conf.add_section(section)
                for (name, value) in six.iteritems(settings):
                    conf.set(section, name, value)
            try:
                return fun(*args, **kwargs)
            finally:
                luigi.configuration.LuigiConfigParser._instance = orig_conf
        return wrapper
