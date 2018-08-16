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
import logging


# IMPORTANT: don't inherit from `object`!
# ConfigParser have some troubles in this case.
# More info: https://stackoverflow.com/a/19323238
class BaseParser:
    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            loaded = cls._instance.reload()
            logging.getLogger('luigi-interface').info('Loaded %r', loaded)

        return cls._instance

    @classmethod
    def add_config_path(cls, path):
        cls._config_paths.append(path)
        cls.reload()

    @classmethod
    def reload(cls):
        return cls.instance().read(cls._config_paths)
