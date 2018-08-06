# -*- coding: utf-8 -*-
#
# Copyright 2018 Vote Inc.
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
import os.path
import logging
import logging.config
from luigi.configuration import get_config

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError


class BaseLogging(object):
    config = get_config()

    @classmethod
    def _section(cls, opts):
        try:
            logging_config = cls.config['logging']
        except (TypeError, KeyError, NoSectionError):
            return False
        logging.config.dictConfig(logging_config)
        return True

    @classmethod
    def setup(cls, opts):
        logger = logging.getLogger('luigi')

        if cls.configured:
            logger.info('logging already configured')
            return False
        cls.configured = True

        if cls.config.getboolean('core', 'no_configure_logging', False):
            logger.info('logging disabled in settings')
            return False

        configured = cls._cli(opts)
        if configured:
            logger = logging.getLogger('luigi')
            logger.info('logging configured via special settings')
            return True

        configured = cls._conf(opts)
        if configured:
            logger = logging.getLogger('luigi')
            logger.info('logging configured via *.conf file')
            return True

        configured = cls._section(opts)
        if configured:
            logger = logging.getLogger('luigi')
            logger.info('logging configured via config section')
            return True

        configured = cls._default(opts)
        if configured:
            logger = logging.getLogger('luigi')
            logger.info('logging configured by default settings')
        return configured


class DaemonLogging(BaseLogging):
    configured = False
    log_format = "%(asctime)s %(name)s[%(process)s] %(levelname)s: %(message)s"

    @classmethod
    def _cli(cls, opts):
        if opts.background:
            logging.getLogger().setLevel(logging.INFO)
            return True

        if opts.logdir:
            logging.basicConfig(
                level=logging.INFO,
                format=cls.log_format,
                filename=os.path.join(opts.logdir, "luigi-server.log"))
            return True

        return False

    @classmethod
    def _conf(cls, opts):
        logging_conf = cls.config.get('core', 'logging_conf_file', None)
        if logging_conf is None:
            return False

        if not os.path.exists(logging_conf):
            # FileNotFoundError added only in Python 3
            # https://docs.python.org/2/library/exceptions.html#exception-hierarchy
            # https://docs.python.org/3/library/exceptions.html#exception-hierarchy
            raise OSError("Error: Unable to locate specified logging configuration file!")

        logging.config.fileConfig(logging_conf)
        return True

    @classmethod
    def _default(cls, opts):
        logging.basicConfig(level=logging.INFO, format=cls.log_format)
        return True


# setup_interface_logging
class InterfaceLogging(BaseLogging):
    configured = False

    @classmethod
    def _cli(cls, opts):
        return False

    @classmethod
    def _conf(cls, opts):
        if not opts.logging_conf_file:
            return False

        if not os.path.exists(opts.logging_conf_file):
            # FileNotFoundError added only in Python 3
            # https://docs.python.org/2/library/exceptions.html#exception-hierarchy
            # https://docs.python.org/3/library/exceptions.html#exception-hierarchy
            raise OSError("Error: Unable to locate specified logging configuration file!")

        logging.config.fileConfig(opts.logging_conf_file, disable_existing_loggers=False)
        return True

    @classmethod
    def _default(cls, opts):
        level = getattr(logging, opts.log_level, logging.DEBUG)

        logger = logging.getLogger('luigi-interface')
        logger.setLevel(level)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)

        formatter = logging.Formatter('%(levelname)s: %(message)s')
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        return True
