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
import itertools
import tempfile
import re
from contextlib import contextmanager

import luigi
import luigi.task_register
import luigi.cmdline_parser
from luigi.cmdline_parser import CmdlineParser
from luigi import six
import os

import unittest


def skipOnTravis(reason):
    return unittest.skipIf(os.getenv('TRAVIS') == 'true', reason)


class with_config(object):
    """
    Decorator to override config settings for the length of a function.

    Usage:

    .. code-block: python

        >>> import luigi.configuration
        >>> @with_config({'foo': {'bar': 'baz'}})
        ... def my_test():
        ...     print(luigi.configuration.get_config().get("foo", "bar"))
        ...
        >>> my_test()
        baz
        >>> @with_config({'hoo': {'bar': 'buz'}})
        ... @with_config({'foo': {'bar': 'baz'}})
        ... def my_test():
        ...     print(luigi.configuration.get_config().get("foo", "bar"))
        ...     print(luigi.configuration.get_config().get("hoo", "bar"))
        ...
        >>> my_test()
        baz
        buz
        >>> @with_config({'foo': {'bar': 'buz'}})
        ... @with_config({'foo': {'bar': 'baz'}})
        ... def my_test():
        ...     print(luigi.configuration.get_config().get("foo", "bar"))
        ...
        >>> my_test()
        baz
        >>> @with_config({'foo': {'bur': 'buz'}})
        ... @with_config({'foo': {'bar': 'baz'}})
        ... def my_test():
        ...     print(luigi.configuration.get_config().get("foo", "bar"))
        ...     print(luigi.configuration.get_config().get("foo", "bur"))
        ...
        >>> my_test()
        baz
        buz
        >>> @with_config({'foo': {'bur': 'buz'}})
        ... @with_config({'foo': {'bar': 'baz'}}, replace_sections=True)
        ... def my_test():
        ...     print(luigi.configuration.get_config().get("foo", "bar"))
        ...     print(luigi.configuration.get_config().get("foo", "bur", "no_bur"))
        ...
        >>> my_test()
        baz
        no_bur

    """

    def __init__(self, config, replace_sections=False):
        self.config = config
        self.replace_sections = replace_sections

    def _make_dict(self, old_dict):
        if self.replace_sections:
            old_dict.update(self.config)
            return old_dict

        def get_section(sec):
            old_sec = old_dict.get(sec, {})
            new_sec = self.config.get(sec, {})
            old_sec.update(new_sec)
            return old_sec

        all_sections = itertools.chain(old_dict.keys(), self.config.keys())
        return {sec: get_section(sec) for sec in all_sections}

    def __call__(self, fun):
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            import luigi.configuration
            orig_conf = luigi.configuration.LuigiConfigParser.instance()
            new_conf = luigi.configuration.LuigiConfigParser()
            luigi.configuration.LuigiConfigParser._instance = new_conf
            orig_dict = {k: dict(orig_conf.items(k)) for k in orig_conf.sections()}
            new_dict = self._make_dict(orig_dict)
            for (section, settings) in six.iteritems(new_dict):
                new_conf.add_section(section)
                for (name, value) in six.iteritems(settings):
                    new_conf.set(section, name, value)
            try:
                return fun(*args, **kwargs)
            finally:
                luigi.configuration.LuigiConfigParser._instance = orig_conf
        return wrapper


class RunOnceTask(luigi.Task):

    def __init__(self, *args, **kwargs):
        super(RunOnceTask, self).__init__(*args, **kwargs)
        self.comp = False

    def complete(self):
        return self.comp

    def run(self):
        self.comp = True


class LuigiTestCase(unittest.TestCase):
    """
    Tasks registred within a test case will get unregistered in a finalizer

    Instance caches are cleared before and after all runs
    """
    def setUp(self):
        super(LuigiTestCase, self).setUp()
        self._stashed_reg = luigi.task_register.Register._get_reg()
        luigi.task_register.Register.clear_instance_cache()

    def tearDown(self):
        luigi.task_register.Register._set_reg(self._stashed_reg)
        super(LuigiTestCase, self).tearDown()
        luigi.task_register.Register.clear_instance_cache()

    def run_locally(self, args):
        """ Helper for running tests testing more of the stack, the command
        line parsing and task from name intstantiation parts in particular. """
        temp = CmdlineParser._instance
        try:
            CmdlineParser._instance = None
            run_exit_status = luigi.run(['--local-scheduler', '--no-lock'] + args)
        finally:
            CmdlineParser._instance = temp
        return run_exit_status

    def run_locally_split(self, space_seperated_args):
        """ Helper for running tests testing more of the stack, the command
        line parsing and task from name intstantiation parts in particular. """
        return self.run_locally(space_seperated_args.split(' '))


class parsing(object):
    """
    Convenient decorator for test cases to set the parsing environment.
    """

    def __init__(self, cmds):
        self.cmds = cmds

    def __call__(self, fun):
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            with CmdlineParser.global_instance(self.cmds, allow_override=True):
                return fun(*args, **kwargs)

        return wrapper


def in_parse(cmds, deferred_computation):
    with CmdlineParser.global_instance(cmds) as cp:
        deferred_computation(cp.get_task_obj())


@contextmanager
def temporary_unloaded_module(python_file_contents):
    """ Create an importable module

    Return the name of importable module name given its file contents (source
    code) """
    with tempfile.NamedTemporaryFile(
            dir='test/',
            prefix="_test_time_generated_module",
            suffix='.py') as temp_module_file:
        temp_module_file.file.write(python_file_contents)
        temp_module_file.file.flush()
        temp_module_path = temp_module_file.name
        temp_module_name = re.search(r'/(_test_time_generated_module.*).py',
                                     temp_module_path).group(1)
        yield temp_module_name
