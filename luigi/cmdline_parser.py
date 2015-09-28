# -*- coding: utf-8 -*-
#
# Copyright 2015 Spotify AB
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
"""
This module contains luigi internal parsing logic. Things exposed here should
be considered internal to luigi.
"""

import argparse
import functools
from contextlib import contextmanager
from luigi.task_register import Register
import cached_property


class CmdlineParser(object):
    """
    Helper for parsing command line arguments and used as part of the
    context when instantiating task objects.

    Normal luigi users should just use :py:func:`luigi.run`.
    """
    _instance = None

    @classmethod
    def get_instance(cls):
        """ Singleton getter """
        return cls._instance

    @classmethod
    @contextmanager
    def global_instance(cls, cmdline_args, allow_override=False):
        """
        Meant to be used as a context manager.
        """
        orig_value = cls._instance
        assert (orig_value is None) or allow_override
        new_value = None
        try:
            new_value = CmdlineParser(cmdline_args)
            cls._instance = new_value
            yield new_value
        finally:
            assert cls._instance is new_value
            cls._instance = orig_value

    def __init__(self, cmdline_args):
        """
        Initialize cmd line args
        """
        self.cmdline_args = cmdline_args
        known_args, _ = self._build_parser().parse_known_args(args=cmdline_args)
        self._attempt_load_module(known_args)
        parser = self._build_parser(active_tasks=self._active_tasks())
        # TODO: Use parse_args instead of parse_known_args, but can't be
        # done just yet.  Once `--task` is forced, it should be possible
        known_args, _ = parser.parse_known_args(args=cmdline_args)
        self._possibly_exit_with_help(parser, known_args)
        if not self._active_tasks():
            raise SystemExit('No task specified')
        self.known_args = known_args  # Also publically expose parsed arguments

    @staticmethod
    def _build_parser(active_tasks=set()):
        parser = argparse.ArgumentParser(add_help=False)

        for task_name, is_without_section, param_name, param_obj in Register.get_all_params():
            add = functools.partial(param_obj._add_to_cmdline_parser, parser,
                                    param_name, task_name, is_without_section=is_without_section)
            add(glob=True)
            if task_name in active_tasks:
                add(glob=False)

        return parser

    @cached_property.cached_property
    def _task_name(self):
        """
        Get the task name

        If the task does not exist, raise SystemExit
        """
        parser = self._build_parser()
        _, unknown = parser.parse_known_args(args=self.cmdline_args)
        if len(unknown) > 0:
            task_name = unknown[0]
            return task_name

    def get_task_cls(self):
        """
        Get the task class
        """
        return Register.get_task_cls(self._task_name)

    def is_local_task(self, task_name):
        """
        Used to see if unqualified command line parameters should be added too.
        """
        return task_name in self._active_tasks()

    def _active_tasks(self):
        """
        Set of task families that should expose their parameters unqualified.
        """
        root_task = self._task_name
        return set([root_task] if root_task else [])

    @staticmethod
    def _attempt_load_module(known_args):
        """
        Load the --module parameter
        """
        module = known_args.module
        if module:
            __import__(module)

    @staticmethod
    def _possibly_exit_with_help(parser, known_args):
        """
        Check if the user passed --help, if so, print a message and exit.
        """
        if known_args.help:
            parser.print_help()
            raise SystemExit('Exiting due to --help was passed')
