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
from contextlib import contextmanager
from luigi.task_register import Register
import sys


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
        known_args, _ = self._build_parser().parse_known_args(args=cmdline_args)
        self._attempt_load_module(known_args)
        # We have to parse again now. As the positionally first unrecognized
        # argument (the task) could be different.
        known_args, _ = self._build_parser().parse_known_args(args=cmdline_args)
        root_task = known_args.root_task
        parser = self._build_parser(root_task=root_task,
                                    help_all=known_args.core_help_all)
        self._possibly_exit_with_help(parser, known_args)
        if not root_task:
            raise SystemExit('No task specified')
        else:
            # Check that what we believe to be the task is correctly spelled
            Register.get_task_cls(root_task)
        known_args = parser.parse_args(args=cmdline_args)
        self.known_args = known_args  # Also publically expose parsed arguments

    @staticmethod
    def _build_parser(root_task=None, help_all=False):
        parser = argparse.ArgumentParser(add_help=False)

        # Unfortunately, we have to set it as optional to argparse, so we can
        # parse out stuff like `--module` before we call for `--help`.
        parser.add_argument('root_task',
                            nargs='?',
                            help='Task family to run. Is not optional.',
                            metavar='Required root task',
                            )

        for task_name, is_without_section, param_name, param_obj in Register.get_all_params():
            is_the_root_task = task_name == root_task
            help = param_obj.description if any((is_the_root_task, help_all, param_obj.always_in_help)) else argparse.SUPPRESS
            flag_name_underscores = param_name if is_without_section else task_name + '_' + param_name
            global_flag_name = '--' + flag_name_underscores.replace('_', '-')
            parser.add_argument(global_flag_name,
                                help=help,
                                action=param_obj._parser_action(),
                                dest=param_obj._parser_global_dest(param_name, task_name)
                                )
            if is_the_root_task:
                local_flag_name = '--' + param_name.replace('_', '-')
                parser.add_argument(local_flag_name,
                                    help=help,
                                    action=param_obj._parser_action(),
                                    dest=param_name
                                    )

        return parser

    def get_task_obj(self):
        """
        Get the task object
        """
        return self._get_task_cls()(**self._get_task_kwargs())

    def _get_task_cls(self):
        """
        Get the task class
        """
        return Register.get_task_cls(self.known_args.root_task)

    def _get_task_kwargs(self):
        """
        Get the local task arguments as a dictionary. The return value is in
        the form ``dict(my_param='my_value', ...)``
        """
        res = {}
        for (param_name, param_obj) in self._get_task_cls().get_params():
            attr = getattr(self.known_args, param_name)
            if attr:
                res.update(((param_name, param_obj.parse(attr)),))

        return res

    @staticmethod
    def _attempt_load_module(known_args):
        """
        Load the --module parameter
        """
        module = known_args.core_module
        if module:
            __import__(module)

    @staticmethod
    def _possibly_exit_with_help(parser, known_args):
        """
        Check if the user passed --help[-all], if so, print a message and exit.
        """
        if known_args.core_help or known_args.core_help_all:
            parser.print_help()
            sys.exit()
