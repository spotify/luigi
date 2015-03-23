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
"""
Define the centralized register of all :class:`~luigi.task.Task` classes.
"""

import abc
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from luigi import six
import logging
logger = logging.getLogger('luigi-interface')


class TaskClassException(Exception):
    pass


class Register(abc.ABCMeta):
    """
    The Metaclass of :py:class:`Task`.

    Acts as a global registry of Tasks with the following properties:

    1. Cache instances of objects so that eg. ``X(1, 2, 3)`` always returns the
       same object.
    2. Keep track of all subclasses of :py:class:`Task` and expose them.
    """
    __instance_cache = {}
    _default_namespace = None
    _reg = []
    AMBIGUOUS_CLASS = object()  # Placeholder denoting an error
    """If this value is returned by :py:meth:`__get_reg` then there is an
    ambiguous task name (two :py:class:`Task` have the same name). This denotes
    an error."""

    def __new__(metacls, classname, bases, classdict):
        """
        Custom class creation for namespacing.

        Also register all subclasses.

        Set the task namespace to whatever the currently declared namespace is.
        """
        if "task_namespace" not in classdict:
            classdict["task_namespace"] = metacls._default_namespace

        cls = super(Register, metacls).__new__(metacls, classname, bases, classdict)
        metacls._reg.append(cls)

        return cls

    def __call__(cls, *args, **kwargs):
        """
        Custom class instantiation utilizing instance cache.

        If a Task has already been instantiated with the same parameters,
        the previous instance is returned to reduce number of object instances.
        """
        def instantiate():
            return super(Register, cls).__call__(*args, **kwargs)

        h = cls.__instance_cache

        if h is None:  # disabled
            return instantiate()

        params = cls.get_params()
        param_values = cls.get_param_values(params, args, kwargs)

        k = (cls, tuple(param_values))

        try:
            hash(k)
        except TypeError:
            logger.debug("Not all parameter values are hashable so instance isn't coming from the cache")
            return instantiate()  # unhashable types in parameters

        if k not in h:
            h[k] = instantiate()

        return h[k]

    @classmethod
    def clear_instance_cache(cls):
        """
        Clear/Reset the instance cache.
        """
        cls.__instance_cache = {}

    @classmethod
    def disable_instance_cache(cls):
        """
        Disables the instance cache.
        """
        cls.__instance_cache = None

    @property
    def task_family(cls):
        """
        The task family for the given class.

        If ``cls.task_namespace is None`` then it's the name of the class.
        Otherwise, ``<task_namespace>.`` is prefixed to the class name.
        """
        if cls.task_namespace is None:
            return cls.__name__
        else:
            return "%s.%s" % (cls.task_namespace, cls.__name__)

    @classmethod
    def __get_reg(cls):
        """Return all of the registered classes.

        :return:  an ``collections.OrderedDict`` of task_family -> class
        """
        # We have to do this on-demand in case task names have changed later
        # We return this in a topologically sorted list of inheritance: this is useful in some cases (#822)
        reg = OrderedDict()
        for cls in cls._reg:
            if cls.run == NotImplemented:
                continue
            name = cls.task_family

            if name in reg and reg[name] != cls and \
                    reg[name] != cls.AMBIGUOUS_CLASS and \
                    not issubclass(cls, reg[name]):
                # Registering two different classes - this means we can't instantiate them by name
                # The only exception is if one class is a subclass of the other. In that case, we
                # instantiate the most-derived class (this fixes some issues with decorator wrappers).
                reg[name] = cls.AMBIGUOUS_CLASS
            else:
                reg[name] = cls

        return reg

    @classmethod
    def task_names(cls):
        """
        List of task names as strings
        """
        return sorted(cls.__get_reg().keys())

    @classmethod
    def tasks_str(cls):
        """
        Human-readable register contents dump.
        """
        return ','.join(cls.task_names())

    @classmethod
    def get_task_cls(cls, name):
        """
        Returns an unambiguous class or raises an exception.
        """
        task_cls = cls.__get_reg().get(name)
        if not task_cls:
            raise TaskClassException('Task %r not found. Candidates are: %s' % (name, cls.tasks_str()))

        if task_cls == cls.AMBIGUOUS_CLASS:
            raise TaskClassException('Task %r is ambiguous' % name)
        return task_cls

    @classmethod
    def get_all_params(cls):
        """
        Compiles and returns all parameters for all :py:class:`Task`.

        :return: a generator of tuples (TODO: we should make this more elegant)
        """
        for task_name, task_cls in six.iteritems(cls.__get_reg()):
            if task_cls == cls.AMBIGUOUS_CLASS:
                continue
            for param_name, param_obj in task_cls.get_params():
                yield task_name, (not task_cls.use_cmdline_section), param_name, param_obj


def load_task(module, task_name, params_str):
    """
    Imports task dynamically given a module and a task name.
    """
    if module is not None:
        __import__(module)
    task_cls = Register.get_task_cls(task_name)
    return task_cls.from_str_params(params_str)
