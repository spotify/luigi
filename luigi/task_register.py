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

from luigi import six
import logging
logger = logging.getLogger('luigi-interface')


class TaskClassException(Exception):
    pass


class TaskClassNotFoundException(TaskClassException):
    pass


class TaskClassAmbigiousException(TaskClassException):
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
    _default_namespace_dict = {}
    _reg = []
    AMBIGUOUS_CLASS = object()  # Placeholder denoting an error
    """If this value is returned by :py:meth:`_get_reg` then there is an
    ambiguous task name (two :py:class:`Task` have the same name). This denotes
    an error."""

    def __new__(metacls, classname, bases, classdict):
        """
        Custom class creation for namespacing.

        Also register all subclasses.

        When the set or inherited namespace evaluates to ``None``, set the task namespace to
        whatever the currently declared namespace is.
        """
        cls = super(Register, metacls).__new__(metacls, classname, bases, classdict)
        cls._namespace_at_class_time = metacls._get_namespace(cls.__module__)
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
        Internal note: This function will be deleted soon.
        """
        if not cls.get_task_namespace():
            return cls.__name__
        else:
            return "{}.{}".format(cls.get_task_namespace(), cls.__name__)

    @classmethod
    def _get_reg(cls):
        """Return all of the registered classes.

        :return:  an ``dict`` of task_family -> class
        """
        # We have to do this on-demand in case task names have changed later
        reg = dict()
        for task_cls in cls._reg:
            if not task_cls._visible_in_registry:
                continue

            name = task_cls.get_task_family()
            if name in reg and \
                    (reg[name] == Register.AMBIGUOUS_CLASS or  # Check so issubclass doesn't crash
                     not issubclass(task_cls, reg[name])):
                # Registering two different classes - this means we can't instantiate them by name
                # The only exception is if one class is a subclass of the other. In that case, we
                # instantiate the most-derived class (this fixes some issues with decorator wrappers).
                reg[name] = Register.AMBIGUOUS_CLASS
            else:
                reg[name] = task_cls

        return reg

    @classmethod
    def _set_reg(cls, reg):
        """The writing complement of _get_reg
        """
        cls._reg = [task_cls for task_cls in reg.values() if task_cls is not cls.AMBIGUOUS_CLASS]

    @classmethod
    def task_names(cls):
        """
        List of task names as strings
        """
        return sorted(cls._get_reg().keys())

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
        task_cls = cls._get_reg().get(name)
        if not task_cls:
            raise TaskClassNotFoundException(cls._missing_task_msg(name))

        if task_cls == cls.AMBIGUOUS_CLASS:
            raise TaskClassAmbigiousException('Task %r is ambiguous' % name)
        return task_cls

    @classmethod
    def get_all_params(cls):
        """
        Compiles and returns all parameters for all :py:class:`Task`.

        :return: a generator of tuples (TODO: we should make this more elegant)
        """
        for task_name, task_cls in six.iteritems(cls._get_reg()):
            if task_cls == cls.AMBIGUOUS_CLASS:
                continue
            for param_name, param_obj in task_cls.get_params():
                yield task_name, (not task_cls.use_cmdline_section), param_name, param_obj

    @staticmethod
    def _editdistance(a, b):
        """ Simple unweighted Levenshtein distance """
        r0 = range(0, len(b) + 1)
        r1 = [0] * (len(b) + 1)

        for i in range(0, len(a)):
            r1[0] = i + 1

            for j in range(0, len(b)):
                c = 0 if a[i] is b[j] else 1
                r1[j + 1] = min(r1[j] + 1, r0[j + 1] + 1, r0[j] + c)

            r0 = r1[:]

        return r1[len(b)]

    @classmethod
    def _missing_task_msg(cls, task_name):
        weighted_tasks = [(Register._editdistance(task_name, task_name_2), task_name_2) for task_name_2 in cls.task_names()]
        ordered_tasks = sorted(weighted_tasks, key=lambda pair: pair[0])
        candidates = [task for (dist, task) in ordered_tasks if dist <= 5 and dist < len(task)]
        if candidates:
            return "No task %s. Did you mean:\n%s" % (task_name, '\n'.join(candidates))
        else:
            return "No task %s. Candidates are: %s" % (task_name, cls.tasks_str())

    @classmethod
    def _get_namespace(mcs, module_name):
        for parent in mcs._module_parents(module_name):
            entry = mcs._default_namespace_dict.get(parent)
            if entry:
                return entry
        return ''  # Default if nothing specifies

    @staticmethod
    def _module_parents(module_name):
        '''
        >>> list(Register._module_parents('a.b'))
        ['a.b', 'a', '']
        '''
        spl = module_name.split('.')
        for i in range(len(spl), 0, -1):
            yield '.'.join(spl[0:i])
        if module_name:
            yield ''


def load_task(module, task_name, params_str):
    """
    Imports task dynamically given a module and a task name.
    """
    if module is not None:
        __import__(module)
    task_cls = Register.get_task_cls(task_name)
    return task_cls.from_str_params(params_str)
