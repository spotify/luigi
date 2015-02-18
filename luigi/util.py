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

import datetime
import functools
import logging

from luigi import six

from luigi import task
from luigi import parameter
from luigi.deprecate_kwarg import deprecate_kwarg  # removing this breaks code

if six.PY3:
    xrange = range

logger = logging.getLogger('luigi-interface')


def common_params(task_instance, task_cls):
    """
    Grab all the values in task_instance that are found in task_cls.
    """
    if not isinstance(task_cls, task.Register):
        raise TypeError("task_cls must be an uninstantiated Task")

    task_instance_param_names = dict(task_instance.get_params()).keys()
    task_cls_param_names = dict(task_cls.get_params()).keys()
    common_param_names = list(set.intersection(set(task_instance_param_names), set(task_cls_param_names)))
    common_param_vals = [(key, dict(task_cls.get_params())[key]) for key in common_param_names]
    common_kwargs = dict([(key, task_instance.param_kwargs[key]) for key in common_param_names])
    vals = dict(task_instance.get_param_values(common_param_vals, [], common_kwargs))
    return vals


def task_wraps(P):
    # In order to make the behavior of a wrapper class nicer, we set the name of the
    # new class to the wrapped class, and copy over the docstring and module as well.
    # This makes it possible to pickle the wrapped class etc.
    # Btw, this is a slight abuse of functools.wraps. It's meant to be used only for
    # functions, but it works for classes too, if you pass updated=[]
    return functools.wraps(P, updated=[])


class inherits(object):
    """
    Task inheritance.

    Usage:

    .. code-block:: python

        class AnotherTask(luigi.Task):
            n = luigi.IntParameter()
            # ...

        @inherits(AnotherTask):
        class MyTask(luigi.Task):
            def requires(self):
               return self.clone_parent()

            def run(self):
               print self.n # this will be defined
               # ...
    """

    def __init__(self, task_to_inherit):
        super(inherits, self).__init__()
        self.task_to_inherit = task_to_inherit

    def __call__(self, task_that_inherits):
        for param_name, param_obj in self.task_to_inherit.get_params():
            if not hasattr(task_that_inherits, param_name):
                setattr(task_that_inherits, param_name, param_obj)

        # Modify task_that_inherits by subclassing it and adding methods
        @task_wraps(task_that_inherits)
        class Wrapped(task_that_inherits):

            def clone_parent(_self, **args):
                return _self.clone(cls=self.task_to_inherit, **args)

        return Wrapped


class requires(object):
    """
    Same as @inherits, but also auto-defines the requires method.
    """

    def __init__(self, task_to_require):
        super(requires, self).__init__()
        self.inherit_decorator = inherits(task_to_require)

    def __call__(self, task_that_requires):
        task_that_requires = self.inherit_decorator(task_that_requires)

        # Modify task_that_requres by subclassing it and adding methods
        @task_wraps(task_that_requires)
        class Wrapped(task_that_requires):

            def requires(_self):
                return _self.clone_parent()

        return Wrapped


class copies(object):
    """
    Auto-copies a task.

    Usage:

    .. code-block:: python

        @copies(MyTask):
        class CopyOfMyTask(luigi.Task):
            def output(self):
               return LocalTarget(self.date.strftime('/var/xyz/report-%Y-%m-%d'))
    """

    def __init__(self, task_to_copy):
        super(copies, self).__init__()
        self.requires_decorator = requires(task_to_copy)

    def __call__(self, task_that_copies):
        task_that_copies = self.requires_decorator(task_that_copies)

        # Modify task_that_copies by subclassing it and adding methods
        @task_wraps(task_that_copies)
        class Wrapped(task_that_copies):

            def run(_self):
                i, o = _self.input(), _self.output()
                f = o.open('w')  # TODO: assert that i, o are Target objects and not complex datastructures
                for line in i.open('r'):
                    f.write(line)
                f.close()

        return Wrapped


def delegates(task_that_delegates):
    """ Lets a task call methods on subtask(s).

    The way this works is that the subtask is run as a part of the task, but
    the task itself doesn't have to care about the requirements of the subtasks.
    The subtask doesn't exist from the scheduler's point of view, and
    its dependencies are instead required by the main task.

    Example:

    .. code-block:: python

        class PowersOfN(luigi.Task):
            n = luigi.IntParameter()
            def f(self, x): return x ** self.n

        @delegates
        class T(luigi.Task):
            def subtasks(self): return PowersOfN(5)
            def run(self): print self.subtasks().f(42)
    """
    if not hasattr(task_that_delegates, 'subtasks'):
        # This method can (optionally) define a couple of delegate tasks that
        # will be accessible as interfaces, meaning that the task can access
        # those tasks and run methods defined on them, etc
        raise AttributeError('%s needs to implement the method "subtasks"' % task_that_delegates)

    @task_wraps(task_that_delegates)
    class Wrapped(task_that_delegates):

        def deps(self):
            # Overrides method in base class
            return task.flatten(self.requires()) + task.flatten([t.deps() for t in task.flatten(self.subtasks())])

        def run(self):
            for t in task.flatten(self.subtasks()):
                t.run()
            task_that_delegates.run(self)

    return Wrapped


def previous(task):
    """
    Return a previous Task of the same family.

    By default checks if this task family only has one non-global parameter and if
    it is a DateParameter, DateHourParameter or DateIntervalParameter in which case
    it returns with the time decremented by 1 (hour, day or interval)
    """
    params = task.get_params()
    previous_params = {}
    previous_date_params = {}

    for param_name, param_obj in params:
        param_value = getattr(task, param_name)

        if isinstance(param_obj, parameter.DateParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(days=1)
        elif isinstance(param_obj, parameter.DateMinuteParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(minutes=1)
        elif isinstance(param_obj, parameter.DateHourParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(hours=1)
        elif isinstance(param_obj, parameter.DateIntervalParameter):
            previous_date_params[param_name] = param_value.prev()
        else:
            previous_params[param_name] = param_value

    previous_params.update(previous_date_params)

    if len(previous_date_params) == 0:
        raise NotImplementedError("No task parameter - can't determine previous task")
    elif len(previous_date_params) > 1:
        raise NotImplementedError("Too many date-related task parameters - can't determine previous task")
    else:
        return task.clone(**previous_params)


def get_previous_completed(task, max_steps=10):
    prev = task
    for _ in xrange(max_steps):
        prev = previous(prev)
        logger.debug("Checking if %s is complete", prev.task_id)
        if prev.complete():
            return prev
    return None
