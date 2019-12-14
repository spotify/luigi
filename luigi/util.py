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
============================================================
Using ``inherits`` and ``requires`` to ease parameter pain
============================================================

Most luigi plumbers will find themselves in an awkward task parameter situation
at some point or another.  Consider the following "parameter explosion"
problem:

.. code-block:: python

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(luigi.Task):
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

        def requires(self):
            return TaskA(param_a=self.param_a)

    class TaskC(luigi.Task):
        param_c = luigi.Parameter()
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

        def requires(self):
            return TaskB(param_b=self.param_b, param_a=self.param_a)


In work flows requiring many tasks to be chained together in this manner,
parameter handling can spiral out of control.  Each downstream task becomes
more burdensome than the last.  Refactoring becomes more difficult.  There
are several ways one might try and avoid the problem.

**Approach 1**:  Parameters via command line or config instead of :func:`~luigi.task.Task.requires`.

.. code-block:: python

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        def requires(self):
            return TaskA()

    class TaskC(luigi.Task):
        param_c = luigi.Parameter()

        def requires(self):
            return TaskB()


Then run in the shell like so:

.. code-block:: bash

    luigi --module my_tasks TaskC --param-c foo --TaskB-param-b bar --TaskA-param-a baz


Repetitive parameters have been eliminated, but at the cost of making the job's
command line interface slightly clunkier.  Often this is a reasonable
trade-off.

But parameters can't always be refactored out every class.  Downstream
tasks might also need to use some of those parameters.  For example,
if ``TaskC`` needs to use ``param_a`` too, then ``param_a`` would still need
to be repeated.


**Approach 2**:  Use a common parameter class

.. code-block:: python

    class Params(luigi.Config):
        param_c = luigi.Parameter()
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

    class TaskA(Params, luigi.ExternalTask):
        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(Params):
        def requires(self):
            return TaskA()

    class TaskB(Params):
        def requires(self):
            return TaskB()


This looks great at first glance, but a couple of issues lurk. Now ``TaskA``
and ``TaskB`` have unnecessary significant parameters.  Significant parameters
help define the identity of a task.  Identical tasks are prevented from
running at the same time by the central planner.  This helps preserve the
idempotent and atomic nature of luigi tasks.  Unnecessary significant task
parameters confuse a task's identity.  Under the right circumstances, task
identity confusion could lead to that task running when it shouldn't, or
failing to run when it should.

This approach should only be used when all of the parameters of the config
class, are significant (or all insignificant) for all of its subclasses.

And wait a second... there's a bug in the above code.  See it?

``TaskA`` won't behave as an ``ExternalTask`` because the parent classes are
specified in the wrong order.  This contrived example is easy to fix (by
swapping the ordering of the parents of ``TaskA``), but real world cases can be
more difficult to both spot and fix.  Inheriting from multiple classes
derived from :class:`~luigi.task.Task` should be undertaken with caution and avoided
where possible.


**Approach 3**: Use :class:`~luigi.util.inherits` and :class:`~luigi.util.requires`

The :class:`~luigi.util.inherits` class decorator in this module copies parameters (and
nothing else) from one task class to another, and avoids direct pythonic
inheritance.

.. code-block:: python

    import luigi
    from luigi.util import inherits

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    @inherits(TaskA)
    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        def requires(self):
            t = self.clone(TaskA)  # or t = self.clone_parent()

            # Wait... whats this clone thingy do?
            #
            # Pass it a task class.  It calls that task.  And when it does, it
            # supplies all parameters (and only those parameters) common to
            # the caller and callee!
            #
            # The call to clone is equivalent to the following (note the
            # fact that clone avoids passing param_b).
            #
            #   return TaskA(param_a=self.param_a)

            return t

    @inherits(TaskB)
    class TaskC(luigi.Task):
        param_c = luigi.Parameter()

        def requires(self):
            return self.clone(TaskB)


This totally eliminates the need to repeat parameters, avoids inheritance
issues, and keeps the task command line interface as simple (as it can be,
anyway).  Refactoring task parameters is also much easier.

The :class:`~luigi.util.requires` helper function can reduce this pattern even further.   It
does everything :class:`~luigi.util.inherits` does,
and also attaches a :class:`~luigi.util.requires` method
to your task (still all without pythonic inheritance).

But how does it know how to invoke the upstream task?  It uses :func:`~luigi.task.Task.clone`
behind the scenes!

.. code-block:: python

    import luigi
    from luigi.util import inherits, requires

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    @requires(TaskA)
    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        # The class decorator does this for me!
        # def requires(self):
        #     return self.clone(TaskA)

Use these helper functions effectively to avoid unnecessary
repetition and dodge a few potentially nasty workflow pitfalls at the same
time. Brilliant!
"""

import datetime
import logging

from luigi import task
from luigi import parameter


logger = logging.getLogger('luigi-interface')


def common_params(task_instance, task_cls):
    """
    Grab all the values in task_instance that are found in task_cls.
    """
    if not isinstance(task_cls, task.Register):
        raise TypeError("task_cls must be an uninstantiated Task")

    task_instance_param_names = dict(task_instance.get_params()).keys()
    task_cls_params_dict = dict(task_cls.get_params())
    task_cls_param_names = task_cls_params_dict.keys()
    common_param_names = set(task_instance_param_names).intersection(set(task_cls_param_names))
    common_param_vals = [(key, task_cls_params_dict[key]) for key in common_param_names]
    common_kwargs = dict((key, task_instance.param_kwargs[key]) for key in common_param_names)
    vals = dict(task_instance.get_param_values(common_param_vals, [], common_kwargs))
    return vals


class inherits:
    """
    Task inheritance.

    *New after Luigi 2.7.6:* multiple arguments support.

    Usage:

    .. code-block:: python

        class AnotherTask(luigi.Task):
            m = luigi.IntParameter()

        class YetAnotherTask(luigi.Task):
            n = luigi.IntParameter()

        @inherits(AnotherTask)
        class MyFirstTask(luigi.Task):
            def requires(self):
               return self.clone_parent()

            def run(self):
               print self.m # this will be defined
               # ...

        @inherits(AnotherTask, YetAnotherTask)
        class MySecondTask(luigi.Task):
            def requires(self):
               return self.clone_parents()

            def run(self):
               print self.n # this will be defined
               # ...
    """

    def __init__(self, *tasks_to_inherit):
        super(inherits, self).__init__()
        if not tasks_to_inherit:
            raise TypeError("tasks_to_inherit cannot be empty")

        self.tasks_to_inherit = tasks_to_inherit

    def __call__(self, task_that_inherits):
        # Get all parameter objects from each of the underlying tasks
        for task_to_inherit in self.tasks_to_inherit:
            for param_name, param_obj in task_to_inherit.get_params():
                # Check if the parameter exists in the inheriting task
                if not hasattr(task_that_inherits, param_name):
                    # If not, add it to the inheriting task
                    setattr(task_that_inherits, param_name, param_obj)

        # Modify task_that_inherits by adding methods
        def clone_parent(_self, **kwargs):
            return _self.clone(cls=self.tasks_to_inherit[0], **kwargs)
        task_that_inherits.clone_parent = clone_parent

        def clone_parents(_self, **kwargs):
            return [
                _self.clone(cls=task_to_inherit, **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            ]
        task_that_inherits.clone_parents = clone_parents

        return task_that_inherits


class requires:
    """
    Same as :class:`~luigi.util.inherits`, but also auto-defines the requires method.

    *New after Luigi 2.7.6:* multiple arguments support.

    """

    def __init__(self, *tasks_to_require):
        super(requires, self).__init__()
        if not tasks_to_require:
            raise TypeError("tasks_to_require cannot be empty")

        self.tasks_to_require = tasks_to_require

    def __call__(self, task_that_requires):
        task_that_requires = inherits(*self.tasks_to_require)(task_that_requires)

        # Modify task_that_requires by adding requires method.
        # If only one task is required, this single task is returned.
        # Otherwise, list of tasks is returned
        def requires(_self):
            return _self.clone_parent() if len(self.tasks_to_require) == 1 else _self.clone_parents()
        task_that_requires.requires = requires

        return task_that_requires


class copies:
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
        @task._task_wraps(task_that_copies)
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

    @task._task_wraps(task_that_delegates)
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
        elif isinstance(param_obj, parameter.DateSecondParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(seconds=1)
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
    for _ in range(max_steps):
        prev = previous(prev)
        logger.debug("Checking if %s is complete", prev)
        if prev.complete():
            return prev
    return None
