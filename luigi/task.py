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
The abstract :py:class:`Task` class.
It is a central concept of Luigi and represents the state of the workflow.
See :doc:`/tasks` for an overview.
"""

try:
    from itertools import imap as map
except ImportError:
    pass
from contextlib import contextmanager
import logging
import traceback
import warnings
import json
import hashlib
import re

from luigi import six

from luigi import parameter
from luigi.task_register import Register

Parameter = parameter.Parameter
logger = logging.getLogger('luigi-interface')


TASK_ID_INCLUDE_PARAMS = 3
TASK_ID_TRUNCATE_PARAMS = 16
TASK_ID_TRUNCATE_HASH = 10
TASK_ID_INVALID_CHAR_REGEX = re.compile(r'[^A-Za-z0-9_]')


def namespace(namespace=None):
    """
    Call to set namespace of tasks declared after the call.

    If called without arguments or with ``None`` as the namespace, the namespace
    is reset, which is recommended to do at the end of any file where the
    namespace is set to avoid unintentionally setting namespace on tasks outside
    of the scope of the current file.

    The namespace of a Task can also be changed by specifying the property
    ``task_namespace``. This solution has the advantage that the namespace
    doesn't have to be restored.

    .. code-block:: python

        class Task2(luigi.Task):
            task_namespace = 'namespace2'
    """
    Register._default_namespace = namespace


def task_id_str(task_family, params):
    """
    Returns a canonical string used to identify a particular task

    :param task_family: The task family (class name) of the task
    :param params: a dict mapping parameter names to their serialized values
    :return: A unique, shortened identifier corresponding to the family and params
    """
    # task_id is a concatenation of task family, the first values of the first 3 parameters
    # sorted by parameter name and a md5hash of the family/parameters as a cananocalised json.
    param_str = json.dumps(params, separators=(',', ':'), sort_keys=True)
    param_hash = hashlib.md5(param_str.encode('utf-8')).hexdigest()

    param_summary = '_'.join(p[:TASK_ID_TRUNCATE_PARAMS]
                             for p in (params[p] for p in sorted(params)[:TASK_ID_INCLUDE_PARAMS]))
    param_summary = TASK_ID_INVALID_CHAR_REGEX.sub('_', param_summary)

    return '{}_{}_{}'.format(task_family, param_summary, param_hash[:TASK_ID_TRUNCATE_HASH])


class BulkCompleteNotImplementedError(NotImplementedError):
    """This is here to trick pylint.

    pylint thinks anything raising NotImplementedError needs to be implemented
    in any subclass. bulk_complete isn't like that. This tricks pylint into
    thinking that the default implementation is a valid implementation and no
    an abstract method."""
    pass


@six.add_metaclass(Register)
class Task(object):
    """
    This is the base class of all Luigi Tasks, the base unit of work in Luigi.

    A Luigi Task describes a unit or work.

    The key methods of a Task, which must be implemented in a subclass are:

    * :py:meth:`run` - the computation done by this task.
    * :py:meth:`requires` - the list of Tasks that this Task depends on.
    * :py:meth:`output` - the output :py:class:`Target` that this Task creates.

    Each :py:class:`~luigi.Parameter` of the Task should be declared as members:

    .. code:: python

        class MyTask(luigi.Task):
            count = luigi.IntParameter()
            second_param = luigi.Parameter()

    In addition to any declared properties and methods, there are a few
    non-declared properties, which are created by the :py:class:`Register`
    metaclass:

    ``Task.task_namespace``
      optional string which is prepended to the task name for the sake of
      scheduling. If it isn't overridden in a Task, whatever was last declared
      using `luigi.namespace` will be used.
    """

    _event_callbacks = {}

    #: Priority of the task: the scheduler should favor available
    #: tasks with higher priority values first.
    #: See :ref:`Task.priority`
    priority = 0
    disabled = False

    #: Resources used by the task. Should be formatted like {"scp": 1} to indicate that the
    #: task requires 1 unit of the scp resource.
    resources = {}

    #: Number of seconds after which to time out the run function.
    #: No timeout if set to 0.
    #: Defaults to 0 or worker-timeout value in config file
    #: Only works when using multiple workers.
    worker_timeout = None

    #: Maximum number of tasks to run together as a batch. Infinite by default
    max_batch_size = float('inf')

    @property
    def batchable(self):
        """
        True if this instance can be run as part of a batch. By default, True
        if it has any batched parameters
        """
        return bool(self.batch_param_names())

    @property
    def retry_count(self):
        """
        Override this positive integer to have different ``retry_count`` at task level
        Check :ref:`scheduler-config`
        """
        return None

    @property
    def disable_hard_timeout(self):
        """
        Override this positive integer to have different ``disable_hard_timeout`` at task level.
        Check :ref:`scheduler-config`
        """
        return None

    @property
    def disable_window_seconds(self):
        """
        Override this positive integer to have different ``disable_window_seconds`` at task level.
        Check :ref:`scheduler-config`
        """
        return None

    @property
    def owner_email(self):
        '''
        Override this to send out additional error emails to task owner, in addition to the one
        defined in `core`.`error-email`. This should return a string or a list of strings. e.g.
        'test@exmaple.com' or ['test1@example.com', 'test2@example.com']
        '''
        return None

    @property
    def use_cmdline_section(self):
        ''' Property used by core config such as `--workers` etc.
        These will be exposed without the class as prefix.'''
        return True

    @classmethod
    def event_handler(cls, event):
        """
        Decorator for adding event handlers.
        """
        def wrapped(callback):
            cls._event_callbacks.setdefault(cls, {}).setdefault(event, set()).add(callback)
            return callback
        return wrapped

    def trigger_event(self, event, *args, **kwargs):
        """
        Trigger that calls all of the specified events associated with this class.
        """
        for event_class, event_callbacks in six.iteritems(self._event_callbacks):
            if not isinstance(self, event_class):
                continue
            for callback in event_callbacks.get(event, []):
                try:
                    # callbacks are protected
                    callback(*args, **kwargs)
                except KeyboardInterrupt:
                    return
                except BaseException:
                    logger.exception("Error in event callback for %r", event)

    @property
    def task_module(self):
        ''' Returns what Python module to import to get access to this class. '''
        # TODO(erikbern): we should think about a language-agnostic mechanism
        return self.__class__.__module__

    @property
    def task_family(self):
        """
        Convenience method since a property on the metaclass isn't directly accessible through the class instances.
        """
        return self.__class__.task_family

    @classmethod
    def get_params(cls):
        """
        Returns all of the Parameters for this Task.
        """
        # We want to do this here and not at class instantiation, or else there is no room to extend classes dynamically
        params = []
        for param_name in dir(cls):
            param_obj = getattr(cls, param_name)
            if not isinstance(param_obj, Parameter):
                continue

            params.append((param_name, param_obj))

        # The order the parameters are created matters. See Parameter class
        params.sort(key=lambda t: t[1]._counter)
        return params

    @classmethod
    def batch_param_names(cls):
        return [name for name, p in cls.get_params() if p._is_batchable()]

    @classmethod
    def get_param_names(cls, include_significant=False):
        return [name for name, p in cls.get_params() if include_significant or p.significant]

    @classmethod
    def get_param_values(cls, params, args, kwargs):
        """
        Get the values of the parameters from the args and kwargs.

        :param params: list of (param_name, Parameter).
        :param args: positional arguments
        :param kwargs: keyword arguments.
        :returns: list of `(name, value)` tuples, one for each parameter.
        """
        result = {}

        params_dict = dict(params)

        task_name = cls.task_family

        # In case any exceptions are thrown, create a helpful description of how the Task was invoked
        # TODO: should we detect non-reprable arguments? These will lead to mysterious errors
        exc_desc = '%s[args=%s, kwargs=%s]' % (task_name, args, kwargs)

        # Fill in the positional arguments
        positional_params = [(n, p) for n, p in params if p.positional]
        for i, arg in enumerate(args):
            if i >= len(positional_params):
                raise parameter.UnknownParameterException('%s: takes at most %d parameters (%d given)' % (exc_desc, len(positional_params), len(args)))
            param_name, param_obj = positional_params[i]
            result[param_name] = param_obj.normalize(arg)

        # Then the keyword arguments
        for param_name, arg in six.iteritems(kwargs):
            if param_name in result:
                raise parameter.DuplicateParameterException('%s: parameter %s was already set as a positional parameter' % (exc_desc, param_name))
            if param_name not in params_dict:
                raise parameter.UnknownParameterException('%s: unknown parameter %s' % (exc_desc, param_name))
            result[param_name] = params_dict[param_name].normalize(arg)

        # Then use the defaults for anything not filled in
        for param_name, param_obj in params:
            if param_name not in result:
                if not param_obj.has_task_value(task_name, param_name):
                    raise parameter.MissingParameterException("%s: requires the '%s' parameter to be set" % (exc_desc, param_name))
                result[param_name] = param_obj.task_value(task_name, param_name)

        def list_to_tuple(x):
            """ Make tuples out of lists and sets to allow hashing """
            if isinstance(x, list) or isinstance(x, set):
                return tuple(x)
            else:
                return x
        # Sort it by the correct order and make a list
        return [(param_name, list_to_tuple(result[param_name])) for param_name, param_obj in params]

    def __init__(self, *args, **kwargs):
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)

        # Set all values on class instance
        for key, value in param_values:
            setattr(self, key, value)

        # Register args and kwargs as an attribute on the class. Might be useful
        self.param_args = tuple(value for key, value in param_values)
        self.param_kwargs = dict(param_values)

        self.task_id = task_id_str(self.task_family, self.to_str_params(only_significant=True))
        self.__hash = hash(self.task_id)

        self.set_tracking_url = None
        self.set_status_message = None

    def initialized(self):
        """
        Returns ``True`` if the Task is initialized and ``False`` otherwise.
        """
        return hasattr(self, 'task_id')

    @classmethod
    def from_str_params(cls, params_str):
        """
        Creates an instance from a str->str hash.

        :param params_str: dict of param name -> value as string.
        """
        kwargs = {}
        for param_name, param in cls.get_params():
            if param_name in params_str:
                param_str = params_str[param_name]
                if isinstance(param_str, list):
                    kwargs[param_name] = param._parse_list(param_str)
                else:
                    kwargs[param_name] = param.parse(param_str)

        return cls(**kwargs)

    def to_str_params(self, only_significant=False):
        """
        Convert all parameters to a str->str hash.
        """
        params_str = {}
        params = dict(self.get_params())
        for param_name, param_value in six.iteritems(self.param_kwargs):
            if (not only_significant) or params[param_name].significant:
                params_str[param_name] = params[param_name].serialize(param_value)

        return params_str

    def clone(self, cls=None, **kwargs):
        """
        Creates a new instance from an existing instance where some of the args have changed.

        There's at least two scenarios where this is useful (see test/clone_test.py):

        * remove a lot of boiler plate when you have recursive dependencies and lots of args
        * there's task inheritance and some logic is on the base class

        :param cls:
        :param kwargs:
        :return:
        """
        k = self.param_kwargs.copy()
        k.update(six.iteritems(kwargs))

        if cls is None:
            cls = self.__class__

        new_k = {}
        for param_name, param_class in cls.get_params():
            if param_name in k:
                new_k[param_name] = k[param_name]

        return cls(**new_k)

    def __hash__(self):
        return self.__hash

    def __repr__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`
        """
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                repr_parts.append('%s=%s' % (param_name, param_objs[param_name].serialize(param_value)))

        task_str = '{}({})'.format(self.task_family, ', '.join(repr_parts))

        return task_str

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.param_args == other.param_args

    def complete(self):
        """
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.

        However, you may freely override this method with custom logic.
        """
        outputs = flatten(self.output())
        if len(outputs) == 0:
            warnings.warn(
                "Task %r without outputs has no custom complete() method" % self,
                stacklevel=2
            )
            return False

        return all(map(lambda output: output.exists(), outputs))

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        """
        Returns those of parameter_tuples for which this Task is complete.

        Override (with an efficient implementation) for efficient scheduling
        with range tools. Keep the logic consistent with that of complete().
        """
        raise BulkCompleteNotImplementedError()

    def output(self):
        """
        The output that this Task produces.

        The output of the Task determines if the Task needs to be run--the task
        is considered finished iff the outputs all exist. Subclasses should
        override this method to return a single :py:class:`Target` or a list of
        :py:class:`Target` instances.

        Implementation note
          If running multiple workers, the output must be a resource that is accessible
          by all workers, such as a DFS or database. Otherwise, workers might compute
          the same output since they don't see the work done by other workers.

        See :ref:`Task.output`
        """
        return []  # default impl

    def requires(self):
        """
        The Tasks that this Task depends on.

        A Task will only run if all of the Tasks that it requires are completed.
        If your Task does not require any other Tasks, then you don't need to
        override this method. Otherwise, a Subclasses can override this method
        to return a single Task, a list of Task instances, or a dict whose
        values are Task instances.

        See :ref:`Task.requires`
        """
        return []  # default impl

    def _requires(self):
        """
        Override in "template" tasks which themselves are supposed to be
        subclassed and thus have their requires() overridden (name preserved to
        provide consistent end-user experience), yet need to introduce
        (non-input) dependencies.

        Must return an iterable which among others contains the _requires() of
        the superclass.
        """
        return flatten(self.requires())  # base impl

    def process_resources(self):
        """
        Override in "template" tasks which provide common resource functionality
        but allow subclasses to specify additional resources while preserving
        the name for consistent end-user experience.
        """
        return self.resources  # default impl

    def input(self):
        """
        Returns the outputs of the Tasks returned by :py:meth:`requires`

        See :ref:`Task.input`

        :return: a list of :py:class:`Target` objects which are specified as
                 outputs of all required Tasks.
        """
        return getpaths(self.requires())

    def deps(self):
        """
        Internal method used by the scheduler.

        Returns the flattened list of requires.
        """
        # used by scheduler
        return flatten(self._requires())

    def run(self):
        """
        The task run method, to be overridden in a subclass.

        See :ref:`Task.run`
        """
        pass  # default impl

    def on_failure(self, exception):
        """
        Override for custom error handling.

        This method gets called if an exception is raised in :py:meth:`run`.
        The returned value of this method is json encoded and sent to the scheduler
        as the `expl` argument. Its string representation will be used as the
        body of the error email sent out if any.

        Default behavior is to return a string representation of the stack trace.
        """

        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string

    def on_success(self):
        """
        Override for doing custom completion handling for a larger class of tasks

        This method gets called when :py:meth:`run` completes without raising any exceptions.

        The returned value is json encoded and sent to the scheduler as the `expl` argument.

        Default behavior is to send an None value"""
        pass

    @contextmanager
    def no_unpicklable_properties(self):
        """
        Remove unpicklable properties before dump task and resume them after.

        This method could be called in subtask's dump method, to ensure unpicklable
        properties won't break dump.

        This method is a context-manager which can be called as below:

        .. code-block: python

            class DummyTask(luigi):

                def _dump(self):
                    with self.no_unpicklable_properties():
                        pickle.dumps(self)

        """
        unpicklable_properties = ('set_tracking_url', 'set_status_message')
        reserved_properties = {}
        for property_name in unpicklable_properties:
            if hasattr(self, property_name):
                reserved_properties[property_name] = getattr(self, property_name)
                setattr(self, property_name, 'placeholder_during_pickling')

        yield

        for property_name, value in six.iteritems(reserved_properties):
            setattr(self, property_name, value)


class MixinNaiveBulkComplete(object):
    """
    Enables a Task to be efficiently scheduled with e.g. range tools, by providing a bulk_complete implementation which checks completeness in a loop.

    Applicable to tasks whose completeness checking is cheap.

    This doesn't exploit output location specific APIs for speed advantage, nevertheless removes redundant scheduler roundtrips.
    """
    @classmethod
    def bulk_complete(cls, parameter_tuples):
        generated_tuples = []
        for parameter_tuple in parameter_tuples:
            if isinstance(parameter_tuple, (list, tuple)):
                if cls(*parameter_tuple).complete():
                    generated_tuples.append(parameter_tuple)
            elif isinstance(parameter_tuple, dict):
                if cls(**parameter_tuple).complete():
                    generated_tuples.append(parameter_tuple)
            else:
                if cls(parameter_tuple).complete():
                    generated_tuples.append(parameter_tuple)
        return generated_tuples


def externalize(task):
    """
    Returns an externalized version of the Task.

    See :py:class:`ExternalTask`.
    """
    task.run = None
    return task


class ExternalTask(Task):
    """
    Subclass for references to external dependencies.

    An ExternalTask's does not have a `run` implementation, which signifies to
    the framework that this Task's :py:meth:`output` is generated outside of
    Luigi.
    """
    run = None


class WrapperTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done if all their requirements exist.
    """

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))


class Config(Task):
    """
    Class for configuration. See :ref:`ConfigClasses`.
    """
    # TODO: let's refactor Task & Config so that it inherits from a common
    # ParamContainer base class
    pass


def getpaths(struct):
    """
    Maps all Tasks in a structured data object to their .output().
    """
    if isinstance(struct, Task):
        return struct.output()
    elif isinstance(struct, dict):
        r = {}
        for k, v in six.iteritems(struct):
            r[k] = getpaths(v)
        return r
    else:
        # Remaining case: assume r is iterable...
        try:
            s = list(struct)
        except TypeError:
            raise Exception('Cannot map %s to Task/dict/list' % str(struct))

        return [getpaths(r) for r in s]


def flatten(struct):
    """
    Creates a flat list of all all items in structured output (dicts, lists, items):

    .. code-block:: python

        >>> sorted(flatten({'a': 'foo', 'b': 'bar'}))
        ['bar', 'foo']
        >>> sorted(flatten(['foo', ['bar', 'troll']]))
        ['bar', 'foo', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, dict):
        for _, result in six.iteritems(struct):
            flat += flatten(result)
        return flat
    if isinstance(struct, six.string_types):
        return [struct]

    try:
        # if iterable
        iterator = iter(struct)
    except TypeError:
        return [struct]

    for result in iterator:
        flat += flatten(result)
    return flat


def flatten_output(task):
    """
    Lists all output targets by recursively walking output-less (wrapper) tasks.

    FIXME order consistently.
    """
    r = flatten(task.output())
    if not r:
        for dep in flatten(task.requires()):
            r += flatten_output(dep)
    return r
