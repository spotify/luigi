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

import abc
import itertools
import logging
import traceback
import warnings

import parameter

Parameter = parameter.Parameter
logger = logging.getLogger('luigi-interface')


def namespace(namespace=None):
    """
    Call to set namespace of tasks declared after the call.

    If called without arguments or with ``None`` as the namespace, the namespace
    is reset, which is recommended to do at the end of any file where the
    namespace is set to avoid unintentionally setting namespace on tasks outside
    of the scope of the current file.
    """
    Register._default_namespace = namespace


def id_to_name_and_params(task_id):
    # DEPRECATED
    import luigi.tools.parse_task
    return luigi.tools.parse_task.id_to_name_and_params(task_id)


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
    """If this value is returned by :py:meth:`get_reg` then there is an
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

        h = Register.__instance_cache

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
        Register.__instance_cache = {}

    @classmethod
    def disable_instance_cache(cls):
        """
        Disables the instance cache.
        """
        Register.__instance_cache = None

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
    def get_reg(cls, include_config_without_section=False):
        """Return all of the registery classes.

        :return:  a ``dict`` of task_family -> class
        """
        # We have to do this on-demand in case task names have changed later
        reg = {}
        for cls in cls._reg:
            if cls.run == NotImplemented:
                continue
            if issubclass(cls, ConfigWithoutSection) and not include_config_without_section:
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
    def tasks_str(cls):
        """
        Human-readable register contents dump.
        """
        return repr(sorted(Register.get_reg().keys()))

    @classmethod
    def get_task_cls(cls, name):
        """
        Returns an unambiguous class or raises an exception.
        """
        task_cls = Register.get_reg().get(name)
        if not task_cls:
            raise Exception('Task %r not found. Candidates are: %s' % (name, Register.tasks_str()))
        if task_cls == Register.AMBIGUOUS_CLASS:
            raise Exception('Task %r is ambiguous' % name)
        return task_cls

    @classmethod
    def get_all_params(cls):
        """
        Compiles and returns all parameters for all :py:class:`Task`.

        :return: a ``dict`` of parameter name -> parameter.
        """
        for task_name, task_cls in cls.get_reg(include_config_without_section=True).iteritems():
            if task_cls == cls.AMBIGUOUS_CLASS:
                continue
            for param_name, param_obj in task_cls.get_params():
                yield task_name, issubclass(task_cls, ConfigWithoutSection), param_name, param_obj


class Task(object):
    """
    This is the base class of all Luigi Tasks, the base unit of work in Luigi.

    A Luigi Task describes a unit or work.

    The key methods of a Task, which must be implemented in a subclass are:

    * :py:meth:`run` - the computation done by this task.
    * :py:meth:`requires` - the list of Tasks that this Task depends on.
    * :py:meth:`output` - the output :py:class:`Target` that this Task creates.

    Parameters to the Task should be declared as members of the class, e.g.::

    .. code-block:: python

        class MyTask(luigi.Task):
            count = luigi.IntParameter()

    Each Task exposes a constructor accepting all :py:class:`Parameter` (and
    values) as kwargs. e.g. ``MyTask(count=10)`` would instantiate `MyTask`.

    In addition to any declared properties and methods, there are a few
    non-declared properties, which are created by the :py:class:`Register`
    metaclass:

    ``Task.task_namespace``
      optional string which is prepended to the task name for the sake of
      scheduling. If it isn't overridden in a Task, whatever was last declared
      using `luigi.namespace` will be used.

    ``Task._parameters``
      list of ``(parameter_name, parameter)`` tuples for this task class
    """
    __metaclass__ = Register

    _event_callbacks = {}

    # Priority of the task: the scheduler should favor available
    # tasks with higher priority values first.
    priority = 0
    disabled = False

    # Resources used by the task. Should be formatted like {"scp": 1} to indicate that the
    # task requires 1 unit of the scp resource.
    resources = {}

    # Number of seconds after which to time out the run function. No timeout if set to 0. Defaults
    # to 0 or value in client.cfg
    worker_timeout = None

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
        for event_class, event_callbacks in self._event_callbacks.iteritems():
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
        # Returns what Python module to import to get access to this class
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
        params.sort(key=lambda t: t[1].counter)
        return params

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
        positional_params = [(n, p) for n, p in params]
        for i, arg in enumerate(args):
            if i >= len(positional_params):
                raise parameter.UnknownParameterException('%s: takes at most %d parameters (%d given)' % (exc_desc, len(positional_params), len(args)))
            param_name, param_obj = positional_params[i]
            result[param_name] = arg

        # Then the optional arguments
        for param_name, arg in kwargs.iteritems():
            if param_name in result:
                raise parameter.DuplicateParameterException('%s: parameter %s was already set as a positional parameter' % (exc_desc, param_name))
            if param_name not in params_dict:
                raise parameter.UnknownParameterException('%s: unknown parameter %s' % (exc_desc, param_name))
            result[param_name] = arg

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
        """
        Constructor to resolve values for all Parameters.

        For example, the Task:

        .. code-block:: python

            class MyTask(luigi.Task):
                count = luigi.IntParameter()

        can be instantiated as ``MyTask(count=10)``.
        """
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)

        # Set all values on class instance
        for key, value in param_values:
            setattr(self, key, value)

        # Register args and kwargs as an attribute on the class. Might be useful
        self.param_args = tuple(value for key, value in param_values)
        self.param_kwargs = dict(param_values)

        # Build up task id
        task_id_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if dict(params)[param_name].significant:
                task_id_parts.append('%s=%s' % (param_name, param_objs[param_name].serialize(param_value)))

        self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))
        self.__hash = hash(self.task_id)

    def initialized(self):
        """
        Returns ``True`` if the Task is initialized and ``False`` otherwise.
        """
        return hasattr(self, 'task_id')

    @classmethod
    def from_str_params(cls, params_str=None):
        """
        Creates an instance from a str->str hash.

        :param params_str: dict of param name -> value.
        """
        if params_str is None:
            params_str = {}
        kwargs = {}
        for param_name, param in cls.get_params():
            value = param.parse_from_input(param_name, params_str[param_name])
            kwargs[param_name] = value

        return cls(**kwargs)

    def to_str_params(self):
        # Convert all parameters to a str->str hash
        params_str = {}
        params = dict(self.get_params())
        for param_name, param_value in self.param_kwargs.iteritems():
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
        k.update(kwargs.iteritems())

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
        return self.task_id

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.param_args == other.param_args

    def complete(self):
        """
        If the task has any outputs, return ``True`` if all outputs exists.
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

        return all(itertools.imap(lambda output: output.exists(), outputs))

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        """
        Returns those of parameter_tuples for which this Task is complete.

        Override (with an efficient implementation) for efficient scheduling
        with range tools. Keep the logic consistent with that of complete().
        """
        raise NotImplementedError

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
        """
        pass  # default impl

    def on_failure(self, exception):
        """
        Override for custom error handling.

        This method gets called if an exception is raised in :py:meth:`run`.
        Return value of this method is json encoded and sent to the scheduler as the `expl` argument. Its string representation will be used as the body of the error email sent out if any.

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


def externalize(task):
    """
    Returns an externalized version of the Task.

    See :py:class:`ExternalTask`.
    """
    task.run = NotImplemented
    return task


class ExternalTask(Task):
    """
    Subclass for references to external dependencies.

    An ExternalTask's does not have a `run` implementation, which signifies to
    the framework that this Task's :py:meth:`output` is generated outside of
    Luigi.
    """
    run = NotImplemented


class WrapperTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done if all their requirements exist.
    """

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))


class Config(Task):

    """Used for configuration that's not specific to a certain task

    TODO: let's refactor Task & Config so that it inherits from a common
    ParamContainer base class
    """
    pass


class ConfigWithoutSection(Task):

    """Used for configuration that doesn't have a particular section

    (eg. --n-workers)
    """
    pass


def getpaths(struct):
    """
    Maps all Tasks in a structured data object to their .output().
    """
    if isinstance(struct, Task):
        return struct.output()
    elif isinstance(struct, dict):
        r = {}
        for k, v in struct.iteritems():
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

        >>> flatten({'a': 'foo', 'b': 'bar'})
        ['foo', 'bar']
        >>> flatten(['foo', ['bar', 'troll']])
        ['foo', 'bar', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, dict):
        for key, result in struct.iteritems():
            flat += flatten(result)
        return flat
    if isinstance(struct, basestring):
        return [struct]

    try:
        # if iterable
        for result in struct:
            flat += flatten(result)
        return flat
    except TypeError:
        pass

    return [struct]


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
