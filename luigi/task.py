# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import abc
import logging
import parameter
import warnings
import traceback

Parameter = parameter.Parameter
logger = logging.getLogger('luigi-interface')


def namespace(namespace=None):
    """ Call to set namespace of tasks declared after the call.

    If called without arguments or with None as the namespace, the namespace is reset, which is recommended to do at the end of any file where the namespace is set to avoid unintentionally setting namespace on tasks outside of the scope of the current file."""
    Register._default_namespace = namespace


def id_to_name_and_params(task_id):
    ''' Turn a task_id into a (task_family, {params}) tuple.
        E.g. calling with 'Foo(bar=bar, baz=baz)' returns ('Foo', {'bar': 'bar', 'baz': 'baz'})
    '''
    lparen = task_id.index('(')
    task_family = task_id[:lparen]
    params = task_id[lparen + 1:-1]

    def split_equals(x):
        equals = x.index('=')
        return x[:equals], x[equals + 1:]
    if params:
        param_list = map(split_equals, params.split(', '))  # TODO: param values with ', ' in them will break this
    else:
        param_list = []
    return task_family, dict(param_list)



class Register(abc.ABCMeta):
    # 1. Cache instances of objects so that eg. X(1, 2, 3) always returns the same object
    # 2. Keep track of all subclasses of Task and expose them
    __instance_cache = {}
    _default_namespace = None
    _reg = []
    AMBIGUOUS_CLASS = object()  # Placeholder denoting an error

    def __new__(metacls, classname, bases, classdict):
        """ Custom class creation for namespacing. Also register all subclasses

        Set the task namespace to whatever the currently declared namespace is
        """
        if "task_namespace" not in classdict:
            classdict["task_namespace"] = metacls._default_namespace

        cls = super(Register, metacls).__new__(metacls, classname, bases, classdict)
        if cls.run != NotImplemented:
            metacls._reg.append(cls)
        return cls

    def __call__(cls, *args, **kwargs):
        """ Custom class instantiation utilizing instance cache.

        If a Task has already been instantiated with the same parameters,
        the previous instance is returned to reduce number of object instances."""
        def instantiate():
            return super(Register, cls).__call__(*args, **kwargs)

        h = Register.__instance_cache

        if h == None:  # disabled
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
    def clear_instance_cache(self):
        Register.__instance_cache = {}

    @classmethod
    def disable_instance_cache(self):
        Register.__instance_cache = None

    @property
    def task_family(cls):
        if cls.task_namespace is None:
            return cls.__name__
        else:
            return "%s.%s" % (cls.task_namespace, cls.__name__)

    @classmethod
    def get_reg(cls):
        reg = {}
        for cls in cls._reg:
            name = cls.task_family
            if name in reg and reg[name] != cls:
                # Registering two different classes - this means we can't instantiate them by name
                reg[name] = cls.AMBIGUOUS_CLASS
            else:
                reg[name] = cls

        return reg

    @classmethod
    def get_global_params(cls):
        global_params = {}
        for cls in cls._reg:
            for param_name, param_obj in cls.get_global_params():
                if param_name in global_params and global_params[param_name] != param_obj:
                    # Could be registered multiple times in case there's subclasses
                    raise Exception('Global parameter %r registered by multiple classes' % param_name)
                global_params[param_name] = param_obj
        return global_params.iteritems()


class Task(object):
    __metaclass__ = Register

    """
    non-declared properties: (created in metaclass):

    `Task.task_namespace` - optional string which is prepended to the task name for the sake of scheduling.
    If it isn't overridden in a Task, whatever was last declared using `luigi.namespace` will be used.

    `Task._parameters` - list of (parameter_name, parameter) tuples for this task class
    """

    @property
    def task_family(self):
        """ Convenience method since a property on the metaclass isn't directly accessible through the class instances"""
        return self.__class__.task_family

    @classmethod
    def get_params(cls):
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
    def get_global_params(cls):
        return [(param_name, param_obj) for param_name, param_obj in cls.get_params() if param_obj.is_global]

    @classmethod
    def get_nonglobal_params(cls):
        return [(param_name, param_obj) for param_name, param_obj in cls.get_params() if not param_obj.is_global]

    @classmethod
    def get_param_values(cls, params, args, kwargs):
        result = {}

        params_dict = dict(params)

        # In case any exceptions are thrown, create a helpful description of how the Task was invoked
        # TODO: should we detect non-reprable arguments? These will lead to mysterious errors
        exc_desc = '%s[args=%s, kwargs=%s]' % (cls.__name__, args, kwargs)

        # Fill in the positional arguments
        positional_params = [(n, p) for n, p in params if not p.is_global]
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
            if params_dict[param_name].is_global:
                raise parameter.ParameterException('%s: can not override global parameter %s' % (exc_desc, param_name))
            result[param_name] = arg

        # Then use the defaults for anything not filled in
        for param_name, param_obj in params:
            if param_name not in result:
                if not param_obj.has_default:
                    raise parameter.MissingParameterException("%s: requires the '%s' parameter to be set" % (exc_desc, param_name))
                result[param_name] = param_obj.default

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

        # Build up task id
        task_id_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if dict(params)[param_name].significant:
                task_id_parts.append('%s=%s' % (param_name, param_objs[param_name].serialize(param_value)))

        self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))
        self.__hash = hash(self.task_id)

    def initialized(self):
        return hasattr(self, 'task_id')

    @classmethod
    def from_input(cls, params, global_params):
        ''' Creates an instance from a str->str hash (to be used for cmd line interaction etc) '''
        for param_name, param in global_params:
            value = param.parse_from_input(param_name, params[param_name])
            param.set_default(value)

        kwargs = {}
        for param_name, param in cls.get_nonglobal_params():
            value = param.parse_from_input(param_name, params[param_name])
            kwargs[param_name] = value

        return cls(**kwargs)

    def clone(self, **kwargs):
        ''' Creates a new instance from an existing instance where some of the args have changed.

        There's at least two scenarios where this is useful (see test/clone_test.py)
        - Remove a lot of boiler plate when you have recursive dependencies and lots of args
        - There's task inheritance and some logic is on the base class
        '''
        k = self.param_kwargs.copy()
        k.update(kwargs.items())

        # remove global params
        for param_name, param_class in self.get_params():
            if param_class.is_global:
                k.pop(param_name)

        return self.__class__(**k)

    def __hash__(self):
        return self.__hash

    def __repr__(self):
        return self.task_id

    def complete(self):
        """
            If the task has any outputs, return true if all outputs exists.
            Otherwise, return whether or not the task has run or not
        """
        outputs = flatten(self.output())
        if len(outputs) == 0:
            # TODO: unclear if tasks without outputs should always run or never run
            warnings.warn("Task %r without outputs has no custom complete() method" % self)
            return False

        for output in outputs:
            if not output.exists():
                return False
        else:
            return True

    def output(self):
        return []  # default impl

    def requires(self):
        return []  # default impl

    def _requires(self):
        '''
        Override in "template" tasks which themselves are supposed to be
        subclassed and thus have their requires() overridden (name preserved to
        provide consistent end-user experience), yet need to introduce
        (non-input) dependencies.

        Must return an iterable which among others contains the _requires() of
        the superclass.
        '''
        return flatten(self.requires())  # base impl

    def input(self):
        return getpaths(self.requires())

    def deps(self):
        # used by scheduler
        return flatten(self._requires())

    def run(self):
        pass  # default impl

    def on_failure(self, exception):
        """ Override for custom error handling

        This method gets called if an exception is raised in :py:meth:`run`.
        Return value of this method is json encoded and sent to the scheduler as the `expl` argument. Its string representation will be used as the body of the error email sent out if any.

        Default behavior is to return a string representation of the stack trace.
        """

        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string

    def on_success(self):
        """ Override for doing custom completion handling for a larger class of tasks

        This method gets called when :py:meth:`run` completes without raising any exceptions.
        The returned value is json encoded and sent to the scheduler as the `expl` argument.
        Default behavior is to send an None value"""
        return None


def externalize(task):
    task.run = NotImplemented
    return task


class ExternalTask(Task):
    """Subclass for references to external dependencies"""
    run = NotImplemented


class WrapperTask(Task):
    """Use for tasks that only wrap other tasks and that by definition are done if all their requirements exist. """
    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))


def getpaths(struct):
    """ Maps all Tasks in a structured data object to their .output()"""
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
    """Cleates a flat list of all all items in structured output (dicts, lists, items)
    Examples:
    > _flatten({'a': foo, b: bar})
    [foo, bar]
    > _flatten([foo, [bar, troll]])
    [foo, bar, troll]
    > _flatten(foo)
    [foo]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, dict):
        for key, result in struct.iteritems():
            flat += flatten(result)
        return flat

    try:
        # if iterable
        for result in struct:
            flat += flatten(result)
        return flat
    except TypeError:
        pass

    return [struct]
