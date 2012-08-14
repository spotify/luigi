import parameter
import warnings

Parameter = parameter.Parameter


def namespace(namespace=None):
    """ Call to set namespace of tasks declared after the call.

    If called without arguments or with None as the namespace, the namespace is reset, which is recommended to do at the end of any file where the namespace is set to avoid unintentionally setting namespace on tasks outside of the scope of the current file."""
    TaskMetaclass._default_namespace = namespace


class TaskMetaclass(type):
    # If we already have an instance of this class, then just return it from the cache
    # The idea is that a Task object X should be able to set up heavy data structures that
    # can be accessed from other Task objects (with dependencies on X). But we need to make
    # sure that X is not instantiated many times.
    __instance_cache = {}
    _default_namespace = None

    def __new__(metacls, classname, bases, classdict):
        """ Custom class creation for namespacing

        Set the task namespace to whatever the currently declared namespace is"""

        if "task_namespace" not in classdict:
            classdict["task_namespace"] = metacls._default_namespace

        return type.__new__(metacls, classname, bases, classdict)

    def __call__(cls, *args, **kwargs):
        """ Custom class instantiation utilizing instance cache.

        If a Task has already been instantiated with the same parameters,
        the previous instance is returned to reduce number of object instances."""
        def instantiate():
            return super(TaskMetaclass, cls).__call__(*args, **kwargs)

        h = TaskMetaclass.__instance_cache

        if h == None:  # disabled
            return instantiate()

        params = cls.get_params()
        param_values = cls.get_param_values(params, args, kwargs)

        k = (cls, tuple(param_values))

        if k not in h:
            h[k] = instantiate()

        return h[k]

    @classmethod
    def clear_instance_cache(self):
        TaskMetaclass.__instance_cache = {}

    @classmethod
    def disable_instance_cache(self):
        TaskMetaclass.__instance_cache = None

    @property
    def task_family(cls):
        if cls.task_namespace is None:
            return cls.__name__
        else:
            return "%s.%s" % (cls.task_namespace, cls.__name__)


class Task(object):
    __metaclass__ = TaskMetaclass

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
    def get_param_values(cls, params, args, kwargs):
        result = {}

        params_dict = dict(params)

        # Fill in the positional arguments
        positional_params = [p for p in params if not p[1].keyword_only]
        for i, arg in enumerate(args):
            param_name, param_obj = positional_params[i]
            result[param_name] = arg

        # Then the optional arguments
        for param_name, arg in kwargs.iteritems():
            assert param_name not in result
            assert param_name in params_dict
            result[param_name] = arg

        # Then use the defaults for anything not filled in
        for param_name, param_obj in params:
            if param_name not in result:
                if not param_obj.has_default:
                    raise parameter.MissingParameterException("'%s' tasks requires the '%s' parameter to be set" % (cls.__name__, param_name))
                result[param_name] = param_obj.default

        def list_to_tuple(x):
            """ Make tuples out of lists to allow hashing """
            if isinstance(x, list):
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

        self.task_id = '%s(%s)' % (self.task_family, ', '.join(['%s=%s' % (str(k), str(v)) for k, v in param_values]))
        self.__hash = hash(self.task_id)

    @classmethod
    def from_input(cls, params):
        # Creates an instance from a str->str hash (to be used for cmd line interaction etc)
        kwargs = {}
        for param_name, param in cls.get_params():
            kwargs[param_name] = param.parse_from_input(param_name, params[param_name])

        return cls(**kwargs)

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

    def input(self):
        return getpaths(self.requires())

    def deps(self):
        # used by scheduler
        return flatten(self.requires())

    def run(self):
        pass  # default impl

    def on_failure(self, exception, traceback):
        """ Override for custom error handling

        This method gets called if an exception is raised in :py:meth:`run`.
        Return value of this method is json encoded and sent to the scheduler as the `expl` argument.
        Default behavior is to return a string representation of the exception and traceback.
        """
        return {"exception": str(exception),
                "traceback": str(traceback)}

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
