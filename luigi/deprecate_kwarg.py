# -*- coding: utf-8 -*-

import warnings


def deprecate_kwarg(old_name, new_name, kw_value):
    """
    Rename keyword arguments, but keep backwards compatibility.

    Usage:

    .. code-block: python

        >>> @deprecate_kwarg('old', 'new', 'defval')
        ... def some_func(old='defval'):
        ...     print(old)
        ...
        >>> some_func(new='yay')
        yay
        >>> some_func(old='yaay')
        yaay
        >>> some_func()
        defval

    """
    def real_decorator(function):
        def new_function(*args, **kwargs):
            value = kw_value
            if old_name in kwargs:
                warnings.warn('Keyword argument {0} is deprecated, use {1}'
                              .format(old_name, new_name))
                value = kwargs[old_name]
            if new_name in kwargs:
                value = kwargs[new_name]
                del kwargs[new_name]
            kwargs[old_name] = value
            return function(*args, **kwargs)
        return new_function
    return real_decorator
