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
