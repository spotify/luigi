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
luigi.configuration provides some convenience wrappers around Python's
ConfigParser to get configuration options from config files.

The default location for configuration files is luigi.cfg (or client.cfg) in the current
working directory, then /etc/luigi/client.cfg.

Configuration has largely been superseded by parameters since they can
do essentially everything configuration can do, plus a tighter integration
with the rest of Luigi.

See :doc:`/configuration` for more info.
"""

import os
import re
import warnings

from configparser import ConfigParser, NoOptionError, NoSectionError, InterpolationError
from configparser import Interpolation, BasicInterpolation

from .base_parser import BaseParser


class InterpolationMissingEnvvarError(InterpolationError):
    """
    Raised when option value refers to a nonexisting environment variable.
    """

    def __init__(self, option, section, value, envvar):
        msg = (
            "Config refers to a nonexisting environment variable {}. "
            "Section [{}], option {}={}"
        ).format(envvar, section, option, value)
        InterpolationError.__init__(self, option, section, msg)


class EnvironmentInterpolation(Interpolation):
    """
    Custom interpolation which allows values to refer to environment variables
    using the ``${ENVVAR}`` syntax.
    """
    _ENVRE = re.compile(r"\$\{([^}]+)\}")  # matches "${envvar}"

    def before_get(self, parser, section, option, value, defaults):
        return self._interpolate_env(option, section, value)

    def _interpolate_env(self, option, section, value):
        rawval = value
        parts = []
        while value:
            match = self._ENVRE.search(value)
            if match is None:
                parts.append(value)
                break
            envvar = match.groups()[0]
            try:
                envval = os.environ[envvar]
            except KeyError:
                raise InterpolationMissingEnvvarError(
                    option, section, rawval, envvar)
            start, end = match.span()
            parts.append(value[:start])
            parts.append(envval)
            value = value[end:]
        return "".join(parts)


class CombinedInterpolation(Interpolation):
    """
    Custom interpolation which applies multiple interpolations in series.

    :param interpolations: a sequence of configparser.Interpolation objects.
    """

    def __init__(self, interpolations):
        self._interpolations = interpolations

    def before_get(self, parser, section, option, value, defaults):
        for interp in self._interpolations:
            value = interp.before_get(parser, section, option, value, defaults)
        return value

    def before_read(self, parser, section, option, value):
        for interp in self._interpolations:
            value = interp.before_read(parser, section, option, value)
        return value

    def before_set(self, parser, section, option, value):
        for interp in self._interpolations:
            value = interp.before_set(parser, section, option, value)
        return value

    def before_write(self, parser, section, option, value):
        for interp in self._interpolations:
            value = interp.before_write(parser, section, option, value)
        return value


class LuigiConfigParser(BaseParser, ConfigParser):
    NO_DEFAULT = object()
    enabled = True
    _instance = None
    _config_paths = [
        '/etc/luigi/client.cfg',  # Deprecated old-style global luigi config
        '/etc/luigi/luigi.cfg',
        'client.cfg',  # Deprecated old-style local luigi config
        'luigi.cfg',
    ]
    _DEFAULT_INTERPOLATION = CombinedInterpolation([BasicInterpolation(), EnvironmentInterpolation()])

    @classmethod
    def reload(cls):
        # Warn about deprecated old-style config paths.
        deprecated_paths = [p for p in cls._config_paths if os.path.basename(p) == 'client.cfg' and os.path.exists(p)]
        if deprecated_paths:
            warnings.warn("Luigi configuration files named 'client.cfg' are deprecated if favor of 'luigi.cfg'. " +
                          "Found: {paths!r}".format(paths=deprecated_paths),
                          DeprecationWarning)

        return cls.instance().read(cls._config_paths)

    def _get_with_default(self, method, section, option, default, expected_type=None, **kwargs):
        """
        Gets the value of the section/option using method.

        Returns default if value is not found.

        Raises an exception if the default value is not None and doesn't match the expected_type.
        """
        try:
            try:
                # Underscore-style is the recommended configuration style
                option = option.replace('-', '_')
                return method(self, section, option, **kwargs)
            except (NoOptionError, NoSectionError):
                # Support dash-style option names (with deprecation warning).
                option_alias = option.replace('_', '-')
                value = method(self, section, option_alias, **kwargs)
                warn = 'Configuration [{s}] {o} (with dashes) should be avoided. Please use underscores: {u}.'.format(
                    s=section, o=option_alias, u=option)
                warnings.warn(warn, DeprecationWarning)
                return value
        except (NoOptionError, NoSectionError):
            if default is LuigiConfigParser.NO_DEFAULT:
                raise
            if expected_type is not None and default is not None and \
               not isinstance(default, expected_type):
                raise
            return default

    def has_option(self, section, option):
        """modified has_option
        Check for the existence of a given option in a given section. If the
        specified 'section' is None or an empty string, DEFAULT is assumed. If
        the specified 'section' does not exist, returns False.
        """

        # Underscore-style is the recommended configuration style
        option = option.replace('-', '_')
        if ConfigParser.has_option(self, section, option):
            return True

        # Support dash-style option names (with deprecation warning).
        option_alias = option.replace('_', '-')
        if ConfigParser.has_option(self, section, option_alias):
            warn = 'Configuration [{s}] {o} (with dashes) should be avoided. Please use underscores: {u}.'.format(
                s=section, o=option_alias, u=option)
            warnings.warn(warn, DeprecationWarning)
            return True

        return False

    def get(self, section, option, default=NO_DEFAULT, **kwargs):
        return self._get_with_default(ConfigParser.get, section, option, default, **kwargs)

    def getboolean(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getboolean, section, option, default, bool)

    def getint(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getint, section, option, default, int)

    def getfloat(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getfloat, section, option, default, float)

    def getintdict(self, section):
        try:
            # Exclude keys from [DEFAULT] section because in general they do not hold int values
            return dict((key, int(value)) for key, value in self.items(section)
                        if key not in {k for k, _ in self.items('DEFAULT')})
        except NoSectionError:
            return {}

    def set(self, section, option, value=None):
        if not ConfigParser.has_section(self, section):
            ConfigParser.add_section(self, section)

        return ConfigParser.set(self, section, option, value)
