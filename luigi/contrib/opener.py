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

"""OpenerTarget support, allows easier testing and configuration by abstracting
out the LocalTarget, S3Target, and MockTarget types.

Example:

.. code-block:: python

    from luigi.contrib.opener import OpenerTarget

    OpenerTarget('/local/path.txt')
    OpenerTarget('s3://zefr/remote/path.txt')

"""

import json

from luigi.file import LocalTarget
from luigi.mock import MockTarget
from luigi.s3 import S3Target
from luigi.target import FileSystemException
from six.moves.urllib.parse import urlsplit, parse_qs

__all__ = ['OpenerError',
           'NoOpenerError',
           'InvalidQuery',
           'OpenerRegistry',
           'Opener',
           'MockOpener',
           'LocalOpener',
           'S3Opener',
           'opener',
           'OpenerTarget']


class OpenerError(FileSystemException):

    """The base exception thrown by openers"""
    pass


class NoOpenerError(OpenerError):

    """Thrown when there is no opener for the given protocol"""
    pass


class InvalidQuery(OpenerError):

    """Thrown when an opener is passed unexpected arguments"""
    pass


class OpenerRegistry(object):

    def __init__(self, openers=[]):
        """An opener registry that  stores a number of opener objects used
        to parse Target URIs

        :param openers: A list of objects inherited from the Opener class.
        :type openers: list

        """
        self.registry = {}
        self.openers = {}
        self.default_opener = 'file'
        for opener in openers:
            self.add(opener)

    def get_opener(self, name):
        """Retrieve an opener for the given protocol

        :param name: name of the opener to open
        :type name: string
        :raises NoOpenerError: if no opener has been registered of that name

        """
        if name not in self.registry:
            raise NoOpenerError("No opener for %s" % name)
        index = self.registry[name]
        return self.openers[index]

    def add(self, opener):
        """Adds an opener to the registry

        :param opener: Opener object
        :type opener: Opener inherited object

        """

        index = len(self.openers)
        self.openers[index] = opener
        for name in opener.names:
            self.registry[name] = index

    def open(self, target_uri, **kwargs):
        """Open target uri.

        :param target_uri: Uri to open
        :type target_uri: string

        :returns: Target object

        """
        target = urlsplit(target_uri, scheme=self.default_opener)

        opener = self.get_opener(target.scheme)
        query = opener.conform_query(target.query)

        target = opener.get_target(
            target.scheme,
            target.path,
            target.fragment,
            target.username,
            target.password,
            target.hostname,
            target.port,
            query,
            **kwargs
        )
        target.opener_path = target_uri

        return target


class Opener(object):

    """Base class for Opener objects.

    """

    # Dictionary of expected kwargs and flag for json loading values (bool/int)
    allowed_kwargs = {}
    # Flag to filter out unexpected kwargs
    filter_kwargs = True

    @classmethod
    def conform_query(cls, query):
        """Converts the query string from a target uri, uses
        cls.allowed_kwargs, and cls.filter_kwargs to drive logic.

        :param query: Unparsed query string
        :type query: urllib.parse.unsplit(uri).query
        :returns: Dictionary of parsed values, everything in cls.allowed_kwargs
            with values set to True will be parsed as json strings.

        """
        query = parse_qs(query, keep_blank_values=True)

        # Remove any unexpected keywords from the query string.
        if cls.filter_kwargs:
            query = {x: y for x, y in query.items() if x in cls.allowed_kwargs}

        for key, vals in query.items():
            # Multiple values of the same name could be passed use first
            # Also params without strings will be treated as true values
            if cls.allowed_kwargs.get(key, False):
                val = json.loads(vals[0] or 'true')
            else:
                val = vals[0] or 'true'

            query[key] = val

        return query

    @classmethod
    def get_target(cls, scheme, path, fragment, username,
                   password, hostname, port, query, **kwargs):
        """Override this method to use values from the parsed uri to initialize
        the expected target.

        """
        raise NotImplemented("get_target must be overridden")


class MockOpener(Opener):

    """Mock target opener, works like LocalTarget but files are all in
    memory.

    example:
    * mock://foo/bar.txt

    """
    names = ['mock']
    allowed_kwargs = {
        'is_tmp': True,
        'mirror_on_stderr': True,
        'format': False,
    }

    @classmethod
    def get_target(cls, scheme, path, fragment, username,
                   password, hostname, port, query, **kwargs):
        full_path = (hostname or '') + path
        query.update(kwargs)
        return MockTarget(full_path, **query)


class LocalOpener(Opener):

    """Local filesystem opener, works with any valid system path. This
    is the default opener and will be used if you don't indicate which opener.

    examples:
    * file://relative/foo/bar/baz.txt (opens a relative file)
    * file:///home/user (opens a directory from a absolute path)
    * foo/bar.baz (file:// is the default opener)

    """
    names = ['file']
    allowed_kwargs = {
        'is_tmp': True,
        'format': False,
    }

    @classmethod
    def get_target(cls, scheme, path, fragment, username,
                   password, hostname, port, query, **kwargs):
        full_path = (hostname or '') + path
        query.update(kwargs)
        return LocalTarget(full_path, **query)


class S3Opener(Opener):

    """Opens a target stored on Amazon S3 storage

    examples:
    * s3://bucket/foo/bar.txt
    * s3://bucket/foo/bar.txt?aws_access_key_id=xxx&aws_secret_access_key=yyy

    """
    names = ['s3', 's3n']
    allowed_kwargs = {
        'format': False,
        'client': True,
    }
    filter_kwargs = False

    @classmethod
    def get_target(cls, scheme, path, fragment, username,
                   password, hostname, port, query, **kwargs):
        query.update(kwargs)
        return S3Target('{scheme}://{hostname}{path}'.format(
            scheme=scheme, hostname=hostname, path=path), **query)


opener = OpenerRegistry([
    MockOpener,
    LocalOpener,
    S3Opener,
])

OpenerTarget = opener.open
