# -*- coding: utf-8 -*-
# Copyright (c) 2015 Spotify AB
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

import warnings

import pyparsing as pp


def id_to_name_and_params(task_id):
    """
    Turn a task_id into a (task_family, {params}) tuple.

    E.g. calling with ``Foo(bar=bar, baz=baz)`` returns ``('Foo', {'bar': 'bar', 'baz': 'baz'})``.
    """

    warnings.warn(
        'id_to_name_and_params is deprecated (and moved to luigi.tools.parse_task). '
        'Please don\'t use task names as a serialization mechanism. Rather, store '
        'the task family and the parameters as separate strings',
        DeprecationWarning,
        stacklevel=2)

    name_chars = pp.alphanums + '_'
    # modified version of pp.printables. Removed '[]', '()', ','
    value_chars = pp.alphanums + '\'!"#$%&*+-./:;<=>?@\\^_`{|}~'
    parameter = (
        (pp.Word(name_chars) +
         pp.Literal('=').suppress() +
         ((pp.Literal('(').suppress() | pp.Literal('[').suppress()) +
          pp.ZeroOrMore(pp.Word(value_chars) +
                        pp.ZeroOrMore(pp.Literal(',')).suppress()) +
          (pp.Literal(')').suppress() |
           pp.Literal(']').suppress()))).setResultsName('list_params',
                                                        listAllMatches=True) |
        (pp.Word(name_chars) +
         pp.Literal('=').suppress() +
         pp.Word(value_chars)).setResultsName('params', listAllMatches=True))

    parser = (
        pp.Word(name_chars).setResultsName('task') +
        pp.Literal('(').suppress() +
        pp.ZeroOrMore(parameter + (pp.Literal(',')).suppress()) +
        pp.ZeroOrMore(parameter) +
        pp.Literal(')').suppress())

    parsed = parser.parseString(task_id).asDict()
    task_name = parsed['task']

    params = {}
    if 'params' in parsed:
        for k, v in parsed['params']:
            params[k] = v
    if 'list_params' in parsed:
        for x in parsed['list_params']:
            params[x[0]] = x[1:]
    return task_name, params
