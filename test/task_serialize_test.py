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
We want to test that task_id is consistent when generated from:

 1. A real task instance
 2. The task_family and a dictionary of parameter values (as strings)
 3. A json representation of #2

We use the hypothesis package to do property-based tests.

"""

from __future__ import print_function

import string
import luigi
import json

import hypothesis as hyp
from hypothesis.extra.datetime import datetimes as hyp_datetimes

_no_value = luigi.parameter._no_value


def _mk_param_strategy(param_cls, param_value_strat, with_default=None):
    if with_default is None:
        default = hyp.strategies.one_of(hyp.strategies.just(_no_value), param_value_strat)
    elif with_default:
        default = param_value_strat
    else:
        default = hyp.strategies.just(_no_value)

    return hyp.strategies.builds(param_cls,
                                 description=hyp.strategies.text(alphabet=string.printable),
                                 default=default)


def _mk_task(name, params):
    return type(name, (luigi.Task, ), params)


# identifiers must be str not unicode in Python2
identifiers = hyp.strategies.builds(str, hyp.strategies.text(alphabet=string.ascii_letters, min_size=1, max_size=16))
text = hyp.strategies.text(alphabet=string.printable)

# Luigi parameters with a default
parameters_def = _mk_param_strategy(luigi.Parameter, text, True)
int_parameters_def = _mk_param_strategy(luigi.IntParameter, hyp.strategies.integers(), True)
float_parameters_def = _mk_param_strategy(luigi.FloatParameter,
                                          hyp.strategies.floats(min_value=-1e100, max_value=+1e100), True)
bool_parameters_def = _mk_param_strategy(luigi.BoolParameter, hyp.strategies.booleans(), True)
date_parameters_def = _mk_param_strategy(luigi.DateParameter, hyp_datetimes(min_year=1900, timezones=[]), True)

any_default_parameters = hyp.strategies.one_of(
    parameters_def, int_parameters_def, float_parameters_def, bool_parameters_def, date_parameters_def
)

# Tasks with up to 3 random parameters
tasks_with_defaults = hyp.strategies.builds(
    _mk_task,
    name=identifiers,
    params=hyp.strategies.dictionaries(identifiers, any_default_parameters, max_size=3)
)


def _task_to_dict(task):
    # Generate the parameter value dictionary.  Use each parameter's serialize() method
    param_dict = {}
    for key, param in task.get_params():
        param_dict[key] = param.serialize(getattr(task, key))

    return param_dict


def _task_from_dict(task_cls, param_dict):
    # Regenerate the task from the dictionary
    task_params = {}
    for key, param in task_cls.get_params():
        task_params[key] = param.parse(param_dict[key])

    return task_cls(**task_params)


@hyp.given(tasks_with_defaults)
def test_serializable(task_cls):
    task = task_cls()

    param_dict = _task_to_dict(task)
    task2 = _task_from_dict(task_cls, param_dict)

    assert task.task_id == task2.task_id


@hyp.given(tasks_with_defaults)
def test_json_serializable(task_cls):
    task = task_cls()

    param_dict = _task_to_dict(task)

    param_dict = json.loads(json.dumps(param_dict))
    task2 = _task_from_dict(task_cls, param_dict)

    assert task.task_id == task2.task_id


@hyp.given(tasks_with_defaults)
def test_task_id_alphanumeric(task_cls):
    task = task_cls()
    task_id = task.task_id
    valid = string.ascii_letters + string.digits + '_'

    assert [x for x in task_id if x not in valid] == []

# TODO : significant an non-significant parameters
