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
Package containing core luigi functionality.
"""

import task
import file  # wtf @ naming
import rpc
import parameter
import configuration
import interface
import target
import event

Event = event.Event

Task = task.Task
Config = task.Config
ConfigWithoutSection = task.ConfigWithoutSection
ExternalTask = task.ExternalTask
WrapperTask = task.WrapperTask
Target = target.Target

File = file.File  # TODO: remove, should be LocalTarget
LocalTarget = File
Parameter = parameter.Parameter
RemoteScheduler = rpc.RemoteScheduler
RPCError = rpc.RPCError

run = interface.run
build = interface.build

# TODO: how can we get rid of these?
DateHourParameter = parameter.DateHourParameter
DateMinuteParameter = parameter.DateMinuteParameter
DateParameter = parameter.DateParameter
IntParameter = parameter.IntParameter
FloatParameter = parameter.FloatParameter
BooleanParameter = parameter.BooleanParameter  # backward compatibility
BoolParameter = parameter.BoolParameter
DateIntervalParameter = parameter.DateIntervalParameter
TimeDeltaParameter = parameter.TimeDeltaParameter

namespace = task.namespace

import tools.range  # just makes the tool classes available from command line
