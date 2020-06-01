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

""" Definitions needed for events. See :ref:`Events` for info on how to use it."""


class Event:
    # TODO nice descriptive subclasses of Event instead of strings? pass their instances to the callback instead of an undocumented arg list?
    DEPENDENCY_DISCOVERED = "event.core.dependency.discovered"  # triggered for every (task, upstream task) pair discovered in a jobflow
    DEPENDENCY_MISSING = "event.core.dependency.missing"
    DEPENDENCY_PRESENT = "event.core.dependency.present"
    BROKEN_TASK = "event.core.task.broken"
    START = "event.core.start"
    #: This event can be fired by the task itself while running. The purpose is
    #: for the task to report progress, metadata or any generic info so that
    #: event handler listening for this can keep track of the progress of running task.
    PROGRESS = "event.core.progress"
    FAILURE = "event.core.failure"
    SUCCESS = "event.core.success"
    PROCESSING_TIME = "event.core.processing_time"
    TIMEOUT = "event.core.timeout"  # triggered if a task times out
    PROCESS_FAILURE = "event.core.process_failure"  # triggered if the process a task is running in dies unexpectedly
