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


class Event(object):
    # TODO nice descriptive subclasses of Event instead of strings? pass their instances to the callback instead of an undocumented arg list?
    DEPENDENCY_DISCOVERED = "event.core.dependency.discovered"  # triggered for every (task, upstream task) pair discovered in a jobflow
    DEPENDENCY_MISSING = "event.core.dependency.missing"
    DEPENDENCY_PRESENT = "event.core.dependency.present"
    BROKEN_TASK = "event.core.task.broken"
    START = "event.core.start"
    FAILURE = "event.core.failure"
    SUCCESS = "event.core.success"
    PROCESSING_TIME = "event.core.processing_time"
