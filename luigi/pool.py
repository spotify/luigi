# Copyright (c) 2013 Spotify AB
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


class Pool(object):
    ''' Represent a resource pool. Each pool name is shared across all Tasks in a scheduler, with at most
        `max_capacity` running at a time.
    '''
    def __init__(self, name, max_capacity=1):
        self.name = name
        if max_capacity < 1:
            raise ValueError("max_capacity must be a positive integer, not %d" % max_capacity)
        self.max_capacity = max_capacity


