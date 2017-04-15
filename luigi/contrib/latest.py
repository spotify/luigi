# -*- coding: utf-8 -*-
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

from luigi.util import class_wraps
from luigi import DateParameter
import datetime
import cPickle

class latest(object):
    ''' The @latest decorator adds a useful static constructor to
        any class with a date parameter. Basically you can invoke
        MyTask.latest() to get the last existing version of a task
        
    Usage:
    @latest()
    class DataDump(luigi.Task):
        date = luigi.DateParameter()
        # ...

    class SomeTask(luigi.Task):
        def requires(self):
           return DataDump.latest()

        # ...
    '''
    def __init__(self, param=None, default_lookback=14):
        self.cache = {}
        self.param_name = param
        self.param_obj = None
        self.default_lookback = 14

    def __call__(self, task_cls):
        self.task_cls = task_cls

        # Find the right parameter
        for param_name, param_obj in task_cls.get_params():
            if param_name == self.param_name:
                self.param_obj = param_obj
                break
            elif isinstance(param_obj, DateParameter):
                # Use last one if several
                self.param_name = param_name
                self.param_obj = param_obj

        if self.param_obj is None:
            raise Exception('Could not find any DateParameter')

        @class_wraps(task_cls)
        class Wrapped(task_cls):
            @classmethod
            def latest(cls, *args, **kwargs):
                # We want to cache this so that requires() is deterministic
                date = kwargs.pop(param_name, None)
                if date is None:
                    date = datetime.date.today()
                    
                lookback = kwargs.pop('lookback', self.default_lookback)

                k = (cls, cPickle.dumps(args), cPickle.dumps(kwargs), lookback, date)
                if k not in self.cache:
                    self.cache[k] = cls.__latest(date, lookback, args, kwargs)

                return self.cache[k]
        
            @classmethod
            def __latest(cls, date, lookback, args, kwargs):
                for i in xrange(lookback):
                    d = date - datetime.timedelta(i)
                    t = cls(d, *args, **kwargs)
                    if t.complete():
                        return t

                raise Exception('Could not find last dump for %s (looked back %d days)' % (task_cls.__name__, lookback))

        return Wrapped
