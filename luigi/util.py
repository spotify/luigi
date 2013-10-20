# Copyright (c) 2012 Spotify AB
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

import task

def common_params(task_instance, task_cls):
    """Grab all the values in task_instance that are found in task_cls"""
    assert isinstance(task_cls, task.Register), "task_cls must be an uninstantiated Task"

    task_instance_param_names = dict(task_instance.get_params()).keys()
    task_cls_param_names = dict(task_cls.get_params()).keys()
    common_param_names = list(set.intersection(set(task_instance_param_names),set(task_cls_param_names)))
    common_param_vals = [(key,dict(task_cls.get_params())[key]) for key in common_param_names]
    common_kwargs = dict([(key,task_instance.param_kwargs[key]) for key in common_param_names])
    vals = dict(task_instance.get_param_values(common_param_vals, [], common_kwargs))
    return vals

class inherits(object):
    """docstring for inherits"""
    def __init__(self, task_to_inherit):
        super(inherits, self).__init__()
        self.task_to_inherit = task_to_inherit
    
    def __call__(self, task_that_inherits):
        this_param_names = dict(task_that_inherits.get_nonglobal_params()).keys()
        for param_name, param_obj in self.task_to_inherit.get_params():
            if not hasattr(task_that_inherits, param_name):
                setattr(task_that_inherits, param_name, param_obj)

        return task_that_inherits


def Derived(parent_cls):
    ''' This is a class factory function. It returns a new class with same parameters as
    the parent class, sets the internal value self.parent_obj to an instance of it, and
    lets you override the rest of it. Useful if you have a class that's an immediate result
    of a previous class and you don't want to reimplement everything. Also useful if you
    want to wrap a class (see wrap_test.py for an example).

    Note 1: The derived class does not inherit from the parent class
    Note 2: You can add more parameters in the derived class

    Usage:
    class AnotherTask(luigi.Task):
        n = luigi.IntParameter()
        # ...

    class MyTask(luigi.uti.Derived(AnotherTask)):
        def requires(self):
           return self.parent_obj
        def run(self):
           print self.n # this will be defined
           # ...
    '''
    class DerivedCls(task.Task):
        def __init__(self, *args, **kwargs):
            param_values = {}
            for k, v in self.get_param_values(self.get_nonglobal_params(), args, kwargs):
                param_values[k] = v

            # Figure out which params the parent need (it's always a subset)
            parent_param_values = {}
            for k, v in parent_cls.get_nonglobal_params():
                parent_param_values[k] = param_values[k]

            self.parent_obj = parent_cls(**parent_param_values)
            super(DerivedCls, self).__init__(*args, **kwargs)

    # Copy parent's params to child
    for param_name, param_obj in parent_cls.get_params():
        setattr(DerivedCls, param_name, param_obj)
    return DerivedCls


def Copy(parent_cls):
    ''' Creates a new Task that copies the old task. Usage:

    class CopyOfMyTask(Copy(MyTask)):
        def output(self):
           return LocalTarget(self.date.strftime('/var/xyz/report-%Y-%m-%d'))
    '''

    class CopyCls(Derived(parent_cls)):
        def requires(self):
            return self.parent_obj

        output = NotImplemented

        def run(self):
            i, o = self.input(), self.output()
            f = o.open('w')  # TODO: assert that i, o are Target objects and not complex datastructures
            for line in i.open('r'):
                f.write(line)
            f.close()
    return CopyCls


class CompositionTask(task.Task):
    # Experimental support for composition task. This is useful if you have two tasks where
    # X has a dependency on Y and X wants to invoke methods on Y. The problem with a normal
    # requires() style dependency is that if X and Y are run in different processes then
    # X can not access Y. To solve this, you can let X own a reference to an Y and have it
    # run it as a part of its own run method.

    def subtasks(self):
        # This method can (optionally) define a couple of delegate tasks that
        # will be accessible as interfaces, meaning that the task can access
        # those tasks and run methods defined on them, etc
        return []  # default impl

    def deps(self):
        # Overrides method in base class
        return task.flatten(self.requires()) + task.flatten([t.deps() for t in task.flatten(self.subtasks())])

    def run_subtasks(self):
        for t in task.flatten(self.subtasks()):
            t.run()

    # Note that your run method must also initialize subtasks
    # def run(self):
    #    self.run_subtasks()
    #    ...
