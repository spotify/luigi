import task

def Derived(parent_cls):
    ''' This is a class factory function. It returns a new class with same parameters as
    the parent class, sets the internal value self.parent_obj to an instance of it, and 
    lets you override the rest of it. Useful if you have a class that's an immediate result
    of a previous class and you don't want to reimplement everything. Also useful if you
    want to wrap a class (see wrap_test.py for an example).

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
            self.parent_obj = parent_cls(*args, **kwargs)
            super(DerivedCls, self).__init__(*args, **kwargs)
    
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
            f = o.open('w') # TODO: assert that i, o are Target objects and not complex datastructures
            for line in i.open('r'):
                f.write(line)
            f.close()
    return CopyCls
