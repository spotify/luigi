import task

def Delegate(parent_cls):
    ''' This is a class factory function. It returns a new class with same parameters as
    the parent class, sets the internal value self.parent_obj to an instance of it, and 
    lets you override the rest of it.

    Usage:
    class MyTask(Delegate(AnotherTask)):
        def requires(self):
           return self.parent_obj
        def run(self):
           # ...
    '''
    class DelegateCls(task.Task):
        def __init__(self, *args, **kwargs):
            self.parent_obj = parent_cls(*args, **kwargs)
            super(DelegateCls, self).__init__(*args, **kwargs)
    
    for param_name, param_obj in parent_cls.get_params():
        setattr(DelegateCls, param_name, param_obj)
    return DelegateCls

def Copy(parent_cls):
    ''' Creates a new Task that copies the old task. Usage:

    class CopyOfMyTask(Copy(MyTask)):
        def output(self):
           return LocalTarget(self.date.strftime('/var/xyz/report-%Y-%m-%d'))
    '''

    class CopyCls(Delegate(parent_cls)):
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
