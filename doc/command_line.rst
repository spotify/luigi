Running from the Command Line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any task can be instantiated and run from the command line

.. code:: python

    class MyTask(luigi.Task):
        x = IntParameter()
        y = IntParameter(default=45)
        def run(self):
            print self.x + self.y

    if __name__ == '__main__':
           luigi.run()

You can run this task from the command line like this:

::

    python my_task.py MyTask --x 123 --y 456

You can also pass *main\_task\_cls=MyTask* to luigi.run() and that way
you can invoke it simply using

::

    python my_task.py --x 123 --y 456