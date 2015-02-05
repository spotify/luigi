.. highlight:: bash

Running from the Command Line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any task can be instantiated and run from the command line:

.. code-block:: python

    import luigi

    class MyTask(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=45)

        def run(self):
            print self.x + self.y

    if __name__ == '__main__':
        luigi.run()

You can run this task from the command line like this::

    $ python my_task.py MyTask --local-scheduler --x 123 --y 456

You can also pass ``main_task_cls=MyTask`` and ``local_scheduler=True`` to ``luigi.run()`` and that way
you can invoke it simply using

::

    $ python my_task.py --x 123 --y 456

The other way to run a Luigi task is to use the builtin *luigi* task.
This will be default on your path and
can be run by providing a module name.
The module will imported dynamically::

    $ luigi --module my_module MyTask --x 123 --y 456
