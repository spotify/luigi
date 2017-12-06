.. _CommandLine:

Running from the Command Line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The prefered way to run Luigi tasks is through the ``luigi`` command line tool
that will be installed with the pip package.

.. code-block:: python

    # my_module.py, available in your sys.path
    import luigi

    class MyTask(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=45)

        def run(self):
            print self.x + self.y

Should be run like this

.. code-block:: console

        $ luigi --module my_module MyTask --x 123 --y 456 --local-scheduler

Or alternatively like this:

.. code-block:: console

        $ python -m luigi --module my_module MyTask --x 100 --local-scheduler

Note that if a parameter name contains '_', it should be replaced by '-'.
For example, if MyTask had a parameter called 'my_parameter':

.. code-block:: console

        $ luigi --module my_module MyTask --my-parameter 100 --local-scheduler


Running from Python code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Also, it is possible to run Luigi tasks from Python code using ``luigi.run(*args, **kwargs)`` function.

.. code-block:: python
        
    class MyTask(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=45)
        special_name_with_underscores = luigi.IntParameter()

        def run(self):
            print self.x + self.y + self.special_name_with_underscores

    if __name__ == "__main__":
        luigi.run(cmdline_args=["--workers=1", "--x=123", "--y=456", "--special-name-with-underscores=1"],
        main_task_cls=MyTask,
        worker_scheduler_factory=None,
        use_dynamic_argparse=None,
        local_scheduler=False)
     
        
