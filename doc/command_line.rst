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
