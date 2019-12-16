Running Luigi
-------------

Running from the Command Line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The preferred way to run Luigi tasks is through the ``luigi`` command line tool
that will be installed with the pip package.

.. code-block:: python

    # my_module.py, available in your sys.path
    import luigi

    class MyTask(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=45)

        def run(self):
            print(self.x + self.y)

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

.. note:: Please make sure to always place task parameters behind the task family!


Running from Python code
^^^^^^^^^^^^^^^^^^^^^^^^

Another way to start tasks from Python code is using ``luigi.build(tasks, worker_scheduler_factory=None, **env_params)``
from ``luigi.interface`` module.

This way of running luigi tasks is useful if you want to get some dynamic parameters from another
source, such as database, or provide additional logic before you start tasks.

One notable difference is that ``build`` defaults to not using the identical process lock.
If you want to change this behaviour, just pass ``no_lock=False``.


.. code-block:: python

    class MyTask1(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=0)

        def run(self):
            print(self.x + self.y)


    class MyTask2(luigi.Task):
        x = luigi.IntParameter()
        y = luigi.IntParameter(default=1)
        z = luigi.IntParameter(default=2)

        def run(self):
            print(self.x * self.y * self.z)


    if __name__ == '__main__':
        luigi.build([MyTask1(x=10), MyTask2(x=15, z=3)])


Also, it is possible to pass additional parameters to ``build`` such as host, port, workers and local_scheduler:

.. code-block:: python

    if __name__ == '__main__':
         luigi.build([MyTask1(x=1)], workers=5, local_scheduler=True)

To achieve some special requirements you can pass to ``build`` your  ``worker_scheduler_factory``
which will return your worker and/or scheduler implementations:

.. code-block:: python

    class MyWorker(Worker):
        # some custom logic


    class MyFactory:
      def create_local_scheduler(self):
          return scheduler.Scheduler(prune_on_get_work=True, record_task_history=False)

      def create_remote_scheduler(self, url):
          return rpc.RemoteScheduler(url)

      def create_worker(self, scheduler, worker_processes, assistant=False):
          # return your worker instance
          return MyWorker(
              scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)


    if __name__ == '__main__':
        luigi.build([MyTask1(x=1)], worker_scheduler_factory=MyFactory())

In some cases (like task queue) it may be useful.



Response of luigi.build()/luigi.run()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Default response** By default *luigi.build()/luigi.run()* returns True if there were no scheduling errors. This is the same as the attribute ``LuigiRunResult.scheduling_succeeded``.

- **Detailed response** This is a response of type :class:`~luigi.execution_summary.LuigiRunResult`. This is obtained by passing a keyword argument ``detailed_summary=True`` to *build/run*. This response contains detailed information about the jobs.

  .. code-block:: python

    if __name__ == '__main__':
         luigi_run_result = luigi.build(..., detailed_summary=True)
         print(luigi_run_result.summary_text)


Luigi on Windows
^^^^^^^^^^^^^^^^

Most Luigi functionality works on Windows. Exceptions:

- Specifying multiple worker processes using the ``workers`` argument for
  ``luigi.build``, or using the ``--workers`` command line argument. (Similarly,
  specifying ``--worker-force-multiprocessing``). For most programs, this will
  result in failure (a common sight is ``BrokenPipeError``). The reason is that
  worker processes are assumed to be forked from the main process. Forking is
  `not possible <https://docs.python.org/dev/library/multiprocessing.html#contexts-and-start-methods>`_
  on Windows.
- Running the Luigi central scheduling server as a daemon (i.e. with ``--background``).
  Again, a Unix-only concept.
