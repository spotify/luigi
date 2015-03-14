Programmatic Execution
^^^^^^^^^^^^^^^^^^^^^^

As seen above, command line integration is achieved by simply adding

.. code:: python

    if __name__ == '__main__':
        luigi.run()

This will read the args from the command line (using argparse) and
invoke everything.

In case you just want to run a Luigi chain from a Python script,
you can do that internally without the command line integration.
The code will look something like

.. code:: python

    task = MyTask(123, 'xyz')
    interface.setup_interface_logging()
    sch = scheduler.CentralPlannerScheduler()
    w = worker.Worker(scheduler=sch)
    w.add(task)
    w.run()
