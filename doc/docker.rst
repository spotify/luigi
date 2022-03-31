Docker
------

To run a luigi scheduler locally using docker there are 2 steps.

Step 1 - Create local client.cfg file.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the one below. We will be injecting this config file as a volume later.

.. code:: bash

    [scheduler]
    record_task_history: True
    state-path: /luigi/state/luigi-state.pickle

    [task_history]
    db_connection: sqlite:////luigi/state/luigi-task-history.db


Step 2 - Build and run your image.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now just build your container and run it WHILE forwarding port 8082 and mounting our config file from earlier.

After you are done, access it by visiting `localhost:8082` in your browser!

.. code:: bash

    docker build -tag luigi-scheduler-staging .
    docker run -p 8082:8082 -v /my/path/to/client.cfg:/etc/luigi/client.cfg luigi-scheduler-staging