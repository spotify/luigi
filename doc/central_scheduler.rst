Using the Central Scheduler
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The central scheduler does not execute anything for you, or help you
with job parallelization. The two purposes it serves are to

-  Make sure two instances of the same task are not running
   simultaneously
-  Provide visualization of everything that's going on.

For running tasks periodically, the easiest thing to do is to trigger a
Python script from cron or from a continuously running process. There is
no central process that automatically triggers job. This model may seem
limited, but we believe that it makes things far more intuitive and easy
to understand.