Periodic tasks
--------------

Luigi's execution model leaves *triggering* to you: something has to
invoke ``luigi`` for a workflow to run. Traditionally that something is
crontab (see :doc:`execution_model`). ``luigi-periodic`` is a small,
optional daemon that plays the same role from within Luigi itself, so
recurring workflows can be declared next to the rest of your Luigi
configuration.

Quick start
~~~~~~~~~~~

Declare one config section per schedule, in your regular Luigi
configuration file:

.. code:: ini

    [periodic my_reports]
    module = all_reports
    task = RangeDaily
    args = --of AllReports --start 2025-01-01
    schedule = 0 2 * * *

then run the daemon:

.. code-block:: console

    luigi-periodic

Every night at 02:00 the daemon launches, as an ordinary subprocess:

.. code-block:: console

    python -m luigi --module all_reports RangeDaily --of AllReports --start 2025-01-01

Because each fire is a normal ``luigi`` invocation, runs show up in the
central scheduler UI and behave exactly as if cron had started them.

Options
~~~~~~~

Each ``[periodic <name>]`` section supports:

``task`` (required)
    The task family to run.

``module``
    Passed as ``--module`` so the task can be imported.

``args``
    Extra command line arguments, e.g. ``--workers 4`` or parameters.
    Values live in Luigi's configuration, which interpolates ``%``, so
    escape a literal percent sign as ``%%`` in cfg files (e.g.
    ``--date-format %%Y-%%m-%%d``).

``schedule``
    A standard 5-field cron expression. Requires the optional
    ``croniter`` dependency: ``pip install luigi[periodic]``.

``every``
    Fire every N seconds instead of on a cron schedule; no extra
    dependency needed. Exactly one of ``schedule`` and ``every`` must be
    set.

``overlap_policy``
    What to do when the entry comes due while its previous run is still
    alive. ``skip`` (default) drops the fire; ``queue`` starts one run
    as soon as the previous finishes (multiple missed fires collapse
    into a single queued run).

``jitter_seconds``
    Delay each fire by a uniformly random amount up to this value, to
    spread load when many entries share a schedule.

``enabled``
    Set to ``false`` to keep the section but stop scheduling it.

The daemon reloads its configuration on ``SIGHUP``. On ``SIGTERM`` or
``SIGINT`` it stops launching new runs, waits for running children, and
exits.

Timing notes
~~~~~~~~~~~~

Schedules are evaluated in the daemon's local time, mirroring crontab:
across a DST spring-forward a ``0 2 * * *`` entry fires late (when the
wall clock reaches 03:00), and across a fall-back it fires once, not
twice. ``every`` intervals are measured from when each fire is
processed, so they drift slightly rather than staying aligned to the
wall clock - use a cron ``schedule`` when alignment matters.

A ``SIGHUP`` reload recomputes every entry's next fire time from "now".
Cron entries are unaffected (their fire times are absolute), but an
``every`` interval restarts, so very frequent reloads can postpone
long-interval entries.

Dashboard
~~~~~~~~~

The daemon pushes its schedule state to the central scheduler, where it
appears on the **Periodic** tab of the luigid web interface: one panel
per daemon showing each entry's schedule, next fire time, whether it is
currently running or queued, and the result of its last run. A daemon
that has stopped reporting is marked stale; one that shut down cleanly
is marked stopped. (The runs themselves appear in the regular Task List
and dependency graph, exactly like cron-launched runs.)

Status is pushed to the scheduler the launched tasks would use
(``[core]`` ``default_scheduler_url``, or ``default_scheduler_host`` /
``default_scheduler_port``). Pushing is controlled by a daemon-level
``[periodic]`` section - note, without an entry name:

.. code:: ini

    [periodic]
    push_status = true   ; set false to disable pushing entirely
    push_interval = 30   ; heartbeat seconds between unchanged-state pushes

A push failure (e.g. luigid is down) is logged and never interferes
with firing; the daemon keeps triggering tasks regardless.

Missed runs and catch-up
~~~~~~~~~~~~~~~~~~~~~~~~

``luigi-periodic`` deliberately keeps no state about past fires. If the
daemon (or its host) is down when a fire was due, that fire is simply
missed - the same behavior as cron. The Luigi way to make recurring
workflows self-healing is the :mod:`luigi.tools.range` family: schedule
``RangeDaily --of YourTask`` rather than ``YourTask`` itself, and any
gap is back-filled on the next successful fire. See
:doc:`luigi_patterns` for details on the Range tools.

For the same reason, an accidental double-trigger is harmless: tasks
whose ``complete()`` returns true are never re-run.

Comparison with cron
~~~~~~~~~~~~~~~~~~~~

``luigi-periodic`` is a convenience, not a distributed scheduler. A
single daemon process is exactly as available as the crontab it
replaces. What you gain is portability (works anywhere Python runs,
including containers and Windows, without a cron implementation),
schedules versioned alongside your Luigi configuration, overlap
handling, and log lines/exit codes reported through the standard
``luigi-interface`` logger. If you already run your workflows from a
full-featured orchestrator, keep doing that.
