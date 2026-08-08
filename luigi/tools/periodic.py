# -*- coding: utf-8 -*-
#
# Copyright 2026 The Luigi Authors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
Built-in periodic triggering of Luigi workflows.

``luigi-periodic`` is a small daemon that replaces the crontab
traditionally used to trigger recurring Luigi workflows. Schedules are
declared in the regular Luigi configuration, one section per entry::

    [periodic my_reports]
    module = all_reports
    task = RangeDaily
    args = --of AllReports --start 2025-01-01
    schedule = 0 2 * * *
    overlap_policy = skip
    jitter_seconds = 0

``schedule`` takes a standard 5-field cron expression and requires the
optional ``croniter`` dependency (``pip install luigi[periodic]``).
Alternatively ``every = <seconds>`` fires at a fixed interval with no
extra dependency. Exactly one of the two must be given.

Each fire launches the entry as a normal ``luigi`` command in a
subprocess, so runs behave exactly as if triggered from cron: they show
up in the central scheduler UI and use the ordinary client
configuration. Missed-run catch-up is deliberately left to the
:mod:`luigi.tools.range` wrappers - schedule ``RangeDaily --of YourTask``
rather than ``YourTask`` itself and gaps self-heal on the next fire.

``overlap_policy`` controls what happens when an entry comes due while
its previous run is still alive: ``skip`` (default) drops the fire,
``queue`` starts one run as soon as the previous one finishes (multiple
missed fires collapse into one). ``jitter_seconds`` delays each fire by
a uniformly random amount, to spread load when many entries share a
schedule.

The daemon reloads its configuration on SIGHUP and stops launching new
runs on SIGTERM/SIGINT, waiting for running children before exiting.

Unless disabled, the daemon also pushes its schedule state (entries,
next fire times, last results) to the central scheduler, where it is
shown on the "Periodic" tab of the luigid visualiser. This uses the
same scheduler the launched tasks talk to (``[core]``
``default-scheduler-url`` / ``default-scheduler-host`` / ``-port``) and
can be tuned in a daemon-level ``[periodic]`` section::

    [periodic]
    push_status = true
    push_interval = 30
"""

import argparse
import configparser
import logging
import os
import random
import shlex
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta

from luigi import configuration

try:
    from croniter import croniter  # type: ignore[import-untyped]
except ImportError:
    croniter = None

logger = logging.getLogger("luigi-interface")

SECTION_PREFIX = "periodic "
DAEMON_SECTION = "periodic"

OVERLAP_SKIP = "skip"
OVERLAP_QUEUE = "queue"

_ENTRY_OPTIONS = frozenset(["module", "task", "args", "schedule", "every", "overlap_policy", "jitter_seconds", "enabled"])


class PeriodicConfigError(Exception):
    pass


class IntervalSchedule:
    def __init__(self, every_seconds):
        if every_seconds <= 0:
            raise PeriodicConfigError("'every' must be a positive number of seconds")
        self.every_seconds = every_seconds

    def next_after(self, dt):
        return dt + timedelta(seconds=self.every_seconds)

    def __str__(self):
        return "every {} seconds".format(self.every_seconds)


class CronSchedule:
    def __init__(self, expression):
        if croniter is None:
            raise PeriodicConfigError(
                "cron schedules require the croniter package; install it with 'pip install luigi[periodic]' or use 'every = <seconds>' instead"
            )
        if not croniter.is_valid(expression):
            raise PeriodicConfigError("invalid cron expression: {!r}".format(expression))
        self.expression = expression

    def next_after(self, dt):
        return croniter(self.expression, dt).get_next(datetime)

    def __str__(self):
        return "cron {!r}".format(self.expression)


class PeriodicEntry:
    def __init__(self, name, task, module=None, args="", schedule=None, overlap_policy=OVERLAP_SKIP, jitter_seconds=0):
        self.name = name
        self.task = task
        self.module = module
        self.args = args
        self.schedule = schedule
        self.overlap_policy = overlap_policy
        self.jitter_seconds = jitter_seconds

    @classmethod
    def from_options(cls, name, options):
        unknown = set(options) - _ENTRY_OPTIONS
        if unknown:
            raise PeriodicConfigError("unknown option(s) {} in section [{}{}]".format(sorted(unknown), SECTION_PREFIX, name))

        task = options.get("task")
        if not task:
            raise PeriodicConfigError("section [{}{}] is missing required option 'task'".format(SECTION_PREFIX, name))

        cron_expression = options.get("schedule")
        every = options.get("every")
        if bool(cron_expression) == bool(every):
            raise PeriodicConfigError("section [{}{}] must set exactly one of 'schedule' (cron) or 'every' (seconds)".format(SECTION_PREFIX, name))
        if cron_expression:
            schedule = CronSchedule(str(cron_expression))
        else:
            try:
                schedule = IntervalSchedule(float(every))
            except ValueError:
                raise PeriodicConfigError("section [{}{}]: 'every' must be a number of seconds, got {!r}".format(SECTION_PREFIX, name, every))

        overlap_policy = str(options.get("overlap_policy", OVERLAP_SKIP)).lower()
        if overlap_policy not in (OVERLAP_SKIP, OVERLAP_QUEUE):
            raise PeriodicConfigError(
                "section [{}{}]: overlap_policy must be '{}' or '{}', got {!r}".format(SECTION_PREFIX, name, OVERLAP_SKIP, OVERLAP_QUEUE, overlap_policy)
            )

        try:
            jitter_seconds = float(options.get("jitter_seconds", 0))
        except ValueError:
            raise PeriodicConfigError("section [{}{}]: jitter_seconds must be a number, got {!r}".format(SECTION_PREFIX, name, options.get("jitter_seconds")))
        if jitter_seconds < 0:
            raise PeriodicConfigError("section [{}{}]: jitter_seconds must be >= 0, got {!r}".format(SECTION_PREFIX, name, options.get("jitter_seconds")))

        return cls(
            name=name,
            task=str(task),
            module=options.get("module"),
            args=str(options.get("args", "")),
            schedule=schedule,
            overlap_policy=overlap_policy,
            jitter_seconds=jitter_seconds,
        )

    def command(self):
        cmd = [sys.executable, "-m", "luigi"]
        if self.module:
            cmd += ["--module", str(self.module)]
        cmd.append(self.task)
        cmd += shlex.split(self.args)
        return cmd


def _config_section_items(config):
    """
    Yield ``(section_name, options_dict)`` for every ``[periodic ...]``
    section, supporting both the cfg and toml parser flavors.
    """
    data = getattr(config, "data", None)
    sections = list(data) if data is not None else config.sections()
    for section in sections:
        if not section.startswith(SECTION_PREFIX):
            continue
        if data is not None:
            yield section, dict(data[section])
        else:
            try:
                yield section, dict(config.items(section))
            except (configparser.Error, ValueError) as e:
                raise PeriodicConfigError("could not read section [{}]: {} (escape a literal '%' as '%%' in cfg files)".format(section, e))


def load_entries(config=None):
    """
    Parse all enabled ``[periodic ...]`` sections into :class:`PeriodicEntry` objects.
    """
    if config is None:
        config = configuration.get_config()
    entries = []
    for section, options in _config_section_items(config):
        name = section[len(SECTION_PREFIX) :].strip()
        if not name:
            raise PeriodicConfigError("periodic section is missing a name: [{}]".format(section))
        enabled = str(options.get("enabled", "true")).lower()
        if enabled in ("false", "0", "no"):
            logger.info("Periodic entry %r is disabled, skipping", name)
            continue
        entries.append(PeriodicEntry.from_options(name, options))
    return entries


class PeriodicDaemon:
    """
    Fires :class:`PeriodicEntry` commands as subprocesses when they come due.

    The clock, sleep and process launching are injectable for testing;
    :meth:`run_once` performs a single reap-and-fire pass so tests can
    drive the loop with a fake clock.
    """

    poll_interval = 1.0

    def __init__(
        self, entries, now_fn=datetime.now, sleep_fn=None, launch_fn=None, jitter_fn=random.uniform, status_pusher=None, push_interval=30, daemon_id=None
    ):
        self._now = now_fn
        self._sleep = sleep_fn if sleep_fn is not None else time.sleep
        self._launch = launch_fn if launch_fn is not None else self._launch_subprocess
        self._jitter = jitter_fn
        self._status_pusher = status_pusher
        self._push_interval = push_interval
        self._daemon_id = daemon_id or "{}:{}".format(socket.gethostname(), os.getpid())
        self._last_push = None
        self._status_dirty = True
        self._stop_requested = False
        self._reload_requested = False
        self._running = {}  # entry name -> process
        self._pending = set()  # entry names queued behind a still-running process
        self._last_result = {}  # entry name -> dict with returncode and finish time
        self._next_fire = {}
        self._entries = {}
        self._set_entries(entries)

    def _set_entries(self, entries):
        self._entries = {entry.name: entry for entry in entries}
        now = self._now()
        self._next_fire = {name: self._schedule_next(entry, now) for name, entry in self._entries.items()}
        self._pending &= set(self._entries)
        self._status_dirty = True
        for name, entry in self._entries.items():
            logger.info("Periodic entry %r (%s) first fires at %s", name, entry.schedule, self._next_fire[name])

    def _schedule_next(self, entry, after):
        next_fire = entry.schedule.next_after(after)
        if entry.jitter_seconds:
            next_fire += timedelta(seconds=self._jitter(0, entry.jitter_seconds))
        return next_fire

    @staticmethod
    def _launch_subprocess(entry):
        return subprocess.Popen(entry.command())

    def request_stop(self, *args):
        self._stop_requested = True

    def request_reload(self, *args):
        self._reload_requested = True

    def _reap(self):
        for name, process in list(self._running.items()):
            if process.poll() is None:
                continue
            del self._running[name]
            self._last_result[name] = {
                "returncode": process.returncode,
                "finished": self._now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            self._status_dirty = True
            if process.returncode == 0:
                logger.info("Periodic entry %r finished successfully", name)
            else:
                logger.error("Periodic entry %r exited with return code %s", name, process.returncode)
            if name in self._pending and name in self._entries and not self._stop_requested:
                self._pending.discard(name)
                self._fire(self._entries[name])

    def _fire(self, entry):
        logger.info("Launching periodic entry %r: %s", entry.name, " ".join(entry.command()))
        try:
            self._running[entry.name] = self._launch(entry)
        except OSError as e:
            logger.error("Could not launch periodic entry %r: %s", entry.name, e)
        self._status_dirty = True

    def status_snapshot(self):
        """
        JSON-serializable state of every entry, as pushed to the central scheduler.
        """
        snapshot = []
        for name in sorted(self._entries):
            entry = self._entries[name]
            last = self._last_result.get(name, {})
            snapshot.append(
                {
                    "name": name,
                    "schedule": str(entry.schedule),
                    "command": " ".join(entry.command()),
                    "overlap_policy": entry.overlap_policy,
                    "next_fire": self._next_fire[name].strftime("%Y-%m-%d %H:%M:%S"),
                    "running": name in self._running,
                    "queued": name in self._pending,
                    "last_returncode": last.get("returncode"),
                    "last_finished": last.get("finished"),
                }
            )
        return snapshot

    def _push_status(self, stopping=False):
        if self._status_pusher is None:
            return
        try:
            self._status_pusher(self._daemon_id, self.status_snapshot(), stopping)
        except Exception as e:
            logger.warning("Could not push periodic status to the scheduler: %s", e)
        self._status_dirty = False
        self._last_push = self._now()

    def _maybe_push_status(self):
        if self._status_pusher is None:
            return
        heartbeat_due = self._last_push is None or (self._now() - self._last_push).total_seconds() >= self._push_interval
        if self._status_dirty or heartbeat_due:
            self._push_status()

    def run_once(self):
        """
        Reap finished runs and fire all due entries; returns seconds until the next fire.
        """
        self._reap()
        now = self._now()
        for name, entry in self._entries.items():
            if self._next_fire[name] > now:
                continue
            if name in self._running:
                if entry.overlap_policy == OVERLAP_QUEUE:
                    logger.info("Periodic entry %r is still running; queueing one run", name)
                    self._pending.add(name)
                else:
                    logger.warning("Periodic entry %r is still running; skipping this fire", name)
            else:
                self._fire(entry)
            self._next_fire[name] = self._schedule_next(entry, now)
        if not self._next_fire:
            return self.poll_interval
        seconds_to_next = (min(self._next_fire.values()) - self._now()).total_seconds()
        return max(0.0, seconds_to_next)

    def run(self):
        logger.info("luigi-periodic starting with %d entries", len(self._entries))
        while not self._stop_requested:
            if self._reload_requested:
                self._reload_requested = False
                logger.info("Reloading periodic configuration")
                try:
                    configuration.get_config().reload()
                    self._set_entries(load_entries())
                except Exception as e:
                    logger.error("Configuration reload failed, keeping previous entries: %s", e)
            seconds_to_next = self.run_once()
            self._maybe_push_status()
            sleep_for = seconds_to_next
            if self._running:
                sleep_for = min(sleep_for, self.poll_interval)
            if self._status_pusher is not None:
                sleep_for = min(sleep_for, self._push_interval)
            self._sleep(max(sleep_for, 0.05))
        self._shutdown()

    def _shutdown(self):
        if self._running:
            logger.info("Stop requested; waiting for %d running entries", len(self._running))
        for name, process in self._running.items():
            process.wait()
            logger.info("Periodic entry %r finished with return code %s", name, process.returncode)
        self._reap()
        self._push_status(stopping=True)
        logger.info("luigi-periodic stopped")


def _scheduler_url(config):
    url = config.get("core", "default_scheduler_url", "")
    if url:
        return url
    return "http://{}:{}/".format(config.get("core", "default_scheduler_host", "localhost"), config.get("core", "default_scheduler_port", 8082))


def build_status_pusher(config=None):
    """
    Return a callable pushing daemon state to the central scheduler, or None if disabled.
    """
    if config is None:
        config = configuration.get_config()
    push_status = str(config.get(DAEMON_SECTION, "push_status", "true")).lower()
    if push_status in ("false", "0", "no"):
        return None
    from luigi import rpc

    # A single quick attempt per push with a short timeout; a slow retry
    # loop here would stall firing while the scheduler is unreachable.
    remote = rpc.RemoteScheduler(_scheduler_url(config), connect_timeout=2)
    remote._rpc_retry_attempts = 1
    remote._rpc_log_retries = False

    def push(daemon_id, entries, stopping):
        remote.update_periodic_status(daemon_id=daemon_id, entries=entries, stopping=stopping)

    return push


def main(argv=None):
    parser = argparse.ArgumentParser(description="luigi-periodic launches Luigi workflows on schedules declared in [periodic ...] config sections")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="daemon logging level")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    config = configuration.get_config()
    try:
        entries = load_entries(config)
    except PeriodicConfigError as e:
        parser.error(str(e))
    if not entries:
        parser.error("no enabled [periodic ...] sections found in the Luigi configuration")

    try:
        push_interval = float(config.get(DAEMON_SECTION, "push_interval", 30))
    except ValueError:
        parser.error("[{}]: push_interval must be a number of seconds".format(DAEMON_SECTION))

    daemon = PeriodicDaemon(entries, status_pusher=build_status_pusher(config), push_interval=push_interval)
    signal.signal(signal.SIGTERM, daemon.request_stop)
    signal.signal(signal.SIGINT, daemon.request_stop)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, daemon.request_reload)
    daemon.run()


if __name__ == "__main__":
    main()
