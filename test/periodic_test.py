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

import sys
import unittest
from datetime import datetime, timedelta

from helpers import LuigiTestCase, with_config

from luigi.tools import periodic
from luigi.tools.periodic import (
    IntervalSchedule, PeriodicConfigError, PeriodicDaemon, PeriodicEntry, load_entries)


class FakeClock:
    def __init__(self, start):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class FakeProcess:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def finish(self, returncode=0):
        self.returncode = returncode


class FakeLauncher:
    def __init__(self):
        self.launched = []

    def __call__(self, entry):
        process = FakeProcess()
        self.launched.append((entry.name, process))
        return process


def make_entry(name='job', every=60, **kwargs):
    kwargs.setdefault('schedule', IntervalSchedule(every))
    return PeriodicEntry(name=name, task='SomeTask', **kwargs)


def make_daemon(entries, clock=None):
    clock = clock or FakeClock(datetime(2026, 1, 1, 0, 0, 0))
    launcher = FakeLauncher()
    daemon = PeriodicDaemon(entries, now_fn=clock, sleep_fn=clock.advance, launch_fn=launcher)
    return daemon, clock, launcher


class ConfigParsingTest(LuigiTestCase):

    @with_config({'periodic my_reports': {
            'module': 'all_reports', 'task': 'RangeDaily',
            'args': '--of AllReports --start 2025-01-01', 'every': '3600'}})
    def test_parses_entry(self):
        entries = load_entries()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.name, 'my_reports')
        self.assertEqual(entry.overlap_policy, periodic.OVERLAP_SKIP)
        self.assertEqual(
            entry.command(),
            [sys.executable, '-m', 'luigi', '--module', 'all_reports', 'RangeDaily',
             '--of', 'AllReports', '--start', '2025-01-01'])

    @with_config({'periodic my_reports': {'task': 'SomeTask', 'every': '60', 'enabled': 'false'}})
    def test_disabled_entry_is_skipped(self):
        self.assertEqual(load_entries(), [])

    @with_config({'periodic my_reports': {'task': 'SomeTask', 'every': '60', 'taks': 'typo'}})
    def test_unknown_option_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'periodic my_reports': {'every': '60'}})
    def test_missing_task_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'periodic my_reports': {'task': 'SomeTask'}})
    def test_missing_schedule_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'periodic my_reports': {'task': 'SomeTask', 'every': '60', 'schedule': '* * * * *'}})
    def test_both_schedules_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'periodic my_reports': {'task': 'SomeTask', 'every': 'often'}})
    def test_non_numeric_every_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'periodic my_reports': {'task': 'SomeTask', 'every': '60', 'overlap_policy': 'kill'}})
    def test_bad_overlap_policy_raises(self):
        with self.assertRaises(PeriodicConfigError):
            load_entries()

    @with_config({'other_section': {'foo': 'bar'}})
    def test_unrelated_sections_ignored(self):
        self.assertEqual([e for e in load_entries() if e.name == 'foo'], [])


class ScheduleTest(unittest.TestCase):

    def test_interval_next_after(self):
        schedule = IntervalSchedule(90)
        self.assertEqual(
            schedule.next_after(datetime(2026, 1, 1, 0, 0, 0)),
            datetime(2026, 1, 1, 0, 1, 30))

    def test_interval_rejects_nonpositive(self):
        with self.assertRaises(PeriodicConfigError):
            IntervalSchedule(0)

    @unittest.skipIf(periodic.croniter is None, 'croniter not installed')
    def test_cron_next_after(self):
        schedule = periodic.CronSchedule('0 2 * * *')
        self.assertEqual(
            schedule.next_after(datetime(2026, 1, 1, 3, 0, 0)),
            datetime(2026, 1, 2, 2, 0, 0))

    @unittest.skipIf(periodic.croniter is None, 'croniter not installed')
    def test_cron_rejects_invalid_expression(self):
        with self.assertRaises(PeriodicConfigError):
            periodic.CronSchedule('not a cron expression')

    def test_cron_without_croniter_raises_helpfully(self):
        original = periodic.croniter
        periodic.croniter = None
        try:
            with self.assertRaises(PeriodicConfigError) as cm:
                periodic.CronSchedule('0 2 * * *')
            self.assertIn('croniter', str(cm.exception))
        finally:
            periodic.croniter = original


class DaemonTest(unittest.TestCase):

    def test_fires_when_due(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60)])
        daemon.run_once()
        self.assertEqual(launcher.launched, [])
        clock.advance(60)
        daemon.run_once()
        self.assertEqual([name for name, _ in launcher.launched], ['job'])

    def test_returns_seconds_until_next_fire(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60)])
        self.assertAlmostEqual(daemon.run_once(), 60.0)
        clock.advance(45)
        self.assertAlmostEqual(daemon.run_once(), 15.0)

    def test_repeated_fires(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60)])
        for _ in range(3):
            clock.advance(60)
            daemon.run_once()
            launcher.launched[-1][1].finish()
        self.assertEqual(len(launcher.launched), 3)

    def test_overlap_skip_drops_fire(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60, overlap_policy=periodic.OVERLAP_SKIP)])
        clock.advance(60)
        daemon.run_once()
        clock.advance(60)
        daemon.run_once()  # previous run still alive
        self.assertEqual(len(launcher.launched), 1)
        launcher.launched[0][1].finish()
        clock.advance(60)
        daemon.run_once()
        self.assertEqual(len(launcher.launched), 2)

    def test_overlap_queue_launches_after_finish(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60, overlap_policy=periodic.OVERLAP_QUEUE)])
        clock.advance(60)
        daemon.run_once()
        clock.advance(60)
        daemon.run_once()  # queued behind the running process
        clock.advance(30)
        daemon.run_once()  # multiple due fires collapse into one pending run
        self.assertEqual(len(launcher.launched), 1)
        launcher.launched[0][1].finish()
        daemon.run_once()
        self.assertEqual(len(launcher.launched), 2)

    def test_failure_is_logged_and_rescheduled(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60)])
        clock.advance(60)
        daemon.run_once()
        launcher.launched[0][1].finish(returncode=2)
        with self.assertLogs('luigi-interface', level='ERROR') as logs:
            daemon.run_once()
        self.assertTrue(any('return code 2' in line for line in logs.output))
        clock.advance(60)
        daemon.run_once()
        self.assertEqual(len(launcher.launched), 2)

    def test_jitter_delays_fire(self):
        entry = make_entry(every=60, jitter_seconds=30)
        clock = FakeClock(datetime(2026, 1, 1, 0, 0, 0))
        launcher = FakeLauncher()
        daemon = PeriodicDaemon([entry], now_fn=clock, sleep_fn=clock.advance,
                                launch_fn=launcher, jitter_fn=lambda a, b: b)
        clock.advance(60)
        daemon.run_once()
        self.assertEqual(launcher.launched, [])
        clock.advance(30)
        daemon.run_once()
        self.assertEqual(len(launcher.launched), 1)

    def test_stop_waits_for_running_children(self):
        daemon, clock, launcher = make_daemon([make_entry(every=60)])
        clock.advance(60)
        daemon.run_once()
        daemon.request_stop()
        daemon.run()
        self.assertEqual(launcher.launched[0][1].returncode, 0)


class FakePusher:
    def __init__(self):
        self.pushes = []

    def __call__(self, daemon_id, entries, stopping):
        self.pushes.append({'daemon_id': daemon_id, 'entries': entries, 'stopping': stopping})


def make_pushing_daemon(entries, push_interval=30):
    clock = FakeClock(datetime(2026, 1, 1, 0, 0, 0))
    launcher = FakeLauncher()
    pusher = FakePusher()
    daemon = PeriodicDaemon(entries, now_fn=clock, sleep_fn=clock.advance, launch_fn=launcher,
                            status_pusher=pusher, push_interval=push_interval, daemon_id='testhost:1')
    return daemon, clock, launcher, pusher


class StatusPushTest(unittest.TestCase):

    def test_snapshot_reflects_entry_state(self):
        daemon, clock, launcher, pusher = make_pushing_daemon([make_entry(every=60)])
        clock.advance(60)
        daemon.run_once()
        snapshot = daemon.status_snapshot()
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]['name'], 'job')
        self.assertTrue(snapshot[0]['running'])
        self.assertIsNone(snapshot[0]['last_returncode'])
        launcher.launched[0][1].finish(returncode=3)
        daemon.run_once()
        snapshot = daemon.status_snapshot()
        self.assertFalse(snapshot[0]['running'])
        self.assertEqual(snapshot[0]['last_returncode'], 3)
        self.assertEqual(snapshot[0]['next_fire'], '2026-01-01 00:02:00')

    def test_pushes_on_state_change_and_heartbeat(self):
        daemon, clock, launcher, pusher = make_pushing_daemon([make_entry(every=60)], push_interval=30)
        daemon._maybe_push_status()  # initial state is dirty
        self.assertEqual(len(pusher.pushes), 1)
        self.assertEqual(pusher.pushes[0]['daemon_id'], 'testhost:1')
        daemon._maybe_push_status()  # nothing changed, interval not elapsed
        self.assertEqual(len(pusher.pushes), 1)
        clock.advance(30)
        daemon._maybe_push_status()  # heartbeat
        self.assertEqual(len(pusher.pushes), 2)
        clock.advance(30)
        daemon.run_once()  # fires -> dirty
        daemon._maybe_push_status()
        self.assertEqual(len(pusher.pushes), 3)
        self.assertTrue(pusher.pushes[-1]['entries'][0]['running'])

    def test_final_push_marks_stopping(self):
        daemon, clock, launcher, pusher = make_pushing_daemon([make_entry(every=60)])
        daemon.request_stop()
        daemon.run()
        self.assertTrue(pusher.pushes[-1]['stopping'])

    def test_pusher_failure_is_not_fatal(self):
        def broken_pusher(daemon_id, entries, stopping):
            raise IOError('scheduler down')
        clock = FakeClock(datetime(2026, 1, 1, 0, 0, 0))
        daemon = PeriodicDaemon([make_entry(every=60)], now_fn=clock, sleep_fn=clock.advance,
                                launch_fn=FakeLauncher(), status_pusher=broken_pusher)
        with self.assertLogs('luigi-interface', level='WARNING') as logs:
            daemon._maybe_push_status()
        self.assertTrue(any('Could not push periodic status' in line for line in logs.output))


class SchedulerStatusTest(unittest.TestCase):

    def test_update_and_read_periodic_status(self):
        import luigi.scheduler
        scheduler = luigi.scheduler.Scheduler()
        entries = [{'name': 'job', 'next_fire': '2026-01-01 00:01:00', 'running': False}]
        scheduler.update_periodic_status(daemon_id='testhost:1', entries=entries)
        status = scheduler.periodic_status()
        self.assertEqual(len(status), 1)
        self.assertEqual(status[0]['daemon_id'], 'testhost:1')
        self.assertEqual(status[0]['entries'], entries)
        self.assertFalse(status[0]['stopping'])
        self.assertGreaterEqual(status[0]['seconds_since_update'], 0)

    def test_repeated_update_replaces_state(self):
        import luigi.scheduler
        scheduler = luigi.scheduler.Scheduler()
        scheduler.update_periodic_status(daemon_id='testhost:1', entries=[{'name': 'a'}])
        scheduler.update_periodic_status(daemon_id='testhost:1', entries=[{'name': 'b'}], stopping=True)
        status = scheduler.periodic_status()
        self.assertEqual(len(status), 1)
        self.assertEqual(status[0]['entries'], [{'name': 'b'}])
        self.assertTrue(status[0]['stopping'])

    def test_remote_scheduler_exposes_rpc_methods(self):
        from luigi.rpc import RemoteScheduler
        self.assertTrue(hasattr(RemoteScheduler, 'update_periodic_status'))
        self.assertTrue(hasattr(RemoteScheduler, 'periodic_status'))


class BuildStatusPusherTest(LuigiTestCase):

    @with_config({'periodic': {'push_status': 'false'}})
    def test_disabled_by_config(self):
        self.assertIsNone(periodic.build_status_pusher())

    @with_config({'core': {'default_scheduler_host': 'sched.example.com', 'default_scheduler_port': '9999'}})
    def test_url_from_host_and_port(self):
        import luigi.configuration
        self.assertEqual(
            periodic._scheduler_url(luigi.configuration.get_config()),
            'http://sched.example.com:9999/')

    @with_config({'core': {'default_scheduler_url': 'http://sched.example.com:8082/prefix/'}})
    def test_url_override_wins(self):
        import luigi.configuration
        self.assertEqual(
            periodic._scheduler_url(luigi.configuration.get_config()),
            'http://sched.example.com:8082/prefix/')


if __name__ == '__main__':
    unittest.main()
