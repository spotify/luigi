# coding=utf-8

import mock
import unittest

import six

import luigi.batch_notifier


BATCH_NOTIFIER_DEFAULTS = {
    'error_lines': 0,
    'error_messages': 0,
    'group_by_error_messages': False,
}


class BatchNotifier(luigi.batch_notifier.BatchNotifier):
    """BatchNotifier class with defaults that produce smaller output for testing"""
    def __init__(self, **kwargs):
        full_args = BATCH_NOTIFIER_DEFAULTS.copy()
        full_args.update(kwargs)
        super(BatchNotifier, self).__init__(**full_args)


class BatchNotifierTest(unittest.TestCase):
    def setUp(self):
        self.time_mock = mock.patch('luigi.batch_notifier.time.time')
        self.time = self.time_mock.start()
        self.time.return_value = 0.0

        self.send_email_mock = mock.patch('luigi.batch_notifier.send_email')
        self.send_email = self.send_email_mock.start()

        self.email_mock = mock.patch('luigi.batch_notifier.email')
        self.email = self.email_mock.start()
        self.email().sender = 'sender@test.com'
        self.email().receiver = 'r@test.com'

    def tearDown(self):
        self.time_mock.stop()
        self.send_email_mock.stop()
        self.email_mock.stop()

    def incr_time(self, minutes):
        self.time.return_value += minutes * 60

    def check_email_send(self, subject, message, receiver='r@test.com', sender='sender@test.com'):
        self.send_email.assert_called_once_with(subject, message, sender, (receiver,))

    def test_send_single_failure(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            '- Task(a=5) (1 failure)'
        )

    def test_do_not_send_single_failure_without_receiver(self):
        self.email().receiver = None
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.send_email()
        self.send_email.assert_not_called()

    def test_send_single_failure_to_owner_only(self):
        self.email().receiver = None
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', ['owner@test.com'])
        bn.send_email()
        self.check_email_send(
            'Luigi: Your tasks have 1 failure in the last 60 minutes',
            '- Task(a=5) (1 failure)',
            receiver='owner@test.com',
        )

    def test_send_single_disable(self):
        bn = BatchNotifier(batch_mode='all')
        for _ in range(10):
            bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_disable('Task(a=5)', 'Task', {'a': 5}, [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 10 failures, 1 disable in the last 60 minutes',
            '- Task(a=5) (10 failures, 1 disable)'
        )

    def test_send_multiple_disables(self):
        bn = BatchNotifier(batch_mode='family')
        for _ in range(10):
            bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
            bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error', [])
        bn.add_disable('Task(a=5)', 'Task', {'a': 5}, [])
        bn.add_disable('Task(a=6)', 'Task', {'a': 6}, [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 20 failures, 2 disables in the last 60 minutes',
            '- Task (20 failures, 2 disables)'
        )

    def test_send_single_scheduling_fail(self):
        bn = BatchNotifier(batch_mode='family')
        bn.add_scheduling_fail('Task()', 'Task', {}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 scheduling failure in the last 60 minutes',
            '- Task (1 scheduling failure)',
        )

    def test_multiple_failures_of_same_job(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 3 failures in the last 60 minutes',
            '- Task(a=5) (3 failures)'
        )

    def test_multiple_failures_of_multiple_jobs(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 3 failures in the last 60 minutes',
            '- Task(a=6) (2 failures)\n'
            '- Task(a=5) (1 failure)'
        )

    def test_group_on_family(self):
        bn = BatchNotifier(batch_mode='family')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('OtherTask(a=6)', 'OtherTask', {'a': 6}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 4 failures in the last 60 minutes',
            '- Task (3 failures)\n'
            '- OtherTask (1 failure)'
        )

    def test_group_on_unbatched_params(self):
        bn = BatchNotifier(batch_mode='unbatched_params')
        bn.add_failure('Task(a=5, b=1)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=5, b=2)', 'Task', {'a': 5}, 'error', [])
        bn.add_failure('Task(a=6, b=1)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('Task(a=6, b=2)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('Task(a=6, b=3)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('Task(a=6, b=4)', 'Task', {'a': 6}, 'error', [])
        bn.add_failure('OtherTask(a=5, b=1)', 'OtherTask', {'a': 5}, 'error', [])
        bn.add_failure('OtherTask(a=6, b=1)', 'OtherTask', {'a': 6}, 'error', [])
        bn.add_failure('OtherTask(a=6, b=2)', 'OtherTask', {'a': 6}, 'error', [])
        bn.add_failure('OtherTask(a=6, b=3)', 'OtherTask', {'a': 6}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 10 failures in the last 60 minutes',
            '- Task(a=6) (4 failures)\n'
            '- OtherTask(a=6) (3 failures)\n'
            '- Task(a=5) (2 failures)\n'
            '- OtherTask(a=5) (1 failure)'
        )

    def test_include_one_expl_includes_latest(self):
        bn = BatchNotifier(batch_mode='family', error_messages=1)
        bn.add_failure('Task(a=1)', 'Task', {'a': 1}, 'error 1', [])
        bn.add_failure('Task(a=2)', 'Task', {'a': 2}, 'error 2', [])
        bn.add_failure('TaskB(a=1)', 'TaskB', {'a': 1}, 'error', [])

        bn.send_email()
        self.check_email_send(
            'Luigi: 3 failures in the last 60 minutes',
            '- Task (2 failures)\n'
            '\n'
            '      error 2\n'
            '\n'
            '- TaskB (1 failure)\n'
            '\n'
            '      error'
        )

    def test_include_two_expls(self):
        bn = BatchNotifier(batch_mode='family', error_messages=2)
        bn.add_failure('Task(a=1)', 'Task', {'a': 1}, 'error 1', [])
        bn.add_failure('Task(a=2)', 'Task', {'a': 2}, 'error 2', [])
        bn.add_failure('TaskB(a=1)', 'TaskB', {'a': 1}, 'error', [])

        bn.send_email()
        self.check_email_send(
            'Luigi: 3 failures in the last 60 minutes',
            '- Task (2 failures)\n'
            '\n'
            '      error 1\n'
            '\n'
            '      error 2\n'
            '\n'
            '- TaskB (1 failure)\n'
            '\n'
            '      error'
        )

    def test_limit_expl_length(self):
        bn = BatchNotifier(batch_mode='family', error_messages=1, error_lines=2)
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'line 1\nline 2\nline 3\nline 4\n', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            '- Task (1 failure)\n'
            '\n'
            '      line 3\n'
            '      line 4'
        )

    def test_expl_varies_by_owner(self):
        bn = BatchNotifier(batch_mode='family', error_messages=1)
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'msg1', owners=['a@test.com'])
        bn.add_failure('Task(a=2)', 'Task', {'a': '2'}, 'msg2', owners=['b@test.com'])
        bn.send_email()
        send_calls = [
            mock.call(
                'Luigi: Your tasks have 1 failure in the last 60 minutes',
                '- Task (1 failure)\n'
                '\n'
                '      msg1',
                'sender@test.com',
                ('a@test.com',),
            ),
            mock.call(
                'Luigi: Your tasks have 1 failure in the last 60 minutes',
                '- Task (1 failure)\n'
                '\n'
                '      msg2',
                'sender@test.com',
                ('b@test.com',),
            ),
            mock.call(
                'Luigi: 2 failures in the last 60 minutes',
                '- Task (2 failures)\n'
                '\n'
                '      msg2',
                'sender@test.com',
                ('r@test.com',),
            ),
        ]
        self.send_email.assert_has_calls(send_calls, any_order=True)

    def test_include_two_expls_html_format(self):
        self.email().format = 'html'
        bn = BatchNotifier(batch_mode='family', error_messages=2)
        bn.add_failure('Task(a=1)', 'Task', {'a': 1}, 'error 1', [])
        bn.add_failure('Task(a=2)', 'Task', {'a': 2}, 'error 2', [])
        bn.add_failure('TaskB(a=1)', 'TaskB', {'a': 1}, 'error', [])

        bn.send_email()
        self.check_email_send(
            'Luigi: 3 failures in the last 60 minutes',
            '<ul>\n'
            '<li>Task (2 failures)\n'
            '<pre>error 1</pre>\n'
            '<pre>error 2</pre>\n'
            '<li>TaskB (1 failure)\n'
            '<pre>error</pre>\n'
            '</ul>'
        )

    def test_limit_expl_length_html_format(self):
        self.email().format = 'html'
        bn = BatchNotifier(batch_mode='family', error_messages=1, error_lines=2)
        bn.add_failure('Task(a=1)', 'Task', {'a': 1}, 'line 1\nline 2\nline 3\nline 4\n', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            '<ul>\n'
            '<li>Task (1 failure)\n'
            '<pre>line 3\n'
            'line 4</pre>\n'
            '</ul>'
        )

    def test_send_clears_backlog(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        bn.add_disable('Task(a=5)', 'Task', {'a': 5}, [])
        bn.add_scheduling_fail('Task(a=6)', 'Task', {'a': 6}, 'scheduling error', [])
        bn.send_email()

        self.send_email.reset_mock()
        bn.send_email()
        self.send_email.assert_not_called()

    def test_send_clears_all_old_data(self):
        bn = BatchNotifier(batch_mode='all', error_messages=100)

        for i in range(100):
            bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error {}'.format(i), [])
            bn.add_disable('Task(a=5)', 'Task', {'a': 5}, [])
            bn.add_scheduling_fail('Task(a=6)', 'Task', {'a': 6}, 'scheduling error {}'.format(i), [])
            bn.send_email()
            self.check_email_send(
                'Luigi: 1 failure, 1 disable, 1 scheduling failure in the last 60 minutes',
                '- Task(a=5) (1 failure, 1 disable)\n'
                '\n'
                '      error {}\n'
                '\n'
                '- Task(a=6) (1 scheduling failure)\n'
                '\n'
                '      scheduling error {}'.format(i, i),
            )
            self.send_email.reset_mock()

    def test_auto_send_on_update_after_time_period(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])

        for i in range(60):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            '- Task(a=5) (1 failure)'
        )

    def test_auto_send_on_update_after_time_period_with_disable_only(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_disable('Task(a=5)', 'Task', {'a': 5}, [])

        for i in range(60):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.check_email_send(
            'Luigi: 1 disable in the last 60 minutes',
            '- Task(a=5) (1 disable)'
        )

    def test_no_auto_send_until_end_of_interval_with_error(self):
        bn = BatchNotifier(batch_mode='all')

        for i in range(90):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error', [])
        for i in range(30):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            '- Task(a=5) (1 failure)'
        )

    def test_send_batch_failure_emails_to_owners(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'error', ['a@test.com', 'b@test.com'])
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'error', ['b@test.com'])
        bn.add_failure('Task(a=2)', 'Task', {'a': '2'}, 'error', ['a@test.com'])
        bn.send_email()

        send_calls = [
            mock.call(
                'Luigi: 3 failures in the last 60 minutes',
                '- Task(a=1) (2 failures)\n'
                '- Task(a=2) (1 failure)',
                'sender@test.com',
                ('r@test.com',),
            ),
            mock.call(
                'Luigi: Your tasks have 2 failures in the last 60 minutes',
                '- Task(a=1) (1 failure)\n'
                '- Task(a=2) (1 failure)',
                'sender@test.com',
                ('a@test.com',),
            ),
            mock.call(
                'Luigi: Your tasks have 2 failures in the last 60 minutes',
                '- Task(a=1) (2 failures)',
                'sender@test.com',
                ('b@test.com',),
            ),
        ]
        self.send_email.assert_has_calls(send_calls, any_order=True)

    def test_send_batch_disable_email_to_owners(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_disable('Task(a=1)', 'Task', {'a': '1'}, ['a@test.com'])
        bn.send_email()

        send_calls = [
            mock.call(
                'Luigi: 1 disable in the last 60 minutes',
                '- Task(a=1) (1 disable)',
                'sender@test.com',
                ('r@test.com',),
            ),
            mock.call(
                'Luigi: Your tasks have 1 disable in the last 60 minutes',
                '- Task(a=1) (1 disable)',
                'sender@test.com',
                ('a@test.com',),
            ),
        ]
        self.send_email.assert_has_calls(send_calls, any_order=True)

    def test_batch_identical_expls(self):
        bn = BatchNotifier(error_messages=1, group_by_error_messages=True)
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'msg1', [])
        bn.add_failure('Task(a=2)', 'Task', {'a': '2'}, 'msg1', [])
        bn.add_failure('Task(a=3)', 'Task', {'a': '3'}, 'msg1', [])
        bn.add_failure('Task(a=4)', 'Task', {'a': '4'}, 'msg2', [])
        bn.add_failure('Task(a=4)', 'Task', {'a': '4'}, 'msg2', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 5 failures in the last 60 minutes',
            '- Task(a=1) (1 failure)\n'
            '  Task(a=2) (1 failure)\n'
            '  Task(a=3) (1 failure)\n'
            '\n'
            '      msg1\n'
            '\n'
            '- Task(a=4) (2 failures)\n'
            '\n'
            '      msg2'
        )

    def test_batch_identical_expls_html(self):
        self.email().format = 'html'
        bn = BatchNotifier(error_messages=1, group_by_error_messages=True)
        bn.add_failure('Task(a=1)', 'Task', {'a': '1'}, 'msg1', [])
        bn.add_failure('Task(a=2)', 'Task', {'a': '2'}, 'msg1', [])
        bn.add_failure('Task(a=3)', 'Task', {'a': '3'}, 'msg1', [])
        bn.add_failure('Task(a=4)', 'Task', {'a': '4'}, 'msg2', [])
        bn.add_failure('Task(a=4)', 'Task', {'a': '4'}, 'msg2', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 5 failures in the last 60 minutes',
            '<ul>\n'
            '<li>Task(a=1) (1 failure)\n'
            '<br>Task(a=2) (1 failure)\n'
            '<br>Task(a=3) (1 failure)\n'
            '<pre>msg1</pre>\n'
            '<li>Task(a=4) (2 failures)\n'
            '<pre>msg2</pre>\n'
            '</ul>'
        )

    def test_unicode_error_message(self):
        bn = BatchNotifier(error_messages=1)
        bn.add_failure('Task()', 'Task', {}, six.u('Érror'), [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            six.u(
                '- Task() (1 failure)\n'
                '\n'
                '      Érror'
            )
        )

    def test_unicode_error_message_html(self):
        self.email().format = 'html'
        bn = BatchNotifier(error_messages=1)
        bn.add_failure('Task()', 'Task', {}, six.u('Érror'), [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            six.u(
                '<ul>\n'
                '<li>Task() (1 failure)\n'
                '<pre>Érror</pre>\n'
                '</ul>'
            ),
        )

    def test_unicode_param_value(self):
        for batch_mode in ('all', 'unbatched_params'):
            self.send_email.reset_mock()
            bn = BatchNotifier(batch_mode=batch_mode)
            bn.add_failure(six.u('Task(a=á)'), 'Task', {'a': six.u('á')}, 'error', [])
            bn.send_email()
            self.check_email_send(
                'Luigi: 1 failure in the last 60 minutes',
                six.u('- Task(a=á) (1 failure)')
            )

    def test_unicode_param_value_html(self):
        self.email().format = 'html'
        for batch_mode in ('all', 'unbatched_params'):
            self.send_email.reset_mock()
            bn = BatchNotifier(batch_mode=batch_mode)
            bn.add_failure(six.u('Task(a=á)'), 'Task', {'a': six.u('á')}, 'error', [])
            bn.send_email()
            self.check_email_send(
                'Luigi: 1 failure in the last 60 minutes',
                six.u(
                    '<ul>\n'
                    '<li>Task(a=á) (1 failure)\n'
                    '</ul>'
                )
            )

    def test_unicode_param_name(self):
        for batch_mode in ('all', 'unbatched_params'):
            self.send_email.reset_mock()
            bn = BatchNotifier(batch_mode=batch_mode)
            bn.add_failure(six.u('Task(á=a)'), 'Task', {six.u('á'): 'a'}, 'error', [])
            bn.send_email()
            self.check_email_send(
                'Luigi: 1 failure in the last 60 minutes',
                six.u('- Task(á=a) (1 failure)')
            )

    def test_unicode_param_name_html(self):
        self.email().format = 'html'
        for batch_mode in ('all', 'unbatched_params'):
            self.send_email.reset_mock()
            bn = BatchNotifier(batch_mode=batch_mode)
            bn.add_failure(six.u('Task(á=a)'), 'Task', {six.u('á'): 'a'}, 'error', [])
            bn.send_email()
            self.check_email_send(
                'Luigi: 1 failure in the last 60 minutes',
                six.u(
                    '<ul>\n'
                    '<li>Task(á=a) (1 failure)\n'
                    '</ul>'
                )
            )

    def test_unicode_class_name(self):
        bn = BatchNotifier()
        bn.add_failure(six.u('Tásk()'), six.u('Tásk'), {}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            six.u('- Tásk() (1 failure)')
        )

    def test_unicode_class_name_html(self):
        self.email().format = 'html'
        bn = BatchNotifier()
        bn.add_failure(six.u('Tásk()'), six.u('Tásk'), {}, 'error', [])
        bn.send_email()
        self.check_email_send(
            'Luigi: 1 failure in the last 60 minutes',
            six.u(
                '<ul>\n'
                '<li>Tásk() (1 failure)\n'
                '</ul>'
            ),
        )
