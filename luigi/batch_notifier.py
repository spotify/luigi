"""
Library for sending batch notifications from the Luigi scheduler. This module
is internal to Luigi and not designed for use in other contexts.
"""

import collections
from datetime import datetime
import time

import luigi
from luigi import six
from luigi.notifications import send_email, email
import luigi.parameter


class batch_email(luigi.Config):
    email_interval = luigi.parameter.IntParameter(
        default=60, description='Number of minutes between e-mail sends (default: 60)')
    batch_mode = luigi.parameter.ChoiceParameter(
        default='unbatched_params', choices=('family', 'all', 'unbatched_params'),
        description='Method used for batching failures in e-mail. If "family" all failures for '
                    'tasks with the same family will be batched. If "unbatched_params", all '
                    'failures for tasks with the same family and non-batched parameters will be '
                    'batched. If "all", tasks will only be batched if they have identical names.')
    error_lines = luigi.parameter.IntParameter(
        default=20, description='Number of lines to show from each error message. 0 means show all')
    error_messages = luigi.parameter.IntParameter(
        default=1, description='Number of error messages to show for each group')
    group_by_error_messages = luigi.parameter.BoolParameter(
        default=True, description='Group items with the same error messages together')


class ExplQueue(collections.OrderedDict):
    def __init__(self, num_items):
        self.num_items = num_items
        super(ExplQueue, self).__init__()

    def enqueue(self, item):
        self.pop(item, None)
        self[item] = datetime.now()
        if len(self) > self.num_items:
            self.popitem(last=False)  # pop first item if past length


def _fail_queue(num_messages):
    return lambda: collections.defaultdict(lambda: ExplQueue(num_messages))


def _plural_format(template, number, plural='s'):
    if number == 0:
        return ''
    return template.format(number, '' if number == 1 else plural)


class BatchNotifier(object):
    def __init__(self, **kwargs):
        self._config = batch_email(**kwargs)
        self._fail_counts = collections.defaultdict(collections.Counter)
        self._disabled_counts = collections.defaultdict(collections.Counter)
        self._scheduling_fail_counts = collections.defaultdict(collections.Counter)
        self._fail_expls = collections.defaultdict(_fail_queue(self._config.error_messages))
        self._update_next_send()

        self._email_format = email().format
        if email().receiver:
            self._default_owner = set(filter(None, email().receiver.split(',')))
        else:
            self._default_owner = set()

    def _update_next_send(self):
        self._next_send = time.time() + 60 * self._config.email_interval

    def _key(self, task_name, family, unbatched_args):
        if self._config.batch_mode == 'all':
            return task_name
        elif self._config.batch_mode == 'family':
            return family
        elif self._config.batch_mode == 'unbatched_params':
            param_str = six.u(', ').join(six.u('{}={}').format(*kv) for kv in six.iteritems(unbatched_args))
            return six.u('{}({})').format(family, param_str)
        else:
            raise ValueError('Unknown batch mode for batch notifier: {}'.format(
                self._config.batch_mode))

    def _format_expl(self, expl):
        lines = expl.rstrip().split('\n')[-self._config.error_lines:]
        if self._email_format == 'html':
            return six.u('<pre>{}</pre>').format('\n'.join(lines))
        else:
            return six.u('\n{}').format(six.u('\n').join(map(six.u('      {}').format, lines)))

    def _expl_body(self, expls):
        lines = [self._format_expl(expl) for expl in expls]
        if lines and self._email_format != 'html':
            lines.append('')
        return '\n'.join(lines)

    def _format_task(self, task_tuple):
        task, failure_count, disable_count, scheduling_count = task_tuple
        counts = [
            _plural_format('{} failure{}', failure_count),
            _plural_format('{} disable{}', disable_count),
            _plural_format('{} scheduling failure{}', scheduling_count),
        ]
        count_str = six.u(', ').join(filter(None, counts))
        return six.u('{} ({})').format(task, count_str)

    def _format_tasks(self, tasks):
        lines = map(self._format_task, sorted(tasks, key=self._expl_key))
        if self._email_format == 'html':
            return six.u('<li>{}').format(six.u('\n<br>').join(lines))
        else:
            return six.u('- {}').format(six.u('\n  ').join(lines))

    def _owners(self, owners):
        return self._default_owner | set(owners)

    def add_failure(self, task_name, family, unbatched_args, expl, owners):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._fail_counts[owner][key] += 1
            self._fail_expls[owner][key].enqueue(expl)

    def add_disable(self, task_name, family, unbatched_args, owners):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._disabled_counts[owner][key] += 1
            self._fail_counts[owner].setdefault(key, 0)

    def add_scheduling_fail(self, task_name, family, unbatched_args, expl, owners):
        key = self._key(task_name, family, unbatched_args)
        for owner in self._owners(owners):
            self._scheduling_fail_counts[owner][key] += 1
            self._fail_expls[owner][key].enqueue(expl)
            self._fail_counts[owner].setdefault(key, 0)

    def _task_expl_groups(self, expls):
        if not self._config.group_by_error_messages:
            return [((task,), msg) for task, msg in six.iteritems(expls)]

        groups = collections.defaultdict(list)
        for task, msg in six.iteritems(expls):
            groups[msg].append(task)
        return [(tasks, msg) for msg, tasks in six.iteritems(groups)]

    def _expls_key(self, expls_tuple):
        expls = expls_tuple[0]
        num_failures = sum(failures + scheduling_fails for (_1, failures, _2, scheduling_fails) in expls)
        num_disables = sum(disables for (_1, _2, disables, _3) in expls)
        min_name = min(expls)[0]
        return -num_failures, -num_disables, min_name

    def _expl_key(self, expl):
        return self._expls_key(((expl,), None))

    def _email_body(self, fail_counts, disable_counts, scheduling_counts, fail_expls):
        expls = {
            (name, fail_count, disable_counts[name], scheduling_counts[name]): self._expl_body(fail_expls[name])
            for name, fail_count in six.iteritems(fail_counts)
        }
        expl_groups = sorted(self._task_expl_groups(expls), key=self._expls_key)
        body_lines = []
        for tasks, msg in expl_groups:
            body_lines.append(self._format_tasks(tasks))
            body_lines.append(msg)
        body = six.u('\n').join(filter(None, body_lines)).rstrip()
        if self._email_format == 'html':
            return six.u('<ul>\n{}\n</ul>').format(body)
        else:
            return body

    def _send_email(self, fail_counts, disable_counts, scheduling_counts, fail_expls, owner):
        num_failures = sum(six.itervalues(fail_counts))
        num_disables = sum(six.itervalues(disable_counts))
        num_scheduling_failures = sum(six.itervalues(scheduling_counts))
        subject_parts = [
            _plural_format('{} failure{}', num_failures),
            _plural_format('{} disable{}', num_disables),
            _plural_format('{} scheduling failure{}', num_scheduling_failures),
        ]
        subject_base = ', '.join(filter(None, subject_parts))
        if subject_base:
            prefix = '' if owner in self._default_owner else 'Your tasks have '
            subject = 'Luigi: {}{} in the last {} minutes'.format(
                prefix, subject_base, self._config.email_interval)
            email_body = self._email_body(fail_counts, disable_counts, scheduling_counts, fail_expls)
            send_email(subject, email_body, email().sender, (owner,))

    def send_email(self):
        for owner, failures in six.iteritems(self._fail_counts):
            self._send_email(
                fail_counts=failures,
                disable_counts=self._disabled_counts[owner],
                scheduling_counts=self._scheduling_fail_counts[owner],
                fail_expls=self._fail_expls[owner],
                owner=owner,
            )
        self._update_next_send()
        self._fail_counts.clear()
        self._disabled_counts.clear()
        self._scheduling_fail_counts.clear()
        self._fail_expls.clear()

    def update(self):
        if time.time() >= self._next_send:
            self.send_email()
