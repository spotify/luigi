# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module provide the function :py:func:`summary` that is used for printing
an `execution summary
<https://github.com/spotify/luigi/blob/master/examples/execution_summary_example.py>`_
at the end of luigi invocations.
"""

import textwrap
import collections
import functools

import luigi


class execution_summary(luigi.Config):
    summary_length = luigi.IntParameter(default=5)


class LuigiRetCodes:
    """
    All possible return codes for **LuigiRunResult.status** when the argument ``detailed_summary=True`` in
    *luigi.run() / luigi.build*. Here are the codes and what they mean:

    =============================  =====  ========================================================== 
    Return Code Name               Value  Meaning 
    =============================  =====  ========================================================== 
    SUCCESS                        0      There were no failed tasks or missing dependencies 
    SUCCESS_WITH_RETRY             1      There were failed tasks but they all succeeded in a retry 
    FAILED                         2      There were failed tasks
    FAILED_AND_SCHEDULING_FAILED   3      There were failed tasks and tasks whose scheduling failed
    SCHEDULING_FAILED              4      There were tasks whose scheduling failed
    NOT_RUN                        5      There were tasks that were not granted run permission by the scheduler
    MISSING_EXT                    6      There were missing external dependencies
    =============================  =====  ==========================================================
    
    """
    SUCCESS = 0
    SUCCESS_WITH_RETRY = 1
    FAILED = 2
    FAILED_AND_SCHEDULING_FAILED = 3
    SCHEDULING_FAILED = 4
    NOT_RUN = 5
    MISSING_EXT = 6

    # Make sure all the retcodes have a (smiley, reason) in this dictionary
    _DETAILS = {
        SUCCESS: (":)", "there were no failed tasks or missing dependencies"),
        SUCCESS_WITH_RETRY: (":)", "there were failed tasks but they all succeeded in a retry"),
        FAILED: (":(" ,"there were failed tasks"),
        FAILED_AND_SCHEDULING_FAILED: (":(", "there were failed tasks and tasks whose scheduling failed"),
        SCHEDULING_FAILED: (":(", "there were tasks whose scheduling failed"),
        NOT_RUN: (":|", "there were tasks that were not granted run permission by the scheduler"),
        MISSING_EXT: (":|", "there were missing external dependencies")
    }


class LuigiRunResult:
    """
    Result of the execution (build/run) will be of type LuigiRunResult instead of
    the regular Boolean response if the keyword argument ``detailed_summary=True`` is passed to
    build/run. A response of type **LuigiRunResult** has the following attributes:

    Attributes:
        * **summary_text_one_line** - One line summary of the progress.
        * **summary_text** - Detailed summary of the progress.
        * **status** - Integer Return Code. See \
        `LuigiRetCodes <https://luigi.readthedocs.io/en/latest/api/luigi.execution_summary.html#luigi.execution_summary.LuigiRetCodes>`_ \
        for what these codes mean.
        * **worker** - Worker object.
        * **execution_succeeded** - Boolean which is *True* if finally, there were no failed tasks.
        * **scheduling_succeeded** - Boolean which is *True* if all the tasks were scheduled without errors.

    """
    def __init__(self, worker, worker_add_run_status=True):
        self.worker = worker
        self.summary_text = summary(worker)
        self.summary_text_one_line, self.status = self._progress_summary(worker)
        self.scheduling_succeeded = worker_add_run_status
        self.execution_succeeded = (True if self.status is LuigiRetCodes.SUCCESS or 
                                            self.status is LuigiRetCodes.SUCCESS_WITH_RETRY
                                    else False)

    def _response(self, detailed_summary = False):
        """ This function returns an object of type **LuigiRunResult** or a **Boolean**
        based on the value passed for the parameter.

        Args:
            *detailed_summary*: Enables/disables a response of type **LuigiRunResult**.

        Returns:
            A response of type **LuigiRunResult** if *detailed_summary* is True, Boolean otherwise.
        """
        if detailed_summary is True:
            return self
        else:
            return self.scheduling_succeeded

    def _progress_summary(self, worker):
        summary_text_one_line, retcode = _tasks_status(_summary_dict(worker))
        return summary_text_one_line, retcode

    def __str__(self):
        return "RETCODE: {0}\nSUMMARY: {1}\n".format(self.status ,self.summary_text_one_line)

    def __repr__(self):
        return self.__str__()


def _partition_tasks(worker):
    """
    Takes a worker and sorts out tasks based on their status.
    Still_pending_not_ext is only used to get upstream_failure, upstream_missing_dependency and run_by_other_worker
    """
    task_history = worker._add_task_history
    pending_tasks = {task for(task, status, ext) in task_history if status == 'PENDING'}
    set_tasks = {}
    set_tasks["completed"] = {task for (task, status, ext) in task_history if status == 'DONE' and task in pending_tasks}
    set_tasks["already_done"] = {task for (task, status, ext) in task_history
                                 if status == 'DONE' and task not in pending_tasks and task not in set_tasks["completed"]}
    set_tasks["ever_failed"] = {task for (task, status, ext) in task_history if status == 'FAILED'}
    set_tasks["failed"] = set_tasks["ever_failed"] - set_tasks["completed"]
    set_tasks["scheduling_error"] = {task for(task, status, ext) in task_history if status == 'UNKNOWN'}
    set_tasks["still_pending_ext"] = {task for (task, status, ext) in task_history
                                      if status == 'PENDING' and task not in set_tasks["ever_failed"] and task not in set_tasks["completed"] and not ext}
    set_tasks["still_pending_not_ext"] = {task for (task, status, ext) in task_history
                                          if status == 'PENDING' and task not in set_tasks["ever_failed"] and task not in set_tasks["completed"] and ext}
    set_tasks["run_by_other_worker"] = set()
    set_tasks["upstream_failure"] = set()
    set_tasks["upstream_missing_dependency"] = set()
    set_tasks["upstream_run_by_other_worker"] = set()
    set_tasks["upstream_scheduling_error"] = set()
    set_tasks["not_run"] = set()
    return set_tasks


def _root_task(worker):
    """
    Return the first task scheduled by the worker, corresponding to the root task
    """
    return worker._add_task_history[0][0]


def _populate_unknown_statuses(set_tasks):
    """
    Add the "upstream_*" and "not_run" statuses my mutating set_tasks.
    """
    visited = set()
    for task in set_tasks["still_pending_not_ext"]:
        _depth_first_search(set_tasks, task, visited)


def _depth_first_search(set_tasks, current_task, visited):
    """
    This dfs checks why tasks are still pending.
    """
    visited.add(current_task)
    if current_task in set_tasks["still_pending_not_ext"]:
        upstream_failure = False
        upstream_missing_dependency = False
        upstream_run_by_other_worker = False
        upstream_scheduling_error = False
        for task in current_task._requires():
            if task not in visited:
                _depth_first_search(set_tasks, task, visited)
            if task in set_tasks["ever_failed"] or task in set_tasks["upstream_failure"]:
                set_tasks["upstream_failure"].add(current_task)
                upstream_failure = True
            if task in set_tasks["still_pending_ext"] or task in set_tasks["upstream_missing_dependency"]:
                set_tasks["upstream_missing_dependency"].add(current_task)
                upstream_missing_dependency = True
            if task in set_tasks["run_by_other_worker"] or task in set_tasks["upstream_run_by_other_worker"]:
                set_tasks["upstream_run_by_other_worker"].add(current_task)
                upstream_run_by_other_worker = True
            if task in set_tasks["scheduling_error"]:
                set_tasks["upstream_scheduling_error"].add(current_task)
                upstream_scheduling_error = True
        if not upstream_failure and not upstream_missing_dependency and \
                not upstream_run_by_other_worker and not upstream_scheduling_error and \
                current_task not in set_tasks["run_by_other_worker"]:
            set_tasks["not_run"].add(current_task)


def _get_str(task_dict, extra_indent):
    """
    This returns a string for each status
    """
    summary_length = execution_summary().summary_length

    lines = []
    task_names = sorted(task_dict.keys())
    for task_family in task_names:
        tasks = task_dict[task_family]
        tasks = sorted(tasks, key=lambda x: str(x))
        prefix_size = 8 if extra_indent else 4
        prefix = ' ' * prefix_size

        line = None

        if summary_length > 0 and len(lines) >= summary_length:
            line = prefix + "..."
            lines.append(line)
            break
        if len(tasks[0].get_params()) == 0:
            line = prefix + '- {0} {1}()'.format(len(tasks), str(task_family))
        elif _get_len_of_params(tasks[0]) > 60 or len(str(tasks[0])) > 200 or \
                (len(tasks) == 2 and len(tasks[0].get_params()) > 1 and (_get_len_of_params(tasks[0]) > 40 or len(str(tasks[0])) > 100)):
            """
            This is to make sure that there is no really long task in the output
            """
            line = prefix + '- {0} {1}(...)'.format(len(tasks), task_family)
        elif len((tasks[0].get_params())) == 1:
            attributes = {getattr(task, tasks[0].get_params()[0][0]) for task in tasks}
            param_class = tasks[0].get_params()[0][1]
            first, last = _ranging_attributes(attributes, param_class)
            if first is not None and last is not None and len(attributes) > 3:
                param_str = '{0}...{1}'.format(param_class.serialize(first), param_class.serialize(last))
            else:
                param_str = '{0}'.format(_get_str_one_parameter(tasks))
            line = prefix + '- {0} {1}({2}={3})'.format(len(tasks), task_family, tasks[0].get_params()[0][0], param_str)
        else:
            ranging = False
            params = _get_set_of_params(tasks)
            unique_param_keys = list(_get_unique_param_keys(params))
            if len(unique_param_keys) == 1:
                unique_param, = unique_param_keys
                attributes = params[unique_param]
                param_class = unique_param[1]
                first, last = _ranging_attributes(attributes, param_class)
                if first is not None and last is not None and len(attributes) > 2:
                    ranging = True
                    line = prefix + '- {0} {1}({2}'.format(len(tasks), task_family, _get_str_ranging_multiple_parameters(first, last, tasks, unique_param))
            if not ranging:
                if len(tasks) == 1:
                    line = prefix + '- {0} {1}'.format(len(tasks), tasks[0])
                if len(tasks) == 2:
                    line = prefix + '- {0} {1} and {2}'.format(len(tasks), tasks[0], tasks[1])
                if len(tasks) > 2:
                    line = prefix + '- {0} {1} ...'.format(len(tasks), tasks[0])
        lines.append(line)
    return '\n'.join(lines)


def _get_len_of_params(task):
    return sum(len(param[0]) for param in task.get_params())


def _get_str_ranging_multiple_parameters(first, last, tasks, unique_param):
    row = ''
    str_unique_param = '{0}...{1}'.format(unique_param[1].serialize(first), unique_param[1].serialize(last))
    for param in tasks[0].get_params():
        row += '{0}='.format(param[0])
        if param[0] == unique_param[0]:
            row += '{0}'.format(str_unique_param)
        else:
            row += '{0}'.format(param[1].serialize(getattr(tasks[0], param[0])))
        if param != tasks[0].get_params()[-1]:
            row += ", "
    row += ')'
    return row


def _get_set_of_params(tasks):
    params = {}
    for param in tasks[0].get_params():
        params[param] = {getattr(task, param[0]) for task in tasks}
    return params


def _get_unique_param_keys(params):
    for param_key, param_values in params.items():
        if len(param_values) > 1:
            yield param_key


def _ranging_attributes(attributes, param_class):
    """
    Checks if there is a continuous range
    """
    next_attributes = {param_class.next_in_enumeration(attribute) for attribute in attributes}
    in_first = attributes.difference(next_attributes)
    in_second = next_attributes.difference(attributes)
    if len(in_first) == 1 and len(in_second) == 1:
        for x in attributes:
            if {param_class.next_in_enumeration(x)} == in_second:
                return next(iter(in_first)), x
    return None, None


def _get_str_one_parameter(tasks):
    row = ''
    count = 0
    for task in tasks:
        if (len(row) >= 30 and count > 2 and count != len(tasks) - 1) or len(row) > 200:
            row += '...'
            break
        param = task.get_params()[0]
        row += '{0}'.format(param[1].serialize(getattr(task, param[0])))
        if count < len(tasks) - 1:
            row += ','
        count += 1
    return row


def _serialize_first_param(task):
    return task.get_params()[0][1].serialize(getattr(task, task.get_params()[0][0]))


def _get_number_of_tasks_for(status, group_tasks):
    if status == "still_pending":
        return (_get_number_of_tasks(group_tasks["still_pending_ext"]) +
                _get_number_of_tasks(group_tasks["still_pending_not_ext"]))
    return _get_number_of_tasks(group_tasks[status])


def _get_number_of_tasks(task_dict):
    return sum(len(tasks) for tasks in task_dict.values())


def _get_comments(group_tasks):
    """
    Get the human readable comments and quantities for the task types.
    """
    comments = {}
    for status, human in _COMMENTS:
        num_tasks = _get_number_of_tasks_for(status, group_tasks)
        if num_tasks:
            space = "    " if status in _PENDING_SUB_STATUSES else ""
            comments[status] = '{space}* {num_tasks} {human}:\n'.format(
                space=space,
                num_tasks=num_tasks,
                human=human)
    return comments


# Oredered in the sense that they'll be printed in this order
_ORDERED_STATUSES = (
    "already_done",
    "completed",
    "ever_failed",
    "failed",
    "scheduling_error",
    "still_pending",
    "still_pending_ext",
    "run_by_other_worker",
    "upstream_failure",
    "upstream_missing_dependency",
    "upstream_run_by_other_worker",
    "upstream_scheduling_error",
    "not_run",
)
_PENDING_SUB_STATUSES = set(_ORDERED_STATUSES[_ORDERED_STATUSES.index("still_pending_ext"):])
_COMMENTS = {
    ("already_done", 'complete ones were encountered'),
    ("completed", 'ran successfully'),
    ("failed", 'failed'),
    ("scheduling_error", 'failed scheduling'),
    ("still_pending", 'were left pending, among these'),
    ("still_pending_ext", 'were missing external dependencies'),
    ("run_by_other_worker", 'were being run by another worker'),
    ("upstream_failure", 'had failed dependencies'),
    ("upstream_missing_dependency", 'had missing dependencies'),
    ("upstream_run_by_other_worker", 'had dependencies that were being run by other worker'),
    ("upstream_scheduling_error", 'had dependencies whose scheduling failed'),
    ("not_run", 'was not granted run permission by the scheduler'),
}


def _get_run_by_other_worker(worker):
    """
    This returns a set of the tasks that are being run by other worker
    """
    task_sets = _get_external_workers(worker).values()
    return functools.reduce(lambda a, b: a | b, task_sets, set())


def _get_external_workers(worker):
    """
    This returns a dict with a set of tasks for all of the other workers
    """
    worker_that_blocked_task = collections.defaultdict(set)
    get_work_response_history = worker._get_work_response_history
    for get_work_response in get_work_response_history:
        if get_work_response['task_id'] is None:
            for running_task in get_work_response['running_tasks']:
                other_worker_id = running_task['worker']
                other_task_id = running_task['task_id']
                other_task = worker._scheduled_tasks.get(other_task_id)
                if other_worker_id == worker._id or not other_task:
                    continue
                worker_that_blocked_task[other_worker_id].add(other_task)
    return worker_that_blocked_task


def _group_tasks_by_name_and_status(task_dict):
    """
    Takes a dictionary with sets of tasks grouped by their status and
    returns a dictionary with dictionaries with an array of tasks grouped by
    their status and task name
    """
    group_status = {}
    for task in task_dict:
        if task.task_family not in group_status:
            group_status[task.task_family] = []
        group_status[task.task_family].append(task)
    return group_status


def _summary_dict(worker):
    set_tasks = _partition_tasks(worker)
    set_tasks["run_by_other_worker"] = _get_run_by_other_worker(worker)
    _populate_unknown_statuses(set_tasks)
    return set_tasks


def _summary_format(set_tasks, worker):
    group_tasks = {}
    for status, task_dict in set_tasks.items():
        group_tasks[status] = _group_tasks_by_name_and_status(task_dict)
    comments = _get_comments(group_tasks)
    num_all_tasks = sum([len(set_tasks["already_done"]),
                         len(set_tasks["completed"]), len(set_tasks["failed"]),
                         len(set_tasks["scheduling_error"]),
                         len(set_tasks["still_pending_ext"]),
                         len(set_tasks["still_pending_not_ext"])])
    str_output = ''
    str_output += 'Scheduled {0} tasks of which:\n'.format(num_all_tasks)
    for status in _ORDERED_STATUSES:
        if status not in comments:
            continue
        str_output += '{0}'.format(comments[status])
        if status != 'still_pending':
            str_output += '{0}\n'.format(_get_str(group_tasks[status], status in _PENDING_SUB_STATUSES))
    ext_workers = _get_external_workers(worker)
    group_tasks_ext_workers = {}
    for ext_worker, task_dict in ext_workers.items():
        group_tasks_ext_workers[ext_worker] = _group_tasks_by_name_and_status(task_dict)
    if len(ext_workers) > 0:
        str_output += "\nThe other workers were:\n"
        count = 0
        for ext_worker, task_dict in ext_workers.items():
            if count > 3 and count < len(ext_workers) - 1:
                str_output += "    and {0} other workers".format(len(ext_workers) - count)
                break
            str_output += "    - {0} ran {1} tasks\n".format(ext_worker, len(task_dict))
            count += 1
        str_output += '\n'
    if num_all_tasks == sum([len(set_tasks["already_done"]),
                             len(set_tasks["scheduling_error"]),
                             len(set_tasks["still_pending_ext"]),
                             len(set_tasks["still_pending_not_ext"])]):
        if len(ext_workers) == 0:
            str_output += '\n'
        str_output += 'Did not run any tasks'
    summary_text_one_line, _ = _tasks_status(set_tasks)
    str_output += "\n{0}".format(summary_text_one_line)
    if num_all_tasks == 0:
        str_output = 'Did not schedule any tasks'
    return str_output


def _tasks_status(set_tasks):
    """
    Given a grouped set of tasks, returns a one line summary and a LuigiRetCode
    """
    if set_tasks["ever_failed"]:
        if not set_tasks["failed"]:
            retcode = LuigiRetCodes.SUCCESS_WITH_RETRY
        else:
            retcode = LuigiRetCodes.FAILED
            if set_tasks["scheduling_error"]:
                retcode = LuigiRetCodes.FAILED_AND_SCHEDULING_FAILED
    elif set_tasks["scheduling_error"]:
        retcode = LuigiRetCodes.SCHEDULING_FAILED
    elif set_tasks["not_run"]:
        retcode = LuigiRetCodes.NOT_RUN
    elif set_tasks["still_pending_ext"]:
        retcode = LuigiRetCodes.MISSING_EXT
    else:
        retcode = LuigiRetCodes.SUCCESS
    smiley, reason = LuigiRetCodes._DETAILS.get(retcode, ("", ""))
    return "This progress looks {0} because {1}".format(smiley, reason), retcode


def _summary_wrap(str_output):
    return textwrap.dedent("""
    ===== Luigi Execution Summary =====

    {str_output}

    ===== Luigi Execution Summary =====
    """).format(str_output=str_output)


def summary(worker):
    """
    Given a worker, return a human readable summary of what the worker have
    done.
    """
    return _summary_wrap(_summary_format(_summary_dict(worker), worker))
# 5
